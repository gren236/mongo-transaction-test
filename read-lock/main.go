package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"log"
	"sync"
)

const (
	dbURI = "mongodb://root:qwerty1234@localhost:27017/test?authSource=admin&replicaSet=replicaset"
)

type fooEntry struct {
	Hello string `json:"hello"`
}

type barEntry struct {
	Answer int `json:"answer"`
}

func FindOneLock(coll *mongo.Collection, ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
	// TODO map options.FindOneOptions to options.FindOneAndUpdateOptions
	return coll.FindOneAndUpdate(ctx, filter, bson.D{{"$set", bson.D{{"lockRandom", primitive.NewObjectID()}}}})
}

func main() {
	ctx := context.Background()

	// Connect to client
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dbURI))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	fooColl := client.Database("test").Collection("foo")
	barColl := client.Database("test").Collection("bar")

	wc := writeconcern.New(writeconcern.WMajority())
	rc := readconcern.Snapshot()
	txnOpts := options.Transaction().SetWriteConcern(wc).SetReadConcern(rc)

	aChan := make(chan bool)
	bChan := make(chan bool)
	var wg sync.WaitGroup

	// Do session A
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := client.UseSession(ctx, func(sessCtx mongo.SessionContext) error {
			sessCtx.StartTransaction(txnOpts)

			var fooRes fooEntry

			fooRec := FindOneLock(fooColl, sessCtx, bson.D{{"_id", "6475eb087660882fa85dff59"}})
			fooRec.Decode(&fooRes)

			log.Println(fooRes.Hello)

			aChan <- true

			if fooRes.Hello != "world" {
				sessCtx.AbortTransaction(sessCtx)

				return fmt.Errorf("failed to compare foo record")
			}

			if t := <-bChan; t {
				_, dErr := barColl.UpdateOne(
					sessCtx,
					bson.D{{"_id", "6475ebec7c8c0d02309b0a46"}},
					bson.D{{"$set", bson.D{{"answer", 43}}}},
				)
				if dErr != nil {
					sessCtx.AbortTransaction(sessCtx)

					return dErr
				}

				log.Println("updated 6475ebec7c8c0d02309b0a46")
			}

			sessCtx.CommitTransaction(sessCtx)

			return nil
		})
		if err != nil {
			log.Println(err)
		}
	}()

	// Do session B
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := client.UseSession(ctx, func(sessCtx mongo.SessionContext) error {
			sessCtx.StartTransaction(txnOpts)

			if t := <-aChan; t {
				_, dErr := fooColl.DeleteOne(
					sessCtx,
					bson.D{{"_id", "6475eb087660882fa85dff59"}},
				)
				if dErr != nil {
					sessCtx.AbortTransaction(sessCtx)

					return dErr
				}

				log.Println("updated 6475eb087660882fa85dff59")
			}

			sessCtx.CommitTransaction(sessCtx)

			log.Println("done B")
			return nil
		})
		if err != nil {
			log.Println(err)
		}

		bChan <- true
	}()

	wg.Wait()
}
