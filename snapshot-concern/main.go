package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
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

		session, err := client.StartSession()
		if err != nil {
			panic(err)
		}
		defer session.EndSession(ctx)

		res, err := session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
			var fooRes fooEntry

			fooRec := fooColl.FindOne(sessCtx, bson.D{{"_id", "6475eb087660882fa85dff59"}})
			fooRec.Decode(&fooRes)

			fmt.Println(fooRes.Hello)

			aChan <- true

			if t := <-bChan; t {
				if fooRes.Hello != "world" {
					return nil, fmt.Errorf("failed to compare foo record")
				}

				_, dErr := barColl.UpdateOne(
					sessCtx,
					bson.D{{"_id", "6475ebec7c8c0d02309b0a46"}},
					bson.D{{"$set", bson.D{{"answer", 43}}}},
				)
				if dErr != nil {
					return nil, dErr
				}

				return "updated 6475ebec7c8c0d02309b0a46", nil
			}

			return "channel B did not send answer", nil
		}, txnOpts)
		if err != nil {
			panic(err)
		}

		fmt.Println(res)
	}()

	// Do session B
	wg.Add(1)
	go func() {
		defer wg.Done()

		session, err := client.StartSession()
		if err != nil {
			panic(err)
		}
		defer session.EndSession(ctx)

		res, err := session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
			if t := <-aChan; t {
				_, dErr := fooColl.UpdateOne(
					sessCtx,
					bson.D{{"_id", "6475eb087660882fa85dff59"}},
					bson.D{{"$set", bson.D{{"hello", "bar"}}}},
				)
				if dErr != nil {
					return nil, dErr
				}

				return "updated 6475eb087660882fa85dff59", nil
			}

			return "done", nil
		}, txnOpts)
		if err != nil {
			panic(err)
		}

		bChan <- true

		fmt.Println(res)
	}()

	wg.Wait()
}
