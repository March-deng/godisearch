package redisearch

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
)

// exemplifies the CreateIndex function with a temporary index specification
func ExampleCreateIndex_temporary() {
	// Create a client. By default a client is schemaless
	// unless a schema is provided when creating the index
	c := NewClient("localhost:6379", "myTemporaryIndex")

	// Create a schema with a temporary period of 60seconds
	sc := NewSchema(*NewOptions().SetTemporaryPeriod(10)).
		AddField(NewTextField("body")).
		AddField(NewTextFieldOptions("title", TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(NewNumericField("date"))

	// Create the index with the given schema
	if err := c.CreateIndex(context.Background(), sc); err != nil {
		log.Fatal(err)
	}

	// Create a document with an id and given score
	doc := NewDocument("ExampleCreateIndex_temporary:doc1", 1.0)
	doc.Set("title", "Hello world").
		Set("body", "foo bar").
		Set("date", time.Now().Unix())

	// Index the document. The API accepts multiple documents at a time
	if err := c.IndexOptions(defaultCtx, DefaultIndexingOptions, doc); err != nil {
		log.Fatal(err)
	}

	docs, total, err := c.Search(defaultCtx, NewQuery("hello world").
		Limit(0, 2).
		SetReturnFields("title"))

	// Verify that the we're able to search on the temporary created index
	fmt.Println(docs[0].Id, docs[0].Properties["title"], total, err)

	time.Sleep(15 * time.Second)
	// Searching with limit and sorting
	_, err = c.Info(defaultCtx)
	fmt.Println(err)
	// Output: ExampleCreateIndex_temporary:doc1 Hello world 1 <nil>
	// Unknown Index name
}

// exemplifies the CreateIndex function with phonetic matching on it in searches by default
func ExampleClient_CreateIndexWithIndexDefinition_phonetic() {
	// Create a client
	host := "localhost:6379"
	password := ""
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}}
	c := NewClientFromPool(pool, "myPhoneticIndex")

	// Create a schema
	schema := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("name", TextFieldOptions{Sortable: true, PhoneticMatcher: PhoneticDoubleMetaphoneEnglish})).
		AddField(NewNumericField("age"))

	// IndexDefinition is available for RediSearch 2.0+
	// Create a index definition for automatic indexing on Hash updates.
	// In this example we will only index keys started by product:
	indexDefinition := NewIndexDefinition().AddPrefix("myPhoneticIndex:")

	// Add the Index Definition
	c.CreateIndexWithIndexDefinition(context.Background(), schema, indexDefinition)

	// Create docs with a name that has the same phonetic matcher
	vanillaConnection := pool.Get()
	vanillaConnection.Do("HSET", "myPhoneticIndex:doc1", "name", "Jon", "age", 25)
	// Create a second document with a name that has the same phonetic matcher
	vanillaConnection.Do("HSET", "myPhoneticIndex:doc2", "name", "John", "age", 20)
	// Create a third document with a name that does not have the same phonetic matcher
	vanillaConnection.Do("HSET", "myPhoneticIndex:doc3", "name", "Pieter", "age", 30)

	// Wait for all documents to be indexed
	info, _ := c.Info(defaultCtx)
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info(defaultCtx)
	}

	_, total, _ := c.Search(defaultCtx, NewQuery("Jon").
		SetReturnFields("name"))

	// Verify that the we've received 2 documents ( Jon and John )
	fmt.Printf("Total docs replied %d\n", total)

	// Output: Total docs replied 2
}
