package redisearch

import (
	"context"
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
)

var (
	ErrDocNotFound = errors.New("document not found")
)

// Client is an interface to redisearch's redis commands
type Client struct {
	pool ConnPool
	name string
}

var maxConns = 500

// NewClient creates a new client connecting to the redis host, and using the given name as key prefix.
// Addr is a single host:port pair
func NewClient(addr, name string) *Client {

	pool := NewSingleHostPool(addr)

	ret := &Client{
		pool: pool,
		name: name,
	}

	return ret
}

// NewClientFromPool creates a new Client with the given pool and index name
func NewClientFromPool(pool *redis.Pool, name string) *Client {
	ret := &Client{
		pool: &SingleHostPool{Pool: pool},
		name: name,
	}
	return ret
}
func (i *Client) GetConn(ctx context.Context) (redis.Conn, error) {
	return i.pool.Get(ctx)
}

// CreateIndex configures the index and creates it on redis
func (i *Client) CreateIndex(ctx context.Context, schema *Schema) (err error) {
	return i.indexWithDefinition(ctx, i.name, schema, nil)
}

// CreateIndexWithIndexDefinition configures the index and creates it on redis
// IndexDefinition is used to define a index definition for automatic indexing on Hash update
func (i *Client) CreateIndexWithIndexDefinition(ctx context.Context, schema *Schema, definition *IndexDefinition) (err error) {
	return i.indexWithDefinition(ctx, i.name, schema, definition)
}

// internal method
func (i *Client) indexWithDefinition(ctx context.Context, indexName string, schema *Schema, definition *IndexDefinition) (err error) {
	args := redis.Args{indexName}
	if definition != nil {
		args = definition.Serialize(args)
	}
	// Set flags based on options
	args, err = SerializeSchema(schema, args)
	if err != nil {
		return err
	}
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("FT.CREATE", args...)
	return
}

// AddField Adds a new field to the index.
func (i *Client) AddField(ctx context.Context, f Field) error {
	args := redis.Args{i.name}
	args = append(args, "SCHEMA", "ADD")
	args, err := serializeField(f, args)
	if err != nil {
		return err
	}
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("FT.ALTER", args...)
	return err
}

// Index indexes a list of documents with the default options
func (i *Client) Index(ctx context.Context, docs ...Document) error {
	return i.IndexOptions(ctx, DefaultIndexingOptions, docs...)
}

// AddDoc add doc to redis with HSETNX command, the Score and Payload field will be ignored.
func (i *Client) AddDoc(ctx context.Context, docs ...Document) error {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	n := 0
	var merr MultiError

	for i, doc := range docs {
		args := make(redis.Args, 0, 1+2*len(doc.Properties))
		args = append(args, doc.Id)
		for k, f := range doc.Properties {
			args = append(args, k, f)
		}

		if err := conn.Send("HSET", args...); err != nil {
			if merr == nil {
				merr = NewMultiError(len(docs))
			}
			merr[i] = err
			return merr
		}
		n++
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	for n > 0 {
		if _, err := conn.Receive(); err != nil {
			if merr == nil {
				merr = NewMultiError(len(docs))
			}
			merr[n-1] = err
		}
		n--
	}

	if merr == nil {
		return nil
	}

	return merr
}

// DeleteDoc delete doc by keys with DEL command
func (i *Client) DeleteDoc(ctx context.Context, keys ...string) error {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	args := make(redis.Args, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}

	_, err = conn.Do("DEL", args...)
	return err
}

func (i *Client) GetDoc(ctx context.Context, docID string) (*Document, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	reply, err := conn.Do("HGETALL", docID)
	if err != nil {
		return nil, err
	}

	var doc *Document

	if reply != nil {

		var array_reply []interface{}
		array_reply, err = redis.Values(reply, err)
		if err != nil {
			return nil, err
		}
		if len(array_reply) == 0 {
			return nil, ErrDocNotFound
		}
		if len(array_reply) > 0 {
			document := NewDocument(docID, 0)
			document.loadFields(array_reply)
			doc = &document
		}
	}
	return doc, nil

}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *Client) Search(ctx context.Context, q *Query) (docs []Document, total int, err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, 0, err
	}

	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.serialize()...)

	res, err := redis.Values(conn.Do("FT.SEARCH", args...))
	if err != nil {
		return
	}

	if total, err = redis.Int(res[0], nil); err != nil {
		return
	}

	docs = make([]Document, 0, len(res)-1)

	skip := 1
	scoreIdx := -1
	fieldsIdx := -1
	payloadIdx := -1
	if q.Flags&QueryWithScores != 0 {
		scoreIdx = 1
		skip++
	}
	if q.Flags&QueryWithPayloads != 0 {
		payloadIdx = skip
		skip++
	}
	if q.Flags&QueryNoContent == 0 {
		fieldsIdx = skip
		skip++
	}
	if len(res) > skip {
		for i := 1; i < len(res); i += skip {

			if d, e := loadDocument(res, i, scoreIdx, payloadIdx, fieldsIdx); e == nil {
				docs = append(docs, d)
			} else {
				log.Print("Error parsing doc: ", e)
			}
		}
	}
	return
}

// AliasAdd adds an alias to an index.
// Indexes can have more than one alias, though an alias cannot refer to another alias.
func (i *Client) AliasAdd(ctx context.Context, name string) (err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	args := redis.Args{name}.Add(i.name)
	_, err = redis.String(conn.Do("FT.ALIASADD", args...))
	return
}

// AliasDel deletes an alias from index.
func (i *Client) AliasDel(ctx context.Context, name string) (err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	args := redis.Args{name}
	_, err = redis.String(conn.Do("FT.ALIASDEL", args...))
	return
}

// AliasUpdate differs from the AliasAdd in that it will remove the alias association with
// a previous index, if any. AliasAdd will fail, on the other hand, if the alias is already
// associated with another index.
func (i *Client) AliasUpdate(ctx context.Context, name string) (err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	args := redis.Args{name}.Add(i.name)
	_, err = redis.String(conn.Do("FT.ALIASUPDATE", args...))
	return
}

// DictAdd adds terms to a dictionary.
func (i *Client) DictAdd(ctx context.Context, dictionaryName string, terms []string) (newTerms int, err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	newTerms = 0
	args := redis.Args{dictionaryName}.AddFlat(terms)
	newTerms, err = redis.Int(conn.Do("FT.DICTADD", args...))
	return
}

// DictDel deletes terms from a dictionary
func (i *Client) DictDel(ctx context.Context, dictionaryName string, terms []string) (deletedTerms int, err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	deletedTerms = 0
	args := redis.Args{dictionaryName}.AddFlat(terms)
	deletedTerms, err = redis.Int(conn.Do("FT.DICTDEL", args...))
	return
}

// DictDump dumps all terms in the given dictionary.
func (i *Client) DictDump(ctx context.Context, dictionaryName string) (terms []string, err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	args := redis.Args{dictionaryName}
	terms, err = redis.Strings(conn.Do("FT.DICTDUMP", args...))
	return
}

// SpellCheck performs spelling correction on a query, returning suggestions for misspelled terms,
// the total number of results, or an error if something went wrong
func (i *Client) SpellCheck(ctx context.Context, q *Query, s *SpellCheckOptions) (suggs []MisspelledTerm, total int, err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.serialize()...)
	args = append(args, s.serialize()...)

	res, err := redis.Values(conn.Do("FT.SPELLCHECK", args...))
	if err != nil {
		return
	}
	total = 0
	suggs = make([]MisspelledTerm, 0)

	// Each misspelled term, in turn, is a 3-element array consisting of
	// - the constant string "TERM" ( 3-element position 0 -- we dont use it )
	// - the term itself ( 3-element position 1 )
	// - an array of suggestions for spelling corrections ( 3-element position 2 )
	termIdx := 1
	suggIdx := 2
	for i := 0; i < len(res); i++ {
		var termArray []interface{} = nil
		termArray, err = redis.Values(res[i], nil)
		if err != nil {
			return
		}

		if d, e := loadMisspelledTerm(termArray, termIdx, suggIdx); e == nil {
			suggs = append(suggs, d)
			if d.Len() > 0 {
				total++
			}
		} else {
			log.Print("Error parsing misspelled suggestion: ", e)
		}
	}

	return
}

// Deprecated: Use AggregateQuery() instead.
func (i *Client) Aggregate(ctx context.Context, q *AggregateQuery) (aggregateReply [][]string, total int, err error) {
	res, err := i.aggregate(ctx, q)

	// has no cursor
	if !q.WithCursor {
		total, aggregateReply, err = processAggReply(res)
		// has cursor
	} else {
		var partialResults, err = redis.Values(res[0], nil)
		if err != nil {
			return aggregateReply, total, err
		}
		q.Cursor.Id, err = redis.Int(res[1], nil)
		if err != nil {
			return aggregateReply, total, err
		}
		total, aggregateReply, err = processAggReply(partialResults)
	}
	return
}

// AggregateQuery replaces the Aggregate() function. The reply is slice of maps, with values of either string or []string.
func (i *Client) AggregateQuery(ctx context.Context, q *AggregateQuery) (total int, aggregateReply []map[string]interface{}, err error) {
	res, err := i.aggregate(ctx, q)

	// has no cursor
	if !q.WithCursor {
		total, aggregateReply, err = processAggQueryReply(res)
		// has cursor
	} else {
		var partialResults, err = redis.Values(res[0], nil)
		if err != nil {
			return total, aggregateReply, err
		}
		q.Cursor.Id, err = redis.Int(res[1], nil)
		if err != nil {
			return total, aggregateReply, err
		}
		total, aggregateReply, err = processAggQueryReply(partialResults)
	}
	return
}

func (i *Client) aggregate(ctx context.Context, q *AggregateQuery) (res []interface{}, err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	validCursor := q.CursorHasResults()
	if !validCursor {
		args := redis.Args{i.name}
		args = append(args, q.Serialize()...)
		res, err = redis.Values(conn.Do("FT.AGGREGATE", args...))
	} else {
		args := redis.Args{"READ", i.name, q.Cursor.Id}
		res, err = redis.Values(conn.Do("FT.CURSOR", args...))
	}
	if err != nil {
		return
	}
	return
}

// Get - Returns the full contents of a document
func (i *Client) Get(ctx context.Context, docId string) (doc *Document, err error) {
	doc = nil
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var reply interface{}
	args := redis.Args{i.name, docId}
	reply, err = conn.Do("FT.GET", args...)
	if reply != nil {
		var array_reply []interface{}
		array_reply, err = redis.Values(reply, err)
		if err != nil {
			return
		}
		if len(array_reply) > 0 {
			document := NewDocument(docId, 1)
			document.loadFields(array_reply)
			doc = &document
		}
	}
	return
}

// MultiGet - Returns the full contents of multiple documents.
// Returns an array with exactly the same number of elements as the number of keys sent to the command.
// Each element in it is either an Document or nil if it was not found.
func (i *Client) MultiGet(ctx context.Context, documentIds []string) (docs []*Document, err error) {
	docs = make([]*Document, len(documentIds))
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var reply interface{}
	args := redis.Args{i.name}.AddFlat(documentIds)
	reply, err = conn.Do("FT.MGET", args...)
	if reply != nil {
		var array_reply []interface{}
		array_reply, err = redis.Values(reply, err)
		if err != nil {
			return
		}
		for i := 0; i < len(array_reply); i++ {

			if array_reply[i] != nil {
				var innerArray []interface{}
				innerArray, err = redis.Values(array_reply[i], nil)
				if err != nil {
					return
				}
				if len(array_reply) > 0 {
					document := NewDocument(documentIds[i], 1)
					document.loadFields(innerArray)
					docs[i] = &document
				}
			} else {
				docs[i] = nil
			}
		}
	}
	return
}

// Explain Return a textual string explaining the query (execution plan)
func (i *Client) Explain(ctx context.Context, q *Query) (string, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.serialize()...)

	return redis.String(conn.Do("FT.EXPLAIN", args...))
}

// Drop deletes the index and all the keys associated with it.
func (i *Client) Drop(ctx context.Context) error {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("FT.DROP", i.name)
	return err
}

// Deletes the secondary index and optionally the associated hashes
//
// Available since RediSearch 2.0.
//
// By default, DropIndex() which is a wrapper for RediSearch FT.DROPINDEX does not delete the document
// hashes associated with the index. Setting the argument deleteDocuments to true deletes the hashes as well.
func (i *Client) DropIndex(ctx context.Context, deleteDocuments bool) error {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deleteDocuments {
		_, err = conn.Do("FT.DROPINDEX", i.name, "DD")
	} else {
		_, err = conn.Do("FT.DROPINDEX", i.name)
	}
	return err
}

// Delete the document from the index, optionally delete the actual document
// WARNING: As of RediSearch 2.0 and above, FT.DEL always deletes the underlying document.
// Deprecated: This function  is deprecated on RediSearch 2.0 and above, use DeleteDocument() instead
func (i *Client) Delete(ctx context.Context, docId string, deleteDocument bool) (err error) {
	return i.delDoc(ctx, docId, deleteDocument)
}

// DeleteDocument delete the document from the index and also delete the HASH key in which the document is stored
func (i *Client) DeleteDocument(ctx context.Context, docId string) (err error) {
	return i.delDoc(ctx, docId, true)
}

// Internal method to be used by Delete() and DeleteDocument()
func (i *Client) delDoc(ctx context.Context, docId string, deleteDocument bool) (err error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deleteDocument {
		_, err = conn.Do("FT.DEL", i.name, docId, "DD")
	} else {
		_, err = conn.Do("FT.DEL", i.name, docId)
	}
	return
}

// Internal method to be used by Info()
func (info *IndexInfo) setTarget(key string, value interface{}) error {
	v := reflect.ValueOf(info).Elem()
	for i := 0; i < v.NumField(); i++ {
		tag := v.Type().Field(i).Tag.Get("redis")
		if tag == key {
			targetInfo := v.Field(i)
			switch targetInfo.Kind() {
			case reflect.String:
				s, _ := redis.String(value, nil)
				targetInfo.SetString(s)
			case reflect.Uint64:
				u, _ := redis.Uint64(value, nil)
				targetInfo.SetUint(u)
			case reflect.Float64:
				f, _ := redis.Float64(value, nil)
				targetInfo.SetFloat(f)
			case reflect.Bool:
				f, _ := redis.Uint64(value, nil)
				if f == 0 {
					targetInfo.SetBool(false)
				} else {
					targetInfo.SetBool(true)
				}
			default:
				panic("Tag set without handler")
			}
			return nil
		}
	}
	return errors.New("setTarget: No handler defined for :" + key)
}

func sliceIndex(haystack []string, needle string) int {
	for pos, elem := range haystack {
		if elem == needle {
			return pos
		}
	}
	return -1
}

func (info *IndexInfo) loadSchema(values []interface{}, options []string) {
	// Values are a list of fields
	scOptions := Options{}
	for _, opt := range options {
		switch strings.ToUpper(opt) {
		case "NOFIELDS":
			scOptions.NoFieldFlags = true
		case "NOFREQS":
			scOptions.NoFrequencies = true
		case "NOOFFSETS":
			scOptions.NoOffsetVectors = true
		}
	}
	sc := NewSchema(scOptions)
	for _, specTmp := range values {
		// spec, isArr := specTmp.([]string)
		// if !isArr {
		// 	panic("Value is not an array of strings!")
		// }
		rawSpec, err := redis.Values(specTmp, nil)
		if err != nil {
			log.Printf("Warning: Couldn't read schema. %s\n", err.Error())
			continue
		}
		spec := make([]string, 0)

		// Convert all to string, if not already string
		for _, elem := range rawSpec {
			s, isString := elem.(string)
			if !isString {
				s, err = redis.String(elem, err)
				if err != nil {
					log.Printf("Warning: Couldn't read schema. %s\n", err.Error())
					continue
				}
			}
			spec = append(spec, s)
		}
		// Name, Type,
		if len(spec) < 3 {
			log.Printf("Invalid spec")
			continue
		}

		var options []string
		if len(spec) > 3 {
			options = spec[3:]
		} else {
			options = []string{}
		}

		f := Field{Name: spec[sliceIndex(spec, "identifier")+1]}
		switch strings.ToUpper(options[2]) {
		case "TAG":
			f.Type = TagField
			tfOptions := TagFieldOptions{
				As: options[0],
			}
			if sliceIndex(options, "NOINDEX") != -1 {
				tfOptions.NoIndex = true
			}
			if sliceIndex(options, "SORTABLE") != -1 {
				tfOptions.Sortable = true
			}
			if sliceIndex(options, "CASESENSITIVE") != -1 {
				tfOptions.CaseSensitive = true
			}
			if wIdx := sliceIndex(options, "SEPARATOR"); wIdx != -1 {
				tfOptions.Separator = options[wIdx+1][0]
			}
			f.Options = tfOptions
			f.Sortable = tfOptions.Sortable
		case "GEO":
			f.Type = GeoField
			gfOptions := GeoFieldOptions{
				As: options[0],
			}
			if sliceIndex(options, "NOINDEX") != -1 {
				gfOptions.NoIndex = true
			}
			f.Options = gfOptions
		case "NUMERIC":
			f.Type = NumericField
			nfOptions := NumericFieldOptions{
				As: options[0],
			}
			if sliceIndex(options, "NOINDEX") != -1 {
				nfOptions.NoIndex = true
			}
			if sliceIndex(options, "SORTABLE") != -1 {
				nfOptions.Sortable = true
			}
			f.Options = nfOptions
			f.Sortable = nfOptions.Sortable
		case "TEXT":
			f.Type = TextField
			tfOptions := TextFieldOptions{
				As: options[0],
			}
			if sliceIndex(options, "NOSTEM") != -1 {
				tfOptions.NoStem = true
			}
			if sliceIndex(options, "NOINDEX") != -1 {
				tfOptions.NoIndex = true
			}
			if sliceIndex(options, "SORTABLE") != -1 {
				tfOptions.Sortable = true
			}
			if wIdx := sliceIndex(options, "WEIGHT"); wIdx != -1 && wIdx+1 != len(spec) {
				weightString := options[wIdx+1]
				weight64, _ := strconv.ParseFloat(weightString, 32)
				tfOptions.Weight = float32(weight64)
			}
			f.Options = tfOptions
			f.Sortable = tfOptions.Sortable
		case "VECTOR":
			f.Type = VectorField
			f.Options = VectorFieldOptions{}
		}
		sc = sc.AddField(f)
	}
	info.Schema = *sc
}

// Info - Get information about the index. This can also be used to check if the
// index exists
func (i *Client) Info(ctx context.Context) (*IndexInfo, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	res, err := redis.Values(conn.Do("FT.INFO", i.name))
	if err != nil {
		return nil, err
	}

	ret := IndexInfo{}
	var schemaAttributes []interface{}
	var indexOptions []string

	// Iterate over the values
	for ii := 0; ii < len(res); ii += 2 {
		key, _ := redis.String(res[ii], nil)
		if err := ret.setTarget(key, res[ii+1]); err == nil {
			continue
		}

		switch key {
		case "index_options":
			indexOptions, _ = redis.Strings(res[ii+1], nil)
		case "fields", "attributes":
			schemaAttributes, _ = redis.Values(res[ii+1], nil)
		}
	}

	if schemaAttributes != nil {
		ret.loadSchema(schemaAttributes, indexOptions)
	}

	return &ret, nil
}

// Set runtime configuration option
func (i *Client) SetConfig(ctx context.Context, option string, value string) (string, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	args := redis.Args{"SET", option, value}
	return redis.String(conn.Do("FT.CONFIG", args...))
}

// Get runtime configuration option value
func (i *Client) GetConfig(ctx context.Context, option string) (map[string]string, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	args := redis.Args{"GET", option}
	values, err := redis.Values(conn.Do("FT.CONFIG", args...))
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	valLen := len(values)
	for i := 0; i < valLen; i++ {
		kvs, _ := redis.Strings(values[i], nil)
		if kvs != nil && len(kvs) == 2 {
			m[kvs[0]] = kvs[1]
		}
	}
	return m, nil
}

// Get the distinct tags indexed in a Tag field
func (i *Client) GetTagVals(ctx context.Context, index string, filedName string) ([]string, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	args := redis.Args{index, filedName}
	return redis.Strings(conn.Do("FT.TAGVALS", args...))
}

// SynAdd adds a synonym group.
// Deprecated: This function is not longer supported on RediSearch 2.0 and above, use SynUpdate instead
func (i *Client) SynAdd(ctx context.Context, indexName string, terms []string) (int64, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	args := redis.Args{indexName}.AddFlat(terms)
	return redis.Int64(conn.Do("FT.SYNADD", args...))
}

// SynUpdate updates a synonym group, with additional terms.
func (i *Client) SynUpdate(ctx context.Context, indexName string, synonymGroupId int64, terms []string) (string, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	args := redis.Args{indexName, synonymGroupId}.AddFlat(terms)
	return redis.String(conn.Do("FT.SYNUPDATE", args...))
}

// SynDump dumps the contents of a synonym group.
func (i *Client) SynDump(ctx context.Context, indexName string) (map[string][]int64, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	args := redis.Args{indexName}
	values, err := redis.Values(conn.Do("FT.SYNDUMP", args...))
	if err != nil {
		return nil, err
	}

	valLen := len(values)
	if valLen%2 != 0 {
		return nil, errors.New("SynDump: expects even number of values result")
	}

	m := make(map[string][]int64, valLen/2)
	for i := 0; i < valLen; i += 2 {
		key := values[i].([]byte)
		gids, err := redis.Int64s(values[i+1], nil)
		if err != nil {
			return nil, err
		}
		m[string(key)] = gids
	}
	return m, nil
}

// Adds a document to the index from an existing HASH key in Redis.
// Deprecated: This function is not longer supported on RediSearch 2.0 and above, use HSET instead
// See the example ExampleClient_CreateIndexWithIndexDefinition for a deeper understanding on how to move towards using hashes on your application
func (i *Client) AddHash(ctx context.Context, docId string, score float32, language string, replace bool) (string, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	args := redis.Args{i.name, docId, score}
	if language != "" {
		args = args.Add("LANGUAGE", language)
	}

	if replace {
		args = args.Add("REPLACE")
	}
	return redis.String(conn.Do("FT.ADDHASH", args...))
}

// Returns a list of all existing indexes.
func (i *Client) List(ctx context.Context) ([]string, error) {
	conn, err := i.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	res, err := redis.Values(conn.Do("FT._LIST"))
	if err != nil {
		return nil, err
	}

	var indexes []string

	// Iterate over the values
	for ii := 0; ii < len(res); ii += 1 {
		key, _ := redis.String(res[ii], nil)
		indexes = append(indexes, key)
	}

	return indexes, nil
}
