// Copyright 2012-2015 Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"gopkg.in/olivere/elastic.v3/uritemplates"
)

type BulkService struct {
	client *Client

	FastParse bool

	index    string
	_type    string
	requests []BulkableRequest
	//replicationType string
	//consistencyLevel string
	timeout string
	refresh *bool
	pretty  bool

	sizeInBytes      int64
	sizeEstimatedPos int
}

func NewBulkService(client *Client) *BulkService {
	builder := &BulkService{
		client:   client,
		requests: make([]BulkableRequest, 0),
	}
	return builder
}

func (s *BulkService) reset() {
	s.requests = make([]BulkableRequest, 0)
	s.sizeInBytes = 0
	s.sizeEstimatedPos = 0
}

func (s *BulkService) Index(index string) *BulkService {
	s.index = index
	return s
}

func (s *BulkService) Type(_type string) *BulkService {
	s._type = _type
	return s
}

func (s *BulkService) Timeout(timeout string) *BulkService {
	s.timeout = timeout
	return s
}

func (s *BulkService) Refresh(refresh bool) *BulkService {
	s.refresh = &refresh
	return s
}

func (s *BulkService) Pretty(pretty bool) *BulkService {
	s.pretty = pretty
	return s
}

func (s *BulkService) Add(r BulkableRequest) *BulkService {
	s.requests = append(s.requests, r)
	return s
}

// EstimatedSizeInBytes returns the size of the underlying JSON payload.
// It is quite costly, use with caution, non thread-safe
func (s *BulkService) EstimatedSizeInBytes() int64 {
	if s.sizeEstimatedPos == len(s.requests) {
		return s.sizeInBytes
	}

	for _, r := range s.requests[s.sizeEstimatedPos:] {
		s.sizeInBytes += s.estimateSizeInBytes(r)
		s.sizeEstimatedPos++
	}

	return s.sizeInBytes
}

func (s *BulkService) estimateSizeInBytes(r BulkableRequest) int64 {
	// +1 for the \n
	return int64(1 + len([]byte(r.String())))
}

func (s *BulkService) NumberOfActions() int {
	return len(s.requests)
}

func (s *BulkService) bodyAsString() (string, error) {
	buf := bytes.NewBufferString("")

	for _, req := range s.requests {
		source, err := req.Source()
		if err != nil {
			return "", err
		}
		for _, line := range source {
			_, err := buf.WriteString(fmt.Sprintf("%s\n", line))
			if err != nil {
				return "", nil
			}
		}
	}

	return buf.String(), nil
}

func (s *BulkService) Do() (*BulkResponse, error) {
	// No actions?
	if s.NumberOfActions() == 0 {
		return nil, errors.New("elastic: No bulk actions to commit")
	}

	// Get body
	body, err := s.bodyAsString()
	if err != nil {
		return nil, err
	}

	// Build url
	path := "/"
	if s.index != "" {
		index, err := uritemplates.Expand("{index}", map[string]string{
			"index": s.index,
		})
		if err != nil {
			return nil, err
		}
		path += index + "/"
	}
	if s._type != "" {
		typ, err := uritemplates.Expand("{type}", map[string]string{
			"type": s._type,
		})
		if err != nil {
			return nil, err
		}
		path += typ + "/"
	}
	path += "_bulk"

	// Parameters
	params := make(url.Values)
	if s.pretty {
		params.Set("pretty", fmt.Sprintf("%v", s.pretty))
	}
	if s.refresh != nil {
		params.Set("refresh", fmt.Sprintf("%v", *s.refresh))
	}
	if s.timeout != "" {
		params.Set("timeout", s.timeout)
	}

	// Get response
	res, err := s.client.PerformRequest("POST", path, params, body)
	if err != nil {
		return nil, err
	}

	// Return results
	if s.FastParse {
		ret, err := s.fastParseResponse(res.Body)
		if err == nil && !ret.Errors {
			s.reset()

			return ret, err
		}
	}

	ret := new(BulkResponse)
	if err := json.Unmarshal(res.Body, ret); err != nil {
		return nil, err
	}

	// Reset so the request can be reused
	s.reset()

	return ret, nil
}

func (s *BulkService) fastParseResponse(rawResp json.RawMessage) (*BulkResponse, error) {
	var took int
	var errors bool
	var err error

	maxLookAhead := 50
	if len(rawResp) > maxLookAhead {
		// use a string for convenience
		resp := string(rawResp[:50])

		// reading: no variable 0, took 1, errors 2
		var reading int
		// variables already read
		var read int

		var pos int
		for pos < maxLookAhead && read < 2 {
			switch resp[pos] {
			case '{':
				pos++
				continue
			case '"':
				pos++
				continue
			case ',':
				pos++
				continue
			case ':':
				// read till next key
				nextKey := pos + 1
				for resp[nextKey] != ',' && nextKey < maxLookAhead {
					nextKey++
				}
				if reading == 1 {
					took, err = strconv.Atoi(resp[pos+1 : nextKey])
				} else if reading == 2 {
					errors, err = strconv.ParseBool(resp[pos+1 : nextKey])
				}
				if err != nil {
					break
				}
				read++
				pos = nextKey
			default:
				if resp[pos:pos+4] == "took" {
					reading = 1
					pos += 4
				} else if resp[pos:pos+6] == "errors" {
					reading = 2
					pos += 6
				} else {
					err = fmt.Errorf("cannot parse %s", resp[pos:])
					break
				}
			}

		}
	}

	return &BulkResponse{Took: took, Errors: errors}, err
}

// BulkResponse is a response to a bulk execution.
//
// Example:
// {
//   "took":3,
//   "errors":false,
//   "items":[{
//     "index":{
//       "_index":"index1",
//       "_type":"tweet",
//       "_id":"1",
//       "_version":3,
//       "status":201
//     }
//   },{
//     "index":{
//       "_index":"index2",
//       "_type":"tweet",
//       "_id":"2",
//       "_version":3,
//       "status":200
//     }
//   },{
//     "delete":{
//       "_index":"index1",
//       "_type":"tweet",
//       "_id":"1",
//       "_version":4,
//       "status":200,
//       "found":true
//     }
//   },{
//     "update":{
//       "_index":"index2",
//       "_type":"tweet",
//       "_id":"2",
//       "_version":4,
//       "status":200
//     }
//   }]
// }
type BulkResponse struct {
	Took   int                            `json:"took,omitempty"`
	Errors bool                           `json:"errors,omitempty"`
	Items  []map[string]*BulkResponseItem `json:"items,omitempty"`
}

// BulkResponseItem is the result of a single bulk request.
type BulkResponseItem struct {
	Index   string        `json:"_index,omitempty"`
	Type    string        `json:"_type,omitempty"`
	Id      string        `json:"_id,omitempty"`
	Version int           `json:"_version,omitempty"`
	Status  int           `json:"status,omitempty"`
	Found   bool          `json:"found,omitempty"`
	Error   *ErrorDetails `json:"error,omitempty"`
}

// Indexed returns all bulk request results of "index" actions.
func (r *BulkResponse) Indexed() []*BulkResponseItem {
	return r.ByAction("index")
}

// Created returns all bulk request results of "create" actions.
func (r *BulkResponse) Created() []*BulkResponseItem {
	return r.ByAction("create")
}

// Updated returns all bulk request results of "update" actions.
func (r *BulkResponse) Updated() []*BulkResponseItem {
	return r.ByAction("update")
}

// Deleted returns all bulk request results of "delete" actions.
func (r *BulkResponse) Deleted() []*BulkResponseItem {
	return r.ByAction("delete")
}

// ByAction returns all bulk request results of a certain action,
// e.g. "index" or "delete".
func (r *BulkResponse) ByAction(action string) []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	items := make([]*BulkResponseItem, 0)
	for _, item := range r.Items {
		if result, found := item[action]; found {
			items = append(items, result)
		}
	}
	return items
}

// ById returns all bulk request results of a given document id,
// regardless of the action ("index", "delete" etc.).
func (r *BulkResponse) ById(id string) []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	items := make([]*BulkResponseItem, 0)
	for _, item := range r.Items {
		for _, result := range item {
			if result.Id == id {
				items = append(items, result)
			}
		}
	}
	return items
}

// Failed returns those items of a bulk response that have errors,
// i.e. those that don't have a status code between 200 and 299.
func (r *BulkResponse) Failed() []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	errors := make([]*BulkResponseItem, 0)
	for _, item := range r.Items {
		for _, result := range item {
			if !(result.Status >= 200 && result.Status <= 299) {
				errors = append(errors, result)
			}
		}
	}
	return errors
}

// Succeeded returns those items of a bulk response that have no errors,
// i.e. those have a status code between 200 and 299.
func (r *BulkResponse) Succeeded() []*BulkResponseItem {
	if r.Items == nil {
		return nil
	}
	succeeded := make([]*BulkResponseItem, 0)
	for _, item := range r.Items {
		for _, result := range item {
			if result.Status >= 200 && result.Status <= 299 {
				succeeded = append(succeeded, result)
			}
		}
	}
	return succeeded
}
