package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	db := server.storage
	reader, err := db.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, nil
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, nil
	}
	if value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Hint: Consider using Storage.Modify to store data to be modified
	db := server.storage
	batch := []storage.Modify{{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}}
	err := db.Write(nil, batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, nil
	}
	return &kvrpcpb.RawPutResponse{Error: ""}, nil

}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	db := server.storage
	batch := []storage.Modify{{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}}
	err := db.Write(nil, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}
	return &kvrpcpb.RawDeleteResponse{Error: ""}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	db := server.storage
	reader, err := db.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, nil
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	var kvs []*kvrpcpb.KvPair
	iter.Seek(req.StartKey)
	for i := uint32(0); i < req.Limit; i++ {
		if !iter.Valid() {
			break
		}
		item := iter.Item()
		value, _ := item.Value()

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
			Error: nil, // TODO:
		})
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil

}
