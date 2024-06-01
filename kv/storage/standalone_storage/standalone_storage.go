package standalone_storage

import (
	"os"
	"path/filepath"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath

	kvPath := filepath.Join(dbPath, "kv")
	err := os.MkdirAll(kvPath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	kvDB := engine_util.CreateDB(kvPath, false)

	s := &StandAloneStorage{
		config:  conf,
		engines: engine_util.NewEngines(kvDB, nil, kvPath, ""),
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.engines.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	tx := s.engines.Kv.NewTransaction(false)
	r := &localReader{txn: tx}
	return r, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.engines.Kv, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engines.Kv, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
