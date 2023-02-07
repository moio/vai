package sqlcache

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"k8s.io/client-go/tools/cache"
	"reflect"
)

// VersionedIndexer extends Indexer by storing a range of revisions in addition to the latest one
type VersionedIndexer struct {
	*Indexer
	versionFunc VersionFunc

	addHistoryStmt    *sql.Stmt
	deleteHistoryStmt *sql.Stmt
	getByVersionStmt  *sql.Stmt
}

type VersionFunc func(obj any) (int, error)

func NewVersionedIndexer(typ reflect.Type, keyFunc cache.KeyFunc, versionFunc VersionFunc, path string, indexers cache.Indexers) (*VersionedIndexer, error) {
	i, err := NewIndexer(typ, keyFunc, path, indexers)

	err = i.InitExec(`CREATE TABLE object_history (
			key VARCHAR NOT NULL,
			version INTEGER NOT NULL,
			deleted INTEGER NOT NULL DEFAULT 0,
			object BLOB NOT NULL,
			PRIMARY KEY (key, version)
	   )`)
	if err != nil {
		return nil, err
	}
	err = i.InitExec(`CREATE INDEX object_history_version ON object_history(version)`)
	if err != nil {
		return nil, err
	}

	v := &VersionedIndexer{
		Indexer:     i,
		versionFunc: versionFunc,
	}
	v.RegisterAfterUpsert(v.AfterUpsert)
	v.RegisterAfterDelete(v.AfterDelete)

	v.addHistoryStmt = v.Prepare(`INSERT INTO object_history(key, version, deleted, object)
		SELECT ?, ?, 0, object
			FROM objects
			WHERE key = ?
			ON CONFLICT
			    DO UPDATE SET object = excluded.object, deleted = 0`)
	v.deleteHistoryStmt = v.Prepare(`UPDATE object_history SET deleted = 1 WHERE key = ? AND version = (SELECT MAX(version) FROM object_history WHERE key = ?)`)
	v.getByVersionStmt = v.Prepare(`SELECT object FROM object_history WHERE key = ? AND version = ?`)

	return v, nil
}

/* Core methods */

// AfterUpsert appends the latest version to the history table
func (v *VersionedIndexer) AfterUpsert(key string, obj any, tx *sql.Tx) error {
	version, err := v.versionFunc(obj)
	if err != nil {
		return err
	}
	_, err = tx.Stmt(v.addHistoryStmt).Exec(key, version, key)
	return err
}

// AfterDelete updates the deleted flag on the history table
func (v *VersionedIndexer) AfterDelete(key string, tx *sql.Tx) error {
	_, err := tx.Stmt(v.deleteHistoryStmt).Exec(key, key)
	return err
}

// GetByKeyAndVersion returns the object associated with the given object's key and version
func (v *VersionedIndexer) GetByKeyAndVersion(key string, version int) (item any, exists bool, err error) {
	result, err := v.QueryObjects(v.getByVersionStmt, key, version)
	if err != nil {
		return nil, false, err
	}

	if len(result) == 0 {
		return nil, false, nil
	}

	return result[0], true, nil
}
