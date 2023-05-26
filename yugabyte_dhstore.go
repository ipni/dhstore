package dhstore

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/multiformats/go-multihash"
)

const (
	host        = "127.0.0.1"
	port        = 5433
	dbName      = "yugabyte"
	dbUser      = "yugabyte"
	dbPassword  = "yugabyte"
	sslMode     = "disable"
	sslRootCert = ""
)

type yugabyteDHStore struct {
	db *sql.DB
}

func NewYugabyteDHStore() (DHStore, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		host, port, dbUser, dbPassword, dbName)

	if sslMode != "" {
		psqlInfo += fmt.Sprintf(" sslmode=%s", sslMode)

		if sslRootCert != "" {
			psqlInfo += fmt.Sprintf(" sslrootcert=%s", sslRootCert)
		}
	}

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}

	ydb := &yugabyteDHStore{
		db: db,
	}

	ydb.createDatabase()
	return ydb, nil

}

func (y *yugabyteDHStore) createDatabase() error {
	stmt := `DROP TABLE IF EXISTS Multihash`
	_, err := y.db.Exec(stmt)
	if err != nil {
		return err
	}

	stmt = `CREATE TABLE Multihash (
                        mh BYTEA PRIMARY KEY,
                        evks BYTEA[])`

	_, err = y.db.Exec(stmt)
	if err != nil {
		return err
	}

	stmt = `DROP TABLE IF EXISTS Metadata`
	_, err = y.db.Exec(stmt)
	if err != nil {
		return err
	}

	stmt = `CREATE TABLE Metadata (
                        hvk BYTEA PRIMARY KEY,
                        emd BYTEA)`

	_, err = y.db.Exec(stmt)
	if err != nil {
		return err.(*pq.Error)
	}

	return nil
}

func (y *yugabyteDHStore) MergeIndex(mh multihash.Multihash, evk EncryptedValueKey) error {
	stmt := `UPDATE Multihash 
			SET evks = ARRAY_APPEND(evks, $1) 
			WHERE mh=$2;`
	_, err := y.db.Exec(stmt, evk, mh)
	return err
}

func (y *yugabyteDHStore) PutMetadata(hvk HashedValueKey, emd EncryptedMetadata) error {
	stmt := `INSERT INTO Metadata (hvk, emd)
			 VALUES ($1, $2)
			 ON CONFLICT (hvk)
			 DO 
			 UPDATE SET emd = $2;`
	_, err := y.db.Exec(stmt, hvk, emd)
	return err
}

func (y *yugabyteDHStore) Lookup(mh multihash.Multihash) ([]EncryptedValueKey, error) {
	stmt := `SELECT evks
			FROM  Multihash 
			WHERE mh=$1;`
	rows, err := y.db.Query(stmt, mh)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var evk []byte
	evks := make([]EncryptedValueKey, 0)
	for rows.Next() {
		err = rows.Scan(&evk)
		if err != nil {
			return nil, err
		}
		evks = append(evks, evk)
	}

	return evks, nil
}

func (y *yugabyteDHStore) DeleteMetadata(hvk HashedValueKey) error {
	stmt := `DELETE FROM Metadata
			WHERE hvk=$1;`
	_, err := y.db.Exec(stmt, hvk)
	return err
}

func (y *yugabyteDHStore) GetMetadata(hvk HashedValueKey) (EncryptedMetadata, error) {
	stmt := `SELECT emd
			FROM  Metadata 
			WHERE hvk=$1;`
	rows, err := y.db.Query(stmt, hvk)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var emd []byte
	for rows.Next() {
		err = rows.Scan(&emd)
		if err != nil {
			return nil, err
		}
		return emd, nil
	}

	return nil, nil
}

func (y *yugabyteDHStore) Close() error {
	return y.db.Close()
}
