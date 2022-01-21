// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// //go:build integration

package source

import (
	"context"
	"database/sql"
	"testing"

	"github.com/conduitio/conduit/pkg/foundation/assert"
	"github.com/conduitio/conduit/pkg/plugins"
	"github.com/conduitio/conduit/pkg/record"

	_ "github.com/lib/pq"
)

const (
	// DBURL is the URI for the Postgres server used for integration tests
	DBURL = "postgres://meroxauser:meroxapass@localhost:5432/meroxadb?sslmode=disable"
	// RepDBURL is the URI for the _logical replication_ server and user.
	// This is separate from the DB_URL used above since it requires a different
	// user and permissions for replication.
	RepDBURL = "postgres://repmgr:repmgrmeroxa@localhost:5432/meroxadb?sslmode=disable"
)

func TestSource_Open(t *testing.T) {
	_ = getTestPostgres(t)
	type fields struct {
		table   string
		columns []string
		key     string
	}
	type args struct {
		ctx context.Context
		cfg plugins.Config
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		wanted  *Source
	}{
		{
			name:   "should default to collect all columns from table",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				cfg: plugins.Config{
					Settings: map[string]string{
						"table":    "records",
						"url":      DBURL,
						"snapshot": "disabled",
						"cdc":      "disabled",
					},
				},
			},
			wantErr: false,
		},
		{
			name:   "should error if no url is provided",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				cfg: plugins.Config{
					Settings: map[string]string{
						"table":    "records",
						"snapshot": "disabled",
						"cdc":      "disabled",
					},
				},
			},
			wantErr: true,
		},
		{
			name:   "should configure plugin to read selected columns",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				cfg: plugins.Config{
					Settings: map[string]string{
						"table":    "records",
						"columns":  "key,column1,column2,column3",
						"url":      DBURL,
						"snapshot": "disabled",
						"cdc":      "disabled",
					},
				},
			},
			wantErr: false,
		},
		{
			name:   "should set key to table primary key by default",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				cfg: plugins.Config{
					Settings: map[string]string{
						"table":    "records",
						"url":      DBURL,
						"snapshot": "disabled",
						"cdc":      "disabled",
					},
				},
			},
			wantErr: false,
		},
		{
			name:   "should handle key being set from config",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				cfg: plugins.Config{
					Settings: map[string]string{
						"table":    "records",
						"url":      DBURL,
						"key":      "key",
						"cdc":      "disabled",
						"snapshot": "disabled",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{
				table:   tt.fields.table,
				columns: tt.fields.columns,
				key:     tt.fields.key,
				db:      nil,
			}
			if err := s.Open(tt.args.ctx, tt.args.cfg); (err != nil) != tt.wantErr {
				t.Errorf("Source.Open() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Ok(t, s.Teardown())
		})
	}
}

func TestValidateDisabledSettings(t *testing.T) {
	_ = getTestPostgres(t)
	s := &Source{}
	cfg := plugins.Config{
		Settings: map[string]string{
			"table":    "records",
			"url":      DBURL,
			"snapshot": "disabled",
			"cdc":      "disabled",
		},
	}
	err := s.Validate(cfg)
	assert.Ok(t, err)
	err = s.Open(context.Background(), cfg)
	assert.Ok(t, err)
	assert.Equal(t, nil, s.snapshotter)
	assert.Equal(t, nil, s.cdc)
	err = s.Teardown()
	assert.Ok(t, err)
}

func TestOpen_Defaults(t *testing.T) {
	_ = getTestPostgres(t)
	s := &Source{}
	err := s.Open(context.Background(), plugins.Config{
		Settings: map[string]string{
			"table": "records",
			"url":   RepDBURL,
		},
	})
	assert.Ok(t, err)
	assert.Equal(t, s.key, "id")
	assert.Equal(t, []string{"id", "key", "column1", "column2", "column3"},
		s.columns)
	assert.True(t, s.snapshotter != nil, "failed to set snapshotter default")
	assert.True(t, s.cdc != nil, "failed to set cdc default")
	// TODO: This context needs to cancel the server correctly
	err = s.Teardown()
	assert.Ok(t, err)
}

func TestCDC(t *testing.T) {
	_ = getTestPostgres(t)
	s := &Source{}
	err := s.Open(context.Background(), plugins.Config{
		Settings: map[string]string{
			"table":            "records",
			"url":              RepDBURL,
			"snapshot":         "disabled",
			"slot_name":        "meroxa",
			"publication_name": "meroxa",
		},
	})
	assert.Ok(t, err)
	assert.Equal(t, s.key, "id")
	assert.Equal(t, []string{"id", "key", "column1", "column2", "column3"},
		s.columns)
	assert.True(t, s.cdc != nil, "failed to set cdc default")
	rec1, err := s.Read(context.Background(), nil)
	assert.Equal(t, err, plugins.ErrEndData)
	assert.Equal(t, record.Record{}, rec1)

	// add records and assert that we  didn't error
	_, err = s.db.Query(`insert into records(column1, column2, column3)
	values ('bizz', 456, false);`)
	assert.Ok(t, err)

	// assert that we received cdc events
	_, err = s.Read(context.Background(), nil)
	assert.Ok(t, err)

	// okay so this test should be working but CDC mode is _not_ working.
	// Once it is this test should get a different result.
}

// getTestPostgres is a testing helper that fails if it can't setup a Postgres
// connection and returns a DB and the connection string.
// * It starts and migrates a db with 5 rows for Test_Read* and Test_Open*
func getTestPostgres(t *testing.T) *sql.DB {
	prepareDB := []string{
		// drop any existing data
		`DROP TABLE IF EXISTS records;`,
		// setup records table
		`CREATE TABLE IF NOT EXISTS records (
		id bigserial PRIMARY KEY,
		key bytea,
		column1 varchar(256),
		column2 integer,
		column3 boolean);`,
		// seed values
		`INSERT INTO records(key, column1, column2, column3)
		VALUES('1', 'foo', 123, false),
		('2', 'bar', 456, true),
		('3', 'baz', 789, false),
		('4', null, null, null);`,
	}
	db, err := sql.Open("postgres", DBURL)
	assert.Ok(t, err)
	db = migrate(t, db, prepareDB)
	assert.Ok(t, err)
	return db
}

// migrate will run a set of migrations on a database to prepare it for a test
// it fails the test if any migrations are not applied.
func migrate(t *testing.T, db *sql.DB, migrations []string) *sql.DB {
	for _, migration := range migrations {
		_, err := db.Exec(migration)
		assert.Ok(t, err)
	}
	return db
}
