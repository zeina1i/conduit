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

package source

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/batchcorp/pgoutput"
	"github.com/conduitio/conduit/pkg/foundation/assert"
	"github.com/conduitio/conduit/pkg/foundation/cerrors"
	"github.com/conduitio/conduit/pkg/plugins"
	"github.com/conduitio/conduit/pkg/plugins/pg/source/mock"
	"github.com/conduitio/conduit/pkg/record"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestSource_Read(t *testing.T) {
	type fields struct {
		killswitch  context.CancelFunc
		db          *sql.DB
		table       string
		columns     []string
		key         string
		cdc         func() Iterator
		snapshotter func() Iterator
	}
	tests := []struct {
		name    string
		fields  fields
		want    record.Record
		wantErr bool
		err     error
	}{
		{
			name: "should read a record from snapshot",
			fields: fields{
				table:   "records",
				columns: []string{"id", "key", "column1", "column2", "column3"},
				key:     "key",
				snapshotter: func() Iterator {
					mockCtrl := gomock.NewController(t)
					mockIterator := mock.NewMockIterator(mockCtrl)
					mockIterator.EXPECT().HasNext().Return(true)
					mockIterator.EXPECT().Next().Return(record.Record{
						Position: record.Position("1"),
						Metadata: map[string]string{
							"key":   "key",
							"table": "records",
						},
						Key: record.StructuredData{
							"key": "1",
						},
						Payload: record.StructuredData{
							"id":      int64(1),
							"key":     "1",
							"column1": "foo",
							"column2": int64(123),
							"column3": false,
						},
					}, nil)
					return mockIterator
				},
				cdc: func() Iterator { return nil },
			},
			want: record.Record{
				Position: record.Position("1"),
				Metadata: map[string]string{
					"key":   "key",
					"table": "records",
				},
				Key: record.StructuredData{
					"key": "1",
				},
				Payload: record.StructuredData{
					"id":      int64(1),
					"key":     "1",
					"column1": "foo",
					"column2": int64(123),
					"column3": false,
				},
			},
			wantErr: false,
		},
		{
			name: "should read a record from cdc when no snapshotter present",
			fields: fields{
				table:   "records",
				columns: []string{"id", "key", "column1", "column2", "column3"},
				key:     "key",
				snapshotter: func() Iterator {
					return nil
				},
				cdc: func() Iterator {
					mockCtrl := gomock.NewController(t)
					t.Cleanup(mockCtrl.Finish)
					mockIterator := mock.NewMockIterator(mockCtrl)
					mockIterator.EXPECT().HasNext().Return(true)
					mockIterator.EXPECT().Next().Return(record.Record{
						Position: record.Position("1"),
						Metadata: map[string]string{
							"key":   "key",
							"table": "records",
						},
						Key: record.StructuredData{
							"key": "1",
						},
						Payload: record.StructuredData{
							"id":      int64(1),
							"key":     "1",
							"column1": "foo",
							"column2": int64(123),
							"column3": false,
						},
					}, nil)
					return mockIterator
				},
			},
			want: record.Record{
				Position: record.Position("1"),
				Metadata: map[string]string{
					"key":   "key",
					"table": "records",
				},
				Key: record.StructuredData{
					"key": "1",
				},
				Payload: record.StructuredData{
					"id":      int64(1),
					"key":     "1",
					"column1": "foo",
					"column2": int64(123),
					"column3": false,
				},
			},
			wantErr: false,
		},
		{
			name: "should return ErrEndData if no buffer and no snapshotter",
			fields: fields{
				table:       "records",
				columns:     []string{"id", "key", "column1", "column2", "column3"},
				key:         "key",
				snapshotter: func() Iterator { return nil },
				cdc:         func() Iterator { return nil },
			},
			wantErr: true,
			err:     plugins.ErrEndData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{
				killswitch:  tt.fields.killswitch,
				db:          tt.fields.db,
				table:       tt.fields.table,
				columns:     tt.fields.columns,
				key:         tt.fields.key,
				cdc:         tt.fields.cdc(),
				snapshotter: tt.fields.snapshotter(),
			}
			// NB: pass nil because Position is not used for this connector
			got, err := s.Read(context.Background(), nil)
			t.Logf("got: %v", got)
			t.Logf("err: %v", err)
			if (err != nil) != tt.wantErr {
				assert.True(t, cerrors.Is(err, tt.err), "failed to get correct error")
				t.Errorf("Source.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			diff := cmp.Diff(got, tt.want,
				cmpopts.IgnoreFields(record.Record{}, "CreatedAt"))
			if diff != "" {
				t.Logf("[DIFF]: %s", diff)
				t.Errorf("Source.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSource_Teardown(t *testing.T) {
	type fields struct {
		subWG       sync.WaitGroup
		killswitch  context.CancelFunc
		Mutex       sync.Mutex
		db          *sql.DB
		table       string
		columns     []string
		key         string
		sub         *pgoutput.Subscription
		subErr      error
		cdc         Iterator
		snapshotter Iterator
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Source{
				subWG:       tt.fields.subWG,
				killswitch:  tt.fields.killswitch,
				Mutex:       tt.fields.Mutex,
				db:          tt.fields.db,
				table:       tt.fields.table,
				columns:     tt.fields.columns,
				key:         tt.fields.key,
				sub:         tt.fields.sub,
				subErr:      tt.fields.subErr,
				cdc:         tt.fields.cdc,
				snapshotter: tt.fields.snapshotter,
			}
			if err := s.Teardown(); (err != nil) != tt.wantErr {
				t.Errorf("Source.Teardown() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
