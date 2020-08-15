// Copyright 2020 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pretty_assertions::assert_eq;

use rocksdb::prelude::*;
use rocksdb::{WriteBatch, WriteBatchWithIndex};

mod util;
use util::DBPath;

#[test]
fn test_write_batch_clear() {
    let mut batch = WriteBatch::default();
    batch.put(b"1", b"2");
    assert_eq!(batch.len(), 1);
    batch.clear();
    assert_eq!(batch.len(), 0);
    assert!(batch.is_empty());
}

#[test]
fn test_write_batch_with_index_clear() {
    let mut batch = WriteBatchWithIndex::default();
    batch.put(b"1", b"2");
    assert_eq!(batch.len(), 1);
    batch.clear();
    assert_eq!(batch.len(), 0);
    assert!(batch.is_empty());
}

#[test]
fn test_write_batch_with_index_get() {
    let path = DBPath::new("_rust_rocksdb_write_batch_with_index_get");
    {
        let db = DB::open_default(&path).unwrap();
        db.put(b"k1", b"v1111").unwrap();
        let mut batch = WriteBatchWithIndex::default();
        batch.put(b"k2", b"v2222");
        assert_eq!(
            batch.get(&db, b"k1").unwrap().unwrap().as_ref(),
            b"v1111".as_ref()
        );
        assert_eq!(
            batch.get(&db, b"k2").unwrap().unwrap().as_ref(),
            b"v2222".as_ref()
        );
    }
}
