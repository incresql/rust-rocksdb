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

use crate::db::GetDBHandle;
use crate::handle::Handle;
use crate::ops::Write;
use crate::write_batch::{writebatch_delete_callback, writebatch_put_callback, WriteBatchWritable};
use crate::{ffi, ColumnFamily, Error, ReadOptions, WriteBatchIterator, WriteOptions, DB};
use libc::{c_char, c_void, size_t};

/// An atomic batch of write operations.
///
/// Making an atomic commit of several writes:
///
/// ```
/// use rocksdb::{prelude::*, WriteBatchWithIndex};
///
/// let path = "_path_for_rocksdb_storage1";
/// {
///     let db = DB::open_default(path).unwrap();
///     let mut batch = WriteBatchWithIndex::default();
///     batch.put(b"my key", b"my value");
///     batch.put(b"key2", b"value2");
///     batch.put(b"key3", b"value3");
///     db.write(batch); // Atomically commits the batch
/// }
/// let _ = DB::destroy(&Options::default(), path);
/// ```
pub struct WriteBatchWithIndex {
    pub(crate) inner: *mut ffi::rocksdb_writebatch_wi_t,
}

impl WriteBatchWithIndex {
    pub fn len(&self) -> usize {
        unsafe { ffi::rocksdb_writebatch_wi_count(self.inner) as usize }
    }

    /// Return WriteBatch serialized size (in bytes).
    pub fn size_in_bytes(&self) -> usize {
        unsafe {
            let mut batch_size: size_t = 0;
            ffi::rocksdb_writebatch_wi_data(self.inner, &mut batch_size);
            batch_size as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate the put and delete operations within this write batch. Note that
    /// this does _not_ return an `Iterator` but instead will invoke the `put()`
    /// and `delete()` member functions of the provided `WriteBatchIterator`
    /// trait implementation.
    pub fn iterate(&self, callbacks: &mut dyn WriteBatchIterator) {
        let state = Box::into_raw(Box::new(callbacks));
        unsafe {
            ffi::rocksdb_writebatch_wi_iterate(
                self.inner,
                state as *mut c_void,
                Some(writebatch_put_callback),
                Some(writebatch_delete_callback),
            );
            // we must manually set the raw box free since there is no
            // associated "destroy" callback for this object
            Box::from_raw(state);
        }
    }

    /// Insert a value into the database under the given key.
    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    pub fn put_cf<K, V>(&mut self, cf: &ColumnFamily, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_put_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    pub fn merge<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_merge(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    pub fn merge_cf<K, V>(&mut self, cf: &ColumnFamily, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_merge_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    /// Removes the database entry for key. Does nothing if the key was not found.
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    pub fn delete_cf<K: AsRef<[u8]>>(&mut self, cf: &ColumnFamily, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_delete_cf(
                self.inner,
                cf.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    /// Remove database entries from start key to end key.
    ///
    /// Removes the database entries in the range ["begin_key", "end_key"), i.e.,
    /// including "begin_key" and excluding "end_key". It is not an error if no
    /// keys exist in the range ["begin_key", "end_key").
    pub fn delete_range<K: AsRef<[u8]>>(&mut self, from: K, to: K) {
        let (start_key, end_key) = (from.as_ref(), to.as_ref());

        unsafe {
            ffi::rocksdb_writebatch_wi_delete_range(
                self.inner,
                start_key.as_ptr() as *const c_char,
                start_key.len() as size_t,
                end_key.as_ptr() as *const c_char,
                end_key.len() as size_t,
            );
        }
    }

    /// Remove database entries in column family from start key to end key.
    ///
    /// Removes the database entries in the range ["begin_key", "end_key"), i.e.,
    /// including "begin_key" and excluding "end_key". It is not an error if no
    /// keys exist in the range ["begin_key", "end_key").
    pub fn delete_range_cf<K: AsRef<[u8]>>(&mut self, cf: &ColumnFamily, from: K, to: K) {
        let (start_key, end_key) = (from.as_ref(), to.as_ref());

        unsafe {
            ffi::rocksdb_writebatch_wi_delete_range_cf(
                self.inner,
                cf.inner,
                start_key.as_ptr() as *const c_char,
                start_key.len() as size_t,
                end_key.as_ptr() as *const c_char,
                end_key.len() as size_t,
            );
        }
    }

    /// Clear all updates buffered in this batch.
    pub fn clear(&mut self) {
        unsafe {
            ffi::rocksdb_writebatch_wi_clear(self.inner);
        }
    }

    /// Get from the db, first looking at the write batch
    pub fn get<K: AsRef<[u8]>>(&self, db: &DB, key: K) -> Result<Option<Slice>, Error> {
        self.get_opt(db, key, &ReadOptions::default())
    }

    /// Get from the db, first looking at the write batch
    pub fn get_opt<K: AsRef<[u8]>>(
        &self,
        db: &DB,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<Slice>, Error> {
        let key = key.as_ref();
        let mut val_len = 0_usize;
        unsafe {
            let val = ffi_try!(ffi::rocksdb_writebatch_wi_get_from_batch_and_db(
                self.inner,
                db.get_db_handle().handle(),
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &mut val_len as *mut size_t
            ));
            if val.is_null() {
                Ok(None)
            } else {
                let slice = Slice {
                    bytes: std::slice::from_raw_parts(val as *const u8, val_len),
                };
                Ok(Some(slice))
            }
        }
    }
}

impl WriteBatchWritable for WriteBatchWithIndex {
    fn write_opt<H: Handle<ffi::rocksdb_t> + Write>(
        &self,
        handle: &H,
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_write_writebatch_wi(
                handle.handle(),
                writeopts.inner,
                self.inner
            ));
        }
        Ok(())
    }
}

impl Default for WriteBatchWithIndex {
    fn default() -> WriteBatchWithIndex {
        WriteBatchWithIndex {
            // reserved_size, overwrite_keys
            inner: unsafe { ffi::rocksdb_writebatch_wi_create(0, 1) },
        }
    }
}

impl Drop for WriteBatchWithIndex {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_writebatch_wi_destroy(self.inner) }
    }
}

/// Bytes that rocks has allocated for us. We must use their free function
/// to free the data
#[derive(Debug, Clone)]
pub struct Slice {
    bytes: &'static [u8],
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        self.bytes
    }
}

impl Drop for Slice {
    fn drop(&mut self) {
        unsafe { ffi::rocksdb_free(self.bytes.as_ptr() as *mut c_void) }
    }
}
