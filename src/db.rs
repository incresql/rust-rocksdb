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
//

use crate::{
    ffi,
    ffi_util::{from_cstr, raw_data, to_cpath},
    handle::Handle,
    ops::{self, GetColumnFamilies},
    ColumnFamily, ColumnFamilyDescriptor, DBWALIterator, Error, IngestExternalFileOptions, Options,
    Snapshot, DEFAULT_COLUMN_FAMILY_NAME,
};

use libc::{self, c_char, c_int, c_uchar};
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::fmt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;
use std::slice;
use std::time::Duration;

/// A RocksDB database.
///
/// See crate level documentation for a simple usage example.
pub struct DB {
    pub(crate) inner: *mut ffi::rocksdb_t,
    cfs: BTreeMap<String, ColumnFamily>,
    path: PathBuf,
}

// Safety note: auto-implementing Send on most db-related types is prevented by the inner FFI
// pointer. In most cases, however, this pointer is Send-safe because it is never aliased and
// rocksdb internally does not rely on thread-local information for its user-exposed types.
unsafe impl Send for DB {}

// Sync is similarly safe for many types because they do not expose interior mutability, and their
// use within the rocksdb library is generally behind a const reference
unsafe impl Sync for DB {}

impl Handle<ffi::rocksdb_t> for DB {
    fn handle(&self) -> *mut ffi::rocksdb_t {
        self.inner
    }
}

impl ops::Read for DB {}
impl ops::Write for DB {}

// Specifies whether open DB for read only.
enum AccessType<'a> {
    ReadWrite,
    ReadOnly { error_if_log_file_exist: bool },
    Secondary { secondary_path: &'a Path },
    WithTTL { ttl: Duration },
}

impl DB {
    /// Opens a database with default options.
    pub fn open_default<P: AsRef<Path>>(path: P) -> Result<DB, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, path)
    }

    /// Opens the database with the specified options.
    pub fn open<P: AsRef<Path>>(opts: &Options, path: P) -> Result<DB, Error> {
        DB::open_cf(opts, path, None::<&str>)
    }

    /// Opens the database for read only with the specified options.
    pub fn open_for_read_only<P: AsRef<Path>>(
        opts: &Options,
        path: P,
        error_if_log_file_exist: bool,
    ) -> Result<DB, Error> {
        DB::open_cf_for_read_only(opts, path, None::<&str>, error_if_log_file_exist)
    }

    /// Opens the database as a secondary.
    pub fn open_as_secondary<P: AsRef<Path>>(
        opts: &Options,
        primary_path: P,
        secondary_path: P,
    ) -> Result<DB, Error> {
        DB::open_cf_as_secondary(opts, primary_path, secondary_path, None::<&str>)
    }

    /// Opens the database with a Time to Live compaction filter.
    pub fn open_with_ttl<P: AsRef<Path>>(
        opts: &Options,
        path: P,
        ttl: Duration,
    ) -> Result<DB, Error> {
        let c_path = to_cpath(&path)?;
        let db = DB::open_raw(opts, &c_path, &AccessType::WithTTL { ttl })?;
        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(DB {
            inner: db,
            cfs: BTreeMap::new(),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Opens a database with the given database options and column family names.
    ///
    /// Column families opened using this function will be created with default `Options`.
    pub fn open_cf<P, I, N>(opts: &Options, path: P, cfs: I) -> Result<DB, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        DB::open_cf_descriptors_internal(opts, path, cfs, &AccessType::ReadWrite)
    }

    /// Opens a database for read only with the given database options and column family names.
    pub fn open_cf_for_read_only<P, I, N>(
        opts: &Options,
        path: P,
        cfs: I,
        error_if_log_file_exist: bool,
    ) -> Result<DB, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        DB::open_cf_descriptors_internal(
            opts,
            path,
            cfs,
            &AccessType::ReadOnly {
                error_if_log_file_exist,
            },
        )
    }

    /// Opens the database as a secondary with the given database options and column family names.
    pub fn open_cf_as_secondary<P, I, N>(
        opts: &Options,
        primary_path: P,
        secondary_path: P,
        cfs: I,
    ) -> Result<DB, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        let cfs = cfs
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name.as_ref(), Options::default()));

        DB::open_cf_descriptors_internal(
            opts,
            primary_path,
            cfs,
            &AccessType::Secondary {
                secondary_path: secondary_path.as_ref(),
            },
        )
    }

    /// Opens a database with the given database options and column family descriptors.
    pub fn open_cf_descriptors<P, I>(opts: &Options, path: P, cfs: I) -> Result<DB, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        DB::open_cf_descriptors_internal(opts, path, cfs, &AccessType::ReadWrite)
    }

    /// Internal implementation for opening RocksDB.
    fn open_cf_descriptors_internal<P, I>(
        opts: &Options,
        path: P,
        cfs: I,
        access_type: &AccessType,
    ) -> Result<DB, Error>
    where
        P: AsRef<Path>,
        I: IntoIterator<Item = ColumnFamilyDescriptor>,
    {
        let cfs: Vec<_> = cfs.into_iter().collect();

        let cpath = to_cpath(&path)?;

        if let Err(e) = fs::create_dir_all(&path) {
            return Err(Error::new(format!(
                "Failed to create RocksDB directory: `{:?}`.",
                e
            )));
        }

        let db: *mut ffi::rocksdb_t;
        let mut cf_map = BTreeMap::new();

        if cfs.is_empty() {
            db = DB::open_raw(opts, &cpath, access_type)?;
        } else {
            let mut cfs_v = cfs;
            // Always open the default column family.
            if !cfs_v.iter().any(|cf| cf.name == DEFAULT_COLUMN_FAMILY_NAME) {
                cfs_v.push(ColumnFamilyDescriptor {
                    name: String::from(DEFAULT_COLUMN_FAMILY_NAME),
                    options: Options::default(),
                });
            }
            // We need to store our CStrings in an intermediate vector
            // so that their pointers remain valid.
            let c_cfs: Vec<CString> = cfs_v
                .iter()
                .map(|cf| CString::new(cf.name.as_bytes()).unwrap())
                .collect();

            let cfnames: Vec<_> = c_cfs.iter().map(|cf| cf.as_ptr()).collect();

            // These handles will be populated by DB.
            let mut cfhandles: Vec<_> = cfs_v.iter().map(|_| ptr::null_mut()).collect();

            let cfopts: Vec<_> = cfs_v
                .iter()
                .map(|cf| cf.options.inner as *const _)
                .collect();

            db = DB::open_cf_raw(
                opts,
                &cpath,
                &cfs_v,
                &cfnames,
                &cfopts,
                &mut cfhandles,
                &access_type,
            )?;
            for handle in &cfhandles {
                if handle.is_null() {
                    return Err(Error::new(
                        "Received null column family handle from DB.".to_owned(),
                    ));
                }
            }

            for (cf_desc, inner) in cfs_v.iter().zip(cfhandles) {
                cf_map.insert(cf_desc.name.clone(), ColumnFamily { inner });
            }
        }

        if db.is_null() {
            return Err(Error::new("Could not initialize database.".to_owned()));
        }

        Ok(DB {
            inner: db,
            cfs: cf_map,
            path: path.as_ref().to_path_buf(),
        })
    }

    fn open_raw(
        opts: &Options,
        cpath: &CString,
        access_type: &AccessType,
    ) -> Result<*mut ffi::rocksdb_t, Error> {
        let db = unsafe {
            match *access_type {
                AccessType::ReadOnly {
                    error_if_log_file_exist,
                } => ffi_try!(ffi::rocksdb_open_for_read_only(
                    opts.inner,
                    cpath.as_ptr() as *const _,
                    error_if_log_file_exist as c_uchar,
                )),
                AccessType::ReadWrite => {
                    ffi_try!(ffi::rocksdb_open(opts.inner, cpath.as_ptr() as *const _))
                }
                AccessType::Secondary { secondary_path } => {
                    ffi_try!(ffi::rocksdb_open_as_secondary(
                        opts.inner,
                        cpath.as_ptr() as *const _,
                        to_cpath(secondary_path)?.as_ptr() as *const _,
                    ))
                }
                AccessType::WithTTL { ttl } => ffi_try!(ffi::rocksdb_open_with_ttl(
                    opts.inner,
                    cpath.as_ptr() as *const _,
                    ttl.as_secs() as c_int,
                )),
            }
        };
        Ok(db)
    }

    fn open_cf_raw(
        opts: &Options,
        cpath: &CString,
        cfs_v: &[ColumnFamilyDescriptor],
        cfnames: &[*const c_char],
        cfopts: &[*const ffi::rocksdb_options_t],
        cfhandles: &mut Vec<*mut ffi::rocksdb_column_family_handle_t>,
        access_type: &AccessType,
    ) -> Result<*mut ffi::rocksdb_t, Error> {
        let db = unsafe {
            match *access_type {
                AccessType::ReadOnly {
                    error_if_log_file_exist,
                } => ffi_try!(ffi::rocksdb_open_for_read_only_column_families(
                    opts.inner,
                    cpath.as_ptr(),
                    cfs_v.len() as c_int,
                    cfnames.as_ptr(),
                    cfopts.as_ptr(),
                    cfhandles.as_mut_ptr(),
                    error_if_log_file_exist as c_uchar,
                )),
                AccessType::ReadWrite => ffi_try!(ffi::rocksdb_open_column_families(
                    opts.inner,
                    cpath.as_ptr(),
                    cfs_v.len() as c_int,
                    cfnames.as_ptr(),
                    cfopts.as_ptr(),
                    cfhandles.as_mut_ptr(),
                )),
                AccessType::Secondary { secondary_path } => {
                    ffi_try!(ffi::rocksdb_open_as_secondary_column_families(
                        opts.inner,
                        cpath.as_ptr() as *const _,
                        to_cpath(secondary_path)?.as_ptr() as *const _,
                        cfs_v.len() as c_int,
                        cfnames.as_ptr(),
                        cfopts.as_ptr(),
                        cfhandles.as_mut_ptr(),
                    ))
                }
                _ => return Err(Error::new("Unsupported access type".to_owned())),
            }
        };
        Ok(db)
    }

    pub fn list_cf<P: AsRef<Path>>(opts: &Options, path: P) -> Result<Vec<String>, Error> {
        let cpath = to_cpath(path)?;
        let mut length = 0;

        unsafe {
            let ptr = ffi_try!(ffi::rocksdb_list_column_families(
                opts.inner,
                cpath.as_ptr() as *const _,
                &mut length,
            ));

            let vec = slice::from_raw_parts(ptr, length)
                .iter()
                .map(|ptr| CStr::from_ptr(*ptr).to_string_lossy().into_owned())
                .collect();
            ffi::rocksdb_list_column_families_destroy(ptr, length);
            Ok(vec)
        }
    }

    pub fn destroy<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = to_cpath(path)?;
        unsafe {
            ffi_try!(ffi::rocksdb_destroy_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }

    pub fn repair<P: AsRef<Path>>(opts: &Options, path: P) -> Result<(), Error> {
        let cpath = to_cpath(path)?;
        unsafe {
            ffi_try!(ffi::rocksdb_repair_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path.as_path()
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self)
    }

    /// The sequence number of the most recent transaction.
    pub fn latest_sequence_number(&self) -> u64 {
        unsafe { ffi::rocksdb_get_latest_sequence_number(self.inner) }
    }

    /// Iterate over batches of write operations since a given sequence.
    ///
    /// Produce an iterator that will provide the batches of write operations
    /// that have occurred since the given sequence (see
    /// `latest_sequence_number()`). Use the provided iterator to retrieve each
    /// (`u64`, `WriteBatch`) tuple, and then gather the individual puts and
    /// deletes using the `WriteBatch::iterate()` function.
    ///
    /// Calling `get_updates_since()` with a sequence number that is out of
    /// bounds will return an error.
    pub fn get_updates_since(&self, seq_number: u64) -> Result<DBWALIterator, Error> {
        unsafe {
            // rocksdb_wal_readoptions_t does not appear to have any functions
            // for creating and destroying it; fortunately we can pass a nullptr
            // here to get the default behavior
            let opts: *const ffi::rocksdb_wal_readoptions_t = ptr::null();
            let iter = ffi_try!(ffi::rocksdb_get_updates_since(self.inner, seq_number, opts));
            Ok(DBWALIterator { inner: iter })
        }
    }

    /// Tries to catch up with the primary by reading as much as possible from the
    /// log files.
    pub fn try_catch_up_with_primary(&self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_try_catch_up_with_primary(self.inner));
        }
        Ok(())
    }

    /// Loads a list of external SST files created with SstFileWriter into the DB with default opts
    pub fn ingest_external_file<P: AsRef<Path>>(&self, paths: Vec<P>) -> Result<(), Error> {
        let opts = IngestExternalFileOptions::default();
        self.ingest_external_file_opts(&opts, paths)
    }

    /// Loads a list of external SST files created with SstFileWriter into the DB
    pub fn ingest_external_file_opts<P: AsRef<Path>>(
        &self,
        opts: &IngestExternalFileOptions,
        paths: Vec<P>,
    ) -> Result<(), Error> {
        let paths_v: Vec<CString> = paths
            .iter()
            .map(|path| to_cpath(&path))
            .collect::<Result<Vec<_>, _>>()?;

        let cpaths: Vec<_> = paths_v.iter().map(|path| path.as_ptr()).collect();

        self.ingest_external_file_raw(&opts, &paths_v, &cpaths)
    }

    /// Loads a list of external SST files created with SstFileWriter into the DB for given Column Family
    /// with default opts
    pub fn ingest_external_file_cf<P: AsRef<Path>>(
        &self,
        cf: &ColumnFamily,
        paths: Vec<P>,
    ) -> Result<(), Error> {
        let opts = IngestExternalFileOptions::default();
        self.ingest_external_file_cf_opts(&cf, &opts, paths)
    }

    /// Loads a list of external SST files created with SstFileWriter into the DB for given Column Family
    pub fn ingest_external_file_cf_opts<P: AsRef<Path>>(
        &self,
        cf: &ColumnFamily,
        opts: &IngestExternalFileOptions,
        paths: Vec<P>,
    ) -> Result<(), Error> {
        let paths_v: Vec<CString> = paths
            .iter()
            .map(|path| to_cpath(&path))
            .collect::<Result<Vec<_>, _>>()?;

        let cpaths: Vec<_> = paths_v.iter().map(|path| path.as_ptr()).collect();

        self.ingest_external_file_raw_cf(&cf, &opts, &paths_v, &cpaths)
    }

    fn ingest_external_file_raw(
        &self,
        opts: &IngestExternalFileOptions,
        paths_v: &[CString],
        cpaths: &[*const c_char],
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_ingest_external_file(
                self.inner,
                cpaths.as_ptr(),
                paths_v.len(),
                opts.inner as *const _
            ));
            Ok(())
        }
    }

    fn ingest_external_file_raw_cf(
        &self,
        cf: &ColumnFamily,
        opts: &IngestExternalFileOptions,
        paths_v: &[CString],
        cpaths: &[*const c_char],
    ) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_ingest_external_file_cf(
                self.inner,
                cf.inner,
                cpaths.as_ptr(),
                paths_v.len(),
                opts.inner as *const _
            ));
            Ok(())
        }
    }

    /// Returns a list of all table files with their level, start key
    /// and end key
    pub fn live_files(&self) -> Result<Vec<LiveFile>, Error> {
        unsafe {
            let files = ffi::rocksdb_livefiles(self.inner);
            if files.is_null() {
                Err(Error::new("Could not get live files".to_owned()))
            } else {
                let n = ffi::rocksdb_livefiles_count(files);

                let mut livefiles = Vec::with_capacity(n as usize);
                let mut key_size: usize = 0;

                for i in 0..n {
                    let name = from_cstr(ffi::rocksdb_livefiles_name(files, i));
                    let size = ffi::rocksdb_livefiles_size(files, i);
                    let level = ffi::rocksdb_livefiles_level(files, i) as i32;

                    // get smallest key inside file
                    let smallest_key = ffi::rocksdb_livefiles_smallestkey(files, i, &mut key_size);
                    let smallest_key = raw_data(smallest_key, key_size);

                    // get largest key inside file
                    let largest_key = ffi::rocksdb_livefiles_largestkey(files, i, &mut key_size);
                    let largest_key = raw_data(largest_key, key_size);

                    livefiles.push(LiveFile {
                        name,
                        size,
                        level,
                        start_key: smallest_key,
                        end_key: largest_key,
                        num_entries: ffi::rocksdb_livefiles_entries(files, i),
                        num_deletions: ffi::rocksdb_livefiles_deletions(files, i),
                    })
                }

                // destroy livefiles metadata(s)
                ffi::rocksdb_livefiles_destroy(files);

                // return
                Ok(livefiles)
            }
        }
    }

    /// Delete sst files whose keys are entirely in the given range.
    ///
    /// Could leave some keys in the range which are in files which are not
    /// entirely in the range.
    ///
    /// Note: L0 files are left regardless of whether they're in the range.
    ///  
    /// Snapshots before the delete might not see the data in the given range.
    pub fn delete_file_in_range<K: AsRef<[u8]>>(&self, from: K, to: K) -> Result<(), Error> {
        let from = from.as_ref();
        let to = to.as_ref();
        unsafe {
            ffi_try!(ffi::rocksdb_delete_file_in_range(
                self.inner,
                from.as_ptr() as *const c_char,
                from.len() as size_t,
                to.as_ptr() as *const c_char,
                to.len() as size_t,
            ));
            Ok(())
        }
    }

    /// Same as `delete_file_in_range` but only for specific column family
    pub fn delete_file_in_range_cf<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        from: K,
        to: K,
    ) -> Result<(), Error> {
        let from = from.as_ref();
        let to = to.as_ref();
        unsafe {
            ffi_try!(ffi::rocksdb_delete_file_in_range_cf(
                self.inner,
                cf.inner,
                from.as_ptr() as *const c_char,
                from.len() as size_t,
                to.as_ptr() as *const c_char,
                to.len() as size_t,
            ));
            Ok(())
        }
    }
}

impl GetColumnFamilies for DB {
    fn get_cfs(&self) -> &BTreeMap<String, ColumnFamily> {
        &self.cfs
    }

    fn get_mut_cfs(&mut self) -> &mut BTreeMap<String, ColumnFamily> {
        &mut self.cfs
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        unsafe {
            for cf in self.cfs.values() {
                ffi::rocksdb_column_family_handle_destroy(cf.inner);
            }
            ffi::rocksdb_close(self.inner);
        }
    }
}

impl fmt::Debug for DB {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RocksDB {{ path: {:?} }}", self.path())
    }
}

/// The metadata that describes a SST file
#[derive(Debug, Clone)]
pub struct LiveFile {
    /// Name of the file
    pub name: String,
    /// Size of the file
    pub size: usize,
    /// Level at which this file resides
    pub level: i32,
    /// Smallest user defined key in the file
    pub start_key: Option<Vec<u8>>,
    /// Largest user defined key in the file
    pub end_key: Option<Vec<u8>>,
    /// Number of entries/alive keys in the file
    pub num_entries: u64,
    /// Number of deletions/tomb key(s) in the file
    pub num_deletions: u64,
}
