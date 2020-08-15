// Copyright 2019 Tyler Neely
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

use ambassador::delegatable_trait;

use crate::write_batch::WriteBatchWritable;
use crate::{ffi, handle::Handle, Error, WriteOptions};

#[delegatable_trait]
pub trait WriteBatchWrite {
    fn write<B: WriteBatchWritable>(&self, batch: B) -> Result<(), Error>;

    fn write_without_wal<B: WriteBatchWritable>(&self, batch: B) -> Result<(), Error>;
}

#[delegatable_trait]
pub trait WriteBatchWriteOpt {
    fn write_opt<B: WriteBatchWritable>(
        &self,
        batch: B,
        writeopts: &WriteOptions,
    ) -> Result<(), Error>;
}

impl<T> WriteBatchWrite for T
where
    T: WriteBatchWriteOpt,
{
    fn write<B: WriteBatchWritable>(&self, batch: B) -> Result<(), Error> {
        self.write_opt(batch, &WriteOptions::default())
    }

    fn write_without_wal<B: WriteBatchWritable>(&self, batch: B) -> Result<(), Error> {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        self.write_opt(batch, &wo)
    }
}

impl<T> WriteBatchWriteOpt for T
where
    T: Handle<ffi::rocksdb_t> + super::Write,
{
    fn write_opt<B: WriteBatchWritable>(
        &self,
        batch: B,
        writeopts: &WriteOptions,
    ) -> Result<(), Error> {
        batch.write_opt(self, writeopts)
    }
}
