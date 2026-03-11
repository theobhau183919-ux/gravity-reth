//! A set of read-only transactions that can be used to read data from the database in parallel.

use super::{cursor::Cursor, tx::Tx, Environment, RO};
use crate::{metrics::DatabaseEnvMetrics, DatabaseError};
use reth_db_api::{
    table::{DupSort, Encode, Table},
    transaction::DbTx,
};
use reth_libmdbx::ffi::MDBX_dbi;
use std::{collections::HashMap, sync::Arc};

/// A set of read-only transactions that can be used to read data from the database in parallel.
#[derive(Debug)]
pub struct ParallelTxRO {
    env: Environment,
    dbis: Arc<HashMap<&'static str, MDBX_dbi>>,
    metrics: Option<Arc<DatabaseEnvMetrics>>,
    disable_long_read_transaction_safety: bool,
}

fn create_tx(
    env: &Environment,
    dbis: Arc<HashMap<&'static str, MDBX_dbi>>,
    metrics: Option<Arc<DatabaseEnvMetrics>>,
) -> Result<Tx<RO>, DatabaseError> {
    Tx::new(env.begin_ro_txn().map_err(|e| DatabaseError::InitTx(e.into()))?, dbis, metrics)
        .map_err(|e| DatabaseError::InitTx(e.into()))
}

impl ParallelTxRO {
    pub(super) fn try_new(
        env: Environment,
        dbis: Arc<HashMap<&'static str, MDBX_dbi>>,
        metrics: Option<Arc<DatabaseEnvMetrics>>,
    ) -> Result<Self, DatabaseError> {
        Ok(Self { env, dbis, metrics, disable_long_read_transaction_safety: false })
    }

    fn execute_tx<R>(
        &self,
        f: impl FnOnce(&Tx<RO>) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut tx = create_tx(&self.env, self.dbis.clone(), self.metrics.clone())?;
        if self.disable_long_read_transaction_safety {
            tx.disable_long_read_transaction_safety();
        }
        f(&tx)
    }

    /// Opens a handle to an MDBX database.
    pub fn open_db(&self, name: Option<&str>) -> reth_libmdbx::Result<reth_libmdbx::Database> {
        create_tx(&self.env, self.dbis.clone(), self.metrics.clone())?.inner.open_db(name)
    }

    /// Retrieves database statistics.
    pub fn db_stat(&self, db: &reth_libmdbx::Database) -> reth_libmdbx::Result<reth_libmdbx::Stat> {
        create_tx(&self.env, self.dbis.clone(), self.metrics.clone())?.inner.db_stat(db)
    }

    pub fn table_entries(&self, name: &str) -> reth_libmdbx::Result<usize> {
        let db = self.open_db(Some(name))?;
        let stat = self.db_stat(&db)?;
        Ok(stat.entries())
    }

    /// Returns a raw pointer to the MDBX environment.
    pub const fn env(&self) -> &Environment {
        &self.env
    }
}

impl DbTx for ParallelTxRO {
    type Cursor<T: Table> = Cursor<RO, T>;
    type DupCursor<T: DupSort> = Cursor<RO, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<<T as Table>::Value>, DatabaseError> {
        self.get_by_encoded_key::<T>(&key.encode())
    }

    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError> {
        self.execute_tx(|tx| tx.get_by_encoded_key::<T>(key))
    }

    fn commit(self) -> Result<bool, DatabaseError> {
        // Do nothing.
        Ok(true)
    }

    fn abort(self) {
        // Do nothing.
    }

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        self.execute_tx(|tx| tx.cursor_read::<T>())
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        self.execute_tx(|tx| tx.cursor_dup_read::<T>())
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        self.execute_tx(|tx| tx.entries::<T>())
    }

    fn disable_long_read_transaction_safety(&mut self) {
        self.disable_long_read_transaction_safety = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{mdbx::DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
    use reth_db_api::{database::Database, models::ClientVersion};
    use tempfile::tempdir;

    #[test]
    fn test_parallel_tx_ro() {
        let dir = tempdir().unwrap();
        let args = DatabaseArguments::new(ClientVersion::default());
        let db = DatabaseEnv::open(dir.path(), DatabaseEnvKind::RW, args).unwrap();
        let tx_ro = db.tx().unwrap();
        std::thread::scope(|s| {
            for _ in 0..16 {
                s.spawn(|| {
                    for _ in 0..10000 {
                        tx_ro.execute_tx(|_| Ok(())).unwrap();
                    }
                });
            }
        });
    }
}
