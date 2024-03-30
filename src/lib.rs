mod currency;

use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::{pg_shmem_init, debug1, error};
use pgrx::lwlock::PgLwLock;
use pgrx::spi::SpiResult;

const MAX_ENTRIES: usize = 8 * 1024;
const MAX_CURRENCIES: usize = 1 * 1024;

const DEFAULT_VALIDATION_QUERY: &str = r#"SELECT count(table_name) = 2
FROM information_schema.tables
WHERE table_schema = 'plan' AND (table_name = 'currency' or table_name = 'fx_rate');"#;

const DEFAULT_GET_CURRENCIES_IDS: &str = r#"SELECT min(c.id), max(c.id) FROM plan.currency c"#;
const DEFAULT_GET_CURRENCIES_ENTRY_COUNT: &str = r#"SELECT cu.currency_id,
(SELECT LOWER(cu.\"xuid\") FROM plan.currency cu WHERE cu.id = cr.currency_id) \"xuid\,
count(*)
FROM plan.fx_rate cr
GROUP by cr.currency_id
ORDER by cr.currency_id asc;"#;
const DEFAULT_GET_CURRENCY_ENTRIES: &str = r#"SELECT cr.currency_id, cr.to_currency_id, cr.rate cr.\"date\"
from plan.fx_rate cr
order by cr.currency_id asc, cr.\"date\" asc;"#;

// Activate PostgreSQL Extension
::pgrx::pg_module_magic!();

// Control Struct

#[derive(Copy, Clone)]
pub struct CurrencyControl {
    currency_count: i32,
    entry_count: i32,
    min_currency_id: i8,
    cache_filled: bool,
}

impl Default for CurrencyControl {
    fn default() -> Self {
        CurrencyControl {
            currency_count: 0,
            entry_count: 0,
            min_currency_id: 0,
            cache_filled: false,
        }
    }
}

unsafe impl PGRXSharedMemory for CurrencyControl {}

// Currency Metadata Struct

#[derive(Copy, Clone,  Debug)]
pub struct Currency {
    id: i8,
    xuid: &'static str,
    rates_size: i32,
    page_size: i32,
    first_page_offset: i32,
    page_map_size: i32,
}

impl Default for Currency {
    fn default() -> Self {
        Currency {
            id: 0,
            xuid: "",
            rates_size: 0,
            page_size: 0,
            first_page_offset: 0,
            page_map_size: 0,
        }
    }
}

unsafe impl PGRXSharedMemory for Currency {}

// Shared Memory Hashmaps
static CURRENCY_CONTROL: PgLwLock<CurrencyControl> = PgLwLock::new();

/// [CURRENCY_ID, CURRENCY_XUID]
static CURRENCY_XUID_MAP: PgLwLock<heapless::FnvIndexMap<&'static str, i8, MAX_CURRENCIES>> = PgLwLock::new();

/// [CURRENCY_ID, CURRENCY_METADATA]
static CURRENCY_ID_METADATA_MAP: PgLwLock<heapless::FnvIndexMap<i8, Currency, MAX_CURRENCIES>> = PgLwLock::new();

/// [CURRENCY_ID, PAGE_MAP[]]
static CURRENCY_ID_PAGE_MAP: PgLwLock<heapless::FnvIndexMap<i8, heapless::Vec<i32, MAX_ENTRIES>, MAX_CURRENCIES>> = PgLwLock::new();

/// [FROM_CURRENCY_ID, TO_CURRENCY_ID, DATE]
static CURRENCY_ID_DATE_MAP: PgLwLock<heapless::FnvIndexMap<i8, heapless::FnvIndexMap<i8, i32, MAX_CURRENCIES>, MAX_CURRENCIES>> = PgLwLock::new();

/// [FROM_CURRENCY_ID, TO_CURRENCY_ID, RATE]
static CURRENCY_ID_RATES_MAP: PgLwLock<heapless::FnvIndexMap<i8, heapless::FnvIndexMap<i8, f32, MAX_CURRENCIES>, MAX_CURRENCIES>> = PgLwLock::new();


// Init Extension - Shared Memory
#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(CURRENCY_CONTROL);
    pg_shmem_init!(CURRENCY_XUID_MAP);
    pg_shmem_init!(CURRENCY_ID_METADATA_MAP);
    pg_shmem_init!(CURRENCY_ID_DATE_MAP);
    pg_shmem_init!(CURRENCY_ID_RATES_MAP);
    pg_shmem_init!(CURRENCY_ID_PAGE_MAP);

    cache_init();
}

// Cache management internals

fn cache_init() {
    debug1!("cache_init()");
    ensure_cache_populated();
}

fn ensure_cache_populated() {
    debug1!("ensure_cache_populated()");
    validate_compatible_db();
    let control = CURRENCY_CONTROL.share().clone();
    if control.cache_filled {
        debug1!("Cache already filled. Skipping loading from DB.");
        return;
    }
    // Currency Min and Max ID
    let currency_min_max: SpiResult<(Option<i32>, Option<i32>)> = Spi::get_two(DEFAULT_GET_CURRENCIES_IDS);
    let (min_id, max_id): (i32, i32) = match currency_min_max {
        Ok(values) => {
            (values.0.unwrap(), values.1.unwrap())
        }
        Err(spi_error) => {
            error!("Cannot execute min/max ID values query or there is no currencies in the table. {}", spi_error)
        }
    };
    if min_id > max_id {
        error!("Min currency ID cannot be greater that max currency ID. Cannot init cache.")
    }
    let currency_count = max_id - min_id + 1;
    debug1!("Min ID: {}, Max ID: {}, Currencies: {}", min_id, max_id, currency_count);
    // Load Currencies (id, xuid and entry count)
    Spi::connect(
        |client| {
            let select = client.select(DEFAULT_GET_CURRENCIES_ENTRY_COUNT, None, None);
            match select {
                Ok(tuple_table) => {
                    for row in tuple_table {
                        let id: i8 = row[1].value().unwrap().unwrap();
                        let xuid: &str = row[2].value().unwrap().unwrap();
                        let entry_count: i32 = row[3].value().unwrap().unwrap();

                        let currency_metadata = Currency {
                            id,
                            xuid,
                            rates_size: entry_count,
                            ..Currency::default()
                        };

                        CURRENCY_ID_METADATA_MAP.exclusive().insert(id, currency_metadata).unwrap();

                        let currency_control = CURRENCY_CONTROL.share().clone();
                        *CURRENCY_CONTROL.exclusive() = CurrencyControl {
                            entry_count: currency_control.entry_count + entry_count,
                            ..currency_control
                        };

                        CURRENCY_XUID_MAP.exclusive().insert(xuid, id).unwrap();

                        debug1!("Currency initialized. ID: {}, xuid: {}, entries: {}", id, xuid, entry_count)
                    }
                }
                Err(spi_error) => {
                    error!("Cannot load currencies. {}", spi_error)
                }
            }
        }
    )
    // TODO: Get Currency Entries
}

// fn cache_insert(id: i8, xuid: &'static str, value: f32) {
//     // CURRENCY_ID_RATE_MAP.exclusive().insert(id, value).unwrap();
//     CURRENCY_XUID_MAP.exclusive().insert(xuid, id).unwrap();
// }
//
// fn get_by_id(id: i32) -> Option<f32> {
//     // CURRENCY_ID_RATE_MAP.share().get(&id).cloned()
//     Some(10.1f32)
// }
//
// fn get_by_xuid(xuid: &'static str) -> Option<f32> {
//     let _id = CURRENCY_XUID_MAP.share().get(xuid).cloned().unwrap();
//     // CURRENCY_ID_RATE_MAP.share().get(&id).cloned()
//     Some(10.1f32)
// }

fn validate_compatible_db() {
    debug1!("Validating database compatibility...");
    let spi_result: SpiResult<Option<bool>> = Spi::get_one(DEFAULT_VALIDATION_QUERY);
    match spi_result {
        Ok(found_tables_opt) => {
            match found_tables_opt {
                None => {
                    debug1!("Valid database.")
                }
                Some(valid) => {
                    if !valid {
                        error!("The current database is not compatible with the KetteQ FX Currency Cache extension.")
                    }
                }
            }
        }
        Err(spi_error) => {
            error!("Cannot validate current database {}", spi_error)
        }
    }
}

// Exported Functions

#[pg_extern]
fn kq_fx_invalidate_cache() -> &'static str {
    debug1!("Waiting for lock...");
    CURRENCY_XUID_MAP.exclusive().clear();
    debug1!("CURRENCY_XUID_MAP cleared");
    *CURRENCY_CONTROL.exclusive() = CurrencyControl::default();
    debug1!("CURRENCY_CONTROL reset");
    // Reload Cache
    cache_init();
    debug1!("Cache invalidated");
    "Cache invalidated."
}

#[pg_extern]
fn kq_fx_get_rate(
    _currency_id: i32,
    _to_currency_id: i32,
    _date: pgrx::Date,
) -> Option<f32> {
    // Mock result
    Some(10.0f32)
}

#[pg_extern]
fn kq_fx_get_rate_xuid(
    _currency_xuid: &'static str,
    _to_currency_xuid: &'static str,
    _date: pgrx::Date,
) -> Option<f32> {
    // Mock result
    Some(11.0f32)
}

// #[cfg(any(test, feature = "pg_test"))]
// #[pg_schema]
// mod tests {
//     use pgrx::prelude::*;
//
//     #[pg_test]
//     fn test_hello_kq_fx_currency() {
//         assert_eq!("Hello, kq_fx_currency", crate::hello_kq_fx_currency());
//     }
//
// }

// This module is required by `cargo pgrx test` invocations.
// It must be visible at the root of your extension crate.
// #[cfg(test)]
// pub mod pg_test {
//     pub fn setup(_options: Vec<&str>) {
//         // perform one-off initialization when the pg_test framework starts
//     }
//
//     pub fn postgresql_conf_options() -> Vec<&'static str> {
//         // return any postgresql.conf settings that are required for your tests
//         vec![]
//     }
// }
