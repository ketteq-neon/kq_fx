mod currency;

use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::spi::SpiResult;
use pgrx::{debug1, error, pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::{c_int, CStr};

// Max allocation params

const MAX_ENTRIES: usize = 8 * 1024;
const MAX_CURRENCIES: usize = 1 * 1024;

// Default Queries

// const DEFAULT_VALIDATION_QUERY: &str = r#"SELECT count(table_name) = 2
// FROM information_schema.tables
// WHERE table_schema = 'plan' AND (table_name = 'currency' or table_name = 'fx_rate');"#;

const DEFAULT_Q1_VALIDATION_QUERY: &CStr = cr#"SELECT count(table_name) = 2
FROM information_schema.tables
WHERE table_schema = 'plan' AND (table_name = 'currency' or table_name = 'fx_rate');"#;

const DEFAULT_Q2_GET_CURRENCIES_IDS_QUERY: &CStr =
    cr#"SELECT min(c.id), max(c.id) FROM plan.currency c"#;

const DEFAULT_Q3_GET_CURRENCIES_ENTRY_COUNT: &CStr = cr#"SELECT cu.currency_id,
(SELECT LOWER(cu.xuid) FROM plan.currency cu WHERE cu.id = cr.currency_id) xuid,
count(*)
FROM plan.fx_rate cr
GROUP by cr.currency_id
ORDER by cr.currency_id asc;"#;

const DEFAULT_Q4_GET_CURRENCY_ENTRIES: &CStr =
    cr#"SELECT cr.currency_id, cr.to_currency_id, cr.rate cr.\"date\"
from plan.fx_rate cr
order by cr.currency_id asc, cr.\"date\" asc;"#;

// Query GUCs

static Q1_VALIDATION_QUERY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(DEFAULT_Q1_VALIDATION_QUERY));

static Q2_GET_CURRENCIES_IDS: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(DEFAULT_Q2_GET_CURRENCIES_IDS_QUERY));

static Q3_GET_CURRENCIES_ENTRY_COUNT: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(DEFAULT_Q3_GET_CURRENCIES_ENTRY_COUNT));

static Q4_GET_CURRENCY_ENTRIES: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(DEFAULT_Q4_GET_CURRENCY_ENTRIES));

// Activate PostgreSQL Extension
::pgrx::pg_module_magic!();

// Control Struct
#[derive(Copy, Clone)]
pub struct CurrencyControl {
    currency_count: i64,
    entry_count: i64,
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
#[derive(Copy, Clone, Debug)]
pub struct Currency {
    id: i64,
    xuid: &'static str,
    rates_size: i64,
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
static CURRENCY_XUID_MAP: PgLwLock<heapless::FnvIndexMap<&'static str, i64, MAX_CURRENCIES>> =
    PgLwLock::new();
/// [CURRENCY_ID, CURRENCY_METADATA]
static CURRENCY_ID_METADATA_MAP: PgLwLock<heapless::FnvIndexMap<i64, Currency, MAX_CURRENCIES>> =
    PgLwLock::new();
/// [CURRENCY_ID, PAGE_MAP[]]
static CURRENCY_ID_PAGE_MAP: PgLwLock<
    heapless::FnvIndexMap<i64, heapless::Vec<i64, MAX_ENTRIES>, MAX_CURRENCIES>,
> = PgLwLock::new();
/// FROM_CURRENCY_ID => TO_CURRENCY_ID => DATE
static CURRENCY_ID_DATE_MAP: PgLwLock<
    heapless::FnvIndexMap<i64, heapless::FnvIndexMap<i64, pgrx::Date, MAX_CURRENCIES>, MAX_CURRENCIES>,
> = PgLwLock::new();
/// FROM_CURRENCY_ID => TO_CURRENCY_ID => RATE
static CURRENCY_ID_RATES_MAP: PgLwLock<
    heapless::FnvIndexMap<i64, heapless::FnvIndexMap<i64, f64, MAX_CURRENCIES>, MAX_CURRENCIES>,
> = PgLwLock::new();

// Get Var from FFI
// extern "C" {
//     static process_shared_preload_libraries_in_progress: *const c_int;
// }
// fn check_process_shared_preload_libraries_in_progress() -> bool {
//     unsafe {
//         if process_shared_preload_libraries_in_progress.is_null() {
//             pgrx::warning!("process_shared_preload_libraries_in_progress is null");
//             false
//         } else {
//             *process_shared_preload_libraries_in_progress != 0
//         }
//     }
// }
// Init Extension - Shared Memory
#[pg_guard]
pub extern "C" fn _PG_init() {
    // if !check_process_shared_preload_libraries_in_progress() {
    //     error!("ketteQ FX Currency Cache Extension needs to be loaded in the preload_shared_libraries section of postgres.conf")
    // }
    pg_shmem_init!(CURRENCY_CONTROL);
    pg_shmem_init!(CURRENCY_XUID_MAP);
    pg_shmem_init!(CURRENCY_ID_METADATA_MAP);
    pg_shmem_init!(CURRENCY_ID_DATE_MAP);
    // pg_shmem_init!(CURRENCY_ID_RATES_MAP);
    pg_shmem_init!(CURRENCY_ID_PAGE_MAP);
    unsafe {
        init_gucs();
    }
    info!("ketteQ FX Currency Cache Extension Loaded (kq_fx_currency)");
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
    info!("Unloaded ketteQ FX Currency Cache Extension (kq_fx_currency)");
}

unsafe fn init_gucs() {
    GucRegistry::define_string_guc(
        "kq.currency.q1_validation",
        "Query to validate the current schema in order to create the ketteQ FX Currency Cache Extension.",
        "",
        &Q1_VALIDATION_QUERY,
        GucContext::Suset,
        GucFlags::empty()
    );
    GucRegistry::define_string_guc(
        "kq.currency.q2_get_currencies_ids",
        "",
        "",
        &Q2_GET_CURRENCIES_IDS,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.currency.q3_get_currencies_entry_count",
        "",
        "",
        &Q3_GET_CURRENCIES_ENTRY_COUNT,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.currency.q4_get_currency_entries",
        "",
        "",
        &Q4_GET_CURRENCY_ENTRIES,
        GucContext::Suset,
        GucFlags::empty(),
    );
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
    let currency_min_max: SpiResult<(Option<i64>, Option<i64>)> =
        Spi::get_two(&get_guc_string(&Q2_GET_CURRENCIES_IDS));
    let (min_id, max_id): (i64, i64) = match currency_min_max {
        Ok(values) => {
            let min_val = match values.0 {
                None => {
                    error!("Cannot get currency min value or currency table is empty")
                }
                Some(min_val) => min_val
            };
            let max_val = match values.1 {
                None => {
                    error!("Cannot get currency min value or currency table is empty")
                }
                Some(max_val) => max_val
            };
            (min_val, max_val)
        },
        Err(spi_error) => {
            error!(
                "Cannot execute min/max ID values query or there is no currencies in the table. {}",
                spi_error
            )
        }
    };
    if min_id > max_id {
        error!("Min currency ID cannot be greater that max currency ID. Cannot init cache.")
    }
    let currency_count = max_id - min_id + 1;
    debug1!(
        "Min ID: {}, Max ID: {}, Currencies: {}",
        min_id,
        max_id,
        currency_count
    );
    // Load Currencies (id, xuid and entry count)
    Spi::connect(|client| {
        let select = client.select(&get_guc_string(&Q3_GET_CURRENCIES_ENTRY_COUNT), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let id: i64 = row[1].value().unwrap().unwrap();
                    let xuid: &str = row[2].value().unwrap().unwrap();
                    let entry_count: i64 = row[3].value().unwrap().unwrap();

                    let currency_metadata = Currency {
                        id,
                        xuid,
                        rates_size: entry_count,
                        ..Currency::default()
                    };

                    CURRENCY_ID_METADATA_MAP
                        .exclusive()
                        .insert(id, currency_metadata)
                        .unwrap();

                    let currency_control = CURRENCY_CONTROL.share().clone();
                    *CURRENCY_CONTROL.exclusive() = CurrencyControl {
                        entry_count: currency_control.entry_count + entry_count,
                        ..currency_control
                    };

                    CURRENCY_XUID_MAP.exclusive().insert(xuid, id).unwrap();

                    debug1!(
                        "Currency initialized. ID: {}, xuid: {}, entries: {}",
                        id,
                        xuid,
                        entry_count
                    )
                }
            }
            Err(spi_error) => {
                error!("Cannot load currencies. {}", spi_error)
            }
        }
    })
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

fn get_guc_string(guc: &GucSetting<Option<&'static CStr>>) -> String {
    String::from_utf8_lossy(guc.get().expect("Cannot get GUC value.").to_bytes())
        .to_string()
        .replace('\n', " ")
}

/// This method prevents using the extension in incompatible databases.
fn validate_compatible_db() -> &'static str {
    let query = get_guc_string(&Q1_VALIDATION_QUERY);
    debug1!("Validating database compatibility... Query: {query}");
    let spi_result: SpiResult<Option<bool>> = Spi::get_one(&query);
    match spi_result {
        Ok(found_tables_opt) => match found_tables_opt {
            None => {
                error!("The current database is not compatible with the KetteQ FX Currency Cache extension.")
            }
            Some(valid) => {
                if !valid {
                    error!("The current database is not compatible with the KetteQ FX Currency Cache extension.")
                } else {
                    "Database is compatible with the extension."
                }
            }
        },
        Err(spi_error) => {
            error!("Cannot validate current database. {}", spi_error)
        }
    }
}

// Exported Functions

#[pg_extern]
fn kq_fx_check_db() -> &'static str {
    validate_compatible_db()
}

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
fn kq_fx_get_rate(_currency_id: i64, _to_currency_id: i64, _date: pgrx::Date) -> Option<f64> {
    // Mock result
    Some(10.0f64)
}

#[pg_extern]
fn kq_fx_get_rate_xuid(
    _currency_xuid: &'static str,
    _to_currency_xuid: &'static str,
    _date: pgrx::Date,
) -> Option<f64> {
    // Mock result
    Some(11.0f64)
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
