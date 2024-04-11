use heapless::Entry;
use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::spi::SpiResult;
use pgrx::{debug1, error, pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::CStr;

// Max allocation params

const MAX_ENTRIES: usize = 8 * 1024;
const MAX_CURRENCIES: usize = 1024;

// Default Queries

const DEFAULT_Q1_VALIDATION_QUERY: &CStr = cr#"SELECT count(table_name) = 2
FROM information_schema.tables
WHERE table_schema = 'plan' AND (table_name = 'currency' or table_name = 'fx_rate');"#;

const DEFAULT_Q2_GET_CURRENCIES_IDS_QUERY: &CStr =
    cr#"SELECT min(c.id), max(c.id) FROM plan.currency c"#;

const DEFAULT_Q3_GET_CURRENCIES_ENTRY_COUNT: &CStr = cr#"SELECT cu.id,
LOWER(cu.xuid),
(SELECT count(*) rate_count FROM plan.fx_rate fr WHERE fr.currency_id = cu.id)
FROM plan.currency cu
GROUP by cu.id
ORDER by cu.id asc;"#;

const DEFAULT_Q4_GET_CURRENCY_ENTRIES: &CStr =
    cr#"SELECT cr.currency_id, cr.to_currency_id, cr."date", cr.rate
from plan.fx_rate cr
order by cr.currency_id asc, cr.to_currency_id asc, cr."date" asc;"#;

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
#[derive(Copy, Clone, Default)]
pub struct CurrencyControl {
    cache_filled: bool,
    currency_count: i64,
    entry_count: i64,
}
unsafe impl PGRXSharedMemory for CurrencyControl {}

// Currency Metadata Struct
#[derive(Default, Copy, Clone, Debug)]
pub struct Currency {
    entry_count: i64,
    id: i64,
    xuid: &'static str,
}
// impl Default for Currency {
//     fn default() -> Self {
//         Currency {
//             entry_count: 0,
//             id: 0,
//             xuid: "",
//         }
//     }
// }

type CurrencyDataMap = PgLwLock<
    heapless::FnvIndexMap<(i64, i64), heapless::Vec<(pgrx::Date, f64), MAX_ENTRIES>, MAX_ENTRIES>,
>;

unsafe impl PGRXSharedMemory for Currency {}

// Shared Memory Hashmaps

static CURRENCY_CONTROL: PgLwLock<CurrencyControl> = PgLwLock::new();
/// CURRENCY_ID => CURRENCY_XUID
static CURRENCY_XUID_MAP: PgLwLock<heapless::FnvIndexMap<&'static str, i64, MAX_CURRENCIES>> =
    PgLwLock::new();
/// CURRENCY_ID => CURRENCY_METADATA
static CURRENCY_ID_METADATA_MAP: PgLwLock<heapless::FnvIndexMap<i64, Currency, MAX_CURRENCIES>> =
    PgLwLock::new();
/// (FROM_CURRENCY_ID, TO_CURRENCY_ID) => (DATE, RATE)
static CURRENCY_DATA_MAP: CurrencyDataMap = PgLwLock::new();

// Init Extension - Shared Memory
#[pg_guard]
pub extern "C" fn _PG_init() {
    // if !check_process_shared_preload_libraries_in_progress() {
    //     error!("ketteQ FX Currency Cache Extension needs to be loaded in the preload_shared_libraries section of postgres.conf")
    // }
    pg_shmem_init!(CURRENCY_CONTROL);
    pg_shmem_init!(CURRENCY_XUID_MAP);
    pg_shmem_init!(CURRENCY_ID_METADATA_MAP);
    pg_shmem_init!(CURRENCY_DATA_MAP);
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

fn ensure_cache_populated() {
    debug1!("ensure_cache_populated()");
    let control = CURRENCY_CONTROL.share().clone();
    if control.cache_filled {
        debug1!("Cache already filled. Skipping loading from DB.");
        return;
    }
    validate_compatible_db();
    // Currency Min and Max ID
    let currency_min_max: SpiResult<(Option<i64>, Option<i64>)> =
        Spi::get_two(&get_guc_string(&Q2_GET_CURRENCIES_IDS));
    let (min_id, max_id): (i64, i64) = match currency_min_max {
        Ok(values) => {
            let min_val = match values.0 {
                None => {
                    error!("Cannot get currency min value or currency table is empty")
                }
                Some(min_val) => min_val,
            };
            let max_val = match values.1 {
                None => {
                    error!("Cannot get currency min value or currency table is empty")
                }
                Some(max_val) => max_val,
            };
            (min_val, max_val)
        }
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
                        entry_count,
                    };

                    CURRENCY_ID_METADATA_MAP
                        .exclusive()
                        .insert(id, currency_metadata)
                        .unwrap();

                    CURRENCY_XUID_MAP.exclusive().insert(xuid, id).unwrap();

                    let currency_control = CURRENCY_CONTROL.share().clone();
                    *CURRENCY_CONTROL.exclusive() = CurrencyControl {
                        currency_count: currency_control.currency_count + 1,
                        entry_count: currency_control.entry_count + entry_count,
                        ..currency_control
                    };

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
    });
    let mut entry_count: i64 = 0;
    Spi::connect(|client| {
        let select = client.select(&crate::get_guc_string(&Q4_GET_CURRENCY_ENTRIES), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let from_id: i64 = row[1].value().unwrap().unwrap();
                    let to_id: i64 = row[2].value().unwrap().unwrap();
                    let date: ::pgrx::Date = row[3].value().unwrap().unwrap();
                    let rate: f64 = row[4].value().unwrap().unwrap();
                    debug1!("From_ID: {from_id}, To_ID: {to_id}, DateADT: {date}, Rate: {rate}");
                    let mut data_map = CURRENCY_DATA_MAP.exclusive();
                    if let Entry::Vacant(v) = data_map.entry((from_id, to_id)) {
                        let mut new_data_vec: heapless::Vec<(pgrx::Date, f64), MAX_ENTRIES> =
                            heapless::Vec::<(pgrx::Date, f64), MAX_ENTRIES>::new();
                        new_data_vec
                            .push((date, rate))
                            .expect("cannot insert more elements into date,rate vector");
                        v.insert(new_data_vec).unwrap();
                    } else if let Entry::Occupied(mut o) = data_map.entry((from_id, to_id)) {
                        let data_vec = o.get_mut();
                        data_vec
                            .push((date, rate))
                            .expect("cannot insert more elements into date,rate vector");
                    }
                    entry_count += 1;
                    debug1!(
                        "Inserted into shared cache: ({},{}) => ({}, {})",
                        from_id,
                        to_id,
                        date,
                        rate
                    );
                }
            }
            Err(spi_error) => {
                error!("Cannot load currency rates. {}", spi_error)
            }
        }
    });

    let currency_control = CURRENCY_CONTROL.share().clone();

    if currency_control.entry_count == entry_count {
        *CURRENCY_CONTROL.exclusive() = CurrencyControl {
            cache_filled: true,
            ..currency_control
        };

        info!("Cache ready, entries: {entry_count}.");
    } else {
        error!(
            "Loaded entries does not match entry count. Entry Count: {}, Entries Cached: {}",
            currency_control.entry_count, entry_count
        )
    }
}

fn get_guc_string(guc: &GucSetting<Option<&'static CStr>>) -> String {
    let value = String::from_utf8_lossy(guc.get().expect("Cannot get GUC value.").to_bytes())
        .to_string()
        .replace('\n', " ");
    debug1!("Query: {value}");
    value
}

/// This method prevents using the extension in incompatible databases.
fn validate_compatible_db() -> &'static str {
    let spi_result: SpiResult<Option<bool>> = Spi::get_one(&get_guc_string(&Q1_VALIDATION_QUERY));
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

// Utility

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum DependencyValue {
    String(String),
    Object {
        version: String,
        features: Vec<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct CargoToml {
    dependencies: HashMap<String, DependencyValue>,
}

// Exported Functions

#[pg_extern]
fn kq_fx_check_db() -> &'static str {
    validate_compatible_db()
}

// #[pg_extern]
// fn kq_fx_cache_status(
// ) -> TableIterator<'static, (name!(property, &'static str), name!(value, &'static str))> {
//     let cargo_toml_raw = include_str!("../Cargo.toml");
//     let cargo_toml: CargoToml = ::toml::from_str(cargo_toml_raw).unwrap();
//     let pgrx_version_dep = cargo_toml.dependencies.get("pgrx").unwrap();
//     // let control = CURRENCY_CONTROL.share().clone();
//     TableIterator::new(vec![
//         ("PostgreSQL SDK Version", pg_sys::get_pg_version_string()),
//         ("PGRX Version", format!("{pgrx_version_dep:#?}"))
//     ])
// }

#[pg_extern]
fn kq_fx_invalidate_cache() -> &'static str {
    debug1!("Waiting for lock...");
    CURRENCY_XUID_MAP.exclusive().clear();
    debug1!("CURRENCY_XUID_MAP cleared");
    CURRENCY_DATA_MAP.exclusive().clear();
    debug1!("CURRENCY_DATA_MAP cleared");
    *CURRENCY_CONTROL.exclusive() = CurrencyControl::default();
    debug1!("CURRENCY_CONTROL reset");
    debug1!("Cache invalidated");
    "Cache invalidated."
}

#[pg_extern(parallel_safe)]
fn kq_fx_get_rate(currency_id: i64, to_currency_id: i64, date: pgrx::Date) -> Option<f64> {
    ensure_cache_populated();
    if let Some(dates_rates) = CURRENCY_DATA_MAP
        .share()
        .get(&(currency_id, to_currency_id))
    {
        let mut date_rate: Option<f64> = None;

        for &entry in dates_rates.iter().rev() {
            if entry.0 <= date {
                debug1!("Found rate exact/previous with date: {}", date);
                date_rate = Some(entry.1);
                break;
            }
        }

        if date_rate.is_none() {
            // Enable the "get-next-rate" feature if we want to get the next future rate if
            // no dates before the requested date exists. This can be converted to a
            // GUC or removed if is not necessary.
            if cfg!(feature = "next-rate") {
                for &entry in dates_rates.iter() {
                    if entry.0 > date {
                        debug1!("Found future rate with date: {}", date);
                        return Some(entry.1);
                    }
                }
            }
            error!("No rate found for the date: {}. If rates table was recently updated, a cache reload is necessary, run `SELECT kq_fx_invalidate_cache()`.", date);
        }

        date_rate
    } else {
        error!(
            "There are no rates with this combination: from_id: {}, to_id: {}. If rates table was recently updated, a cache reload is necessary, run `SELECT kq_fx_invalidate_cache()`.",
            currency_id,
            to_currency_id
        );
    }
}

#[pg_extern(parallel_safe)]
fn kq_fx_get_rate_xuid(
    currency_xuid: &'static str,
    to_currency_xuid: &'static str,
    date: pgrx::Date,
) -> Option<f64> {
    ensure_cache_populated();
    let xuid_map = CURRENCY_XUID_MAP.share();
    let from_id = match xuid_map.get(currency_xuid) {
        None => {
            error!("From currency xuid not found. {currency_xuid}")
        }
        Some(currency_id) => currency_id,
    };
    let to_id = match xuid_map.get(to_currency_xuid) {
        None => {
            error!("Target currency xuid not found. {to_currency_xuid}")
        }
        Some(currency_id) => currency_id,
    };
    kq_fx_get_rate(*from_id, *to_id, date)
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
