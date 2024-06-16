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

type ExtDate = pgrx::Date;
type StoreDate = i32;
type FromToIdPair = (i64, i64);
type StoreDateRatePair = (StoreDate, f64);

type CurrencyDataMap = PgLwLock<
    heapless::FnvIndexMap<FromToIdPair, heapless::Vec<StoreDateRatePair, MAX_ENTRIES>, MAX_ENTRIES>,
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
    info!("ketteQ FX Currency Cache Extension Loaded (kq_fx)");
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
    info!("Unloaded ketteQ FX Currency Cache Extension (kq_fx)");
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
        "Query to get all the currencies IDs.",
        "",
        &Q2_GET_CURRENCIES_IDS,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.currency.q3_get_currencies_entry_count",
        "Query to get the currencies entry count.",
        "",
        &Q3_GET_CURRENCIES_ENTRY_COUNT,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.currency.q4_get_currency_entries",
        "Query to actually get the currencies and store it in the shared memory cache.",
        "",
        &Q4_GET_CURRENCY_ENTRIES,
        GucContext::Suset,
        GucFlags::empty(),
    );
}

// Cache management internals

fn ensure_cache_populated() {
    debug3!("ensure_cache_populated()");
    let control = CURRENCY_CONTROL.share().clone();
    if control.cache_filled {
        debug2!("Cache already filled. Skipping loading from DB.");
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
    debug2!(
        "Currencies: {}",
        currency_count
    );
    // Load Currencies (id, xuid and entry count)
    let mut currencies_count: i64 = 0;
    let mut total_entry_count: i64 = 0;
    Spi::connect(|client| {
        let mut id_data_map = CURRENCY_ID_METADATA_MAP.exclusive();
        let mut xuid_map = CURRENCY_XUID_MAP.exclusive();
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

                    id_data_map.insert(id, currency_metadata).unwrap();

                    xuid_map.insert(xuid, id).unwrap();

                    total_entry_count += entry_count;
                    currencies_count += 1;

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

    {
        // Update control struct
        let currency_control = CURRENCY_CONTROL.share().clone();
        *CURRENCY_CONTROL.exclusive() = CurrencyControl {
            currency_count: currencies_count,
            entry_count: total_entry_count,
            ..currency_control
        };
    }

    let mut entry_count: i64 = 0;

    Spi::connect(|client| {
        let mut data_map = CURRENCY_DATA_MAP.exclusive();
        let select = client.select(&crate::get_guc_string(&Q4_GET_CURRENCY_ENTRIES), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let from_id: i64 = row[1].value().unwrap().unwrap();
                    let to_id: i64 = row[2].value().unwrap().unwrap();
                    let date: pgrx::Date = row[3].value().unwrap().unwrap();
                    let rate: f64 = row[4].value().unwrap().unwrap();
                    debug2!("From_ID: {from_id}, To_ID: {to_id}, DateADT: {date}, Rate: {rate}");
                    if let Entry::Vacant(v) = data_map.entry((from_id, to_id)) {
                        let mut new_data_vec: heapless::Vec<StoreDateRatePair, MAX_ENTRIES> =
                            heapless::Vec::<StoreDateRatePair, MAX_ENTRIES>::new();
                        new_data_vec
                            .push((date.to_pg_epoch_days(), rate))
                            .expect("cannot insert more elements into date,rate vector");
                        v.insert(new_data_vec).unwrap();
                    } else if let Entry::Occupied(mut o) = data_map.entry((from_id, to_id)) {
                        let data_vec = o.get_mut();
                        data_vec
                            .push((date.to_pg_epoch_days(), rate))
                            .expect("cannot insert more elements into date,rate vector");
                    }
                    entry_count += 1;
                    debug2!(
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

    if total_entry_count == entry_count {
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
    debug2!("Query: {value}");
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
    debug2!("Waiting for lock...");
    CURRENCY_XUID_MAP.exclusive().clear();
    debug2!("CURRENCY_XUID_MAP cleared");
    CURRENCY_DATA_MAP.exclusive().clear();
    debug2!("CURRENCY_DATA_MAP cleared");
    *CURRENCY_CONTROL.exclusive() = CurrencyControl::default();
    debug2!("CURRENCY_CONTROL reset");
    debug1!("Cache invalidated");
    "Cache invalidated."
}

#[pg_extern]
fn kq_fx_display_cache() -> TableIterator<
    'static,
    (
        name!(currency_id, i64),
        name!(to_currency_id, i64),
        name!(date, ExtDate),
        name!(rate, f64),
    ),
> {
    ensure_cache_populated();
    let result_vec: Vec<(_, _, _, _)> = CURRENCY_DATA_MAP
        .share()
        .iter()
        .flat_map(|((from_id, to_id), data_vec)| {
            data_vec.iter().map(move |date_rate| unsafe {
                let date = pgrx::Date::from_pg_epoch_days(date_rate.0);
                (*from_id, *to_id, date, date_rate.1)
            })
        })
        .collect();
    TableIterator::new(result_vec)
}

#[pg_extern(parallel_safe)]
fn kq_fx_get_rate(currency_id: i64, to_currency_id: i64, date: ExtDate) -> Option<f64> {
    ensure_cache_populated();
    let date: i32 = date.to_pg_epoch_days();
    if let Some(dates_rates) = CURRENCY_DATA_MAP
        .share()
        .get(&(currency_id, to_currency_id))
    {
        let &(first_date, first_rate) = dates_rates.first().unwrap();
        if date < first_date {
            return None;
        } else if date == first_date {
            return Some(first_rate);
        }
        let &(last_date, last_rate) = dates_rates.last().unwrap();
        if date >= last_date {
            return Some(last_rate);
        }
        let result = dates_rates.binary_search_by(|&(cache_date, _)| cache_date.cmp(&date));
        match result {
            Ok(index) => {
                // debug1!("Found rate exactly with date: {}", date);
                Some(dates_rates[index].1)
            }
            Err(index) => {
                if index > 0 {
                    // debug1!("Found previous rate with date: {}", date);
                    Some(dates_rates[index - 1].1)
                } else {
                    None
                }
            }
        }
    } else {
        // debug1!("There are no rates for this currency pair: from_id: {}, to_id: {}.", currency_id, to_currency_id);
        None
    }
}

#[pg_extern(parallel_safe)]
fn kq_fx_get_rate_xuid(
    currency_xuid: &'static str,
    to_currency_xuid: &'static str,
    date: ExtDate,
) -> Option<f64> {
    ensure_cache_populated();
    let xuid_map = CURRENCY_XUID_MAP.share();
    let currency_xuid = currency_xuid.to_lowercase().as_str();
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

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    extension_sql_file!("../sql/test_data.sql");

    #[pg_test]
    fn test_validate_db() {
        assert_eq!(
            "Database is compatible with the extension.",
            crate::kq_fx_check_db()
        );
    }

    #[pg_test]
    fn test_get_rate_by_id() {
        assert_eq!(
            Some(0.6583555372901019f64),
            crate::kq_fx_get_rate(1, 2, pgrx::Date::new(2010, 2, 1).unwrap())
        );
        assert_eq!(
            Some(1.6285458614035657f64),
            crate::kq_fx_get_rate(
                3590000203070,
                3590000231158,
                pgrx::Date::new(2030, 1, 10).unwrap()
            )
        );
    }

    #[pg_test]
    fn test_get_rate_by_xuid() {
        assert_eq!(
            Some(0.6583555372901019f64),
            crate::kq_fx_get_rate_xuid("usd", "cad", pgrx::Date::new(2010, 2, 1).unwrap())
        );
        assert_eq!(
            Some(1.6285458614035657f64),
            crate::kq_fx_get_rate_xuid("aud", "nzd", pgrx::Date::new(2030, 1, 10).unwrap())
        );
    }

    #[pg_test]
    fn test_try_get_less_than_min_date() {
        assert_eq!(
            None,
            crate::kq_fx_get_rate(2, 1, pgrx::Date::new(1999, 1, 1).unwrap())
        );
    }

    #[pg_test]
    fn test_try_get_greater_than_max_date() {
        assert_eq!(
            Some(1.3539f64),
            crate::kq_fx_get_rate(
                2,
                1,
                pgrx::Date::new(2100, 1, 1).unwrap() // Max Date: 2024-03-01
            )
        );
    }
}

// This module is required by `cargo pgrx test` invocations.
// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'kq_fx'"]
    }
}
