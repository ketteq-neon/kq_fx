use heapless::Entry;
use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::spi::SpiResult;
use pgrx::{debug1, error, pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

// Max allocation params

const MAX_ENTRIES: usize = 512;
const MAX_CURRENCIES: usize = 256;
const CURRENCY_XUID_MAX_LEN: usize = 128;

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
    cr#"WITH filtered_fx_rate AS (
    SELECT cr.currency_id, cr.to_currency_id, cr."date", cr.rate
    FROM plan.fx_rate cr
    JOIN plan.data_date dd ON cr."date" < dd."date"
),
ranked_rates AS (
    SELECT
        currency_id,
        to_currency_id,
        "date",
        rate,
        ROW_NUMBER() OVER (PARTITION BY currency_id, to_currency_id ORDER BY "date" DESC) AS rn
    FROM filtered_fx_rate
)
SELECT
    currency_id,
    to_currency_id,
    "date",
    rate
FROM ranked_rates
WHERE rn <= 128
ORDER BY currency_id DESC, to_currency_id DESC, "date" DESC;"#;

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
#[derive(Clone, Default)]
pub struct CurrencyControl {
    cache_filled: bool,
    // entry_count: i64,
}
unsafe impl PGRXSharedMemory for CurrencyControl {}

type PgDate = pgrx::Date;
type StoreDate = i32;
type FromToIdPair = (i64, i64);
type StoreDateRatePair = (StoreDate, f64);

type CurrencyXuid = heapless::String<CURRENCY_XUID_MAX_LEN>;

type CurrencyDataMap = PgLwLock<
    heapless::FnvIndexMap<FromToIdPair, heapless::Vec<StoreDateRatePair, MAX_ENTRIES>, MAX_ENTRIES>,
>;

// Shared Memory Hashmaps

static CURRENCY_CONTROL: PgLwLock<CurrencyControl> = PgLwLock::new();
/// CURRENCY_ID => CURRENCY_XUID
static CURRENCY_XUID_MAP: PgLwLock<heapless::FnvIndexMap<CurrencyXuid, i64, MAX_CURRENCIES>> =
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
    debug2!("Currencies: {}", currency_count);
    // Load Currencies (id, xuid and entry count)
    let mut currencies_count: i64 = 0;
    Spi::connect(|client| {
        let mut xuid_map = CURRENCY_XUID_MAP.exclusive();
        let select = client.select(&get_guc_string(&Q3_GET_CURRENCIES_ENTRY_COUNT), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let id = row[1].value::<i64>()
                        .unwrap_or_else(|err| {
                            error!("server interface error - {err}")
                        })
                        .unwrap_or_else(|| {
                            error!("cannot get currency_id")
                        });

                    let xuid  = row[2].value::<String>()
                        .unwrap_or_else(|err| {
                            error!("server interface error - {err}")
                        })
                        .unwrap_or_else(|| {
                            error!("cannot get currency_xuid")
                        });

                    let xuid_str = CurrencyXuid::from(xuid.as_str());

                    xuid_map.insert(xuid_str, id).unwrap();

                    currencies_count += 1;

                    debug1!(
                        "Currency initialized. ID: {}, xuid: {}",
                        id,
                        xuid
                    )
                }
            }
            Err(spi_error) => {
                error!("Cannot load currencies. {}", spi_error)
            }
        }
    });

    // {
    //     // Update control struct
    //     let currency_control = CURRENCY_CONTROL.share().clone();
    //     *CURRENCY_CONTROL.exclusive() = CurrencyControl {
    //         entry_count: total_entry_count,
    //         ..currency_control
    //     };
    // }

    let mut entry_count: i64 = 0;
    Spi::connect(|client| {
        let mut data_map = CURRENCY_DATA_MAP.exclusive();
        let select = client.select(&crate::get_guc_string(&Q4_GET_CURRENCY_ENTRIES), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let from_id = row[1].value::<i64>()
                        .unwrap_or_else(|err| {
                            error!("server interface error - {err}")
                        })
                        .unwrap_or_else(|| {
                            error!("cannot get from_id")
                        });

                    let to_id = row[2].value::<i64>()
                        .unwrap_or_else(|err| {
                            error!("server interface error - {err}")
                        })
                        .unwrap_or_else(|| {
                            error!("cannot get to_id")
                        });

                    let date = row[3].value::<PgDate>()
                        .unwrap_or_else(|err| {
                            error!("server interface error - {err}")
                        })
                        .unwrap_or_else(|| {
                            error!("cannot get date")
                        });

                    let rate: f64 = row[4].value::<f64>()
                        .unwrap_or_else(|err| {
                            error!("server interface error - {err}")
                        })
                        .unwrap_or_else(|| {
                            error!("cannot get rate")
                        });

                    if let Entry::Vacant(v) = data_map.entry((from_id, to_id)) {
                        let new_data_vec: heapless::Vec<StoreDateRatePair, MAX_ENTRIES> =
                            heapless::Vec::<StoreDateRatePair, MAX_ENTRIES>::new();
                        v.insert(new_data_vec).unwrap();
                        debug2!("entries vector From_ID: {from_id}, To_ID: {to_id} created");
                    }

                    if let Entry::Occupied(mut o) = data_map.entry((from_id, to_id)) {
                        let data_vec = o.get_mut();
                        data_vec
                            .push((date.to_pg_epoch_days(), rate))
                            .expect("cannot insert more elements into (date, rate) vector");
                    } else {
                        error!("entries vector for From_ID: {from_id}, To_ID: {to_id} cannot be obtained")
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

    {
        // Binary search will not work with DESC ordered items
        for (_, data_vec) in CURRENCY_DATA_MAP.exclusive().iter_mut() {
            data_vec.sort_by_key(|d| d.0)
        }
    }

    *CURRENCY_CONTROL.exclusive() = CurrencyControl {
        cache_filled: true,
        // entry_count
    };

    info!("Cache ready, entries: {entry_count}.");
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

// Exported Functions

#[pg_extern]
fn kq_fx_check_db() -> &'static str {
    validate_compatible_db()
}

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
        name!(date, PgDate),
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
fn kq_fx_get_rate(currency_id: i64, to_currency_id: i64, date: PgDate) -> Option<f64> {
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
            return Some(first_rate)
        }

        let &(last_date, last_rate) = dates_rates.last().unwrap();
        if date >= last_date {
            return Some(last_rate);
        }

        let result = dates_rates.binary_search_by(|&(cache_date, _)| cache_date.cmp(&date));
        match result {
            Ok(index) => {
                let rate = dates_rates[index].1;
                Some(rate)
            }
            Err(index) => {
                if index > 0 {
                    let index = index - 1;
                    let rate = dates_rates[index].1;
                    Some(rate)
                } else {
                    None
                }
            }
        }
    } else {
        None
    }
}

#[pg_extern(parallel_safe)]
fn kq_fx_get_rate_xuid(
    currency_xuid: &'static str,
    to_currency_xuid: &'static str,
    date: PgDate,
) -> Option<f64> {
    let currency_xuid = CurrencyXuid::from(currency_xuid.to_lowercase().as_str());
    let to_currency_xuid = CurrencyXuid::from(to_currency_xuid.to_lowercase().as_str());
    ensure_cache_populated();
    let xuid_map = CURRENCY_XUID_MAP.share();
    let from_id = match xuid_map.get(&currency_xuid) {
        None => {
            error!("From currency xuid not found. {currency_xuid}")
        }
        Some(currency_id) => currency_id,
    };
    let to_id = match xuid_map.get(&to_currency_xuid) {
        None => {
            error!("Target currency xuid not found. {to_currency_xuid}")
        }
        Some(currency_id) => currency_id,
    };
    kq_fx_get_rate(*from_id, *to_id, date)
}


#[pg_extern(parallel_safe)]
fn kq_get_arr_value(dates: Vec<PgDate>, values: Vec<f64>, date: PgDate, default_value: Option<f64>) -> Option<f64> {
    if dates.is_empty() || values.is_empty() {
        return default_value;
    }

    if dates.len() != values.len() {
        error!("dates and values arrays does not have the same quantity of elements")
    }

    let dates: Vec<i32> = dates.iter().map(|date| date.to_pg_epoch_days()).collect();
    let date = date.to_pg_epoch_days();

    let pos = match dates.binary_search(&date) {
        Ok(idx) => idx, // exact match
        Err(idx) => {
            if idx == 0 { // date precedes first element
                return default_value;
            } else { // <= value
                idx - 1
            }
        }
    };

    values.get(pos).copied().or(default_value)
}

#[pg_extern(parallel_safe)]
fn kq_get_arr_value2(dates: Vec<PgDate>, values: Vec<f64>, date: PgDate, default_value: Option<f64>) -> Option<f64> {
    if dates.is_empty() || values.is_empty() {
        return default_value;
    }

    if dates.len() != values.len() {
        error!("dates and values arrays does not have the same quantity of elements")
    }

    let pos = match dates.binary_search(&date) {
        Ok(idx) => idx, // exact match
        Err(idx) => {
            if idx == 0 { // date precedes first element
                return default_value;
            } else { // <= value
                idx - 1
            }
        }
    };

    values.get(pos).copied().or(default_value)
}

#[pg_extern(parallel_safe)]
fn kq_get_arr_value3(dates: Vec<PgDate>, values: Vec<f64>, date: PgDate, default_value: Option<f64>) -> Option<f64> {
    if dates.is_empty() || values.is_empty() {
        return default_value;
    }

    if dates.len() != values.len() {
        error!("dates and values arrays does not have the same quantity of elements")
    }

    if dates.first().unwrap() > &date {
        return default_value
    }

    for idx in (0..dates.len()).rev() {
        if dates[idx] <= date {
            return values.get(idx).copied();
        }
    }

    default_value
}

#[pg_extern(parallel_safe)]
fn kq_get_arr_value4(dates: Vec<PgDate>, values: Vec<f64>, date: PgDate, default_value: Option<f64>) -> Option<f64> {
    if dates.is_empty() || values.is_empty() {
        return default_value;
    }

    if dates.len() != values.len() {
        error!("dates and values arrays does not have the same quantity of elements")
    }

    // if dates.first().unwrap() > &date {
    //     return default_value
    // }

    let dates: Vec<i32> = dates.iter().map(|date| date.to_pg_epoch_days()).collect();
    let date = date.to_pg_epoch_days();

    for idx in (0..dates.len()).rev() {
        if dates[idx] <= date {
            return values.get(idx).copied();
        }
    }

    default_value
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
            Some(0.6782540169620296f64),
            crate::kq_fx_get_rate(1, 2, pgrx::Date::new(2015, 5, 1).unwrap())
        );
        assert_eq!(
            Some(1.4450710028764924f64),
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
            Some(0.7335380076416401f64),
            // 1 -> 2
            crate::kq_fx_get_rate_xuid("usd", "cad", pgrx::Date::new(2014, 2, 1).unwrap())
        );
        assert_eq!(
            Some(1.4450710028764924f64),
            // 3590000203070 -> 3590000231158
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

    fn create_date(year: i32, month: u8, day: u8) -> Date {
        Date::new(year, month, day).expect("Failed to create date")
    }

    #[pg_test]
    fn test_kq_get_arr_value() {
        let dates = vec![
            create_date(2000, 1, 1),
            create_date(2000, 1, 2),
            create_date(2000, 1, 3),
            create_date(2000, 1, 5),
            create_date(2000, 1, 8),
        ];
        let values = vec![10.0, 20.0, 30.0, 50.0, 80.0];
        let default_value = Some(0.0);
        // intermediate date (within range but not an exact match)
        assert_eq!(
            crate::kq_get_arr_value(dates.clone(), values.clone(), create_date(2000, 1, 4), default_value),
            Some(30.0)
        );

        // exact date match
        assert_eq!(
            crate::kq_get_arr_value(dates.clone(), values.clone(), create_date(2000, 1, 8), default_value),
            Some(80.0)
        );

        // date before any value in dates
        assert_eq!(
            crate::kq_get_arr_value(dates.clone(), values.clone(), create_date(1999, 12, 31), default_value),
            default_value
        );

        // date after the last value in dates
        assert_eq!(
            crate::kq_get_arr_value(dates.clone(), values.clone(), create_date(2000, 1, 9), default_value),
            Some(80.0)
        );

        // dates and values empty
        assert_eq!(
            crate::kq_get_arr_value(vec![], vec![], create_date(2000, 1, 9), default_value),
            default_value
        );

        // exact date match to the first value in dates
        assert_eq!(
            crate::kq_get_arr_value(dates.clone(), values.clone(), create_date(2000, 1, 1), default_value),
            Some(10.0)
        );
    }

    #[pg_test]
    fn test_kq_get_arr_value2() {
        let dates = vec![
            create_date(2000, 1, 1),
            create_date(2000, 1, 2),
            create_date(2000, 1, 3),
            create_date(2000, 1, 5),
            create_date(2000, 1, 8),
        ];
        let values = vec![10.0, 20.0, 30.0, 50.0, 80.0];
        let default_value = Some(0.0);
        // intermediate date (within range but not an exact match)
        assert_eq!(
            crate::kq_get_arr_value2(dates.clone(), values.clone(), create_date(2000, 1, 4), default_value),
            Some(30.0)
        );

        // exact date match
        assert_eq!(
            crate::kq_get_arr_value2(dates.clone(), values.clone(), create_date(2000, 1, 8), default_value),
            Some(80.0)
        );

        // date before any value in dates
        assert_eq!(
            crate::kq_get_arr_value2(dates.clone(), values.clone(), create_date(1999, 12, 31), default_value),
            default_value
        );

        // date after the last value in dates
        assert_eq!(
            crate::kq_get_arr_value2(dates.clone(), values.clone(), create_date(2000, 1, 9), default_value),
            Some(80.0)
        );

        // dates and values empty
        assert_eq!(
            crate::kq_get_arr_value2(vec![], vec![], create_date(2000, 1, 9), default_value),
            default_value
        );

        // exact date match to the first value in dates
        assert_eq!(
            crate::kq_get_arr_value2(dates.clone(), values.clone(), create_date(2000, 1, 1), default_value),
            Some(10.0)
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
        vec![
            "shared_preload_libraries = 'kq_fx'",
            "log_min_messages = debug2",
            "log_min_error_statement = debug2",
            "client_min_messages = debug2",
        ]
    }
}
