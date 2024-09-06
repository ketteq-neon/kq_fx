use heapless::Entry;
use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::spi::SpiResult;
use pgrx::{error, pg_shmem_init, GucContext, GucFlags, GucRegistry, GucSetting};
use serde::Deserialize;
use serde::Serialize;
use std::ffi::CStr;
use std::num::NonZeroUsize;
use std::time::Duration;

// Capacity params
// IMPORTANT: All capacity values MUST be a power of 2. E.g. 2^8 = 256, 2^9 = 512, 2^10 = 1024

const MAX_ENTRIES: usize = 512;
const MAX_CURRENCIES: usize = 64;
const MAX_ID_PAIRS: usize = 1024;
const CURRENCY_XUID_MAX_LEN: usize = 16;

// Default Queries

const DEFAULT_Q1_VALIDATION_QUERY: &CStr = cr#"
    SELECT
        count(table_name) = 2
    FROM
        information_schema.tables
    WHERE
        table_schema = 'plan' AND (table_name = 'currency' or table_name = 'fx_rate')
;"#;

const DEFAULT_Q2_GET_CURRENCIES_XUID_INIT: &CStr = cr#"
    SELECT
        cu.id, cu.xuid
    FROM
        plan.currency cu
    ORDER BY
        cu.id ASC
;"#;

const DEFAULT_Q3_GET_CURRENCY_ENTRIES: &CStr = cr#"
    WITH
        fx_rate AS (
            SELECT
                cr.currency_id,
                cr.to_currency_id,
                cr."date",
                cr.rate,
                ROW_NUMBER() OVER (PARTITION BY currency_id, to_currency_id ORDER BY "date" DESC) AS rn
            FROM
                plan.fx_rate cr
            ORDER BY
                1, 2, 3 DESC
        )
    SELECT
        currency_id,
        to_currency_id,
        date,
        rate
    FROM
        fx_rate
    WHERE
        rn <= 512
    ORDER BY
        1, 2, 3
;"#;

// Query GUCs

static Q1_VALIDATION_QUERY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(DEFAULT_Q1_VALIDATION_QUERY));

static Q2_GET_CURRENCIES_XUID_INIT: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(DEFAULT_Q2_GET_CURRENCIES_XUID_INIT));

static Q3_GET_CURRENCY_ENTRIES: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(DEFAULT_Q3_GET_CURRENCY_ENTRIES));

// Activate PostgreSQL Extension
::pgrx::pg_module_magic!();

extension_sql!(
    "\
CREATE TYPE kq_date_value AS (
    \"date\" date,
    value float8
);",
    name = "create_composites",
    bootstrap
);

const KQ_DATE_VALUE_COMPOSITE_TYPE: &str = "kq_date_value";

// Control Struct
#[derive(Clone, Default)]
pub struct CurrencyControl {
    cache_filled: bool,
    cache_being_filled: bool,
}

unsafe impl PGRXSharedMemory for CurrencyControl {}

// Types

type PgDate = pgrx::Date;
type StoreDate = i32;
type FromToIdPair = (i64, i64);
type StoreDateRatePair = (StoreDate, f64);
type CurrencyXuid = heapless::String<CURRENCY_XUID_MAX_LEN>;
type CurrencyDataMap = heapless::FnvIndexMap<
    FromToIdPair,
    heapless::Vec<StoreDateRatePair, MAX_ENTRIES>,
    MAX_ID_PAIRS,
>;
type CurrencyXuidMap = heapless::FnvIndexMap<CurrencyXuid, i64, MAX_CURRENCIES>;

// Shared Memory Structs

static CURRENCY_CONTROL: PgLwLock<CurrencyControl> = PgLwLock::new();
/// CURRENCY_ID => CURRENCY_XUID
static CURRENCY_XUID_MAP: PgLwLock<CurrencyXuidMap> = PgLwLock::new();
/// (FROM_CURRENCY_ID, TO_CURRENCY_ID) => (DATE, RATE)
static CURRENCY_DATA_MAP: PgLwLock<CurrencyDataMap> = PgLwLock::new();

// Init Extension

#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(CURRENCY_CONTROL);
    pg_shmem_init!(CURRENCY_XUID_MAP);
    pg_shmem_init!(CURRENCY_DATA_MAP);
    unsafe {
        init_gucs();
    }
    info!("ketteQ FX Extension (kq_fx) Loaded");
}

#[pg_guard]
pub extern "C" fn _PG_fini() {
    info!("ketteQ FX Extension (kq_fx) Unloaded");
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
        "kq.currency.q2_get_currencies_xuid",
        "Query to get the currencies IDs and XUIDs.",
        "",
        &Q2_GET_CURRENCIES_XUID_INIT,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "kq.currency.q3_get_currency_entries",
        "Query to actually get the currencies and store it in the shared memory cache.",
        "",
        &Q3_GET_CURRENCY_ENTRIES,
        GucContext::Suset,
        GucFlags::empty(),
    );
}

fn is_cache_filled() -> bool {
    if CURRENCY_CONTROL.share().cache_filled {
        return true;
    }

    if CURRENCY_CONTROL.share().cache_being_filled {
        while CURRENCY_CONTROL.share().cache_being_filled {
            std::thread::sleep(Duration::from_millis(1));
        }
        return true;
    }

    false
}

// Cache management internals
fn ensure_cache_populated() {
    if is_cache_filled() {
        return;
    }

    if let Err(msg) = validate_compatible_db() {
        error!("{}", msg);
    }

    let mut xuid_map = CURRENCY_XUID_MAP.exclusive();

    //someone else might have filled it already
    if is_cache_filled() {
        return;
    }

    CURRENCY_CONTROL.exclusive().cache_being_filled = true;

    // Init Currencies (id and xuid) & lock shmem maps
    let mut data_map = CURRENCY_DATA_MAP.exclusive();
    let mut currencies_count: i64 = 0;
    Spi::connect(|client| {
        let select = client.select(&get_guc_string(&Q2_GET_CURRENCIES_XUID_INIT), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let id = row[1]
                        .value::<i64>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get currency_id"));

                    let xuid = row[2]
                        .value::<String>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get currency_xuid"));

                    let xuid_str = CurrencyXuid::from(xuid.as_str());

                    xuid_map.insert(xuid_str, id).unwrap();

                    currencies_count += 1;

                    debug2!("Currency initialized. ID: {}, xuid: {}", id, xuid)
                }
            }
            Err(spi_error) => {
                error!("Cannot load currencies. {}", spi_error)
            }
        }
    });

    let mut entry_count: i64 = 0;
    Spi::connect(|client| {
        let select = client.select(&crate::get_guc_string(&Q3_GET_CURRENCY_ENTRIES), None, None);
        match select {
            Ok(tuple_table) => {
                for row in tuple_table {
                    let from_id = row[1]
                        .value::<i64>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get from_id"));

                    let to_id = row[2]
                        .value::<i64>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get to_id"));

                    let date = row[3]
                        .value::<PgDate>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get date"));

                    let rate: f64 = row[4]
                        .value::<f64>()
                        .unwrap_or_else(|err| error!("server interface error - {err}"))
                        .unwrap_or_else(|| error!("cannot get rate"));

                    let entry = (date.to_pg_epoch_days(), rate);

                    match data_map.entry((from_id, to_id)) {
                        Entry::Vacant(v) => {
                            let mut new_data_vec: heapless::Vec<StoreDateRatePair, MAX_ENTRIES> =
                                heapless::Vec::<StoreDateRatePair, MAX_ENTRIES>::new();
                            new_data_vec.push(entry).unwrap();
                            v.insert(new_data_vec).unwrap();
                            debug2!("entries vector From_ID: {from_id}, To_ID: {to_id} created");
                        }
                        Entry::Occupied(mut o) => {
                            let data_vec = o.get_mut();
                            data_vec
                                .push(entry)
                                .unwrap_or_else(|e| error!("cannot insert more elements into (date, rate) vector, ({},{}, curr: {}, max: {})", e.0, e.1, data_vec.len(), data_vec.capacity()));
                        }
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
        *CURRENCY_CONTROL.exclusive() = CurrencyControl {
            cache_filled: true,
            cache_being_filled: false,
        };
    }

    debug2!("Cache ready, entries: {entry_count}.");
}

fn get_guc_string(guc: &GucSetting<Option<&'static CStr>>) -> String {
    let value = String::from_utf8_lossy(guc.get().expect("Cannot get GUC value.").to_bytes())
        .to_string()
        .replace('\n', " ");
    debug2!("Query: {value}");
    value
}

/// This method prevents using the extension in incompatible databases.
fn validate_compatible_db() -> Result<(), String> {
    let spi_result: SpiResult<Option<bool>> = Spi::get_one(&get_guc_string(&Q1_VALIDATION_QUERY));
    match spi_result {
        Ok(found_tables_opt) => match found_tables_opt {
            None => Err(
                "The current database is not compatible with the ketteQ FX extension.".to_string(),
            ),
            Some(valid) => {
                if !valid {
                    Err(
                        "The current database is not compatible with the ketteQ FX extension."
                            .to_string(),
                    )
                } else {
                    Ok(())
                }
            }
        },
        Err(spi_error) => Err(format!("Cannot validate current database. {}", spi_error)),
    }
}

// Exported Functions

#[pg_extern]
fn kq_fx_check_db() -> String {
    match validate_compatible_db() {
        Ok(_) => "Database is compatible with the extension.".to_string(),
        Err(error_msg) => error_msg,
    }
}

#[pg_extern]
fn kq_fx_invalidate_cache() -> &'static str {
    debug2!("Waiting for lock...");
    let mut xuid_map = CURRENCY_XUID_MAP.exclusive();

    for (_, data_vec) in CURRENCY_DATA_MAP.exclusive().iter_mut() {
        data_vec.clear();
    }

    CURRENCY_DATA_MAP.exclusive().clear();

    *CURRENCY_CONTROL.exclusive() = CurrencyControl {
        cache_being_filled: false,
        ..CurrencyControl::default()
    };

    xuid_map.clear();
    "Cache invalidated."
}

#[pg_extern(parallel_safe)]
fn kq_fx_populate_cache() -> &'static str {
    ensure_cache_populated();
    "Cache populated."
}

#[pg_extern(parallel_safe)]
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
#[allow(clippy::comparison_chain)]
fn kq_fx_get_rate(currency_id: i64, to_currency_id: i64, date: PgDate) -> Option<f64> {
    if currency_id == to_currency_id {
        return Some(1.0);
    }

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
    let currency_xuid = CurrencyXuid::from(currency_xuid);
    let to_currency_xuid = CurrencyXuid::from(to_currency_xuid);

    if currency_xuid.eq(&to_currency_xuid) {
        return Some(1.0);
    }

    ensure_cache_populated();

    let xuid_map = CURRENCY_XUID_MAP.share();
    let from_id = match xuid_map.get(&currency_xuid) {
        None => {
            error!("From currency xuid not found: {currency_xuid}")
        }
        Some(currency_id) => currency_id,
    };
    let to_id = match xuid_map.get(&to_currency_xuid) {
        None => {
            error!("Target currency xuid not found: {to_currency_xuid}")
        }
        Some(currency_id) => currency_id,
    };
    kq_fx_get_rate(*from_id, *to_id, date)
}

#[pg_extern(parallel_safe)]
fn kq_get_value_from_arrays(
    dates: Vec<PgDate>,
    values: Vec<f64>,
    date: PgDate,
    default_value: Option<f64>,
) -> Option<f64> {
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
            if idx == 0 {
                // date precedes first element
                return default_value;
            } else {
                // <= value
                idx - 1
            }
        }
    };

    values.get(pos).copied().or(default_value)
}

#[derive(PostgresType, Serialize, Deserialize, Clone)]
pub struct DateValue {
    date: PgDate,
    value: f64,
}

#[pg_extern(parallel_safe)]
fn kq_get_value_from_custom_type(
    date_values: Vec<DateValue>,
    date: PgDate,
    default_value: Option<f64>,
) -> Option<f64> {
    if date_values.is_empty() {
        return default_value;
    }

    let dates: Vec<i32> = date_values
    .iter()
    .map(|date_value| {
        date_value.date.to_pg_epoch_days()
    })
    .collect();

    let date = date.to_pg_epoch_days();

    let pos = match dates.binary_search(&date) {
        Ok(idx) => idx, // exact match
        Err(idx) => {
            if idx == 0 {
                // date precedes first element
                return default_value;
            } else {
                // <= value
                idx - 1
            }
        }
    };

    if let Some(date_value) = date_values.get(pos) {
        Some(date_value.value)
    } else {
        default_value
    }
}

#[pg_extern(parallel_safe)]
fn kq_get_value_from_pairs(
    pairs: Vec<pgrx::composite_type!(KQ_DATE_VALUE_COMPOSITE_TYPE)>,
    date: PgDate,
    default_value: Option<f64>,
) -> Option<f64> {
    if pairs.is_empty() {
        return default_value;
    }

    let dates: Vec<i32> = pairs
        .iter()
        .map(|pair| unsafe {
            pair.get_by_index::<PgDate>(NonZeroUsize::new_unchecked(1))
                .unwrap()
                .unwrap()
                .to_pg_epoch_days()
        })
        .collect();

    let date = date.to_pg_epoch_days();

    let pos = match dates.binary_search(&date) {
        Ok(idx) => idx, // exact match
        Err(idx) => {
            if idx == 0 {
                // date precedes first element
                return default_value;
            } else {
                // <= value
                idx - 1
            }
        }
    };

    match pairs.get(pos) {
        Some(pair) => unsafe {
            let value = pair
                .get_by_index::<f64>(NonZeroUsize::new_unchecked(2))
                .unwrap()
                .unwrap();
            Some(value)
        },
        None => default_value,
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    use crate::DateValue;

    extension_sql_file!("../sql/test_data.sql");

    #[pg_test]
    fn test_validate_db() {
        crate::kq_fx_invalidate_cache();
        assert_eq!(
            "Database is compatible with the extension.",
            crate::kq_fx_check_db()
        );
    }

    #[pg_test]
    fn test_get_rate_same_id() {
        assert_eq!(
            Some(1.0),
            crate::kq_fx_get_rate(1, 1, pgrx::Date::new(2015, 5, 1).unwrap())
        );
    }

    #[pg_test]
    fn test_get_rate_by_id() {
        assert_eq!(
            Some(1.2092987606763552f64),
            crate::kq_fx_get_rate(2, 1, pgrx::Date::new(2019, 12, 1).unwrap())
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
            Some(0.7335380076416401f64),
            // 1 -> 2
            crate::kq_fx_get_rate_xuid("usd", "cad", pgrx::Date::new(2014, 2, 1).unwrap())
        );
        assert_eq!(
            Some(1.6285458614035657f64),
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
    fn test_kq_get_value_from_arrays() {
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
            crate::kq_get_value_from_arrays(
                dates.clone(),
                values.clone(),
                create_date(2000, 1, 4),
                default_value
            ),
            Some(30.0)
        );

        // exact date match
        assert_eq!(
            crate::kq_get_value_from_arrays(
                dates.clone(),
                values.clone(),
                create_date(2000, 1, 8),
                default_value
            ),
            Some(80.0)
        );

        // date before any value in dates
        assert_eq!(
            crate::kq_get_value_from_arrays(
                dates.clone(),
                values.clone(),
                create_date(1999, 12, 31),
                default_value
            ),
            default_value
        );

        // date after the last value in dates
        assert_eq!(
            crate::kq_get_value_from_arrays(
                dates.clone(),
                values.clone(),
                create_date(2000, 1, 9),
                default_value
            ),
            Some(80.0)
        );

        // dates and values empty
        assert_eq!(
            crate::kq_get_value_from_arrays(vec![], vec![], create_date(2000, 1, 9), default_value),
            default_value
        );

        // exact date match to the first value in dates
        assert_eq!(
            crate::kq_get_value_from_arrays(
                dates.clone(),
                values.clone(),
                create_date(2000, 1, 1),
                default_value
            ),
            Some(10.0)
        );
    }

    #[pg_test]
    fn test_kq_get_value_from_pairs() -> Result<(), pgrx::spi::Error> {
        let date_pairs_sql = r#"
            ARRAY[
                ROW('2000-01-01'::date, 10.0)::kq_date_value, 
                ROW('2000-01-02'::date, 20.0)::kq_date_value,
                ROW('2000-01-03'::date, 30.0)::kq_date_value,
                ROW('2000-01-05'::date, 50.0)::kq_date_value,
                ROW('2000-01-08'::date, 80.0)::kq_date_value
            ]
        "#
        .replace("\n", " ");

        // intermediate date (within range but not an exact match)
        let retval = Spi::get_one::<f64>(
            format!(
                "\
            SELECT * FROM kq_get_value_from_pairs({date_pairs_sql}, '2000-01-04'::date, 0)
        "
            )
            .as_str(),
        )?
        .expect("failed executing SQL");
        assert_eq!(retval, 30.0f64);

        // exact date match
        let retval = Spi::get_one::<f64>(
            format!(
                "\
            SELECT * FROM kq_get_value_from_pairs({date_pairs_sql}, '2000-01-08'::date, 0)
        "
            )
            .as_str(),
        )?
        .expect("failed executing SQL");
        assert_eq!(retval, 80.0f64);

        // date before any value in dates
        let retval = Spi::get_one::<f64>(
            format!(
                "\
            SELECT * FROM kq_get_value_from_pairs({date_pairs_sql}, '1999-12-31'::date, 0)
        "
            )
            .as_str(),
        )?
        .expect("failed executing SQL");
        assert_eq!(retval, 0.0f64);

        // date after the last value in dates
        let retval = Spi::get_one::<f64>(
            format!(
                "\
            SELECT * FROM kq_get_value_from_pairs({date_pairs_sql}, '2000-01-09'::date, 0)
        "
            )
            .as_str(),
        )?
        .expect("failed executing SQL");
        assert_eq!(retval, 80.0f64);

        // dates pairs empty
        let retval = Spi::get_one::<f64>(
            "\
            SELECT * FROM kq_get_value_from_pairs(ARRAY[]::kq_date_value[], '2000-01-09'::date, 0)
        ",
        )?
        .expect("failed executing SQL");
        assert_eq!(retval, 0.0f64);

        // exact date match to the first value in dates
        let retval = Spi::get_one::<f64>(
            format!(
                "\
            SELECT * FROM kq_get_value_from_pairs({date_pairs_sql}, '2000-01-01'::date, 0)
        "
            )
            .as_str(),
        )?
        .expect("failed executing SQL");
        assert_eq!(retval, 10.0f64);

        Ok(())
    }

    #[pg_test]
    fn test_kq_get_value_from_custom_type() {
        let date_values = vec![
            DateValue {
                date: create_date(2000, 1, 1),
                value: 10.0
            },
            DateValue {
                date: create_date(2000, 1, 2),
                value: 20.0
            },
            DateValue {
                date: create_date(2000, 1, 3),
                value: 30.0
            },
            DateValue {
                date: create_date(2000, 1, 5),
                value: 50.0
            },
            DateValue {
                date: create_date(2000, 1, 8),
                value: 80.0
            },
        ];
        let default_value = Some(0.0);

        // intermediate date (within range but not an exact match)
        assert_eq!(
            crate::kq_get_value_from_custom_type(
                date_values.clone(),
                create_date(2000, 1, 4),
                default_value
            ),
            Some(30.0)
        );

        // exact date match
        assert_eq!(
            crate::kq_get_value_from_custom_type(
                date_values.clone(),
                create_date(2000, 1, 8),
                default_value
            ),
            Some(80.0)
        );

        // date before any value in dates
        assert_eq!(
            crate::kq_get_value_from_custom_type(
                date_values.clone(),
                create_date(1999, 12, 31),
                default_value
            ),
            default_value
        );

        // date after the last value in dates
        assert_eq!(
            crate::kq_get_value_from_custom_type(
                date_values.clone(),
                create_date(2000, 1, 9),
                default_value
            ),
            Some(80.0)
        );

        // dates and values empty
        assert_eq!(
            crate::kq_get_value_from_custom_type(vec![], create_date(2000, 1, 9), default_value),
            default_value
        );

        // exact date match to the first value in dates
        assert_eq!(
            crate::kq_get_value_from_custom_type(
                date_values,
                create_date(2000, 1, 1),
                default_value
            ),
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
