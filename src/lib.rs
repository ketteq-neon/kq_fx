use pgrx::prelude::*;
use pgrx::shmem::*;
use pgrx::pg_shmem_init;
use pgrx::lwlock::PgLwLock;

// Activate PostgreSQL
::pgrx::pg_module_magic!();

// Shared Memory Type
static CURRENCY_ID_MAP: PgLwLock<heapless::FnvIndexMap<i32, f32, 4>> = PgLwLock::new();
// static CURRENCY_XUID_MAP: PgLwLock<heapless::FnvIndexMap<String, i32, 4>> = PgLwLock::new();

// Init Shared Memory
#[pg_guard]
pub extern "C" fn _PG_init() {
    pg_shmem_init!(CURRENCY_ID_MAP);
    // pg_shmem_init!(CURRENCY_XUID_MAP);
}

#[pg_extern]
fn hash_insert(key: i32, value: f32) {
    CURRENCY_ID_MAP.exclusive().insert(key, value).unwrap();
}

#[pg_extern]
fn hash_get(key: i32) -> Option<f32> {
    CURRENCY_ID_MAP.share().get(&key).cloned()
}

#[pg_extern]
fn kq_fx_invalidate_cache() -> &'static str {
    // Mock result
    "Cache invalidated."
}

#[pg_extern]
fn kq_fx_get_rate(
    _currency_id: i32,
    _to_currency_id: i32,
    _date: pgrx::Date
) -> Option<f32> {
    // Mock result
    Some(10.0f32)
}

#[pg_extern]
fn kq_fx_get_rate_xuid(
    _currency_xuid: &'static str,
    _to_currency_xuid: &'static str,
    _date: pgrx::Date
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
