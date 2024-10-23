# ketteQ FX Cache

© ketteQ

ketteQ FX Currency Cache is a PostgreSQL server extension that caches calendar-based currency
conversion information into the server's shared memory, loading one time and making it available to
all users in the same schema.

# Installation

Build the extension using included `cargo` build flow with the release option. After the shared 
library is built, install it manually or use the included `cargo pgrx install` script to do it automatically
in the local server installation.

This extension uses PostgreSQL shared memory features, and so it must be loaded using the 
`shared_preload_libraries` in the `postgresql.conf` file. Then the server must be restarted before
executing the `CREATE EXTENSION` query.

# Compatibility

The PGRX build system allows to target different PostgreSQL version automatically adjusting the output for them.

Supported by this extension: PostgreSQL 17 (default) and 16.

See the Build section to target a different PostgreSQL version, please note that if you use the automatic installer
provided by the PGRX crate it will use the correct target PostgreSQL version.

# Architecture

The extension will load currency conversion data from specific tables stored in the current schema.

## Technology

- Rust + pgrx

# Usage

| Function                                                                       | Description                                 |
|--------------------------------------------------------------------------------|---------------------------------------------|
| kq_fx_invalidate_cache()                                                       | Invalidates the loaded cache.               |
| float8 kq_fx_get_rate(currency_id int8, to_currency_id int8, date)             | Gets the rate for the currency id.          |
| float8 kq_fx_get_rate_by_xuid(currency_xuid text, to_currency_xuid text, date) | Gets the rate for the currency by its xuid. |

# Build instructions

## Pre Requisites

- Latest Rust toolchain, update using `rustup update`.
- PGRX installed and initialized: `cargo install --locked cargo-pgrx` and `cargo pgrx init`.

## Building

- `cargo pgrx schema` to build the SQL schema file, add the `-o SCHEMA_FILE.sql` to write the output to a file, `stdout` is the default output.
- `cargo b --release`
- The shared library will be located in the `./target/release` folder.

To set a specific target PostgreSQL version, add `pg17` or `pg16` at the end of the `cargo pgrx schema` command.

## Install

After the shared library is built, you can manually install using the `kq_fx.control`, `SCHEMA_FILE.sql` 
and the shared library `libkq_fx.so`.

### Automatic install

This will require an available `pg_config` in the $PATH.

- `cargo pgrx install --release`, add the `--sudo` flag to invoke sudo while installing.

Note: This installation method will automatically pick the correct target PostgreSQL version.

