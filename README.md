# importer
Pre-processes provided CSV files for usage in production databases

## Demo
[![asciicast](https://asciinema.org/a/q2sB4WEdl1hiRYR4keoh3AFGw.svg)](https://asciinema.org/a/q2sB4WEdl1hiRYR4keoh3AFGw)

## Dependencies
- Docker
- Grade data:
    - in CSV format with the schema seen in `sample/sample.csv`

## Running
- `cd importer/`
- Generate SQLite and SQL files: `./generate.sh [directory with csvfiles | csvfile] --cleanup`
    - `records.db` and a `records.sql` files will be copied to `importer/` before deleting the container and images generated

