Python script `main.py` downloads and imports Latvian addresses into PostgreSQL database. Data contains parishes,
counties, cities, towns, and streets.

Shell script `import-kadastrs.sh`
downloads and imports parcels shapefiles (cadastral groups, buildings, engineering structures, parcels, parcel borders,
parcel errors, parcel parts, surveying statuses, and way restrictions)..

## Requirements

* PostgreSQL with PostGIS extension
* Python3 with httpx and psycopg2 modules
* shp2pgsql (shipped with PostGIS), curl, and [jq command-line JSON processor](https://stedolan.github.io/jq/) for
  parcel import script.

# Possible future work

- [ ] Add import of addresses related shapefiles (same dataset, different archive file; contributors welcome, I have no
  use case for it just now)
- [x] Add a utility script to download and import
  of [parcels data](https://data.gov.lv/dati/lv/dataset/kadastra-informacijas-sistemas-atverti-telpiskie-dati) (there's
  a lot of shapefiles)

# Contributions

These are welcome (use issues to report or discuss, pull requests to implement).

# Usage

To fetch and import addresses, create schema (`psql schema <schema.sql`) and then
run `VZD_DBNAME=schema python3 main.py --verbose`.

To download and import all parcel shapefiles, run `VZD_DBNAME=schema ./import-kadastrs.sh`. This will take a while.

# Behaviour

## Addresses import

Python script checks
against [Latvian address register open data](https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati)
, using If-Modified-Since header, which it saves to a file for future reference, so data is being downloaded only if it
has been updated. This means that it can be dropped into cron job to download data when it is updated.

If data has been downloaded, it's unzipped into `data/csv` and then imported into PostgreSQL. Schema has to be created
(it can be found in [schema.sql](schema.sql))

If data has invalid coordinates (latitude or longitude is not a number), it's skipped.

For `aw_eka` table column `geom` is created, and an spatial index is added. SRID 4326 is used, so some offsets may
arise.

## Parcels import

Shell script does nothing fancy. It just fetches JSON metadata, extracts list of all shapefile zip's, downloads them,
then imports into database (via dropping, creating and populating tables). Spatial index on `geom` is also created.

# Data

Data has been released by the State Land Service of the Republic of Latvia under goverment's OpenData initiative, and is
available
at [this data.gov.lv page](https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati)
. It's released under CC BY 4.0 license.

[Spec for address data](https://www.vzd.gov.lv/lv/VAR-atversana)
, [spec for parcel data](https://www.vzd.gov.lv/lv/kadastra-telpisko-datu-atversana).

# License

CC BY 4.0, which means that it can be used for free, however attribution is required, and no additional restrictions on
this data can be imposed. This script follows suit.