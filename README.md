Repository contains scripts for fetching and importing State Land Service of the Republic of Latvia open data (addresses
and parcels).

- [General info](#general-info)
    * [Requirements](#requirements)
    * [Possible future work](#possible-future-work)
    * [Contributions](#contributions)
    * [Usage](#usage)
- [Behaviour](#behaviour)
    * [Addresses import](#addresses-import)
    * [Denormalized addresses](#denormalized-addresses)
    * [Parcels import](#parcels-import)
- [Legal](#legal)
    * [Data](#data)
    * [License](#license)

# General info

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

## Possible future work

- [ ] Add import of addresses related shapefiles (same dataset, different archive file; contributors welcome, I have no
  use case for it just now). You can contribute via a PR.
- [x] Add a utility script to download and import
  of [parcels data](https://data.gov.lv/dati/lv/dataset/kadastra-informacijas-sistemas-atverti-telpiskie-dati) (there's
  a lot of shapefiles)

## Contributions

These are welcome (use issues to report or discuss, pull requests to implement).

## Usage

To fetch and import addresses, create schema (`psql schema <schema.sql`) and then
run `VZD_DBNAME=schema python3 main.py --verbose`. For more options see `python3 main.py --help`.

To download and import all parcel shapefiles, run `VZD_DBNAME=schema ./import-kadastrs.sh`. This will take a while.

# Behaviour

## Addresses import

Python script checks against
[Latvian address register open data](https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati)
, using `If-Modified-Since` header, which it saves to a file for future reference, so data is being downloaded only if
it has been updated. This means that it can be dropped into cron job to download data when it is updated.

If data has been downloaded, it's unzipped into `data/csv` and then imported into PostgreSQL. Schema has to be created
(it can be found in [schema.sql](schema.sql))

If an entry has invalid coordinates (latitude or longitude is not a number), it's skipped.

For `aw_eka` table column `geom` is created, and a spatial index is added. SRID 4326 is used, so some offsets may arise.

## Denormalized addresses

If you would like to have a denormalized view of the data, a materialized view `aw_full_addresses` is created. Resulting
dataset contains only existing houses and schema is as follows.

| column       | type | description                                     |
|--------------|------|-------------------------------------------------|
| code         | int  | Code, primary key                               |
| name         | text | Name (house number or name, if not on a street) |
| iela_code    | int  | Street code                                     |
| iela_name    | text | Street name                                     |
| ciems_code   | int  | Vilalge code                                    |
| ciems_name   | text | Vilalge name                                    |
| pilseta_code | int  | Town code                                       |
| pilseta_name | text | Town name                                       |
| pagasts_code | int  | Parish code                                     |
| pagasts_name | text | Parish name                                     |
| novads_code  | int  | County code                                     |
| novads_name  | text | County name                                     |
| full_name    | text | Full address of the building                    |
| parent_code  | int  | Building's parent code                          |
| parent_code  | int  | Building's parent type                          |
| geom         | geom | Geometry (Point)                                |

Parent type decodes as:

| type | description                          |
|------|--------------------------------------|
| 101  | Latvijas Republika                   |
| 102  | Rajons                               |
| 104  | Pilsēta                              |
| 105  | Pagasts                              |
| 106  | Ciems/mazciems                       |
| 107  | Iela                                 |
| 108  | Ēka, apbūvei paredzēta zemes vienība |
| 109  | Telpu grupa                          |
| 113  | Novads                               |

## Parcels import

Shell script does nothing fancy. It just fetches JSON metadata, extracts list of all shapefile zip's, downloads them,
then imports into database (via dropping, creating and populating tables). Spatial index on `geom` is also created.

# Legal

## Data

Data has been released by the State Land Service of the Republic of Latvia under goverment's OpenData initiative, and is
available
at [this data.gov.lv page](https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati)
. It's released under CC BY 4.0 license.

[Spec for address data](https://www.vzd.gov.lv/lv/VAR-atversana)
, [spec for parcel data](https://www.vzd.gov.lv/lv/kadastra-telpisko-datu-atversana).

## License

CC BY 4.0, which means that it can be used for free, however attribution is required, and no additional restrictions on
this data can be imposed. This script follows suit.