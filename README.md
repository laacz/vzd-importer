Repository contains scripts for fetching and importing State Land Service of the Republic of Latvia open data (addresses and parcels).

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

## Denormalized addresses

If you would like to have a denormalized view of the data, here is a SQL select for that. Resulting dataset contains only existing houses and schema is as follows.

| column       | type | description                                     |
| ------------ | ---- | ----------------------------------------------- |
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
| ---- | ------------------------------------ |
| 101  | Latvijas Republika                   |
| 102  | Rajons                               |
| 104  | Pilsēta                              |
| 105  | Pagasts                              |
| 106  | Ciems/mazciems                       |
| 107  | Iela                                 |
| 108  | Ēka, apbūvei paredzēta zemes vienība |
| 109  | Telpu grupa                          |
| 113  | Novads                               |

SQL itself:

```sql
-- Create table from this select or truncate and re-insert data. Up to you. 
select e.code,
       e.name,
       iela.name as iela_name,
       coalesce(
               ciems.code,
               ciems_no_ielas.code
           )     as ciems_code,
       coalesce(
               ciems.name,
               ciems_no_ielas.name
           )     as ciems_name,
       coalesce(
               pilseta.code,
               pilseta_no_ielas.code
           )     as pilseta_code,
       coalesce(
               pilseta.name,
               pilseta_no_ielas.name
           )     as pilseta_name,
       coalesce(
               pagasts.code,
               pagasts_no_ciema.code,
               pagasts_no_ciema_no_ielas.code,
               pagasts_no_pilsetas.code
           )     as pagasts_code,
       coalesce(
               pagasts.name,
               pagasts_no_ciema.name,
               pagasts_no_ciema_no_ielas.name,
               pagasts_no_pilsetas.name
           )     as pagasts_name,
       coalesce(
               novads_no_pagasta.code,
               novads_no_pagasta_no_ciema.code,
               novads_no_pagasta_no_ciema_no_ielas.code,
               novads_no_pilsetas.code,
               novads_no_pilsetas_no_ielas.code
           )     as novads_code,
       coalesce(
               novads_no_pagasta.name,
               novads_no_pagasta_no_ciema.name,
               novads_no_pagasta_no_ciema_no_ielas.name,
               novads_no_pilsetas.name,
               novads_no_pilsetas_no_ielas.name
           )     as novads_name,
       e.full_name,
       e.parent_code,
       e.parent_type,
       geom
from vzd_aw_eka e
         left join vzd_aw_iela iela on iela.code = e.parent_code

    -- House is directly in a village
         left join vzd_aw_ciems ciems on ciems.code = e.parent_code
    -- House is on a street in a village
         left join vzd_aw_ciems ciems_no_ielas on ciems_no_ielas.code = iela.parent_code

    -- House is directly in a city
         left join vzd_aw_pilseta pilseta on pilseta.code = e.parent_code
    -- House is on a street in a city
         left join vzd_aw_pilseta pilseta_no_ielas on pilseta_no_ielas.code = iela.parent_code

    -- House is directly in a parish
         left join vzd_aw_pagasts pagasts on pagasts.code = e.parent_code
    -- House is directly in a village in a parish
         left join vzd_aw_pagasts pagasts_no_ciema on pagasts_no_ciema.code = ciems.parent_code
    -- House is on a street in a village in a parish
         left join vzd_aw_pagasts pagasts_no_ciema_no_ielas
                   on pagasts_no_ciema_no_ielas.code = ciems_no_ielas.parent_code
    -- [!] We do not have a town inside a parish. Parishes are for villages.

    -- House in in a parish in a county
         left join vzd_aw_novads novads_no_pagasta on novads_no_pagasta.code = pagasts.parent_code
    -- House is in a village in a parish in a county
         left join vzd_aw_novads novads_no_pagasta_no_ciema
                   on novads_no_pagasta_no_ciema.code = pagasts_no_ciema.parent_code
    -- House is on a street in a village in a parish in a county
         left join vzd_aw_novads novads_no_pagasta_no_ciema_no_ielas
                   on novads_no_pagasta_no_ciema_no_ielas.code = pagasts_no_ciema_no_ielas.parent_code
    -- House is directly in a town in a county
         left join vzd_aw_novads novads_no_pilsetas on novads_no_pilsetas.code = pilseta.parent_code
    -- House is on a street in a town in a county
         left join vzd_aw_novads novads_no_pilsetas_no_ielas
                   on novads_no_pilsetas_no_ielas.code = pilseta_no_ielas.parent_code
     -- [!] We do not have any house which is directly in a county without an intermediate parish
     -- [!] We do not have a village which would be directly inside a county without a parish inbetween.
where e.status = 'EKS'
;
```

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