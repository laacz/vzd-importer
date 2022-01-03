This python script downloads and imports Latvian addresses into PostgreSQL database.

## Requirements

* PostgreSQL with PostGIS extension
* Python3 with httpx and psycopg2 modules

# Possible future work

- [ ] Add import of addresses related shapefiles (same dataset, different archive file)
- [ ] Add a utility script to download and import
  of [parcels data](https://data.gov.lv/dati/lv/dataset/kadastra-informacijas-sistemas-atverti-telpiskie-dati) (there's
  a lot of shapefiles)

# Contributions

They are welcome (use issues to report or discuss, pull requests to implement).

# Usage

See `./main.py --help`

# Behaviour

Script checks
against [Latvian address register open data](https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati)
, using If-Modified-Since header, which it saves to a file for future reference, so data is being downloaded only if it
has been updated. This means that it can be dropped into cron job to download data when it is updated.

If data has been downloaded, it's unzipped into `data/csv` and then imported into PostgreSQL. Schema has to be created
(it can be found in [schema.sql](schema.sql))

If data has invalid coordinates (latitude or longitude is not a number), it's skipped.

For `aw_eka` table column `geom` is created, and an spatial index is added. SRID 4326 is used, so some offsets may
arise.

# Data

Data has been released by the State Land Service of the Republic of Latvia under goverment's OpenData initiative, and is
available
at [this data.gov.lv page](https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati)
. It's released under CC BY 4.0 license.

# License

CC BY 4.0, which means that it can be used for free, however attribution is required, and no additional restrictions on
this data can be imposed. This script follows suit.