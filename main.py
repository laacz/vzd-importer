#!/usr/bin/env python3

import csv
import inspect
import logging
import os.path
import argparse
import re
import zipfile
from os import environ
from os.path import exists
import psycopg2

import httpx

SCRIPT_PATH = os.path.dirname(os.path.abspath(inspect.getframeinfo(inspect.currentframe()).filename))
DATA_PATH = f'{SCRIPT_PATH}/data'

uri = 'https://data.gov.lv/dati/dataset/0c5e1a3b-0097-45a9-afa9-7f7262f3f623/resource/1d3cbdf2-ee7d-4743-90c7-97d38824d0bf/download/aw_csv.zip'

args_parser = argparse.ArgumentParser(
    description="""
Downloads and imports Valsts Zemes Dienests address database into postgis enabled postgresql database. 

Database connection parameters are to be provided as environment variables: VZD_DBNAME, VZD_USER, VZD_PASSWORD, VZD_HOST, VZD_PORT

Usage example: 
VZD_DBNAME=vzd ./import.py --verbose

""", formatter_class=argparse.RawTextHelpFormatter)
args_parser.add_argument('--force-import', action='store_true', help='Force import of data even if it has not been changed')
args_parser.add_argument('--verbose', action='store_true', help='Loglevel = DEBUG')
args_parser.add_argument('--quiet', action='store_true', help='Loglevel = ERROR')
args_parser.add_argument('--skip-extract', action='store_true', help='Do not extract data (useful for quicker debugging)')
args_parser.add_argument('--only', help='Ar komatu atdalīti failu nosaukumi, kurus apstrādāt')

args = args_parser.parse_args()

FORCE_IMPORT = args.force_import
SKIP_EXTRACT = args.skip_extract
FILES = ()
if args.only is not None:
    FILES = args.only.split(',')
if args.verbose:
    LOGLEVEL = logging.DEBUG
elif args.quiet:
    LOGLEVEL = logging.ERROR
else:
    LOGLEVEL = logging.INFO

PSQL_CONNECTION_STRING = ''
for param in ['dbname', 'user', 'password', 'host', 'port']:
    if f'VZD_{param.upper()}' in environ:
        PSQL_CONNECTION_STRING += f'{param}=%s ' % (environ[f'VZD_{param.upper()}'],)

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %I:%M:%S'))
logger.addHandler(handler)

conn = psycopg2.connect(PSQL_CONNECTION_STRING)

FILES_MAP = {
    'AW_PILSETA.CSV': {
        'table': 'aw_pilseta',
        'columns': {
            'code': 'int',
            'type': 'int',
            'name': 'string',
            'parent_code': 'int',
            'parent_type': 'int',
            'approved': 'bool',
            'approve_degree': 'int',
            'status': 'string',
            'sort_name': 'string',
            'created_at': 'date',
            'modified_at': 'date',
            'deleted_at': 'date',
            'atvk': 'bool',
            'full_name': 'string',
        },
    },
    'AW_NOVADS.CSV': {
        'table': 'aw_novads',
        'columns': {
            'code': 'int',
            'type': 'int',
            'name': 'string',
            'parent_code': 'int',
            'parent_type': 'int',
            'approved': 'bool',
            'approve_degree': 'int',
            'status': 'string',
            'sort_name': 'string',
            'created_at': 'date',
            'modified_at': 'date',
            'deleted_at': 'date',
            'atvk': 'bool',
            'full_name': 'string',
        },
    },
    'AW_CIEMS.CSV': {
        'table': 'aw_ciems',
        'columns': {
            'code': 'int',
            'type': 'int',
            'name': 'string',
            'parent_code': 'int',
            'parent_type': 'int',
            'approved': 'bool',
            'approve_degree': 'int',
            'status': 'string',
            'sort_name': 'string',
            'created_at': 'date',
            'modified_at': 'date',
            'deleted_at': 'date',
            'is_small': 'bool',
            'full_name': 'string',
        },
    },
    'AW_PAGASTS.CSV': {
        'table': 'aw_pagasts',
        'columns': {
            'code': 'int',
            'type': 'int',
            'name': 'string',
            'parent_code': 'int',
            'parent_type': 'int',
            'approved': 'bool',
            'approve_degree': 'int',
            'status': 'string',
            'sort_name': 'string',
            'created_at': 'date',
            'modified_at': 'date',
            'deleted_at': 'date',
            'atvk': 'string',
            'full_name': 'string',
        },
    },
    'AW_IELA.CSV': {
        'table': 'aw_iela',
        'columns': {
            'code': 'int',
            'type': 'int',
            'name': 'string',
            'parent_code': 'int',
            'parent_type': 'int',
            'approved': 'bool',
            'approve_degree': 'int',
            'status': 'string',
            'sort_name': 'string',
            'created_at': 'date',
            'modified_at': 'date',
            'deleted_at': 'date',
            'attr': 'string',
            'full_name': 'string',
        },
    },
    'AW_EKA.CSV': {
        'table': 'aw_eka',
        'columns': {
            'code': 'int',
            'type': 'int',
            'status': 'string',
            'approved': 'bool',
            'approve_degree': 'int',
            'parent_code': 'int',
            'parent_type': 'int',
            'name': 'string',
            'sort_name': 'string',
            'postal_code': 'string',
            'postal_office_area_code': 'int',
            'created_at': 'date',
            'modified_at': 'date',
            'deleted_at': 'date',
            'for_build': 'bool',
            'planned_address': 'bool',
            'full_name': 'string',
            'x': 'float',
            'y': 'float',
            'lat': 'float',
            'lng': 'float',
        },
        'geom': True,
    },
}


def downloaded(uri):
    logger.debug(f'Checking to download {uri}')
    target_file_name = f'{DATA_PATH}/{os.path.basename(uri)}'
    etag_file_name = f'{target_file_name}.etag'

    headers = {}

    if exists(target_file_name) and exists(etag_file_name):
        with open(etag_file_name, 'r+') as f:
            headers['If-None-Match'] = f.read(1024)
            logger.debug('Etag: %s' % headers['If-None-Match'])

    response = httpx.get(uri, headers=headers)

    if response.status_code == 200:
        logger.info(f'File {target_file_name} updated')

        if 'etag' in response.headers:
            logger.debug('New etag: %s' % response.headers['etag'])
            with open(etag_file_name, 'w+') as f:
                f.write(response.headers['etag'])

        with open(target_file_name, 'wb+') as f:
            f.write(response.content)

        return True

    logger.debug('File not modified')

    return False


def vzd_iterator(conf, reader):
    for row in reader:
        r = {}
        for idx, column in enumerate(conf.get('columns').keys()):
            value = row[idx]
            col_type = conf['columns'][column] if row[0] != '\ufeff#KODS#' and row[0] != '#KODS#' else 'string'
            if not len(value) and col_type != 'bool':
                value = None
            elif col_type == 'int':
                value = int(value)
            elif col_type == 'float':
                value = float(value)
            elif col_type == 'bool':
                value = {'Y': True, '1': True}.get(value, False)
            elif col_type == 'date':
                value = value.replace('.', '-') if re.match(r'^\d\d\d\d', value) else re.sub(
                    r'(\d\d)\.(\d\d)\.(\d\d\d\d)', '\\3-\\2-\\1', value)
            elif col_type == 'string':
                pass
            else:
                raise RuntimeError(f'Unknown data type {col_type}')
            r[column] = value
        yield r


def process_file(file_name):
    if file_name not in FILES_MAP:
        logger.debug(f'Skipping {file_name} (unsupported file)')
        return

    if len(FILES) and file_name not in FILES:
        logger.debug(f'Skipping {file_name}')
        return

    logger.info(f'Processing {file_name}')

    conf = FILES_MAP.get(file_name)

    with conn:
        with conn.cursor() as cur:
            logger.debug('Set updated fo false')
            cur.execute(f'UPDATE {conf["table"]} SET updated = False')
            if 'geom' in conf:
                logger.debug('Dropping geom index')
                cur.execute(f'DROP INDEX IF EXISTS {conf["table"]}_geom_idx')

            logger.debug("Reading CSV file")
            with open(f'{DATA_PATH}/{file_name}', 'r+') as file:
                reader = vzd_iterator(conf, csv.reader(file, delimiter=";", quotechar='#'))
                # Skip header
                next(reader)
                columns = list(conf['columns'].keys())
                placeholders = ["%s"] * len(columns);
                if 'geom' in conf:
                    columns.append('geom')
                    placeholders.append('ST_GeomFromText(%s, 4326)')

                logger.debug(f'Inserting rows')

                i = 0
                for row in reader:
                    i += 1
                    values = list(row.values())
                    if 'geom' in conf:
                        if row['lng'] is None:
                            continue
                        values.append('POINT(%s %s)' % (row['lng'], row['lat']))

                    sets = ', '.join(f'{column} = {placeholder}' for column, placeholder in
                                     zip(columns + ['updated'], placeholders + ['True']))
                    sql = f'INSERT INTO {conf["table"]} ({", ".join(columns)}, updated) VALUES ({", ".join(placeholders)}, true) ON CONFLICT (code) DO UPDATE SET {sets}'
                    try:
                        cur.execute(sql, values * 2)
                    except Exception as e:
                        logger.error(f'Error inserting row {i}')
                        logger.error(e)
                        logger.error(sql)
                        raise e
                    if i % 10000 == 0:
                        logger.debug(f'{i} rows inserted')

                logger.debug(f'{i} rows inserted')

            if 'geom' in conf:
                logger.debug('Creating geom index')
                cur.execute(f'CREATE INDEX {conf["table"]}_geom_idx ON {conf["table"]} USING GIST (geom)')

            logger.debug('Cleaning up')
            cur.execute(f'DELETE FROM {conf["table"]} WHERE updated = False')
            logger.debug("Committing")


def process(file_name):
    if not SKIP_EXTRACT:
        with zipfile.ZipFile(f'{DATA_PATH}/{file_name}', 'r') as zip:
            logger.debug(f'Extracting files to {DATA_PATH}')
            zip.extractall(DATA_PATH)

    for file in os.listdir(os.fsencode(DATA_PATH)):
        file_name = os.fsdecode(file)
        if file_name.lower().endswith('.csv'):
            process_file(file_name)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM aw_full_addresses LIMIT 1')
                logger.info('Refreshing materialized view aw_full_addresses')
                cur.execute('REFRESH MATERIALIZED VIEW aw_full_addresses')
                logger.debug('Refreshed materialized view aw_full_addresses')
    except Exception as e:
        logger.debug('No materialized view to refresh')


if __name__ == '__main__':
    if downloaded(uri) or FORCE_IMPORT:
        process(os.path.basename(uri))
