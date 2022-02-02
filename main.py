#!/usr/bin/env python3

import inspect
import logging
import os.path
import argparse
import zipfile
from os import environ
import psycopg2

import httpx

from defs import get_file_importer

SCRIPT_PATH = os.path.dirname(os.path.abspath(inspect.getframeinfo(inspect.currentframe()).filename))
DATA_PATH = f'{SCRIPT_PATH}/data'
BASE_URI = 'https://data.gov.lv/dati/dataset/'

uris = {
    # Addresses
    'addresses': [
        '0c5e1a3b-0097-45a9-afa9-7f7262f3f623/resource/1d3cbdf2-ee7d-4743-90c7-97d38824d0bf/download/aw_csv.zip',
    ],
}

args_parser = argparse.ArgumentParser(
    description="""
Downloads and imports Valsts Zemes Dienests address database into postgis enabled postgresql database. 

Database connection parameters are to be provided as environment variables: VZD_DBNAME, VZD_USER, VZD_PASSWORD, VZD_HOST, VZD_PORT

Usage example: 
VZD_DBNAME=vzd ./import.py --verbose

""", formatter_class=argparse.RawTextHelpFormatter)
args_parser.add_argument('--force-import', action='store_true',
                         help='Force import of data even if it has not been changed')
args_parser.add_argument('--verbose', action='store_true', help='Loglevel = DEBUG')
args_parser.add_argument('--quiet', action='store_true', help='Loglevel = ERROR')
args_parser.add_argument('--only', help='Ar komatu atdalīti failu nosaukumi, kurus apstrādāt')

args = args_parser.parse_args()

FORCE_IMPORT = args.force_import

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


def downloaded(uri):
    logger.debug(f'Checking to download {uri}')
    target_file_name = f'{DATA_PATH}/{os.path.basename(uri)}'
    etag_file_name = f'{target_file_name}.etag'

    headers = {}

    if os.path.exists(target_file_name) and os.path.exists(etag_file_name):
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


def process_archive(file_name):
    with zipfile.ZipFile(f'{DATA_PATH}/{file_name}', 'r') as zip:
        logger.debug(f'Extracting files to {DATA_PATH}')
        zip.extractall(DATA_PATH)

        for file in zip.filelist:
            importer_class = get_file_importer(f'{DATA_PATH}/{file.filename}')
            if len(FILES) and os.path.basename(file.filename).lower() not in [x.lower() for x in FILES]:
                logger.debug(f'Skipping {file.filename}')
            elif importer_class:
                logger.info(f'Using importer {importer_class.__name__} to import {file.filename}')
                importer = importer_class(conn=conn, file=f'{DATA_PATH}/{file.filename}', logger=logger)
                importer.process()
            else:
                logger.warning(f'Unknown file {file.filename}')

            logger.debug(f'Removing {DATA_PATH}/{file.filename}')
            os.unlink(f'{DATA_PATH}/{file.filename}')


if __name__ == '__main__':
    for group, uris in uris.items():
        for uri in uris:
            uri = BASE_URI + uri
            print(group, uri)
            if downloaded(uri) or FORCE_IMPORT:
                process_archive(os.path.basename(uri))
