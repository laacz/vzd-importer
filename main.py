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
    # https://data.gov.lv/dati/lv/dataset/valsts-adresu-registra-informacijas-sistemas-atvertie-dati
    'addresses': [
        '0c5e1a3b-0097-45a9-afa9-7f7262f3f623/resource/1d3cbdf2-ee7d-4743-90c7-97d38824d0bf/download/aw_csv.zip',
    ],

    # Parcel metadata (sample data, no production data available as of yet)
    # https://data.gov.lv/dati/lv/dataset/kadastra-informacijas-sistemas-atverto-teksta-datu-paraugs/resource/fd4b0122-37c7-46a2-9e17-e86f35a72e25?inner_span=True
    'parcels': {
        '698ac307-c739-40dd-977e-8736877fe42b/resource/593c9f03-79d9-4f9b-b2ba-6187fbbb477b/download/mark.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/fd4b0122-37c7-46a2-9e17-e86f35a72e25/download/valuation.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/6cdbdc40-90b4-4d41-9881-b0154f4127f0/download/premisegroup.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/90f64c9c-153d-4ff3-84d8-cab51f647fd3/download/ownership.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/4393708c-da24-4b0d-8f1c-4c849b838f00/download/encumbrance.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/dab51594-04d7-4a66-8514-bf272dc0184b/download/property.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/c49bc6c5-72b2-4431-9248-0ecec60b441c/download/address.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/b189de84-0559-40d6-bb8f-22476a3a72f0/download/parcel.zip',
        '698ac307-c739-40dd-977e-8736877fe42b/resource/518806bc-dc74-4be7-b9a7-9989ffd23ed9/download/parcelpart.zip',
    },
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
args_parser.add_argument('--only',
                         help='Comma separated list of files to process, defaults to empty (all). For addresses it has '
                              'to be archived file name (aw_iela.csv, etc), for kadastrs metadata it is zip name '
                              '(mark.zip, etc)')
args_parser.add_argument('--groups',
                         help='Comma separated list of groups to process. Defaults to "addresses,parcels"',
                         default='addresses,parcels')

args = args_parser.parse_args()

FORCE_IMPORT = args.force_import
GROUPS = args.groups.split(',')

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


def should_skip(file):
    if file.lower().endswith('.zip'):
        return any([x.lower().endswith('.zip') for x in FILES]) and file.lower() not in [x.lower() for x in FILES]
    elif file.lower().endswith('.csv'):
        return any([x.lower().endswith('.csv') for x in FILES]) and file.lower() not in [x.lower() for x in FILES]
    return False


def process_archive(file_name):
    if should_skip(file_name):
        logger.debug(f'Skipping {file_name}')
        return
    with zipfile.ZipFile(f'{DATA_PATH}/{file_name}', 'r') as zip:
        logger.debug(f'Extracting files to {DATA_PATH}')
        zip.extractall(DATA_PATH)

        for file in zip.filelist:
            importer_class = get_file_importer(f'{DATA_PATH}/{file.filename}')
            if should_skip(file.filename):
                logger.debug(f'Skipping {file.filename}')
            elif importer_class:
                logger.info(f'Using importer {importer_class.__name__} to import {file.filename}')
                importer = importer_class(conn=conn, file=f'{DATA_PATH}/{file.filename}', logger=logger)
                importer.process()
            else:
                logger.warning(f'Unknown file {file.filename}')

            logger.debug(f'Removing {DATA_PATH}/{file.filename}')
            # os.unlink(f'{DATA_PATH}/{file.filename}')


if __name__ == '__main__':
    for group, uris in uris.items():
        if group in GROUPS:
            for uri in uris:
                uri = BASE_URI + uri
                if should_skip(os.path.basename(uri)):
                    logger.debug(f'Skipping {uri}')
                    continue
                if downloaded(uri) or FORCE_IMPORT:
                    process_archive(os.path.basename(uri))
        else:
            logger.debug(f'Skipping group {group}')
