import csv
import os.path
import re

import psycopg2


class GenericImporter:
    conn = None
    file = None
    logger = None

    def __init__(self, conn, file, logger):
        self.conn = conn
        self.file = file
        self.logger = logger


class AddressesImporter(GenericImporter):
    geom = False
    table = None
    columns = {}

    def iterator(self, reader):
        for row in reader:
            r = {}
            for idx, column in enumerate(self.columns.keys()):
                value = row[idx]
                col_type = self.columns[column] if row[0] != '\ufeff#KODS#' and row[0] != '#KODS#' else 'string'
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

    def process(self):
        with self.conn.cursor() as cur:
            cur.execute(f'UPDATE {self.table} SET updated = False')
            if self.geom:
                self.logger.debug('Dropping geom index')
                cur.execute(f'DROP INDEX IF EXISTS {self.table}_geom_idx')

            self.logger.debug("Reading CSV file")
            with open(self.file, 'r+') as file:
                reader = self.iterator(csv.reader(file, delimiter=";", quotechar='#'))
                # Skip header
                next(reader)
                columns = list(self.columns.keys())
                placeholders = ["%s"] * len(columns);
                if self.geom:
                    columns.append('geom')
                    placeholders.append('ST_GeomFromText(%s, 4326)')

                self.logger.debug(f'Inserting rows')

                i = 0
                for row in reader:
                    i += 1
                    values = list(row.values())
                    if self.geom:
                        if row['lng'] is None:
                            continue
                        values.append('POINT(%s %s)' % (row['lng'], row['lat']))

                    sets = ', '.join(f'{column} = {placeholder}' for column, placeholder in
                                     zip(columns + ['updated'], placeholders + ['True']))
                    sql = f'INSERT INTO {self.table} ({", ".join(columns)}, updated) VALUES ({", ".join(placeholders)}, true) ON CONFLICT (code) DO UPDATE SET {sets}'
                    try:
                        cur.execute(sql, values * 2)
                    except Exception as e:
                        self.logger.error(f'Error inserting row {i}')
                        self.logger.error(e)
                        self.logger.error(sql)
                        raise e
                    if i % 10000 == 0:
                        self.logger.debug(f'{i} rows inserted')

                self.logger.debug(f'{i} rows inserted')

            if self.geom:
                self.logger.debug('Creating geom index')
                cur.execute(f'CREATE INDEX {self.table}_geom_idx ON {self.table} USING GIST (geom)')

            self.logger.debug('Cleaning up')
            cur.execute(f'DELETE FROM {self.table} WHERE updated = False')
            self.logger.debug("Done")

            self.postprocess()

    def postprocess(self):
        pass


class CitiesImporter(AddressesImporter):
    table = 'aw_pilseta'
    columns = {
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
    }


class CountiesImporter(AddressesImporter):
    table = 'aw_novads'
    columns = {
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
    }


class VillagesImporter(AddressesImporter):
    table = 'aw_ciems'
    columns = {
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
    }


class ParishesImporter(AddressesImporter):
    table = 'aw_pagasts'
    columns = {
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
    }


class StreetsImporter(AddressesImporter):
    table = 'aw_iela'
    columns = {
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
    }


class HousesImporter(AddressesImporter):
    table = 'aw_eka'
    columns = {
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
    }
    geom = True

    def postprocess(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute('SELECT 1 FROM aw_full_addresses LIMIT 1')
                self.logger.info('Refreshing materialized view aw_full_addresses')
                cur.execute('REFRESH MATERIALIZED VIEW aw_full_addresses')
                self.logger.debug('Refreshed materialized view aw_full_addresses')
        except psycopg2.Error as e:
            self.logger.debug('No materialized view to refresh')


def get_file_importer(file_name):
    if file_name.lower().endswith('.csv'):
        csv_map = {
            'aw_pilseta.csv': CitiesImporter,
            'aw_ciems.csv': VillagesImporter,
            'aw_novads.csv': CountiesImporter,
            'aw_pagasts.csv': ParishesImporter,
            'aw_iela.csv': StreetsImporter,
            'aw_eka.csv': HousesImporter,
        }

        fname = os.path.basename(file_name.lower())
        if fname in csv_map:
            return csv_map[fname]

    return None
