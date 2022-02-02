import csv
import os.path
import re

import psycopg2
import psycopg2.extras
from lxml import objectify


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


class ParcelMetadataImporter(GenericImporter):
    table = None
    pkey = True
    props = {}
    base = {}

    def process(self):
        tree = objectify.parse(self.file)
        root = tree.getroot()
        root_name = root.tag[root.tag.rfind('}') + 1:]

        dataset = root_name.replace('FullData', '')
        list_name = dataset + 'ItemList'
        item_name = dataset + 'ItemData'

        total = len(root[list_name][item_name])
        i = 0
        # with self.conn.cursor() as cur:
        #     cur.execute(f'UPDATE {self.table} SET updated = false')
        for item in root[list_name][item_name]:
            i += 1
            self.base = self.getObjectRelation(item)
            self.processItem(item)
            if i % 1000 == 0:
                if self.conn.status == psycopg2.extensions.STATUS_BEGIN:
                    self.conn.commit()
                self.logger.debug(f'Processing {i}/{total}')

        self.logger.debug(f'Processed {i} of {total} record(s)')
        if self.conn.status == psycopg2.extensions.STATUS_BEGIN:
            # with self.conn.cursor() as cur:
            #     cur.execute(f'DELETE FROM {self.table} WHERE updated = false')
            self.conn.commit()

        # print(self.props)

    @staticmethod
    def getattrbypath(item, path, default=None, cast=None):
        for part in path.split('.'):
            if not hasattr(item, part):
                item = default
                break
            item = getattr(item, part)

        item = default if item is None else item
        item = item.text if item is not None and 'text' in item else item
        item = cast(item) if cast is not None and item != default else item

        return item

    def getObjectRelation(self, item):
        return {
            'cadastre_nr': self.getattrbypath(item, 'ObjectRelation.ObjectCadastreNr', None, str),
            'object_type': self.getattrbypath(item, 'ObjectRelation.ObjectType', None, str),
        } if self.getattrbypath(item, 'ObjectRelation.ObjectCadastreNr', None, str) is not None else {}

    def processItem(self, item):
        pass

    def saveItem(self, row):
        cur = self.conn.cursor()
        try:
            row['updated'] = True
            columns = list(row.keys())
            placeholders = [f'%({key})s' for key in row.keys()]
            sets = ', '.join(f'{column} = {placeholder}' for column, placeholder in
                             zip(columns, placeholders))

            sql = f'INSERT INTO {self.table} ({",".join(columns)}) VALUES ({",".join(placeholders)})'
            if self.pkey:
                sql += f' ON CONFLICT ON CONSTRAINT {self.table + "_pkey"} DO UPDATE SET {sets}'

            cur.execute(sql, row)
        except psycopg2.Error as e:
            print(cur.query)
            print(e)
            raise e


class MarksImporter(ParcelMetadataImporter):
    table = 'marks'

    def processItem(self, item):
        for mark in item.MarkList:
            mark_type = self.getattrbypath(mark, 'MarkRecData.MarkType', None, int)
            date = self.getattrbypath(mark, 'MarkRecData.MarkDate', None, str)
            description = self.getattrbypath(mark, 'MarkRecData.MarkDescription', None, str)

            record = self.base | {
                'mark_type': mark_type,
                'date': date,
            }

            cur = self.conn.cursor()
            try:
                cur.execute("""
                    INSERT INTO mark_types 
                    (id, description) 
                    VALUES (%s, %s)
                    ON CONFLICT ON CONSTRAINT mark_types_pkey DO NOTHING
                    """,
                            (mark_type, description))
            except psycopg2.Error as e:
                pass

            self.saveItem(record)


class ValuationsImporter(ParcelMetadataImporter):
    table = 'valuations'

    def processItem(self, item):
        self.props = self.props | item.__dict__.keys()

        record = self.base | {
            'property_valuation': self.getattrbypath(item, 'PropertyValuation', None, int),
            'property_valuation_date': self.getattrbypath(item, 'PropertyValuationDate', None, str),
            'property_cadastral_value': self.getattrbypath(item, 'PropertyCadastralValue', None, int),
            'property_cadastral_value_date': self.getattrbypath(item, 'PropertyCadastralValueDate', None, str),
            'object_cadastral_value': self.getattrbypath(item, 'ObjectCadastralValue', None, int),
            'object_cadastral_value_date': self.getattrbypath(item, 'ObjectCadastralValueDate', None, str),
            'object_forest_value': self.getattrbypath(item, 'ObjectForestValue', None, int),
            'object_forest_value_date': self.getattrbypath(item, 'ObjectForestValueDate', None, str),
        }

        self.saveItem(record)


class AddressesImporter(ParcelMetadataImporter):
    table = 'addresses'

    def processItem(self, item):
        self.props = self.props | item.__dict__.keys()

        record = self.base | {
            'ar_code': self.getattrbypath(item, 'AddressData.ARCode', None, int),
            'post_index': self.getattrbypath(item, 'AddressData.PostIndex', None, str),
            'county': self.getattrbypath(item, 'AddressData.County', None, str),
            'parish': self.getattrbypath(item, 'AddressData.Parish', None, str),
            'town': self.getattrbypath(item, 'AddressData.Town', None, str),
            'village': self.getattrbypath(item, 'AddressData.Village', None, str),
            'house': self.getattrbypath(item, 'AddressData.House', None, str),
        }

        self.saveItem(record)


class OwnershipsImporter(ParcelMetadataImporter):
    table = 'ownerships'
    ownership_statuses = {}
    ownership_person_statuses = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT id, description FROM ownership_statuses')
            self.ownership_statuses = cur.fetchall()
            cur.execute('SELECT id, description FROM ownership_person_statuses')
            self.ownership_person_statuses = cur.fetchall()

    def processItem(self, item):
        self.props = self.props | item.__dict__.keys()

        for kind in self.getattrbypath(item, 'OwnershipStatusKindList.OwnershipStatusKind', []):
            status = self.getattrbypath(kind, 'OwnershipStatusKind.OwnershipStatus', None, str)
            person_status = self.getattrbypath(kind, 'OwnershipStatusKind.PersonStatus', None, str)

            ownership_status_id = next(
                (x for x in self.ownership_statuses if x['description'] == status),
                {}
            ).get('id', None)
            person_status_id = next(
                (x for x in self.ownership_person_statuses if x['description'] == person_status),
                {}
            ).get('id', None)

            if not ownership_status_id:
                with self.conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO ownership_statuses (description) VALUES (%s) RETURNING id',
                        (status,)
                    )
                    ownership_status_id = cur.fetchone()[0]
                    self.ownership_statuses.append({'id': ownership_status_id, 'description': status})

            if not person_status_id:
                with self.conn.cursor() as cur:
                    cur.execute(
                        'INSERT INTO ownership_person_statuses (description) VALUES (%s) RETURNING id',
                        (person_status,)
                    )
                    person_status_id = cur.fetchone()[0]
                    self.ownership_person_statuses.append({'id': person_status_id, 'description': person_status})

            record = self.base | {
                'ownership_status_id': ownership_status_id,
                'person_status_id': person_status_id,
            }

            self.saveItem(record)


def get_file_importer(file_name):
    importers_map = {}
    checkstr = None
    if file_name.lower().endswith('.csv'):
        importers_map = {
            'aw_pilseta.csv': CitiesImporter,
            'aw_ciems.csv': VillagesImporter,
            'aw_novads.csv': CountiesImporter,
            'aw_pagasts.csv': ParishesImporter,
            'aw_iela.csv': StreetsImporter,
            'aw_eka.csv': HousesImporter,
        }
        checkstr = os.path.basename(file_name.lower())

    elif file_name.lower().endswith('.xml'):
        importers_map = {
            'MarkFullData': MarksImporter,  # Atzīmi raksturojošie dati
            'ValuationFullData': ValuationsImporter,  # Kadastra objektu novērtējumi un kadastrālās vērtības
            'AddressFullData': AddressesImporter,  # Kadastra objektam reģistrētās adreses
            'OwnershipFullData': OwnershipsImporter,  # Nekustamo īpašumu un būvju īpašumtiesību statusi
            # 'PropertyFullData': PropertiesImporter,  # Nekustamais īpašums un tā sastāvs
            # 'ParcelPartFullData': ParcelPartsImporter,  # Zemes vienību daļas raksturojošie dati
            # 'EncumbranceFullData': EncumbrancesImporter,  # Kadastra objektam reģistrētie apgrūtinājumi
            # 'ParcelFullData': ParcelsImporter,  # Zemes vienības raksturojošie dati
            # 'PremiseGroupFullData': PremiseGroupsImporter,  # Telpu grupu raksturojošie dati
            # 'BuildingFullData': BuildingsImporter,  # Būves raksturojošie dati
        }

        tree = objectify.parse(file_name)
        root = tree.getroot()
        checkstr = root.tag[root.tag.rfind('}') + 1:]

    if checkstr in importers_map:
        return importers_map[checkstr]

    return None
