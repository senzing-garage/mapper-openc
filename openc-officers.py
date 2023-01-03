#! /usr/bin/env python3

import sys
import os
import argparse
import csv
import orjson
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import gzip
import io
import sqlite3
import hashlib

max_records_per_entity = 10000
max_relationships_per_role = 1000

def make_database(dbname, use_existing_db):
    temp_dbo = sqlite3.connect(dbname)
    temp_dbo_cursor = temp_dbo.cursor()
    temp_dbo_cursor.execute('pragma synchronous = 0')
    temp_dbo_cursor.execute('pragma cache_size = -16000000')
    temp_dbo_cursor.execute('pragma secure_delete = 0')
    temp_dbo_cursor.execute('pragma page_size = 65536')
    temp_dbo_cursor.execute('pragma journal_mode = off')
    temp_dbo_cursor.execute('pragma temp_store = MEMORY')
    temp_dbo_cursor.execute('pragma locking_mode = EXCLUSIVE')
    temp_dbo_cursor.execute('pragma isolation_level = None')
    #temp_dbo_cursor.execute('create table hashes(hash CHAR(40), base_json VARCHAR(255))')
    #temp_dbo_cursor.execute('create table records(hash CHAR(40), id VARCHAR(50), rels VARCHAR(100))')

    if not use_existing_db:
        #temp_dbo_cursor.execute('create table hashes(hash CHAR(40), record_count int, base_json VARCHAR(255), ids TEXT, rels TEXT)')
        temp_dbo_cursor.execute('create table hashes(hash CHAR(40), base_json VARCHAR(255))')
        temp_dbo.commit()

    return temp_dbo


#=========================
class mapper():

    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}


    def map(self, raw_data):
        json_data = {}
        payload_data = {}

        # clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        # place any filters needed here

        # place any calculations needed here

        # mandatory attributes
        json_data['DATA_SOURCE'] = args.data_source

        # record type replaces entity type helps keeps persons from resolving to companies
        # columnName: type
        # 13.09 populated, 0.05 unique
        #      Person (3764)
        #      Company (419)
        if raw_data['type'].upper() == 'COMPANY':
            json_data['RECORD_TYPE'] = 'ORGANIZATION'
        else:
            json_data['RECORD_TYPE'] = 'PERSON'

        # the record_id should be unique, remove this mapping if there is not one
        record_id = raw_data['id']
        json_data['RECORD_ID'] = record_id

        # column mappings

        # columnName: id
        # 100.0 populated, 100.0 unique
        #      201499267 (1)
        #      201499282 (1)
        #      201499298 (1)
        #      201499319 (1)
        #      201499336 (1)
        #json_data['id'] = raw_data['id']

        # columnName: company_number
        # 100.0 populated, 38.52 unique
        #      543 (95)
        #      5455618 (52)
        #      05149111 (46)
        #      551 (46)
        #      142962 (46)
        #json_data['company_number'] = raw_data['company_number']

        # columnName: jurisdiction_code
        # 100.0 populated, 0.24 unique
        #      pa (2047)
        #      us_ky (1295)
        #      ca_ns (1070)
        #      us_fl (1030)
        #      us_ma (938)
        #json_data['jurisdiction_code'] = raw_data['jurisdiction_code']

        # columnName: position
        # 97.73 populated, 1.62 unique
        #      agent (8511)
        #      director (6458)
        #      president (1328)
        #      incorporator (1293)
        #      secretary (1265)
        #payload_data['position'] = raw_data['position']

        # columnName: name
        # 99.96 populated, 71.24 unique
        #      C T CORPORATION SYSTEM (290)
        #      CORPORATION SERVICE COMPANY (200)
        #      REGISTERED AGENTS INC (137)
        #      Registered Agents Inc (96)
        #      LEGALINC CORPORATE SERVICES INC (82)
        if json_data['RECORD_TYPE'] == 'ORGANIZATION':
            json_data['PRIMARY_NAME_ORG'] = raw_data['name']
            self.update_stat('_FYI', 'NAME_ORG_CNT', record_id)
        else:

            # use full name if parsed name not populated
            if raw_data['name'] and not raw_data['last_name']:
                json_data['PRIMARY_NAME_FULL'] = raw_data['name']
                self.update_stat('_FYI', 'NAME_FULL_CNT', record_id)

            elif raw_data['first_name'] or raw_data['last_name']:
                self.update_stat('_FYI', 'NAME_PARSED_CNT', record_id)

                # columnName: title
                # 0.83 populated, 3.77 unique
                #      MR (175)
                #      MRS (49)
                #      MISS (14)
                #      MS (9)
                #      MR. (7)
                json_data['PRIMARY_NAME_PREFIX'] = raw_data['title']

                # columnName: first_name
                # 5.35 populated, 71.93 unique
                #      LOREN (28)
                #      JOHN (13)
                #      MICHAEL (13)
                #      John (11)
                #      DAVID (9)
                json_data['PRIMARY_NAME_FIRST'] = raw_data['first_name']

                # columnName: last_name
                # 5.68 populated, 65.91 unique
                #      NESS (28)
                #      CAUGHEY (14)
                #      SMITH (10)
                #      DOBSON (10)
                #      SCOTT (8)
                json_data['PRIMARY_NAME_LAST'] = raw_data['last_name']

            else:
                self.update_stat('_FYI', 'NAME_MISSING_CNT', record_id)

        # columnName: occupation
        # 1.16 populated, 28.73 unique
        #      DIRECTOR (115)
        #      COMPANY DIRECTOR (41)
        #      CONSULTANT (20)
        #      ACCOUNTANT (14)
        #      SECRETARY (11)
        payload_data['occupation'] = raw_data['occupation']

        # columnName: start_date
        # 14.06 populated, 32.55 unique
        #      2021-02-25 (220)
        #      2021-03-01 (144)
        #      2019-03-05 (103)
        #      2016-03-21 (90)
        #      2012-08-06 (83)
        #payload_data['start_date'] = raw_data['start_date']

        # columnName: person_number
        # 1.76 populated, 87.19 unique
        #      064559030001 (6)
        #      900001120001 (4)
        #      900001110001 (4)
        #      124967240002 (3)
        #      143139560001 (3)
        json_data['OC_OFFICER_NUMBER'] = raw_data['person_number']

        # columnName: person_uid
        # 7.51 populated, 74.07 unique
        #      10130615 (14)
        #      831909905 (13)
        #      4000448343 (11)
        #      121486802 (10)
        #      284541 (9)
        json_data['OC_OFFICER_UID'] = raw_data['person_uid']

        # columnName: end_date
        # 4.57 populated, 57.6 unique
        #      2019-12-04 (13)
        #      2018-04-25 (13)
        #      1994-09-29 (12)
        #      1995-12-01 (10)
        #      2004-04-22 (10)
        #payload_data['end_date'] = raw_data['end_date']

        # columnName: nationality
        # 3.42 populated, 4.39 unique
        #      BG (483)
        #      BRITISH (447)
        #      ENGLISH (17)
        #      RU (17)
        #      IT (16)
        json_data['NATIONALITY'] = raw_data['nationality']

        # columnName: country_of_residence
        # 0.93 populated, 7.07 unique
        #      UNITED KINGDOM (157)
        #      ENGLAND (99)
        #      NORTHERN IRELAND (7)
        #      GERMANY (5)
        #      WALES (4)
        json_data['COUNTRY_OF_ASSOCIATION'] = raw_data['country_of_residence']

        # columnName: partial_date_of_birth
        # 1.3 populated, 67.07 unique
        #      1972-03 (6)
        #      1961-09 (5)
        #      1965-05 (5)
        #      1970-05 (4)
        #      1966-07 (4)
        json_data['DATE_OF_BIRTH'] = self.format_dob(raw_data['partial_date_of_birth'])

        # log if parsed address is different than full address if both populated
        if raw_data['address.street_address'] and raw_data['address.in_full']:
            if raw_data['address.street_address'].upper() not in raw_data['address.in_full'].upper():
                self.update_stat('_FYI', 'ADDR1_NOT_IN_ADDR_FULL', json_data['RECORD_ID'])

        # columnName: address.in_full
        # 63.75 populated, 67.27 unique
        #      UNITED STATES (119)
        #      3300 Publix Corporate Parkway, Lakeland, FL, 33811 (51)
        #      400 CORNERSTONE DR #240, WILLISTON, VT, 05495, USA (47)
        #      27821 36TH AVE NW, STANWOOD, WA, 98292-9461, UNITED STATES (42)
        #      C/O THE BLACKSTONE GROUP, 345 PARK AVE. NEW YORK, NY 10154 USA (42)
        if raw_data['address.in_full']:
            self.update_stat('_FYI', 'ADDR_FULL_COUNT', record_id)
            json_data['PRIMARY_ADDR_FULL'] = self.remove_line_feeds(raw_data['address.in_full']).upper()
            json_data['PRIMARY_ADDR_COUNTRY'] = raw_data['address.country']


        elif raw_data['address.street_address'] or raw_data['address.locality'] or raw_data['address.region'] or \
             raw_data['address.postal_code'] or raw_data['address.country']:
            self.update_stat('_FYI', 'ADDR_PARSED_COUNT', json_data['RECORD_ID'])

            # columnName: address.street_address
            # 16.1 populated, 68.48 unique
            #      27821 36TH AVE NW (42)
            #      90 STATE STREET, SUITE 700, OFFICE 40 (22)
            #      PO BOX 27740 (18)
            #      450 VETERANS MEMORIAL PARKWAY, SUITE 7A (17)
            #      222 JEFFERSON BOULEVARD, SUITE 200 (15)
            json_data['PRIMARY_ADDR_LINE1'] = raw_data['address.street_address']

            # columnName: address.locality
            # 14.36 populated, 28.65 unique
            #      LAS VEGAS (240)
            #      LONDON (121)
            #      PORTLAND (83)
            #      NEW YORK (64)
            #      BOSTON, (49)
            json_data['PRIMARY_ADDR_CITY'] = raw_data['address.locality']

            # columnName: address.region
            # 11.74 populated, 3.04 unique
            #      WA (463)
            #      NV (446)
            #      OR (314)
            #      NEW YORK (251)
            #      MI (210)
            json_data['PRIMARY_ADDR_STATE'] = raw_data['address.region']

            # columnName: address.postal_code
            # 14.81 populated, 45.52 unique
            #      98292-9461 (42)
            #      89701 (24)
            #      02888 (23)
            #      12207 (23)
            #      99801 (21)
            json_data['PRIMARY_ADDR_POSTAL_CODE'] = raw_data['address.postal_code']

            # columnName: address.country
            # 6.45 populated, 1.41 unique
            #      USA (763)
            #      UNITED STATES (610)
            #      United States (252)
            #      NZ (165)
            #      UNITED KINGDOM (133)
            json_data['PRIMARY_ADDR_COUNTRY'] = raw_data['address.country']

        # columnName: retrieved_at
        # 100.0 populated, 24.93 unique
        #      2020-10-01 00:00:00 UTC (738)
        #      2020-11-22 18:30:00 UTC (579)
        #      2021-02-21 00:00:00 UTC (538)
        #      2021-03-03 09:10:18 UTC (462)
        #      2020-12-13 00:00:00 UTC (430)
        #if raw_data['retrieved_at']:  # stoped mapping as affected de-dupe without adding value
        #    payload_data['retrieved_at'] = raw_data['retrieved_at'][0:10]

        # columnName: source_url
        # 1.76 populated, 35.41 unique
        #      https://beta.companieshouse.gov.uk/company/05149111 (46)
        #      https://beta.companieshouse.gov.uk/company/SC227540 (10)
        #      https://beta.companieshouse.gov.uk/company/01278121 (10)
        #      https://beta.companieshouse.gov.uk/company/NI044930 (8)
        #      https://beta.companieshouse.gov.uk/company/05671230 (8)
        payload_data['source_url'] = raw_data['source_url']

        # add the payload data (original kept separate to analyze de-dupe with and without)
        json_data.update(payload_data)

        json_data = self.remove_empty_json_values(json_data)
        self.capture_mapped_stats(json_data)

        # point the officer to the company they work for
        rel_data = {'REL_POINTER_DOMAIN': 'OPENC',
                    'REL_POINTER_KEY': raw_data['company_number'] + '-' + raw_data['jurisdiction_code'],
                    'REL_POINTER_ROLE': raw_data['position'][0:50]} # some job titles (position) can be extra long
        json_data['RELATIONSHIPS'] = [rel_data]

        if raw_data['start_date']:
            rel_data['REL_POINTER_FROM_DATE'] = raw_data['start_date']
        if raw_data['end_date']:
            rel_data['REL_POINTER_THRU_DATE'] = raw_data['end_date']
        self.capture_mapped_stats(rel_data)

        return json_data


    def load_reference_data(self):

        # garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']


    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value


    def remove_line_feeds(self, raw_value):
        if not raw_value:
            return ''
        return raw_value.replace('\\n', ' ')


    def format_dob(self, raw_date):
        try: new_date = dateparse(raw_date)
        except: return ''

        # correct for prior century dates
        if new_date.year > datetime.now().year:
            new_date = datetime(new_date.year - 100, new_date.month, new_date.day)

        if len(raw_date) == 4:
            output_format = '%Y'
        elif len(raw_date) in (5,6):
            output_format = '%m-%d'
        elif len(raw_date) in (7,8):
            output_format = '%Y-%m'
        else:
            output_format = '%Y-%m-%d'

        return datetime.strftime(new_date, output_format)


    def remove_empty_json_values(self, value):
        """
        Recursively remove all None values from dictionaries and lists, and returns
        the result as a new dictionary or list.
        """
        if isinstance(value, list):
            return [self.remove_empty_json_values(val) for val in value if val is not None and len(str(val).strip()) > 0]
        elif isinstance(value, dict):
            return {
                key: self.remove_empty_json_values(val)
                for key, val in value.items()
                if val is not None and len(str(val).strip()) > 0
            }
        else:
            return value


    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example
        return


    def capture_mapped_stats(self, json_data):

        if 'DATA_SOURCE' in json_data:
            data_source = json_data['DATA_SOURCE']
        else:
            data_source = 'UNKNOWN_DSRC'

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(data_source, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(data_source, key2, subrecord[key2])

def safe_csv_next(reader, counter):
    while True:
        try:
            counter += 1
            return counter, next(reader)
        except StopIteration:
            return counter, None
        except Exception as err:
            counter += 1
            print(f"error: row {input_row_count} {err}")


def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return


if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    csv_dialect = 'excel'
    data_source = 'OPENC-OFFICER'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file_name', dest='input_file_name', help='the name of an open corporates csv file for officers')
    parser.add_argument('-o', '--output_file_name', dest='output_file_name', help='the name of the output file')
    parser.add_argument('-t', '--temp_database_name', dest='temp_database_name', help='the name for the temporary database required to de-dupe officers')
    parser.add_argument('-d', '--data_source', dest='data_source', default=data_source, help='the name of the data source code to use, defaults to: ' + data_source)
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    parser.add_argument('-U', '--use_existing_db', dest='use_existing_db', action='store_true', default=False, help='use existing database, skips step 1')
    args = parser.parse_args()

    if not args.output_file_name:
        print('\nPlease supply a valid output file name on the command line\n')
        sys.exit(1)

    if not args.input_file_name or not os.path.exists(args.input_file_name):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)

    if not args.temp_database_name:
        print('\nPlease supply a temporary database file name on the command line\n')
        sys.exit(1)

    if os.path.exists(args.temp_database_name) and not args.use_existing_db:
        os.remove(args.temp_database_name)
        #print(f"\n{dbname} already exists, please remove it first if you want to continue\n")
        #sys.exit(1)
    mapper = mapper()

    temp_dbo = make_database(args.temp_database_name, args.use_existing_db)
    temp_dbo_cursor = temp_dbo.cursor()

    input_row_count = 0
    output_row_count = 0

    file_name = args.input_file_name
    base_file_name, file_extension = os.path.splitext(file_name)
    compressed_file = file_extension.upper() == '.GZ'
    if compressed_file:
        base_file_name, file_extension = os.path.splitext(base_file_name)
        input_file_handle = gzip.open(file_name, 'r')
        csv_reader = csv.DictReader(io.TextIOWrapper(io.BufferedReader(input_file_handle), encoding='utf-8', errors='ignore'))
    else:
        input_file_handle = open(file_name, 'r')
        csv_reader = csv.DictReader(input_file_handle, dialect='excel')

    # for de-dupe
    hashes_mapped = {}

    # to cache multiple updates to same hash
    hash_duplicates = {}

    # step 1 - reading
    if not args.use_existing_db:
        print (f'\nStep 1: Mapping {file_name} ...\n')
        batch_start_time = time.time()

        input_row_count, input_row = safe_csv_next(csv_reader, input_row_count)
        while input_row:
            json_data = mapper.map(input_row)
            if json_data:

                # extract attributes for compression
                record_id = json_data['RECORD_ID']
                rel_data = json_data['RELATIONSHIPS'][0]
                base_json_data = dict(json_data)
                del base_json_data['RECORD_ID']
                del base_json_data['RELATIONSHIPS']
                record_hash = hashlib.md5(orjson.dumps(base_json_data, option=orjson.OPT_SORT_KEYS)).hexdigest()

                if record_hash not in hashes_mapped:
                    hashes_mapped[record_hash] = 1
                    temp_dbo_cursor.execute('INSERT INTO hashes VALUES (?, ?)', (record_hash, orjson.dumps(json_data).decode()))
                else:
                    if record_hash not in hash_duplicates: # cache updates
                        hash_duplicates[record_hash] = []
                    hash_duplicates[record_hash].append([record_id, rel_data])

            if input_row_count % 100000 == 0:
                temp_dbo.commit()
                batch_seconds = round(time.time() - batch_start_time, 1)
                total_minutes = round((time.time() - proc_start_time) / 60, 1)
                print(f"{input_row_count:,} rows read, {len(hashes_mapped):,} unique hashes processed after {total_minutes:,} minutes, batch rate {batch_seconds} seconds")
                batch_start_time = time.time()

            if shut_down:
                break

            input_row_count, input_row = safe_csv_next(csv_reader, input_row_count)

        temp_dbo.commit()
        elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
        run_status = ('completed in' if not shut_down else 'aborted after') + f" {elapsed_mins:,} minutes"
        print(f"{input_row_count:,} rows read, {len(hashes_mapped):,} unique hashes {run_status}\n")
        input_file_handle.close()

    # step 2 - output
    if True: #not shut_down (for testing you may want to stop step 1 and still generate json in step 2
        shut_down = False

        output_file_name = args.output_file_name
        if output_file_name.endswith('.gz'):
            output_file_handle = gzip.open(output_file_name, 'wb')
        else:
            output_file_handle = open(output_file_name, 'w', encoding='utf-8')

        print (f'\nStep 2: Writing {output_file_name} ...\n')
        write_start_time = time.time()
        batch_start_time = time.time()
        batch_records = []

        temp_dbo_cursor.execute('select hash, base_json from hashes')
        record = temp_dbo_cursor.fetchone()
        while record:
            record_hash = record[0]
            new_json_data = orjson.loads(record[1])

            record_list = [new_json_data['RECORD_ID']]
            relation_data = new_json_data['RELATIONSHIPS'][0]
            relations_by_role = {relation_data['REL_POINTER_ROLE']: [relation_data]}
            if record_hash in hash_duplicates:
                for additional_data in hash_duplicates[record_hash]:
                    record_list.append(additional_data[0])
                    relation_data = additional_data[1]
                    if relation_data['REL_POINTER_ROLE'] not in relations_by_role:
                        relations_by_role[relation_data['REL_POINTER_ROLE']] = []
                    relations_by_role[relation_data['REL_POINTER_ROLE']].append(relation_data)

            new_json_data['RECORD_ID'] = record_hash
            new_json_data['RECORD_COUNT'] = len(record_list)
            if new_json_data['RECORD_COUNT'] > 10000:
                mapper.update_stat('_FYI', f"RECORD_HASH>10000", f"{new_json_data['RECORD_ID']}={new_json_data['RECORD_COUNT']}")
            elif new_json_data['RECORD_COUNT'] > 1000:
                mapper.update_stat('_FYI', f"RECORD_HASH>1000", f"{new_json_data['RECORD_ID']}={new_json_data['RECORD_COUNT']}")
            elif new_json_data['RECORD_COUNT'] > 100:
                mapper.update_stat('_FYI', f"RECORD_HASH>100", f"{new_json_data['RECORD_ID']}={new_json_data['RECORD_COUNT']}")
            if new_json_data['RECORD_COUNT'] < 4:
                new_json_data['RECORD_IDS'] = ' | '.join(record_list)
            else:
                new_json_data['RECORD_IDS'] = ' | '.join(sorted(record_list[0:3])) + f" | + {len(record_list)-3} more"
            new_json_data['RECORD_LIST'] = [{'id': x} for x in record_list[0:max_records_per_entity]] # cap so egregious offenders don't slow down the system

            relation_list = []
            for role in relations_by_role:
                if len(relations_by_role[role]) > max_relationships_per_role:
                    mapper.update_stat('_FYI', f"ROLE-{role}-SUPPRESSED", f"{new_json_data['RECORD_ID']}={len(relations_by_role[role])}")
                    new_json_data[f"Suppressed {role} relationships"] = len(relations_by_role[role])
                else:
                    relation_list.extend(relations_by_role[role])
            if relation_list:
                new_json_data['RELATIONSHIPS'] = relation_list

            if output_file_name.endswith('.gz'):
                batch_records.append((orjson.dumps(new_json_data).decode()+'\n').encode('utf-8'))
            else:
                batch_records.append((orjson.dumps(new_json_data).decode()+'\n'))
            output_row_count += 1

            if output_row_count % 1000000 == 0:
                elapsed_seconds = round(time.time() - batch_start_time, 1)
                print(f"{output_row_count:,} rows written in {elapsed_seconds:,} seconds")
                batch_start_time = time.time()

            # separated from progress interval to create even more randomness
            if output_row_count % 10000000 == 0:
                random.shuffle(batch_records)
                output_file_handle.writelines(batch_records)
                batch_records = []

            if shut_down:
                break

            record = temp_dbo_cursor.fetchone()

        if batch_records:
            random.shuffle(batch_records)
            output_file_handle.writelines(batch_records)
        output_file_handle.close()

        elapsed_mins = round((time.time() - write_start_time) / 60, 1)
        run_status = ('completed in' if not shut_down else 'aborted after') + f" {elapsed_mins:,} minutes"
        print(f"{output_row_count:,} rows written {run_status}\n")

        elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
        print(f"process {('completed in' if not shut_down else 'aborted after')} {elapsed_mins:,} minutes\n")


    # write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            outfile.write(orjson.dumps(mapper.stat_pack, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS).decode())
        print('Mapping stats written to %s\n' % args.log_file)

    sys.exit(0)
