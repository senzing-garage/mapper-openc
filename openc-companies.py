#! /usr/bin/env python3

import sys
import os
import argparse
import time
import csv
import orjson
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import sqlite3
import gzip
import io
import hashlib

import multiprocessing
from queue import Empty, Full

class IOQueueProcessor():

    def __init__(self, input_class, output_class, **kwargs):

        self.process_count = kwargs.get('process_count', multiprocessing.cpu_count())
        self.all_stop = multiprocessing.Value('i', 0)

        self.input_class = input_class
        self.output_class = output_class
        self.input_queue = multiprocessing.Queue(self.process_count * 10)
        self.output_queue = multiprocessing.Queue(self.process_count * 10)

        self.input_queue_read_cnt = multiprocessing.Value('i', 0)
        self.output_queue_read_cnt = multiprocessing.Value('i', 0)

        self.kwargs = kwargs
        self.process_list = []

    def start_up(self):

        self.process_list.append(multiprocessing.Process(target=self.output_queue_reader, args=(0, self.output_queue, self.output_class), kwargs=self.kwargs))
        for process_number in range(self.process_count - 1):
            self.process_list.append(multiprocessing.Process(target=self.input_queue_reader, args=(process_number+1, self.input_queue, self.output_queue, self.input_class), kwargs=self.kwargs))
        for process in self.process_list:
            process.start()

    def finish_up(self):

        # wait for queues
        try:
            while self.input_queue.qsize() or self.output_queue.qsize():
                print(f"waiting for {self.input_queue.qsize()} input and {self.output_queue.qsize()} output queue records")
                time.sleep(1)
        except: # qsize does not work on mac
            while not self.input_queue.empty() or not self.output_queue.empty():
                if not self.input_queue.empty():
                    print("waiting for input queue to finish")
                elif not self.output_queue.empty():
                    print("waiting for output queue to finish")
                time.sleep(1)

        with self.all_stop.get_lock():
            self.all_stop.value = 1

        start = time.time()
        while time.time() - start <= 15:
            if not any(process.is_alive() for process in self.process_list):
                break
            time.sleep(1)

        for process in self.process_list:
            if process.is_alive():
                print(process.name, 'did not terminate gracefully')
                process.terminate()
            process.join()

        self.input_queue.close()
        self.output_queue.close()

    def queue_read(self, q):
        try:
            return q.get(True, 1)
        except Empty:
            return None

    def queue_write(self, q, msg):
        while True:
            try:
                q.put(msg, True, 1)
            except Full:
                continue
            break

    def input_queue_reader(self, process_number, input_queue, output_queue, function_ref, **kwargs):

        kwargs['process_number'] = process_number
        input_class = function_ref(**kwargs)

        while self.all_stop.value == 0:
            queue_data = self.queue_read(input_queue)
            if queue_data:
                with self.input_queue_read_cnt.get_lock():
                    self.input_queue_read_cnt.value += 1
                result = input_class.run(queue_data)
                if result:
                    self.queue_write(output_queue, result)

        input_class.close()

    def output_queue_reader(self, process_number, output_queue, function_ref, **kwargs):

        kwargs['process_number'] = process_number
        output_class = function_ref(**kwargs)

        while self.all_stop.value == 0:
            queue_data = self.queue_read(output_queue)
            if queue_data:
                with self.output_queue_read_cnt.get_lock():
                    self.output_queue_read_cnt.value += 1
                output_class.run(queue_data)

        output_class.close()

    def process(self, msg):
        self.queue_write(self.input_queue, msg)

    def get_input_queue_read_cnt(self):
        return self.input_queue_read_cnt.value

    def get_output_queue_read_cnt(self):
        return self.output_queue_read_cnt.value


class writer():

    def __init__(self, **kwargs):
        self.process_number = kwargs.get('process_number', -1)
        self.output_file_name = kwargs['output_file_name']
        self.log_file = kwargs.get('log_file', None)
        self.log_duplicates = kwargs.get('log_duplicates', None)
        self.proc_start_time = kwargs.get('proc_start_time', time.time())
        self.progress_interval = kwargs.get('progress_interval', 100000)

        print(f"process {self.process_number} opened {self.output_file_name}")

        if self.output_file_name.endswith('.gz'):
            self.output_file_handle = gzip.open(self.output_file_name, 'wb')
        else:
            self.output_file_handle = open(self.output_file_name, 'w', encoding='utf-8')

        self.stat_pack = {}
        self.output_row_count = 0
        self.batch_start_time = time.time()
        self.batch_records = []
        self.record_cache = {}

    def close(self):

        if self.batch_records:
            random.shuffle(self.batch_records)
            self.output_file_handle.writelines(self.batch_records)

        self.output_file_handle.close()
        print(f"process {self.process_number} closed {self.output_file_name}")

        # write statistics file
        if self.log_file:
            with open(self.log_file, 'w') as outfile:
                outfile.write(orjson.dumps(self.stat_pack, option=orjson.OPT_INDENT_2).decode())
            print('Mapping stats written to %s\n' % args.log_file)

        # this dumps the pure duplicates to a file for research
        if self.log_duplicates:
            dupes_1000_cnt = 0
            dupes_100_cnt = 0
            dupes_10_cnt = 0
            dupes_small = 0
            largest_dupe_cnt = 0
            largest_dupe_hash = ''
            with open('dup_hashes.csv','w') as outfile:
                for record_hash in self.record_cache:
                    if self.record_cache[record_hash]['cnt'] > 1:
                        print(f"{record_hash} | {self.record_cache[record_hash]['cnt']}", file=outfile)
                        if self.record_cache[record_hash]['cnt'] >= 1000:
                            dupes_1000_cnt += 1
                        elif self.record_cache[record_hash]['cnt'] >= 100:
                            dupes_100_cnt += 1
                        elif self.record_cache[record_hash]['cnt'] >= 10:
                            dupes_10_cnt += 1
                        else:
                            dupes_small += 1
                        if self.record_cache[record_hash]['cnt'] > largest_dupe_cnt:
                            largest_dupe_cnt = self.record_cache[record_hash]['cnt']
                            largest_dupe_hash = record_hash
            print('duplicate hashes written to dup_hashes.csv')
            print(f' hashes >= 1000     {dupes_1000_cnt}')
            print(f' hashes >= 100      {dupes_100_cnt}')
            print(f' hashes >= 10       {dupes_10_cnt}')
            print(f' hashes < 10        {dupes_small}')
            print(f'\n largest hash = "{largest_dupe_hash}" with {largest_dupe_cnt} records\n')


    def run(self, mapped_data):

        record_id, record_hash, json_data, payload_data, relationship_list, stat_update_list = mapped_data

        for stat_data in stat_update_list:
            self.update_stat(stat_data[0], stat_data[1], stat_data[2])

        new_json_data = {'DATA_SOURCE': json_data['DATA_SOURCE']}
        new_json_data['RECORD_ID'] = record_id
        new_json_data['OC_COMPANY_ID'] = record_id
        new_json_data.update(json_data)
        new_json_data['RELATIONSHIP_LIST'] = relationship_list
        new_json_data.update(payload_data)
        self.capture_mapped_stats(new_json_data)

        if self.output_file_name.endswith('.gz'):
            self.batch_records.append((orjson.dumps(new_json_data).decode()+'\n').encode('utf-8'))
        else:
            self.batch_records.append((orjson.dumps(new_json_data).decode()+'\n'))

        if self.log_duplicates:
            if record_hash not in self.record_cache:
                self.record_cache[record_hash] = {}
                self.record_cache[record_hash]['cnt'] = 1
                self.record_cache[record_hash]['ids'] = [record_id]
            else:
                self.record_cache[record_hash]['cnt'] += 1
                if len(self.record_cache[record_hash]['ids']) < 10:
                    self.record_cache[record_hash]['ids'].append(record_id)

        self.output_row_count += 1
        if self.output_row_count % self.progress_interval == 0:
            batch_seconds = round((time.time() - self.batch_start_time), 1)
            total_minutes = round((time.time() - self.proc_start_time) / 60, 1)
            print(f"{self.output_row_count:,} rows processed after {total_minutes:,} minutes, batch rate {batch_seconds} seconds")
            self.batch_start_time = time.time()

        # separated from progress interval to create even more randomness
        if self.output_row_count % 10000000 == 0:
            random.shuffle(self.batch_records)
            self.output_file_handle.writelines(self.batch_records)
            self.batch_records = []


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
                if len(self.stat_pack[cat1][cat2]['examples']) < 10:
                    self.stat_pack[cat1][cat2]['examples'].append(example)

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

class mapper():

    def __init__(self, **kwargs):
        self.process_number = kwargs.get('process_number', -1)
        self.data_source = kwargs['data_source']
        self.child_table_list = kwargs['child_table_list']
        self.child_database_name = kwargs['child_database_name']
        self.dbo = sqlite3.connect(f'file:{self.child_database_name}?mode=ro', uri=True)
        print(f"process {self.process_number} opened {self.child_database_name}")

    def close(self):
        self.dbo.close()
        print(f"process {self.process_number} closed {self.child_database_name}")

    def run(self, raw_data):
        json_data = {}
        payload_data = {}
        stat_update_list = []

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--for list of previous and alt names
        other_names_list = []

        #--mandatory attributes
        json_data['DATA_SOURCE'] = self.data_source
        #--record type replaces entity type helps keeps persons from resolving to companies
        json_data['RECORD_TYPE'] = 'ORGANIZATION'

        #--the record_id should be unique, remove this mapping if there is not one
        record_id = raw_data['company_number'] + '-' + raw_data['jurisdiction_code']

        #--column mappings
        # columnName: company_number
        # 100.0 populated, 97.42 unique
        #      1 (9)
        #      10 (7)
        #      3 (6)
        #      100 (5)
        #      11 (5)
        #json_data['company_number'] = raw_data['company_number']

        # columnName: jurisdiction_code
        # 100.0 populated, 0.5 unique
        #      us_tx (200)
        #      us_nd (200)
        #      us_tn (200)
        #      br (200)
        #      pl (200)
        #json_data['jurisdiction_code'] = raw_data['jurisdiction_code']

        # columnName: name
        # 100.0 populated, 99.63 unique
        #      GRUPO RHJ DE LA LAGUNA S.A. DE C.V. (4)
        #      A & C, INC. (3)
        #      Public Consulting Group Holdings, Inc. (3)
        #      1 800 TOW TRUCK, INC. (3)
        #      NEW VALLEY CORPORATION (3)
        json_data['PRIMARY_NAME_ORG'] = raw_data['name'][0:250].upper()  # note that all additional names will go into the other names list array for de-dupe and standardization

        # columnName: normalised_name
        # 100.0 populated, 99.38 unique
        #      public consulting group holdings incorporated (4)
        #      grupo rhj de la laguna sa de cv (4)
        #      a and c incorporated (3)
        #      shiftpixy staffing incorporated (3)
        #      1 800 tow truck incorporated (3)
        if raw_data['normalised_name']:
            other_names_list = [{'NAME_TYPE': 'NORMALIZED', 'NAME_ORG': raw_data['normalised_name']}]

        # columnName: company_type
        # 86.82 populated, 2.97 unique
        #      Domestic Limited Liability Company (1078)
        #      Limited Liability Company (919)
        #      DOMESTIC LIMITED LIABILITY COMPANY (433)
        #      Corporation (386)
        #      Domestic Profit Corporation (296)
        payload_data['company_type'] = raw_data['company_type']

        # columnName: nonprofit
        # 100.0 populated, 0.01 unique
        #      false (25125)
        #      true (1075)
        payload_data['nonprofit'] = 'Yes' if raw_data['nonprofit'].upper() == 'TRUE' else ''

        # columnName: current_status
        # 73.93 populated, 1.77 unique
        #      Active (5062)
        #      Good Standing (1118)
        #      Registered (1093)
        #      Dissolved (920)
        #      Inactive (667)
        payload_data['current_status'] = raw_data['current_status']

        # columnName: incorporation_date
        # 73.88 populated, 37.47 unique
        #      2021-02-26 (924)
        #      2021-02-25 (612)
        #      2021-02-18 (279)
        #      2021-02-24 (213)
        #      2021-02-22 (200)
        json_data['REGISTRATION_DATE'] = raw_data['incorporation_date']

        # columnName: dissolution_date
        # 12.69 populated, 66.55 unique
        #      2031-02-27 (90)
        #      2022-07-15 (81)
        #      2021-03-01 (35)
        #      1969-12-31 (23)
        #      2022-07-30 (22)
        payload_data['dissolution_date'] = raw_data['dissolution_date']

        # columnName: branch
        # 6.85 populated, 0.11 unique
        #      F (1771)
        #      L (24)
        payload_data['branch'] = raw_data['branch']

        # columnName: business_number
        # 0.76 populated, 100.0 unique
        #      42007284273 (1)
        #      53053806241 (1)
        #      95056184206 (1)
        #      78057121230 (1)
        #      23057161574 (1)
        payload_data['business_number'] = raw_data['business_number']

        # columnName: current_alternative_legal_name
        # 4.16 populated, 99.73 unique
        #      STE TUNISO-ITALIENNE GELATTERIE-PATISSERIE (2)
        #      CANTUNIS (2)
        #      လမ်းသစ်ဆန်းကုမ္ပဏီလီမိတက် (2)
        #      អ៊ែតវ៉ានស៍ សឺវេ អ៉ិនស្ទ្រូម៉ិន​ ( អេអេសអាយ ) (1)
        #      ជេប៊ីអិល មេគង្គ ខូ អិលធីឌី (1)
        if raw_data['current_alternative_legal_name']:
            other_names_list = [{'NAME_TYPE': 'CURRENT-LEGAL', 'NAME_ORG': raw_data['current_alternative_legal_name']}]

        # columnName: current_alternative_legal_name_language
        # 4.02 populated, 0.67 unique
        #      en (349)
        #      my (199)
        #      km (189)
        #      zh (168)
        #      fr (102)
        payload_data['current_alternative_legal_name_language'] = raw_data['current_alternative_legal_name_language']

        # columnName: home_jurisdiction_text
        # 14.09 populated, 8.72 unique
        #      FL (207)
        #      NV (197)
        #      KS (179)
        #      IRELAND (175)
        #      New Mexico (161)
        payload_data['home_jurisdiction_text'] = raw_data['home_jurisdiction_text']

        # columnName: native_company_number
        # 8.74 populated, 94.36 unique
        #      1 (34)
        #      2 (26)
        #      4 (17)
        #      3 (15)
        #      5 (15)
        payload_data['native_company_number'] = raw_data['native_company_number']

        # columnName: previous_names
        # 4.33 populated, 99.47 unique
        #      Zubair Furnishing (Europe) Limited (2)
        #      Zulu Limited (2)
        #      Zurich Investments Limited (2)
        #      Zurich Enterprises Limited (2)
        #      Z ENTERPRISES, LLC (2)
        if raw_data['previous_names']:
            for name_org in raw_data['previous_names'].split('|'):
                if not name_org:
                    continue
                other_names_list.append({'NAME_TYPE': 'PREVIOUS', 'NAME_ORG': name_org})

        # columnName: retrieved_at
        # 100.0 populated, 73.84 unique
        #      2018-08-02 18:30:00 UTC (200)
        #      2021-02-05 00:00:00 UTC (200)
        #      2020-11-22 18:30:00 UTC (200)
        #      2021-02-26 00:00:00 UTC (199)
        #      2020-12-13 00:00:00 UTC (198)
        payload_data['retrieved_at'] = raw_data['retrieved_at']

        # columnName: registry_url
        # 58.44 populated, 96.5 unique
        #      http://dati.ur.gov.lv/ (200)
        #      https://data.brreg.no/enhetsregisteret/api/enheter/lastned (103)
        #      http://www.moic.gov.bh/CReServices/inquiry/javascript:__doPostBack('grdCR$ctl02$lnkCrBr','') (61)
        #      http://www.moic.gov.bh/CReServices/inquiry/javascript:__doPostBack('grdCR$ctl03$lnkCrBr','') (52)
        #      http://www.moic.gov.bh/CReServices/inquiry/javascript:__doPostBack('grdCR$ctl04$lnkCrBr','') (41)
        payload_data['registry_url'] = raw_data['registry_url']

        # columnName: restricted_for_marketing
        # 0.93 populated, 0.41 unique
        #      true (244)
        payload_data['restricted_for_marketing'] = 'Yes' if raw_data['restricted_for_marketing'].upper() == 'TRUE' else ''

        # columnName: inactive
        # 75.51 populated, 0.01 unique
        #      false (13688)
        #      true (6096)
        payload_data['inactive'] = 'Yes' if raw_data['inactive'].upper() == 'TRUE' else ''

        # columnName: accounts_next_due
        # 0.47 populated, 13.82 unique
        #      2022-12-01 (100)
        #      2005-01-31 (3)
        #      2021-07-31 (3)
        #      2021-12-31 (2)
        #      2021-09-30 (2)
        payload_data['accounts_next_due'] = raw_data['accounts_next_due']

        # columnName: accounts_reference_date
        # 0.76 populated, 13.0 unique
        #      03-31 (116)
        #      31-01 (9)
        #      30-04 (8)
        #      31-12 (8)
        #      30-06 (6)
        payload_data['accounts_reference_date'] = raw_data['accounts_reference_date']

        # columnName: accounts_last_made_up_date
        # 0.29 populated, 76.32 unique
        #      2003-03-31 (3)
        #      2007-12-31 (3)
        #      2008-01-31 (3)
        #      2017-03-31 (3)
        #      2019-08-31 (2)
        payload_data['accounts_last_made_up_date'] = raw_data['accounts_last_made_up_date']

        # columnName: annual_return_next_due
        # 0.77 populated, 41.79 unique
        #      2021-06-10 (100)
        #      2020-09-30 (8)
        #      2021-09-30 (7)
        #      2015-09-30 (3)
        #      2020-12-31 (2)
        payload_data['annual_return_next_due'] = raw_data['annual_return_next_due']

        # columnName: annual_return_last_made_up_date
        # 0.73 populated, 90.0 unique
        #      2019-09-30 (5)
        #      2020-09-30 (3)
        #      2014-09-30 (3)
        #      2004-02-18 (2)
        #      2007-04-26 (2)
        payload_data['annual_return_last_made_up_date'] = raw_data['annual_return_last_made_up_date']

        # columnName: has_been_liquidated
        # 0.18 populated, 4.35 unique
        #      false (38)
        #      true (8)
        payload_data['has_been_liquidated'] = 'Yes' if raw_data['has_been_liquidated'].upper() == 'TRUE' else ''

        # columnName: has_insolvency_history
        # 0.53 populated, 1.43 unique
        #      false (129)
        #      true (11)
        payload_data['has_insolvency_history'] = 'Yes' if raw_data['has_insolvency_history'].upper() == 'TRUE' else ''

        # columnName: has_charges
        # 0.52 populated, 1.46 unique
        #      false (131)
        #      true (6)
        payload_data['has_charges'] = 'Yes' if raw_data['has_charges'].upper() == 'TRUE' else ''


        # columnName: number of employees
        payload_data['number_of_employees'] = raw_data['number_of_employees']


        # log if parsed address is different than full address if both populated
        if raw_data['registered_address.street_address'] and raw_data['registered_address.in_full']:
            if raw_data['registered_address.street_address'].upper() not in raw_data['registered_address.in_full'].upper():
                stat_update_list.append(['_FYI', 'REGISTERED_ADDR1_NOT_IN_ADDR_FULL', record_id])

        registered_address_for_dedupe = {}
        if raw_data['registered_address.in_full']:
            stat_update_list.append(['_FYI', 'REGISTERED_ADDR_FULL_COUNT', record_id])

            # columnName: registered_address.in_full
            # 58.55 populated, 90.23 unique
            #      PROVINCIA PANAMÁ, Panama (90)
            #      CORREGIMIENTO CIUDAD DE PANAMÁ, DISTRITO PANAMÁ, PROVINCIA PANAMÁ, Panama (69)
            #      BUITENLAND, Aruba (67)
            #      KUALA LUMPUR, WILAYAH PERSEKUTUAN, Malaysia (37)
            #      DISTRITO PANAMÁ, PROVINCIA PANAMÁ, Panama (36)
            json_data['REGISTERED_ADDR_FULL'] = remove_line_feeds(raw_data['registered_address.in_full']).upper()
            json_data['REGISTERED_ADDR_COUNTRY'] = remove_line_feeds(raw_data['registered_address.country']).upper()  #--can help determine address parsing rules
            registered_address_for_dedupe['ADDR_FULL'] = json_data['REGISTERED_ADDR_FULL']
            registered_address_for_dedupe['ADDR_COUNTRY'] = json_data['REGISTERED_ADDR_COUNTRY']

        elif raw_data['registered_address.street_address'] or raw_data['registered_address.locality'] or raw_data['registered_address.region'] or \
             raw_data['registered_address.postal_code'] or raw_data['registered_address.country']:
            stat_update_list.append(['_FYI', 'REGISTERED_ADDR_PARSED_COUNT', record_id])

            # columnName: registered_address.street_address
            # 57.45 populated, 90.21 unique
            #      PROVINCIA PANAMÁ (90)
            #      CORREGIMIENTO CIUDAD DE PANAMÁ, DISTRITO PANAMÁ, PROVINCIA PANAMÁ (69)
            #      BUITENLAND (67)
            #      DISTRITO PANAMÁ, PROVINCIA PANAMÁ (36)
            #      NEW ORLEANS, LA 70150 (34)
            json_data['REGISTERED_ADDR_LINE1'] = remove_line_feeds(raw_data['registered_address.street_address'].upper())
            registered_address_for_dedupe['ADDR_LINE1'] = json_data['REGISTERED_ADDR_LINE1']

            # columnName: registered_address.locality
            # 20.17 populated, 47.25 unique
            #      VALLETTA (91)
            #      ΑΤΤΙΚΗΣ (86)
            #      กรุงเทพมหานคร (78)
            #      DOUGLAS (76)
            #      St. John's (73)
            json_data['REGISTERED_ADDR_CITY'] = remove_line_feeds(raw_data['registered_address.locality']).upper()
            registered_address_for_dedupe['ADDR_CITY'] = json_data['REGISTERED_ADDR_CITY']

            # columnName: registered_address.region
            # 13.94 populated, 16.41 unique
            #      NC (204)
            #      Missouri (184)
            #      WA (161)
            #      Virginia (143)
            #      MA (142)
            json_data['REGISTERED_ADDR_STATE'] = remove_line_feeds(raw_data['registered_address.region']).upper()
            registered_address_for_dedupe['ADDR_STATE'] = json_data['REGISTERED_ADDR_STATE']

            # columnName: registered_address.postal_code
            # 19.49 populated, 72.0 unique
            #      10000 (79)
            #      JE4 9WG (39)
            #      27615 (23)
            #      34100 (21)
            #      28210 (17)
            json_data['REGISTERED_ADDR_POSTAL_CODE'] = remove_line_feeds(raw_data['registered_address.postal_code']).upper()
            registered_address_for_dedupe['ADDR_POSTAL_CODE'] = json_data['REGISTERED_ADDR_POSTAL_CODE']

            # columnName: registered_address.country
            # 58.29 populated, 0.46 unique
            #      United States (4569)
            #      Canada (549)
            #      USA (335)
            #      UNITED STATES (298)
            #      Ukraine (200)
            json_data['REGISTERED_ADDR_COUNTRY'] = remove_line_feeds(raw_data['registered_address.country']).upper()
            registered_address_for_dedupe['ADDR_COUNTRY'] = json_data['REGISTERED_ADDR_COUNTRY']

        # columnName: home_jurisdiction_code
        # 3.83 populated, 5.88 unique
        #      us_de (248)
        #      us_ny (57)
        #      us_ca (56)
        #      us_fl (49)
        #      us_tx (43)
        payload_data['home_jurisdiction_code'] = raw_data['home_jurisdiction_code']

        # columnName: home_jurisdiction_company_number
        # 3.83 populated, 96.71 unique
        #      2021-000973162 (3)
        #      802380338 (3)
        #      0100743157 (3)
        #      9860 (3)
        #      112 (3)
        payload_data['home_jurisdiction_company_number'] = raw_data['home_jurisdiction_company_number']

        # columnName: industry_code_uids
        # 6.86 populated, 56.9 unique
        #      gl_gb_2000-050105 (57)
        #      us_naics_2007-9999 (44)
        #      me_kd_2010-4719|eu_nace_2-4719|isic_4-4719|eu_nace_2-471|eu_nace_2-47|eu_nace_2-G|isic_4-471|isic_4-47|isic_4-G (40)
        #      th_tsic_2009-41002|isic_4-4100|isic_4-410|isic_4-41|isic_4-F (31)
        #      gl_gb_2000-050100 (27)
        payload_data['industry_code_uids'] = raw_data['industry_code_uids']

        # columnName: latest_accounts_date
        # 0.58 populated, 20.53 unique
        #      2020-12-31 (43)
        #      2014-12-31 (31)
        #      2020-09-30 (20)
        #      2016-12-31 (8)
        #      2015-12-31 (5)
        payload_data['latest_accounts_date'] = raw_data['latest_accounts_date']

        # columnName: latest_accounts_cash
        # 0.02 populated, 100.0 unique
        #      1425 (1)
        #      65 (1)
        #      50793 (1)
        #      549111 (1)
        payload_data['latest_accounts_cash'] = raw_data['latest_accounts_cash']

        # columnName: latest_accounts_assets
        # 0.49 populated, 89.92 unique
        #      0 (4)
        #      2376195 (4)
        #      550 (3)
        #      1000 (2)
        #      82485 (2)
        payload_data['latest_accounts_assets'] = raw_data['latest_accounts_assets']

        # columnName: latest_accounts_liabilities
        # 0.02 populated, 100.0 unique
        #      582551 (1)
        #      689639 (1)
        #      7930 (1)
        #      662 (1)
        #      897482 (1)
        payload_data['latest_accounts_liabilities'] = raw_data['latest_accounts_liabilities']

        # alias name child table
        if 'alias' in self.child_table_list:
            sql = 'select * from alias where company_number = ? and jurisdiction_code = ?'
            for record in sql_fetch_all(sql_exec(self.dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
                if not record['NAME']:
                    continue
                name_type = 'ALIAS' if not record['TYPE'] else record['TYPE'].upper()
                stat_update_list.append(['_FYI', 'NAME_TYPES', name_type])
                other_names_list.append({'NAME_TYPE': name_type, 'NAME_ORG': record['NAME']})

        #--add the accumulated other names, truncating any super long ones, and getting rid of any duplicates
        if other_names_list:
            dedupe_names_list = []
            dedupe_names_list.append(raw_data['name'].upper())
            corrected_name_list = []
            for other_name_data in other_names_list:
                name_org = other_name_data['NAME_ORG']
                if len(name_org.split()) > 16:
                    stat_update_list.append(['_FYI', 'longNameCnt', record_id + ' | ' + other_name_data['NAME_ORG']])
                    name_org = ' '.join(name_org.split()[:16])
                if name_org.upper() in dedupe_names_list:
                    stat_update_list.append(['_FYI', 'DUPLICATE_NAME_IGNORED', record_id + ' | ' + other_name_data['NAME_ORG']])
                    continue
                other_name_data['NAME_ORG'] = name_org
                dedupe_names_list.append(name_org.upper())
                corrected_name_list.append(other_name_data)
            if corrected_name_list:
                json_data['OTHER_NAMES'] = corrected_name_list

        #--registered address above, plus child file addresses
        if 'address' in self.child_table_list:
            dedupe_addrs_list = [orjson.dumps(registered_address_for_dedupe, option=orjson.OPT_SORT_KEYS)]
            addr_list = []
            sql = 'select * from address where company_number = ? and jurisdiction_code = ?'
            for addr_record in sql_fetch_all(sql_exec(self.dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
                addr_type = 'UNKNOWN' if not addr_record['ADDRESS_TYPE'] else addr_record['ADDRESS_TYPE'].upper()
                stat_update_list.append(['_FYI', 'ADDRESS_TYPES', addr_type])

                # log if parsed address is different than full address if both populated
                if addr_record['STREET_ADDRESS'] and addr_record['IN_FULL']:
                    stat_update_list.append(['_FYI', 'NON_REG_ADDR_FULL_COUNT', record_id])
                    if addr_record['STREET_ADDRESS'].upper() not in addr_record['IN_FULL'].upper():
                        stat_update_list.append(['_FYI', 'NONREG_ADDR1_NOT_IN_ADDR_FULL', record_id])
                else:
                    stat_update_list.append(['_FYI', 'NON_REG_ADDR_PARSED_COUNT', record_id])

                addr_data = {}
                if addr_record['IN_FULL']:
                    addr_data['ADDR_FULL'] = remove_line_feeds(addr_record['IN_FULL']).upper()
                    addr_data['ADDR_COUNTRY'] = remove_line_feeds(addr_record['COUNTRY']).upper()
                else:
                    addr_data['ADDR_LINE1'] = remove_line_feeds(addr_record['STREET_ADDRESS']).upper()
                    addr_data['ADDR_CITY'] = remove_line_feeds(addr_record['LOCALITY']).upper()
                    addr_data['ADDR_STATE'] = remove_line_feeds(addr_record['REGION']).upper()
                    addr_data['ADDR_POSTAL_CODE'] = remove_line_feeds(addr_record['POSTAL_CODE']).upper()
                    addr_data['ADDR_COUNTRY'] = remove_line_feeds(addr_record['COUNTRY']).upper()

                if orjson.dumps(addr_data, option=orjson.OPT_SORT_KEYS).decode() in dedupe_addrs_list:
                    stat_update_list.append(['_FYI', 'DUPLICATE_ADDR_IGNORED', record_id + ' | ' + orjson.dumps(addr_data).decode()])
                    continue

                addr_data['ADDR_TYPE'] = addr_type.upper()
                addr_list.append(addr_data)

            if addr_list:
                json_data['NON_REG_ADDRESSES'] = addr_list
                if len(addr_list) > 1:
                    stat_update_list.append(['_FYI', 'HAS_MULTIPLE_NON_REG_ADDRS', record_id])

        additional_list = []

        # identifier child table
        if 'identifier' in self.child_table_list:
            sql = 'select * from identifier where company_number = ? and jurisdiction_code = ?'
            for record in sql_fetch_all(sql_exec(self.dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
                if not record['UID']:
                    continue
                identifier_data = {}
                if record['IDENTIFIER_SYSTEM_CODE'].endswith('bn'):
                    identifier_data['NATIONAL_ID_NUMBER'] = record['UID']
                    stat_update_list.append(['_FYI', 'IDENTIFIER_TYPES', 'BN=NATIONAL_ID'])
                elif record['IDENTIFIER_SYSTEM_CODE'].endswith('tin'):
                    identifier_data['TAX_ID_NUMBER'] = record['UID']
                    stat_update_list.append(['_FYI', 'IDENTIFIER_TYPES', 'TIN=TAX_ID'])
                else:
                    identifier_data['OTHER_ID_TYPE'] = record['IDENTIFIER_SYSTEM_CODE']
                    identifier_data['OTHER_ID_NUMBER'] = record['UID']
                    stat_update_list.append(['_FYI', 'IDENTIFIER_TYPES', record['IDENTIFIER_SYSTEM_CODE'] + '=TAX_ID'])
                if identifier_data not in additional_list:  #--lot of dupes in here!
                    additional_list.append(identifier_data)

        # telephone child table (eventually convert their type field)
        if 'telephone' in self.child_table_list:
            sql = 'select * from telephone where company_number = ? and jurisdiction_code = ?'
            for record in sql_fetch_all(sql_exec(self.dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
                if not record['NUMBER']:
                    continue
                phone_data = {"PHONE_NUMBER": record['NUMBER']}
                if phone_data not in additional_list:
                    additional_list.append(phone_data)

        # website child table
        if 'website' in self.child_table_list:
            website_list = []
            sql = 'select * from website where company_number = ? and jurisdiction_code = ?'
            for record in sql_fetch_all(sql_exec(self.dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
                if not record['URL']:
                    continue
                website_data = {"WEBSITE_ADDRESS": record['URL']}
                if website_data not in additional_list:
                    additional_list.append(website_data)

        if additional_list:
            json_data['ADDITIONAL_DATA'] = additional_list


        # compute record hash with this data
        json_data = remove_empty_json_values(json_data)
        base_json_string = orjson.dumps(json_data, option=orjson.OPT_SORT_KEYS).decode()
        record_hash = hashlib.md5(bytes(base_json_string, 'utf-8')).hexdigest()

        #--create the relationship anchor for officers/and headquarters
        relationship_list = [{'REL_ANCHOR_DOMAIN': 'OPENC', 'REL_ANCHOR_KEY': record_id}]

        #--create the relationship pointer from branches to their headquarters
        if raw_data['home_jurisdiction_company_number']:
            relationship_data = {}
            relationship_data['REL_POINTER_KEY'] = raw_data['home_jurisdiction_company_number'] + '-' + raw_data['home_jurisdiction_code']
            relationship_data['REL_POINTER_DOMAIN'] = "OPENC"
            relationship_data['REL_POINTER_ROLE'] = "BRANCH_OF"
            relationship_list.append(relationship_data)

        payload_data = remove_empty_json_values(payload_data)

        return record_id, record_hash, json_data, payload_data, relationship_list, stat_update_list


def clean_value(raw_value):
    if not raw_value:
        return ''
    new_value = ' '.join(str(raw_value).strip().split())
    if new_value.upper() in ['NULL', 'NUL', 'N/A']:
        return ''
    return new_value


def remove_line_feeds(raw_value):
    if not raw_value:
        return ''
    return raw_value.replace('\\n', ' ')


def remove_empty_json_values(value):
    """
    Recursively remove all None values from dictionaries and lists, and returns
    the result as a new dictionary or list.
    """
    if isinstance(value, list):
        return [remove_empty_json_values(val) for val in value if val is not None and len(str(val).strip()) > 0]
    elif isinstance(value, dict):
        return {
            key: remove_empty_json_values(val)
            for key, val in value.items()
            if val is not None and len(str(val).strip()) > 0
        }
    else:
        return value



def sql_exec(dbo, sql, parm_list=None):
    try:
        exec_cursor = dbo.cursor()
        if parm_list:
            if type(parm_list) not in (list, tuple):
                parm_list = [parm_list]
            exec_cursor.execute(sql, parm_list)
        else:
            exec_cursor.execute(sql)
    except Exception as err:
        raise Exception(err)

    cursor_data = {}
    if exec_cursor:
        cursor_data['SQL'] = sql
        cursor_data['CURSOR'] = exec_cursor
        cursor_data['ROWS_AFFECTED'] = exec_cursor.rowcount
        if exec_cursor.description:
            cursor_data['FIELD_LIST'] = [field_data[0].upper() for field_data in exec_cursor.description]
    return cursor_data


def sql_fetch_next(cursor_data):
    row = cursor_data['CURSOR'].fetchone()
    if row:
        type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
        return dict(list(zip(cursor_data['FIELD_LIST'], type_fixed_row)))


def sql_fetch_all(cursor_data):
    ''' fetch all the rows with column names '''
    row_list = []
    for row in cursor_data['CURSOR'].fetchall():
        type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
        row_dict = dict(list(zip(cursor_data['FIELD_LIST'], type_fixed_row)))
        row_list.append(row_dict)
    return row_list


def safe_csv_next(reader, counter):
    while True:
        try:
            counter += 1
            return next(reader), counter
        except StopIteration:
            return None, counter
        except Exception as err:
            counter += 1
            print(f"error: row {input_row_count} {err}")


def format_statistic(amt):
    amt = int(amt)
    if amt > 1000000:
        return "{:,.2f}m".format(round(amt / 1000000, 2))
    else:
        return "{:,}".format(amt)


def signal_handler(signal, frame):
    global shut_down
    shut_down = 9


if __name__ == "__main__":
    shut_down = 0
    signal.signal(signal.SIGINT, signal_handler)

    csv_dialect = 'excel'
    data_source = 'OPENC-COMPANY'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file_name', dest='input_file_name', help='the name of an open corporates csv file for companies')
    parser.add_argument('-o', '--output_file_name', dest='output_file_name', help='the name of the output file')
    parser.add_argument('-c', '--child_database_name', dest='child_database_name', help='the name of the child database created in the prior step')
    parser.add_argument('-d', '--data_source', dest='data_source', default=data_source, help='the name of the data source code to use, defaults to: ' + data_source)
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    parser.add_argument('-w', '--max_workers', type=int, help='defaults to system processors, may need to reduce if running other things at same time')
    parser.add_argument('-D', '--log_duplicates', dest='log_duplicates', action='store_true', default=False, help='perform duplicate analysis')
    args = parser.parse_args()

    if not args.output_file_name:
        print('\nPlease supply a valid output file name on the command line\n')
        sys.exit(1)

    if not args.input_file_name or not os.path.exists(args.input_file_name):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)

    if not args.child_database_name or not os.path.exists(args.child_database_name):
        print('\nPlease supply a child database file name on the command line\n')
        sys.exit(1)

    child_dbo = sqlite3.connect(f'file:{args.child_database_name}?mode=ro', uri=True, check_same_thread=False)
    child_table_list = [x[0] for x in child_dbo.cursor().execute("select name from sqlite_master where type='table'").fetchall()]
    if 'finished' not in child_table_list:
        print('\nThe child database load is not complete.  The make-openc-child-db.py process must first run to successful completion.')
        sys.exit(1)
    child_dbo.close()

    progress_interval = 100000
    proc_start_time = time.time()
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

    output_file_name = args.output_file_name
    print (f'\nMapping {file_name} into {output_file_name} ...\n')

    kwargs = {'data_source': args.data_source,
              'child_database_name': args.child_database_name,
              'child_table_list': child_table_list,
              'output_file_name': output_file_name,
              'log_file': args.log_file,
              'log_duplicates': args.log_duplicates,
              'progress_interval': progress_interval,
              'proc_start_time': proc_start_time}
    if args.max_workers:
        kwargs['process_count'] = args.max_workers

    queue_processor = IOQueueProcessor(mapper, writer, **kwargs)
    print(f"\nstarting {queue_processor.process_count} processes\n")
    queue_processor.start_up()

    input_row, input_row_count = safe_csv_next(csv_reader, input_row_count)
    while input_row:

        queue_processor.process(input_row)
        input_row, input_row_count = safe_csv_next(csv_reader, input_row_count)
        if shut_down:
            break

    queue_processor.finish_up()
    input_file_handle.close()

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + f' {elapsed_mins:,} minutes'
    print(f"{input_row_count:,} rows processed {run_status}\n")


    sys.exit(0)

