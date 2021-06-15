import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random


#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

        self.field_names = ['id',
                            'company_number',
                            'jurisdiction_code',
                            'name',
                            'title',
                            'first_name',
                            'last_name',
                            'position',
                            'start_date',
                            'person_number',
                            'person_uid',
                            'end_date',
                            'current_status',
                            'occupation',
                            'nationality',
                            'country_of_residence',
                            'partial_date_of_birth',
                            'type',
                            'address.in_full',
                            'address.street_address',
                            'address.locality',
                            'address.region',
                            'address.postal_code',
                            'address.country',
                            'retrieved_at',
                            'source_url']

    #----------------------------------------
    def map(self, raw_data):
        json_data = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--mandatory attributes
        json_data['DATA_SOURCE'] = args.data_source
        json_data['ENTITY_TYPE'] = 'GENERIC'

        #--record type replaces entity type helps keeps persons from resolving to companies
        # columnName: type
        # 13.09 populated, 0.05 unique
        #      Person (3764)
        #      Company (419)
        if raw_data['type'].upper() == 'COMPANY':
            json_data['RECORD_TYPE'] = 'ORGANIZATION'
        else:
            json_data['RECORD_TYPE'] = 'PERSON'

        #--the record_id should be unique, remove this mapping if there is not one 
        json_data['RECORD_ID'] = raw_data['id']

        #--column mappings

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
        json_data['position'] = raw_data['position']

        #--point the officer to the company they work for
        json_data['REL_POINTER_DOMAIN'] = 'OPENC'
        json_data['REL_POINTER_KEY'] = raw_data['company_number'] + '-' + raw_data['jurisdiction_code']
        json_data['REL_POINTER_ROLE'] = raw_data['position'][0:50] #--some job titles (position) can be extra long

        # columnName: name
        # 99.96 populated, 71.24 unique
        #      C T CORPORATION SYSTEM (290)
        #      CORPORATION SERVICE COMPANY (200)
        #      REGISTERED AGENTS INC (137)
        #      Registered Agents Inc (96)
        #      LEGALINC CORPORATE SERVICES INC (82)
        if json_data['RECORD_TYPE'] == 'ORGANIZATION':
            json_data['PRIMARY_NAME_ORG'] = raw_data['name']
            self.update_stat('_FYI', 'NAME_ORG_CNT', json_data['RECORD_ID'])
        else:

            #--use full name if parsed name not populated
            if raw_data['name'] and not raw_data['last_name']:
                json_data['PRIMARY_NAME_FULL'] = raw_data['name']
                self.update_stat('_FYI', 'NAME_FULL_CNT', json_data['RECORD_ID'])

            elif raw_data['first_name'] or raw_data['last_name']:
                self.update_stat('_FYI', 'NAME_PARSED_CNT', json_data['RECORD_ID'])

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
                self.update_stat('_FYI', 'NAME_MISSING_CNT', json_data['RECORD_ID'])

        # columnName: occupation
        # 1.16 populated, 28.73 unique
        #      DIRECTOR (115)
        #      COMPANY DIRECTOR (41)
        #      CONSULTANT (20)
        #      ACCOUNTANT (14)
        #      SECRETARY (11)
        json_data['occupation'] = raw_data['occupation']

        # columnName: start_date
        # 14.06 populated, 32.55 unique
        #      2021-02-25 (220)
        #      2021-03-01 (144)
        #      2019-03-05 (103)
        #      2016-03-21 (90)
        #      2012-08-06 (83)
        json_data['start_date'] = raw_data['start_date']

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
        json_data['end_date'] = raw_data['end_date']

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
        json_data['country_of_residence'] = raw_data['country_of_residence']

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
            self.update_stat('_FYI', 'ADDR_FULL_COUNT', json_data['RECORD_ID'])
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
        json_data['retrieved_at'] = raw_data['retrieved_at']

        # columnName: source_url
        # 1.76 populated, 35.41 unique
        #      https://beta.companieshouse.gov.uk/company/05149111 (46)
        #      https://beta.companieshouse.gov.uk/company/SC227540 (10)
        #      https://beta.companieshouse.gov.uk/company/01278121 (10)
        #      https://beta.companieshouse.gov.uk/company/NI044930 (8)
        #      https://beta.companieshouse.gov.uk/company/05671230 (8)
        json_data['source_url'] = raw_data['source_url']

        #--capture the stats
        json_data = self.remove_empty_json_values(json_data)
        self.capture_mapped_stats(json_data)

        return json_data

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #-----------------------------------
    def remove_line_feeds(self, raw_value):
        if not raw_value:
            return ''
        return raw_value.replace('\\n', ' ')

    #----------------------------------------
    def format_dob(self, raw_date):
        try: new_date = dateparse(raw_date)
        except: return ''

        #--correct for prior century dates
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

    #----------------------------------------
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

    #----------------------------------------
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

    #----------------------------------------
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

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return

#----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    input_file_set = 'openc-files.json'
    csv_dialect = 'excel'
    data_source = 'OPENC-OFFICER'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file_set', dest='input_file_set', default = input_file_set, help='the name of the json file containing the list of open corporates csv files')
    parser.add_argument('-o', '--output_file_dir', dest='output_file_dir', help='the name of the output file directory')
    parser.add_argument('-d', '--data_source', dest='data_source', default=data_source, help='the name of the data source code to use, defaults to: ' + data_source)
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.input_file_set or not os.path.exists(args.input_file_set):
        print('\nPlease supply a valid open corporatres input file set json file name on the command line\n')
        sys.exit(1)
    if not os.path.isdir(args.output_file_dir):
        print('\nPlease supply a valid output file directory on the command line\n') 
        sys.exit(1)

    try: input_file_data = json.load(open(args.input_file_set, 'r'))
    except ValueError as err:
        print(f'\nError: {err} in {args.input_file_set}\n')
        sys.exit(1)

    mapper = mapper()

    for input_file_name in [item['file_path'] for item in input_file_data['file_list'] if item['file_type'] == 'officer']:

        input_base_name, input_file_ext = os.path.splitext(os.path.basename(input_file_name))
        output_file_name = args.output_file_dir + (os.path.sep if args.output_file_dir[-1] != os.path.sep else '') + input_base_name + '.json'

        print (f'\nMapping {input_file_name}\n   into {output_file_name} ...\n')

        input_file_handle = open(input_file_name, 'r', encoding='utf-8')
        output_file_handle = open(output_file_name, 'w', encoding='utf-8')

        #--skip header if present
        if next(csv.reader(input_file_handle)) == mapper.field_names:
            print('skipping header\n')
        else:
            input_file_handle.seek(0)

        input_row_count = 0
        output_row_count = 0
        for input_row in csv.DictReader(input_file_handle, dialect=csv_dialect, fieldnames=mapper.field_names):
            input_row_count += 1
            if input_row == mapper.field_names:
                continue

            json_data = mapper.map(input_row)
            if json_data:
                output_file_handle.write(json.dumps(json_data) + '\n')
                output_row_count += 1

            if input_row_count % 1000 == 0:
                print('%s rows processed, %s rows written' % (input_row_count, output_row_count))
            if shut_down:
                break

        elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
        run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
        print('%s rows processed, %s rows written, %s\n' % (input_row_count, output_row_count, run_status))

        output_file_handle.close()
        input_file_handle.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)


    sys.exit(0)

