#! /usr/bin/env python3

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
import sqlite3
import pandas

#=========================
class mapper():

    #----------------------------------------
    def __init__(self, input_file_data):

        self.load_reference_data()
        self.stat_pack = {}

        self.load_child_files(input_file_data)

        self.field_names = ['company_number',  
                            'jurisdiction_code',
                            'name',
                            'normalised_name',
                            'company_type',
                            'nonprofit',
                            'current_status',
                            'incorporation_date',
                            'dissolution_date',
                            'branch',
                            'business_number',
                            'current_alternative_legal_name',
                            'current_alternative_legal_name_language',
                            'home_jurisdiction_text',
                            'native_company_number',
                            'previous_names',
                            'alternative_names',
                            'retrieved_at',
                            'registry_url',
                            'restricted_for_marketing',
                            'inactive',
                            'accounts_next_due',
                            'accounts_reference_date',
                            'accounts_last_made_up_date',
                            'annual_return_next_due',
                            'annual_return_last_made_up_date',
                            'has_been_liquidated',
                            'has_insolvency_history',
                            'has_charges',
                            'registered_address.street_address',
                            'registered_address.locality',
                            'registered_address.region',
                            'registered_address.postal_code',
                            'registered_address.country',
                            'registered_address.in_full',
                            'home_jurisdiction_code',
                            'home_jurisdiction_company_number',
                            'industry_code_uids',
                            'latest_accounts_date',
                            'latest_accounts_cash',
                            'latest_accounts_assets',
                            'latest_accounts_liabilities']    







    #----------------------------------------
    def load_child_files(self, input_file_data):

        possible_file_types = ['address']  #-- may eventually add websites and phones
        self.child_file_types = set([item['file_type'] for item in input_file_data['file_list'] if item['file_type'] in possible_file_types])
        if not self.child_file_types:
            return

        self.child_file_dbname = 'oc_temp.db'
        if os.path.exists(self.child_file_dbname):  #--always reload
            os.remove(self.child_file_dbname)
        self.child_file_dbconn = sqlite3.connect(self.child_file_dbname)

        for child_file_type in self.child_file_types:
            for child_file_name in [item['file_path'] for item in input_file_data['file_list'] if item['file_type'] == child_file_type]:
                print(f'loading {child_file_name} ...')
                df = pandas.read_csv(child_file_name, low_memory=False, encoding="utf-8", quotechar='"')
                df.to_sql(child_file_type, self.child_file_dbconn, if_exists="append")
            self.child_file_dbconn.cursor().execute(f'create index ix_{child_file_type} on {child_file_type} (company_number, jurisdiction_code)')

    # ----------------------------------------
    def close(self):
        if self.child_file_types:
            self.child_file_dbconn.close()
            os.remove(self.child_file_dbname)

    # --------------------------------------
    def sql_exec(self, sql, parm_list=None):
        try:
            exec_cursor = self.child_file_dbconn.cursor()
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

    # --------------------------------------
    def fetch_next(self, cursor_data):
        row = cursor_data['CURSOR'].fetchone()
        if row:
            type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
            return dict(list(zip(cursor_data['FIELD_LIST'], type_fixed_row)))

    # --------------------------------------
    def fetch_all(self, cursor_data):
        ''' fetch all the rows with column names '''
        row_list = []
        for row in cursor_data['CURSOR'].fetchall():
            type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
            row_dict = dict(list(zip(cursor_data['FIELD_LIST'], type_fixed_row)))
            row_list.append(row_dict)
        return row_list

    #----------------------------------------
    def map(self, raw_data):
        json_data = {}

        #--clean values
        for attribute in raw_data:
            raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--for list of previous and alt names
        other_names_list = []

        #--mandatory attributes
        json_data['DATA_SOURCE'] = args.data_source
        json_data['ENTITY_TYPE'] = 'GENERIC'
        #--record type replaces entity type helps keeps persons from resolving to companies
        json_data['RECORD_TYPE'] = 'ORGANIZATION'


        #--the record_id should be unique, remove this mapping if there is not one 
        json_data['RECORD_ID'] = raw_data['company_number'] + '-' + raw_data['jurisdiction_code']

        #--create the relationship anchor for officers/and head quarters
        json_data['REL_ANCHOR_DOMAIN'] = 'OPENC'
        json_data['REL_ANCHOR_KEY'] = raw_data['company_number'] + '-' + raw_data['jurisdiction_code']


        #--create the relationship pointer from branches to their headquarters
        if raw_data['home_jurisdiction_company_number']:
            json_data['REL_POINTER_KEY'] = raw_data['home_jurisdiction_company_number'] + '-' + raw_data['home_jurisdiction_code']
            json_data['REL_POINTER_DOMAIN'] = "OPENC"
            json_data['REL_POINTER_ROLE'] = "BRANCH_OF"

        #--also map the unique key as an exclusive feature for searching and to keep close company values from resolving
        #--this is reference data and they are telling us these are different companies
        json_data['OC_COMPANY_ID'] = raw_data['company_number'] + '-' + raw_data['jurisdiction_code'].upper()

        #--column mappings


        # columnName: company_number
        # 100.0 populated, 97.42 unique
        #      1 (9)
        #      10 (7)
        #      3 (6)
        #      100 (5)
        #      11 (5)
        json_data['company_number'] = raw_data['company_number']

        # columnName: jurisdiction_code
        # 100.0 populated, 0.5 unique
        #      us_tx (200)
        #      us_nd (200)
        #      us_tn (200)
        #      br (200)
        #      pl (200)
        json_data['jurisdiction_code'] = raw_data['jurisdiction_code']

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
        json_data['company_type'] = raw_data['company_type']

        # columnName: nonprofit
        # 100.0 populated, 0.01 unique
        #      false (25125)
        #      true (1075)
        json_data['nonprofit'] = 'Yes' if raw_data['nonprofit'].upper() == 'TRUE' else ''

        # columnName: current_status
        # 73.93 populated, 1.77 unique
        #      Active (5062)
        #      Good Standing (1118)
        #      Registered (1093)
        #      Dissolved (920)
        #      Inactive (667)
        json_data['current_status'] = raw_data['current_status']

        # columnName: incorporation_date
        # 73.88 populated, 37.47 unique
        #      2021-02-26 (924)
        #      2021-02-25 (612)
        #      2021-02-18 (279)
        #      2021-02-24 (213)
        #      2021-02-22 (200)
        json_data['incorporation_date'] = raw_data['incorporation_date']

        # columnName: dissolution_date
        # 12.69 populated, 66.55 unique
        #      2031-02-27 (90)
        #      2022-07-15 (81)
        #      2021-03-01 (35)
        #      1969-12-31 (23)
        #      2022-07-30 (22)
        json_data['dissolution_date'] = raw_data['dissolution_date']

        # columnName: branch
        # 6.85 populated, 0.11 unique
        #      F (1771)
        #      L (24)
        json_data['branch'] = raw_data['branch']

        # columnName: business_number
        # 0.76 populated, 100.0 unique
        #      42007284273 (1)
        #      53053806241 (1)
        #      95056184206 (1)
        #      78057121230 (1)
        #      23057161574 (1)
        json_data['business_number'] = raw_data['business_number']

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
        json_data['current_alternative_legal_name_language'] = raw_data['current_alternative_legal_name_language']

        # columnName: home_jurisdiction_text
        # 14.09 populated, 8.72 unique
        #      FL (207)
        #      NV (197)
        #      KS (179)
        #      IRELAND (175)
        #      New Mexico (161)
        json_data['home_jurisdiction_text'] = raw_data['home_jurisdiction_text']

        # columnName: native_company_number
        # 8.74 populated, 94.36 unique
        #      1 (34)
        #      2 (26)
        #      4 (17)
        #      3 (15)
        #      5 (15)
        json_data['native_company_number'] = raw_data['native_company_number']

        # columnName: previous_names
        # 4.33 populated, 99.47 unique
        #      Zubair Furnishing (Europe) Limited (2)
        #      Zulu Limited (2)
        #      Zurich Investments Limited (2)
        #      Zurich Enterprises Limited (2)
        #      Z ENTERPRISES, LLC (2)
        if raw_data['previous_names']:
            for name_org in raw_data['previous_names'].split('|'):
                other_names_list = [{'NAME_TYPE': 'PREVIOUS', 'NAME_ORG': name_org}]

        # columnName: alternative_names
        # 8.14 populated, 99.67 unique
        #      No trading name (4)
        #      STE TUNISO-ITALIENNE GELATTERIE-PATISSERIE|S O T I G E P (2)
        #      CANTUNIS|كــان تونس (2)
        #      . (2)
        #      လမ်းသစ်ဆန်းကုမ္ပဏီလီမိတက် (2)
        if raw_data['alternative_names']:
            for name_org in raw_data['alternative_names'].split('|'):
                other_names_list = [{'NAME_TYPE': 'ALTERNATIVE', 'NAME_ORG': name_org}]

        # columnName: retrieved_at
        # 100.0 populated, 73.84 unique
        #      2018-08-02 18:30:00 UTC (200)
        #      2021-02-05 00:00:00 UTC (200)
        #      2020-11-22 18:30:00 UTC (200)
        #      2021-02-26 00:00:00 UTC (199)
        #      2020-12-13 00:00:00 UTC (198)
        json_data['retrieved_at'] = raw_data['retrieved_at']

        # columnName: registry_url
        # 58.44 populated, 96.5 unique
        #      http://dati.ur.gov.lv/ (200)
        #      https://data.brreg.no/enhetsregisteret/api/enheter/lastned (103)
        #      http://www.moic.gov.bh/CReServices/inquiry/javascript:__doPostBack('grdCR$ctl02$lnkCrBr','') (61)
        #      http://www.moic.gov.bh/CReServices/inquiry/javascript:__doPostBack('grdCR$ctl03$lnkCrBr','') (52)
        #      http://www.moic.gov.bh/CReServices/inquiry/javascript:__doPostBack('grdCR$ctl04$lnkCrBr','') (41)
        json_data['registry_url'] = raw_data['registry_url']

        # columnName: restricted_for_marketing
        # 0.93 populated, 0.41 unique
        #      true (244)
        json_data['restricted_for_marketing'] = 'Yes' if raw_data['restricted_for_marketing'].upper() == 'TRUE' else ''

        # columnName: inactive
        # 75.51 populated, 0.01 unique
        #      false (13688)
        #      true (6096)
        json_data['inactive'] = 'Yes' if raw_data['inactive'].upper() == 'TRUE' else ''

        # columnName: accounts_next_due
        # 0.47 populated, 13.82 unique
        #      2022-12-01 (100)
        #      2005-01-31 (3)
        #      2021-07-31 (3)
        #      2021-12-31 (2)
        #      2021-09-30 (2)
        json_data['accounts_next_due'] = raw_data['accounts_next_due']

        # columnName: accounts_reference_date
        # 0.76 populated, 13.0 unique
        #      03-31 (116)
        #      31-01 (9)
        #      30-04 (8)
        #      31-12 (8)
        #      30-06 (6)
        json_data['accounts_reference_date'] = raw_data['accounts_reference_date']

        # columnName: accounts_last_made_up_date
        # 0.29 populated, 76.32 unique
        #      2003-03-31 (3)
        #      2007-12-31 (3)
        #      2008-01-31 (3)
        #      2017-03-31 (3)
        #      2019-08-31 (2)
        json_data['accounts_last_made_up_date'] = raw_data['accounts_last_made_up_date']

        # columnName: annual_return_next_due
        # 0.77 populated, 41.79 unique
        #      2021-06-10 (100)
        #      2020-09-30 (8)
        #      2021-09-30 (7)
        #      2015-09-30 (3)
        #      2020-12-31 (2)
        json_data['annual_return_next_due'] = raw_data['annual_return_next_due']

        # columnName: annual_return_last_made_up_date
        # 0.73 populated, 90.0 unique
        #      2019-09-30 (5)
        #      2020-09-30 (3)
        #      2014-09-30 (3)
        #      2004-02-18 (2)
        #      2007-04-26 (2)
        json_data['annual_return_last_made_up_date'] = raw_data['annual_return_last_made_up_date']

        # columnName: has_been_liquidated
        # 0.18 populated, 4.35 unique
        #      false (38)
        #      true (8)
        json_data['has_been_liquidated'] = 'Yes' if raw_data['has_been_liquidated'].upper() == 'TRUE' else ''

        # columnName: has_insolvency_history
        # 0.53 populated, 1.43 unique
        #      false (129)
        #      true (11)
        json_data['has_insolvency_history'] = 'Yes' if raw_data['has_insolvency_history'].upper() == 'TRUE' else ''

        # columnName: has_charges
        # 0.52 populated, 1.46 unique
        #      false (131)
        #      true (6)
        json_data['has_charges'] = 'Yes' if raw_data['has_charges'].upper() == 'TRUE' else ''


        # log if parsed address is different than full address if both populated
        if raw_data['registered_address.street_address'] and raw_data['registered_address.in_full']:
            if raw_data['registered_address.street_address'].upper() not in raw_data['registered_address.in_full'].upper():
                self.update_stat('_FYI', 'REGISTERED_ADDR1_NOT_IN_ADDR_FULL', json_data['RECORD_ID'])

        registered_address_for_dedupe = {}
        if raw_data['registered_address.in_full']:
            self.update_stat('_FYI', 'REGISTERED_ADDR_FULL_COUNT', json_data['RECORD_ID'])

            # columnName: registered_address.in_full
            # 58.55 populated, 90.23 unique
            #      PROVINCIA PANAMÁ, Panama (90)
            #      CORREGIMIENTO CIUDAD DE PANAMÁ, DISTRITO PANAMÁ, PROVINCIA PANAMÁ, Panama (69)
            #      BUITENLAND, Aruba (67)
            #      KUALA LUMPUR, WILAYAH PERSEKUTUAN, Malaysia (37)
            #      DISTRITO PANAMÁ, PROVINCIA PANAMÁ, Panama (36)
            json_data['REGISTERED_ADDR_FULL'] = self.remove_line_feeds(raw_data['registered_address.in_full']).upper()
            json_data['REGISTERED_ADDR_COUNTRY'] = self.remove_line_feeds(raw_data['registered_address.country']).upper()  #--can help determine address parsing rules
            registered_address_for_dedupe['ADDR_FULL'] = json_data['REGISTERED_ADDR_FULL']
            registered_address_for_dedupe['ADDR_COUNTRY'] = json_data['REGISTERED_ADDR_COUNTRY']

        elif raw_data['registered_address.street_address'] or raw_data['registered_address.locality'] or raw_data['registered_address.region'] or \
             raw_data['registered_address.postal_code'] or raw_data['registered_address.country']:
            self.update_stat('_FYI', 'REGISTERED_ADDR_PARSED_COUNT', json_data['RECORD_ID'])

            # columnName: registered_address.street_address
            # 57.45 populated, 90.21 unique
            #      PROVINCIA PANAMÁ (90)
            #      CORREGIMIENTO CIUDAD DE PANAMÁ, DISTRITO PANAMÁ, PROVINCIA PANAMÁ (69)
            #      BUITENLAND (67)
            #      DISTRITO PANAMÁ, PROVINCIA PANAMÁ (36)
            #      NEW ORLEANS, LA 70150 (34)
            json_data['REGISTERED_ADDR_LINE1'] = self.remove_line_feeds(raw_data['registered_address.street_address'].upper())
            registered_address_for_dedupe['ADDR_LINE1'] = json_data['REGISTERED_ADDR_LINE1']

            # columnName: registered_address.locality
            # 20.17 populated, 47.25 unique
            #      VALLETTA (91)
            #      ΑΤΤΙΚΗΣ (86)
            #      กรุงเทพมหานคร (78)
            #      DOUGLAS (76)
            #      St. John's (73)
            json_data['REGISTERED_ADDR_CITY'] = self.remove_line_feeds(raw_data['registered_address.locality']).upper()
            registered_address_for_dedupe['ADDR_CITY'] = json_data['REGISTERED_ADDR_CITY']

            # columnName: registered_address.region
            # 13.94 populated, 16.41 unique
            #      NC (204)
            #      Missouri (184)
            #      WA (161)
            #      Virginia (143)
            #      MA (142)
            json_data['REGISTERED_ADDR_STATE'] = self.remove_line_feeds(raw_data['registered_address.region']).upper()
            registered_address_for_dedupe['ADDR_STATE'] = json_data['REGISTERED_ADDR_STATE']

            # columnName: registered_address.postal_code
            # 19.49 populated, 72.0 unique
            #      10000 (79)
            #      JE4 9WG (39)
            #      27615 (23)
            #      34100 (21)
            #      28210 (17)
            json_data['REGISTERED_ADDR_POSTAL_CODE'] = self.remove_line_feeds(raw_data['registered_address.postal_code']).upper()
            registered_address_for_dedupe['ADDR_POSTAL_CODE'] = json_data['REGISTERED_ADDR_POSTAL_CODE']

            # columnName: registered_address.country
            # 58.29 populated, 0.46 unique
            #      United States (4569)
            #      Canada (549)
            #      USA (335)
            #      UNITED STATES (298)
            #      Ukraine (200)
            json_data['REGISTERED_ADDR_COUNTRY'] = self.remove_line_feeds(raw_data['registered_address.country']).upper()
            registered_address_for_dedupe['ADDR_COUNTRY'] = json_data['REGISTERED_ADDR_COUNTRY']

        # columnName: home_jurisdiction_code
        # 3.83 populated, 5.88 unique
        #      us_de (248)
        #      us_ny (57)
        #      us_ca (56)
        #      us_fl (49)
        #      us_tx (43)
        json_data['home_jurisdiction_code'] = raw_data['home_jurisdiction_code']

        # columnName: home_jurisdiction_company_number
        # 3.83 populated, 96.71 unique
        #      2021-000973162 (3)
        #      802380338 (3)
        #      0100743157 (3)
        #      9860 (3)
        #      112 (3)
        json_data['home_jurisdiction_company_number'] = raw_data['home_jurisdiction_company_number']

        # columnName: industry_code_uids
        # 6.86 populated, 56.9 unique
        #      gl_gb_2000-050105 (57)
        #      us_naics_2007-9999 (44)
        #      me_kd_2010-4719|eu_nace_2-4719|isic_4-4719|eu_nace_2-471|eu_nace_2-47|eu_nace_2-G|isic_4-471|isic_4-47|isic_4-G (40)
        #      th_tsic_2009-41002|isic_4-4100|isic_4-410|isic_4-41|isic_4-F (31)
        #      gl_gb_2000-050100 (27)
        json_data['industry_code_uids'] = raw_data['industry_code_uids']

        # columnName: latest_accounts_date
        # 0.58 populated, 20.53 unique
        #      2020-12-31 (43)
        #      2014-12-31 (31)
        #      2020-09-30 (20)
        #      2016-12-31 (8)
        #      2015-12-31 (5)
        json_data['latest_accounts_date'] = raw_data['latest_accounts_date']

        # columnName: latest_accounts_cash
        # 0.02 populated, 100.0 unique
        #      1425 (1)
        #      65 (1)
        #      50793 (1)
        #      549111 (1)
        json_data['latest_accounts_cash'] = raw_data['latest_accounts_cash']

        # columnName: latest_accounts_assets
        # 0.49 populated, 89.92 unique
        #      0 (4)
        #      2376195 (4)
        #      550 (3)
        #      1000 (2)
        #      82485 (2)
        json_data['latest_accounts_assets'] = raw_data['latest_accounts_assets']

        # columnName: latest_accounts_liabilities
        # 0.02 populated, 100.0 unique
        #      582551 (1)
        #      689639 (1)
        #      7930 (1)
        #      662 (1)
        #      897482 (1)
        json_data['latest_accounts_liabilities'] = raw_data['latest_accounts_liabilities']

        #--add the accumulated other names, truncating any super long ones, and getting rid of any duplicates
        if other_names_list:
            dedupe_names_list = []
            dedupe_names_list.append(raw_data['name'].upper())
            corrected_name_list = []
            for other_name_data in other_names_list:
                other_name_data['NAME_ORG'] = other_name_data['NAME_ORG'].upper()
                if other_name_data['NAME_ORG'] in dedupe_names_list:
                    self.update_stat('_FYI', 'DUPLICATE_NAME_IGNORED', json_data['RECORD_ID'] + ' -> ' + other_name_data['NAME_ORG'])
                    continue
                dedupe_names_list.append(other_name_data['NAME_ORG'].upper())
                if len(other_name_data['NAME_ORG']) > 250:
                    other_name_data['NAME_ORG'] = other_name_data['NAME_ORG'][0:250]
                corrected_name_list.append(other_name_data)
            if corrected_name_list:
                json_data['OTHER_NAMES'] = corrected_name_list

        #--child file addresses
        if 'address' in self.child_file_types:
            dedupe_addrs_list = [json.dumps(registered_address_for_dedupe, sort_keys = True)]
            addr_list = []
            sql = 'select * from address where company_number = ? and jurisdiction_code = ?'
            for addr_record in mapper.fetch_all(mapper.sql_exec(sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
                addr_type = 'UNKNOWN' if not addr_record['ADDRESS_TYPE'] else addr_record['ADDRESS_TYPE'].upper()
                self.update_stat('_FYI', 'ADDRESS_TYPES', addr_type)

                # log if parsed address is different than full address if both populated
                if addr_record['STREET_ADDRESS'] and addr_record['IN_FULL']:
                    self.update_stat('_FYI', 'NON_REG_ADDR_FULL_COUNT', json_data['RECORD_ID'])
                    if addr_record['STREET_ADDRESS'].upper() not in addr_record['IN_FULL'].upper():
                        self.update_stat('_FYI', 'NONREG_ADDR1_NOT_IN_ADDR_FULL', json_data['RECORD_ID'])
                else:
                    self.update_stat('_FYI', 'NON_REG_ADDR_PARSED_COUNT', json_data['RECORD_ID'])

                addr_data = {}
                if addr_record['IN_FULL']:
                    addr_data['ADDR_FULL'] = self.remove_line_feeds(addr_record['IN_FULL']).upper()
                    addr_data['ADDR_COUNTRY'] = self.remove_line_feeds(addr_record['COUNTRY']).upper()
                else:
                    addr_data['ADDR_LINE1'] = self.remove_line_feeds(addr_record['STREET_ADDRESS']).upper()
                    addr_data['ADDR_CITY'] = self.remove_line_feeds(addr_record['LOCALITY']).upper()
                    addr_data['ADDR_STATE'] = self.remove_line_feeds(addr_record['REGION']).upper()
                    addr_data['ADDR_POSTAL_CODE'] = self.remove_line_feeds(addr_record['POSTAL_CODE']).upper()
                    addr_data['ADDR_COUNTRY'] = self.remove_line_feeds(addr_record['COUNTRY']).upper()

                if json.dumps(addr_data, sort_keys = True) in dedupe_addrs_list:
                    self.update_stat('_FYI', 'DUPLICATE_ADDR_IGNORED', json_data['RECORD_ID'] + ' -> ' + json.dumps(addr_data))
                    continue

                addr_data['ADDR_TYPE'] = addr_type.upper()
                addr_list.append(addr_data)

            if addr_list:
                json_data['NON_REG_ADDRESSES'] = addr_list
                if len(addr_list) > 1:
                    self.update_stat('_FYI', 'HAS_MULTIPLE_NON_REG_ADDRS', json_data['RECORD_ID'])


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
    data_source = 'OPENC-COMPANY'

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

    mapper = mapper(input_file_data)

    for input_file_name in [item['file_path'] for item in input_file_data['file_list'] if item['file_type'] == 'company']:

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

    mapper.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)

    sys.exit(0)

