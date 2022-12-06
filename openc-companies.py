#! /usr/bin/env python3

import sys
import os
import argparse
import time
import csv
import orjson as json
#import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import sqlite3
import glob
import gzip
import io
import hashlib
import multiprocessing
import concurrent.futures


def init_map(map_function, data_source, child_database_name, child_table_list):
    print(f"pid {os.getpid()} initialized")
    map_function.data_source = data_source
    map_function.child_table_list = child_table_list

    #map_function.child_dbo = sqlite3.connect(f'file:{child_database_name}?mode=ro', uri=True)
    #map_function.child_dbo.cursor().execute('PRAGMA query_only=ON')

    map_function.child_dbo = sqlite3.connect(child_database_name, isolation_level=None)
    map_function.child_dbo.cursor().execute('pragma synchronous = 0')
    map_function.child_dbo.cursor().execute('PRAGMA query_only=ON')
    map_function.child_dbo.cursor().execute('pragma cache_size = -16000000')
    map_function.child_dbo.cursor().execute('pragma journal_mode = off')
    map_function.child_dbo.cursor().execute('pragma temp_store = MEMORY')


def map(raw_data):
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
    json_data['DATA_SOURCE'] = map.data_source
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
    if 'alias' in map.child_table_list:
        sql = 'select * from alias where company_number = ? and jurisdiction_code = ?'
        for record in sql_fetch_all(sql_exec(map.child_dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
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
    if 'address' in map.child_table_list:
        dedupe_addrs_list = [json.dumps(registered_address_for_dedupe, option=json.OPT_SORT_KEYS)]
        addr_list = []
        sql = 'select * from address where company_number = ? and jurisdiction_code = ?'
        for addr_record in sql_fetch_all(sql_exec(map.child_dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
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

            if json.dumps(addr_data, option=json.OPT_SORT_KEYS).decode() in dedupe_addrs_list:
                stat_update_list.append(['_FYI', 'DUPLICATE_ADDR_IGNORED', record_id + ' | ' + json.dumps(addr_data).decode()])
                continue

            addr_data['ADDR_TYPE'] = addr_type.upper()
            addr_list.append(addr_data)

        if addr_list:
            json_data['NON_REG_ADDRESSES'] = addr_list
            if len(addr_list) > 1:
                stat_update_list.append(['_FYI', 'HAS_MULTIPLE_NON_REG_ADDRS', record_id])

    additional_list = []

    # identifier child table
    if 'identifier' in map.child_table_list:
        sql = 'select * from identifier where company_number = ? and jurisdiction_code = ?'
        for record in sql_fetch_all(sql_exec(map.child_dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
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
    if 'telephone' in map.child_table_list:
        sql = 'select * from telephone where company_number = ? and jurisdiction_code = ?'
        for record in sql_fetch_all(sql_exec(map.child_dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
            if not record['NUMBER']:
                continue
            phone_data = {"PHONE_NUMBER": record['NUMBER']}
            if phone_data not in additional_list:
                additional_list.append(phone_data)

    # website child table
    if 'website' in map.child_table_list:
        website_list = []
        sql = 'select * from website where company_number = ? and jurisdiction_code = ?'
        for record in sql_fetch_all(sql_exec(map.child_dbo, sql, [raw_data['company_number'], raw_data['jurisdiction_code']])):
            if not record['URL']:
                continue
            website_data = {"WEBSITE_ADDRESS": record['URL']}
            if website_data not in additional_list:
                additional_list.append(website_data)

    if additional_list:
        json_data['ADDITIONAL_DATA'] = additional_list


    # compute record hash with this data
    json_data = remove_empty_json_values(json_data)
    base_json_string = json.dumps(json_data, option=json.OPT_SORT_KEYS).decode()
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


def capture_mapped_stats(stat_pack, json_data):

    if 'DATA_SOURCE' in json_data:
        data_source = json_data['DATA_SOURCE']
    else:
        data_source = 'UNKNOWN_DSRC'

    for key1 in json_data:
        if type(json_data[key1]) != list:
            stat_pack = update_stat(stat_pack, data_source, key1, json_data[key1])
        else:
            for subrecord in json_data[key1]:
                for key2 in subrecord:
                    stat_pack = update_stat(stat_pack, data_source, key2, subrecord[key2])
    return stat_pack

def update_stat(stat_pack, cat1, cat2, example=None):

    if cat1 not in stat_pack:
        stat_pack[cat1] = {}
    if cat2 not in stat_pack[cat1]:
        stat_pack[cat1][cat2] = {}
        stat_pack[cat1][cat2]['count'] = 0

    stat_pack[cat1][cat2]['count'] += 1
    if example:
        if 'examples' not in stat_pack[cat1][cat2]:
            stat_pack[cat1][cat2]['examples'] = []
        if example not in stat_pack[cat1][cat2]['examples']:
            if len(stat_pack[cat1][cat2]['examples']) < 10:
                stat_pack[cat1][cat2]['examples'].append(example)
    return stat_pack


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
            return next(reader)
        except StopIteration:
            return None
        except Exception as err:
            print(f"error: row {input_row_count} {err}")


def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return


if __name__ == "__main__":
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    csv_dialect = 'excel'
    data_source = 'OPENC-COMPANY'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file_dir', dest='input_file_dir', help='the name of the open corporates csv file directory')
    parser.add_argument('-o', '--output_file_dir', dest='output_file_dir', help='the name of the output file directory')
    parser.add_argument('-c', '--child_database_name', dest='child_database_name', help='the name of the child database created in the prior step')
    parser.add_argument('-d', '--data_source', dest='data_source', default=data_source, help='the name of the data source code to use, defaults to: ' + data_source)
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.output_file_dir or not os.path.isdir(args.output_file_dir):
        print('\nPlease supply a valid output file directory on the command line\n') 
        sys.exit(1)

    if not args.input_file_dir or not os.path.isdir(args.input_file_dir):
        print('\nPlease supply a valid input file directory on the command line\n')
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

    file_list = [x for x in sorted(glob.glob(args.input_file_dir + os.sep + '*')) if 'companies' in x]
    if not file_list:
        print('\nNo company files found!\nCompany file names must contain: "*companies*".\n')
        sys.exit(1)

    progress_interval = 100000
    proc_start_time = time.time()
    stat_pack = {}
    record_cache = {}
    duplicate_hash_count = 0
    input_row_count = 0
    output_row_count = 0

    for file_name in file_list:

        base_file_name, file_extension = os.path.splitext(file_name)
        compressed_file = file_extension.upper() == '.GZ'
        if compressed_file:
            base_file_name, file_extension = os.path.splitext(base_file_name)
            input_file_handle = gzip.open(file_name, 'r')
            csv_reader = csv.DictReader(io.TextIOWrapper(io.BufferedReader(input_file_handle), encoding='utf-8', errors='ignore'))
        else:
            input_file_handle = open(file_name, 'r')
            csv_reader = csv.DictReader(input_file_handle, dialect='excel')

        output_file_name = os.path.splitext(args.output_file_dir + (os.path.sep if args.output_file_dir[-1] != os.path.sep else '') + os.path.split(base_file_name)[1])[0] + '.json'
        if False: #compressed_file: don't compress output as data is sorted
            output_file_name += '.gz'
            output_file_handle = gzip.open(output_file_name, 'wb')
        else:
            output_file_handle = open(output_file_name, 'w', encoding='utf-8')

        print (f'\nMapping {file_name} into {output_file_name} ...\n')
        batch_start_time = time.time()
        batch_records = []

        if True:  # single threaded is faster
            init_map(map, args.data_source, args.child_database_name, child_table_list)
            input_row = safe_csv_next(csv_reader, input_row_count)
            while input_row:
                record_id, record_hash, json_data, payload_data, relationship_list, stat_update_list = map(input_row)

                # # this was determining how many pure dupes there were
                # if record_hash in record_cache:
                #     duplicate_hash_count += 1
                #     record_cache[record_hash]['cnt'] += 1
                #     record_cache[record_hash]['records'] += (', ' + record_id)
                # else:
                #     record_cache[record_hash] = {'cnt': 1, 'records': record_id, 'json': json_data}

                for stat_data in stat_update_list:
                    stat_pack = update_stat(stat_pack, stat_data[0], stat_data[1], stat_data[2])

                new_json_data = {'DATA_SOURCE': json_data['DATA_SOURCE']}
                new_json_data['RECORD_ID'] = record_id
                new_json_data['OC_COMPANY_ID'] = record_id
                new_json_data.update(json_data)
                new_json_data['RELATIONSHIP_LIST'] = relationship_list
                new_json_data.update(payload_data)
                stat_pack = capture_mapped_stats(stat_pack, new_json_data)

                batch_records.append((json.dumps(new_json_data).decode()+'\n'))
                output_row_count += 1
                if output_row_count % progress_interval == 0:
                    output_file_handle.writelines(batch_records)
                    elapsed_seconds = round((time.time() - batch_start_time), 1)
                    print(f"{output_row_count} rows processed, {duplicate_hash_count} duplicate hashes, in {elapsed_seconds} seconds")
                    batch_start_time = time.time()
                    batch_records = []
                if shut_down:
                    break

                input_row = safe_csv_next(csv_reader, input_row_count)

        else: # this was abandoned as slower
            with concurrent.futures.ProcessPoolExecutor(max_workers=4, initializer=init_map, initargs=(map, args.data_source, args.child_database_name, child_table_list)) as executor:
                futures = {}

                while input_row_count <= executor._max_workers:
                    input_row = safe_csv_next(csv_reader, input_row_count)
                    if not input_row:
                        break
                    futures[executor.submit(map, input_row)] = input_row
                    input_row_count += 1

                while futures:
                    for f in concurrent.futures.as_completed(futures.keys()):

                        record_id, record_hash, json_data, payload_data, relationship_list, stat_update_list = f.result()

                        if record_hash in record_cache:
                            duplicate_hash_count += 1
                            record_cache[record_hash]['cnt'] += 1
                            record_cache[record_hash]['records'] += (', ' + record_id)
                        else:
                            record_cache[record_hash] = {'cnt': 1, 'records': record_id, 'json': json_data}

                        for stat_data in stat_update_list:
                            stat_pack = update_stat(stat_pack, stat_data[0], stat_data[1], stat_data[2])

                        new_json_data = {'DATA_SOURCE': json_data['DATA_SOURCE']}
                        new_json_data['RECORD_ID'] = record_id
                        new_json_data.update(json_data)
                        new_json_data['RELATIONSHIP_LIST'] = relationship_list
                        new_json_data.update(payload_data)

                        #if compressed_file:
                        batch_records.append((json.dumps(new_json_data).decode()+'\n').encode('utf-8'))
                        #else:
                        #    batch_records.append((json.dumps(new_json_data).decode().encode('utf-8'))

                        futures.pop(f)
                        output_row_count += 1
                        if output_row_count % progress_interval == 0:
                            output_file_handle.writelines(batch_records)
                            elapsed_seconds = round((time.time() - batch_start_time), 1)
                            print(f"{output_row_count} rows processed, {duplicate_hash_count} duplicate hashes, in {elapsed_seconds} seconds")
                            batch_start_time = time.time()
                            batch_records = []
                        if shut_down:
                            break

                        input_row = safe_csv_next(csv_reader, input_row_count)
                        if input_row:
                            futures[executor.submit(map, input_row)] = input_row
                            input_row_count += 1

        if batch_records:
            output_file_handle.writelines(batch_records)


        elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
        run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
        print(f"{output_row_count} rows processed, {duplicate_hash_count} duplicate hashes, {run_status}")

        input_file_handle.close()
        output_file_handle.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            outfile.write(json.dumps(stat_pack, option=json.OPT_INDENT_2).decode())
        print('Mapping stats written to %s\n' % args.log_file)

    # # this dumps the pure duplicates to a file for research
    # with open('dup_hashes.csv','w') as outfile:
    #     for record_hash in record_cache:
    #         if record_cache[record_hash]['cnt'] > 1:
    #             print(str(record_cache[record_hash]['cnt']) + '|' + record_cache[record_hash]['records'] + '|' + json.dumps(record_cache[record_hash]['json']).decode(), file=outfile)


    sys.exit(0)

