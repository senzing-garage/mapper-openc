#! /usr/bin/env python3

import sys
import os
import glob
import sqlite3
import pandas
import time
import multiprocessing
import argparse

def make_database(dbname, child_file_types):
    child_dbo = sqlite3.connect(dbname, isolation_level=None, timeout=20)

    if 'address' in child_file_types:
        sql = 'CREATE TABLE IF NOT EXISTS address (' \
              ' company_number TEXT,' \
              ' jurisdiction_code TEXT,' \
              ' address_type TEXT,' \
              ' street_address TEXT,' \
              ' locality TEXT,' \
              ' region TEXT,' \
              ' postal_code TEXT,' \
              ' country TEXT,' \
              ' country_code TEXT,' \
              ' in_full TEXT,' \
              ' start_date TEXT,' \
              ' end_date TEXT' \
              ' )'
        child_dbo.cursor().execute(sql)

    if 'alias' in child_file_types:
        sql = 'CREATE TABLE IF NOT EXISTS alias (' \
              ' company_number TEXT,' \
              ' jurisdiction_code TEXT,' \
              ' name TEXT,' \
              ' type TEXT,' \
              ' start_date TEXT,' \
              ' end_date TEXT' \
              ' )'
        child_dbo.cursor().execute(sql)

    if 'identifier' in child_file_types:
        sql = 'CREATE TABLE IF NOT EXISTS identifier (' \
              ' company_number TEXT,' \
              ' jurisdiction_code TEXT,' \
              ' uid TEXT,' \
              ' identifier_system_code TEXT' \
              ' )'
        child_dbo.cursor().execute(sql)

    if 'telephone' in child_file_types:
        sql = 'CREATE TABLE IF NOT EXISTS telephone (' \
              ' company_number TEXT,' \
              ' jurisdiction_code TEXT,' \
              ' country_code TEXT,' \
              ' number TEXT,' \
              ' raw_number TEXT,' \
              ' number_type TEXT,' \
              ' start_date TEXT,' \
              ' end_date TEXT' \
              ' )'
        child_dbo.cursor().execute(sql)

    if 'website' in child_file_types:
        sql = 'CREATE TABLE IF NOT EXISTS website (' \
              ' company_number TEXT,' \
              ' jurisdiction_code TEXT,' \
              ' country_code TEXT,' \
              ' url TEXT,' \
              ' raw_url TEXT,' \
              ' number_type TEXT,' \
              ' start_date TEXT,' \
              ' end_date TEXT' \
              ' )'
        child_dbo.cursor().execute(sql)

    child_dbo.close()


def import_file(child_dbo, filedata, record_chunk_size):
    filetype = filedata[0]
    filename = filedata[1]
    print(f"{filename}: started")

    if os.path.splitext(filename)[1].upper() == '.GZ':
        file_compression = 'gzip'
    else:
        file_compression = None

    cnt = 0
    for chunk in pandas.read_csv(filename, chunksize=record_chunk_size, encoding='utf-8', dtype = str, compression=file_compression):
        timer_start = time.time()
        chunk.to_sql(filetype, child_dbo, index=False, method='multi', chunksize=1000, if_exists='append')
        cnt += len(chunk)
        print(f"{filename}: {cnt:,} records loaded, batch rate {round(time.time() - timer_start, 1)} seconds")

    return f"{filename} completed!"


def index_database(child_dbo, filetype):
    print(f"indexing {filetype} ...")
    timer_start = time.time()
    child_dbo.cursor().execute(f'create index ix_{filetype} on {filetype} (company_number, jurisdiction_code)')
    print(f"indexing {filetype} completed in {round(time.time() - timer_start, 1)} seconds")
    return f"{filetype} indexing complete"


def complete_database(child_dbo):
    child_dbo.cursor().execute('create table finished (dummy integer)')


if __name__ == "__main__":

    record_chunk_size = 1000000

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file_dir', dest='input_file_dir', help='the name of the open corporates csv file directory')
    parser.add_argument('-c', '--child_database_name', dest='child_database_name', help='the name of the database file to create')
    args = parser.parse_args()

    if not args.input_file_dir or not os.path.isdir(args.input_file_dir):
        print('\nPlease supply a valid input file directory on the command line\n')
        sys.exit(1)

    if not args.child_database_name:
        print('\nPlease supply a sqlite database file name on the command line\n')
        sys.exit(1)

    # note: telephones and websites are experimental and may not be present

    child_file_types = []
    child_files = []
    for file_name in sorted(glob.glob(args.input_file_dir + os.sep + '*')):
        base_name = os.path.basename(file_name)
        if 'non_reg_addresses' in base_name:
            file_type = 'address'
        elif 'alternative_names' in base_name:
            file_type = 'alias'
        elif 'additional_identifiers' in base_name:
            file_type = 'identifier'
        elif 'telephone' in base_name:
            file_type = 'telephone'
        elif 'website' in base_name:
            file_type = 'website'
        else:
            continue
        child_file_types.append(file_type)
        child_files.append([file_type,file_name])
    if not child_files:
        print('\nNo child files found!')
        print('Child file names must contain: "*non_reg_addresses*", "*alternative_names*" or "*additional_identifiers*".\n')
        sys.exit(1)

    if os.path.exists(args.child_database_name):
        os.remove(args.child_database_name)
        #print(f"\n{dbname} already exists, please remove it first if you want to continue\n")
        #sys.exit(1)

    proc_start = time.time()
    print(f"\n{len(child_files)} files to load\n")
    make_database(args.child_database_name, child_file_types)

    child_dbo = sqlite3.connect(args.child_database_name)
    child_dbo_cursor = child_dbo.cursor()
    child_dbo_cursor.execute('pragma synchronous = 0')
    child_dbo_cursor.execute('pragma journal_mode = off')
    child_dbo_cursor.execute('pragma temp_store = MEMORY')
    child_dbo_cursor.execute('pragma locking_mode = EXCLUSIVE')
    child_dbo_cursor.execute('pragma isolation_level = None')
    for filedata in child_files:
        print()
        import_file(child_dbo, filedata, record_chunk_size)
    for filetype in child_file_types:
        print()
        index_database(child_dbo, filetype)
    complete_database(child_dbo)

    child_dbo.close()

    print(f"\nProcess completed in {round((time.time() - proc_start) / 60, 1)} minutes\n")
