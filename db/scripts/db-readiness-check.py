#!/usr/bin/env python

import argparse
from configparser import ConfigParser
import psycopg2
import sys
import time


def config(filename='/db/database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def is_db_ready(db_version_to_check):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        params = config()

        print('Attempt to connect to the database...')
        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        cur.execute(
            'select * from databasechangelog where exectype = %s and tag = %s',
            ('EXECUTED', db_version_to_check)
        )

        db_update = cur.fetchone()

        cur.close()

        return db_update is not None
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def readiness_check(db_version_to_check, attempts_count):
    while attempts_count > 0:
        if is_db_ready(db_version_to_check):
            return True
        print('current attempts count: ', attempts_count)
        attempts_count -= 1
        time.sleep(20)
    print('Attempts is over: ', attempts_count)
    return False


def main(db_version_to_check):
    attempts_count = 3
    success = readiness_check(db_version_to_check, attempts_count)
    if success:
        print('Script finished with exit code 0')
        sys.exit(0)
    else:
        print('Script finished with exit code 1')
        sys.exit(1)


parser = argparse.ArgumentParser()
parser.add_argument('db_version', metavar='db_version', type=str)
args = parser.parse_args()
main(args.db_version)
