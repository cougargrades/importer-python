#!/usr/bin/env python3

import os
import os.path
import sys
import sqlite3
import argparse
import time
from tqdm import tqdm

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('dbfile', metavar='records.db', type=str,
                    help='an integer for the accumulator')
parser.add_argument('--key', dest='key', default=None,
                    help='Path to Firebase Service account private key (see: README) ')
parser.add_argument('--offset', dest='offset', default=0, type=int,
                    help='Number of SQLite entries to skip before writing them to Firestore. Useful for preventing extra billing charges.')
parser.add_argument('--limit', dest='limit', default=float('inf'), type=int, 
                    help='Maximum number of SQLite entries to write to Firestore, quitting early if necessary. Useful for preventing extra billing charges.')

args = parser.parse_args()
print(args)

if not os.path.isfile(args.dbfile):
    print(f'{args.dbfile} is not a file.')
    exit(1)

if args.key == None or not os.path.isfile(args.key):
    print(f'{args.key} is not a file')
    exit(1)

# https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# setup sqlite
conn = sqlite3.connect(args.dbfile)
conn.row_factory = dict_factory
c = conn.cursor()
row_count = 0

# calculate row count
c.execute('SELECT ID FROM records;')
while c.fetchone() != None:
    row_count += 1
print(row_count)

# prepare Firebase, use a service account
cred = credentials.Certificate(args.key)
firebase_admin.initialize_app(cred)
db = firestore.client()

records = db.collection(u'records')

# iterate over every row
c.execute('SELECT * FROM records;')

print(f'📝 Writing {row_count} SQLite rows to Firestore. Make sure you\'re within the usage limits of your plan. (See --limit and --offset)')

for i in tqdm(range(row_count)):
    if (i - args.offset) > args.limit:
        tqdm.write(f'Number of rows processed has reached the limit of {args.limit}')
        break
    if i < args.offset:
        if i == 0:
            tqdm.write(f'Skipping up to {args.offset}')
        continue
    row = c.fetchone()
    doc_ref = records.document(str(row["FIRESTORE_KEY"]))
    doc_ref.set(row)
    #tqdm.write(f'{row["ID"]}')
    #time.sleep(1) # 1 second


exit()
