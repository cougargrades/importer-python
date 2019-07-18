#!/usr/bin/env python3

import os
import os.path
import sys
import sqlite3
import argparse
import time
from tqdm import tqdm
from halo import Halo

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

parser = argparse.ArgumentParser(description='Import a SQLite database into Google Firestore')
parser.add_argument('dbfile', metavar='records.db', type=str,
                    help='Path to the SQLite database generated by csv2db.py')
parser.add_argument('--key', dest='key', default=None,
                    help='Path to Firebase Service account private key (see: README) ')
parser.add_argument('--offset', dest='offset', default=0, type=int,
                    help='Number of SQLite entries to skip before writing them to Firestore. Useful for preventing extra billing charges.')
parser.add_argument('--limit', dest='limit', default=float('inf'), type=int, 
                    help='Maximum number of SQLite entries to write to Firestore, quitting early if necessary. Useful for preventing extra billing charges.')
parser.add_argument('--delete', dest='delete', action='store_true',
                    help='When enabled, Firestore documents that are NOT rows in the database will be deleted.')

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

if args.delete:
    spinner = Halo(text=f'Fetching rows from Firestore ...', spinner='dots')
    spinner.start()
    collectionRef = records.stream()
    docs = [x for x in collectionRef]
    spinner.succeed(text=f'Rows fetched')

    spinner.text = f'Deleting rows from Firestore ...'
    spinner.start()
    i = 0
    for doc in docs:
        spinner.text = f'Deleting rows from Firestore (doc # {i} of ???)...'
        # doc.id
        # print(doc.id)
        c.execute('SELECT FIRESTORE_KEY FROM records WHERE FIRESTORE_KEY=?', tuple([str(doc.id)]))
        if len(c.fetchall()) == 0:
            # document exists in Firestore, but not locally
            doc.reference.delete()
            spinner.stop_and_persist(symbol='🗑'.encode('utf-8'), text=f'Deleted {doc.id}')
            spinner.start(text=f'Deleting rows from Firestore (doc # {i} of ???)...', spinner='dots')
        i += 1
    spinner.succeed()
    exit()

# iterate over every row
c.execute('SELECT * FROM records;')

print(f'📝 Writing {row_count} SQLite rows to Firestore. Make sure you\'re within the usage limits of your plan. (See --limit and --offset)')

for i in tqdm(range(row_count)):
    if (i - args.offset) > args.limit:
        tqdm.write(f'Number of rows processed has reached the limit of {args.limit}')
        break
    row = c.fetchone() # needed to progress
    if i < args.offset:
        if i == 0:
            tqdm.write(f'Skipping up to {args.offset}')
        # skipped operations are done at 1/200th of a second 
        # because 0 delay breaks tqdm (sometimes) for some reason
        time.sleep(1/200)
    if i >= args.offset:
        # uses document.set instead of collection.add to prevent duplicates
        doc_ref = records.document(str(row["FIRESTORE_KEY"]))
        doc_ref.set(row)
        
print('Firebase import completed.')
exit()
