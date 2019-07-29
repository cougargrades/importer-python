#!/usr/bin/env python3

import os
import os.path
import sys
import sqlite3
import json
import argparse
from tqdm import tqdm
from halo import Halo

parser = argparse.ArgumentParser(description='Prepare a SQLite database into Firestore-ready JSONL files')
parser.add_argument('dbfile', metavar='records.db', type=str,
                    help='Path to the SQLite database generated by csv2db.py')
parser.add_argument('--out', dest='folder', default=None,
                    help='Folder to store .jsonl files in')

args = parser.parse_args()

# check arguments
if not os.path.isfile(args.dbfile):
    print(f'{args.dbfile} is not a file.')
    exit(1)

if args.folder == None:
    print(f'--out must be a folder name.')
    exit(1)
else:
    # if not a directory and not an existing file
    if not os.path.isdir(args.folder) and not os.path.isfile(args.folder):
        # create the folder
        os.mkdir(args.folder)

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
c.execute('SELECT DISTINCT DEPT, CATALOG_NBR FROM records ORDER BY DEPT, CATALOG_NBR;')
unique_courses = c.fetchall()
c.execute('SELECT COUNT(*) FROM records;')
total_rows = c.fetchone()["COUNT(*)"]

print(f'{len(unique_courses)} distinct courses and {total_rows} total rows in {args.dbfile}')

# assign an outfile file
for row in unique_courses:
    row["outfile"] = f'{row["DEPT"]} {row["CATALOG_NBR"]}.jsonl'

# progress bar
with tqdm(total=total_rows, unit="rows") as t:
    i = 1 # used in the progress bar description to indicate what course is being processed
    # for every unique course ()
    for row in unique_courses:
        t.set_description(f'[{i}/{len(unique_courses)}] {row["outfile"]}')
        # get all sections
        c.execute('SELECT * FROM records WHERE DEPT=? AND CATALOG_NBR=?', (row["DEPT"], row["CATALOG_NBR"]))
        sections = c.fetchall()
        # the first line is a header
        meta = {
            "department": row["DEPT"],
            "catalogNumber": row["CATALOG_NBR"],
            "description": sections[0]["COURSE_DESCR"],
            "cumulativeGPAmin": None,
            "cumulativeGPAmax": None,
            "cumulativeGPA": None,
            "sectionCount": 0
        }
        # write the file
        with open(os.path.join(args.folder, row["outfile"]), 'w') as f:
            # write the header line
            f.write(f'{json.dumps(meta)}\n')
            # for every section
            for sec in sections:
                # write JSON in the new schema
                f.write(f'''{json.dumps({
                    "term": sec["TERM_CODE"],
                    "termString": sec["TERM"],
                    "sectionNumber": sec["CLASS_SECTION"],
                    "instructorFirstName": sec["INSTR_FIRST_NAME"],
                    "instructorsLastName": sec["INSTR_LAST_NAME"],
                    "semesterGPA": sec["AVG_GPA"],
                    "A": sec["A"],
                    "B": sec["B"],
                    "C": sec["C"],
                    "D": sec["D"],
                    "F": sec["F"],
                    "Q": sec["Q"],
                    "instructor": None,
                    "instructorTermGPAmin": sec["PROF_MIN"],
                    "instructorTermGPAmax": sec["PROF_MAX"],
                    "instructorTermGPA": sec["PROF_AVG"],
                    "instructorTermSectionsTaught": sec["PROF_COUNT"]
                })}\n''')
                t.update() # update progress bar
        i += 1 # increment the course counter