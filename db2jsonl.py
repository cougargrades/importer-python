#!/usr/bin/env python3

import os
import os.path
import sys
import sqlite3
import json
import argparse
import copy
import statistics
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
    if(not os.path.isdir(os.path.join(args.folder, 'catalog')) and not os.path.isfile(os.path.join(args.folder, 'catalog'))):
        # create the subfolder
        os.mkdir(os.path.join(args.folder, 'catalog'))
    if(not os.path.isdir(os.path.join(args.folder, 'catalog_meta')) and not os.path.isfile(os.path.join(args.folder, 'catalog_meta'))):
        # create the subfolder
        os.mkdir(os.path.join(args.folder, 'catalog_meta'))
    if(not os.path.isdir(os.path.join(args.folder, 'instructors')) and not os.path.isfile(os.path.join(args.folder, 'instructors'))):
        # create the subfolder
        os.mkdir(os.path.join(args.folder, 'instructors'))


print('''
TODO
  ◯ Write `catalog` collection
  ◯ Write `instructors` collection
  ◯ Compute statistics for `instructors` collection
  ◯ Compute statistics for `catalog` collection
''')

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def update_instructor(name, data, merge=False):
    post = {}
    if os.path.isfile(os.path.join(args.folder, 'instructors', name)):
        with open(os.path.join(args.folder, 'instructors', name), 'r') as f:
            pre = json.loads(f.read())
            post = copy.deepcopy(pre)
            for key, value in data.items():
                # when merging...
                if merge:
                    # if a value is numeric and existed in the previous file
                    if str(value).isnumeric() and pre[key] != None:
                        # treat the provided value as a delta
                        post[key] += value
                else:
                    post[key] = value
    else:
        post = copy.deepcopy(data)
    with open(os.path.join(args.folder, 'instructors', name), 'w') as f:
        f.write(f'''{json.dumps(post)}\n''')

def get_instructor(name):
    if os.path.isfile(os.path.join(args.folder, 'instructors', name)):
        with open(os.path.join(args.folder, 'instructors', name), 'r') as f:
            return json.loads(f.read())
    else:
        return None

def update_course(name, data, merge=False):
    post = {}
    lines = []
    if os.path.isfile(os.path.join(args.folder, 'catalog', name)):
        with open(os.path.join(args.folder, 'catalog', name), 'r') as f:
            lines = f.readlines()
            pre = json.loads(lines[0])
            post = copy.deepcopy(pre)
            for key, value in data.items():
                post[key] = value
    else:
        post = copy.deepcopy(data)
    with open(os.path.join(args.folder, 'catalog', name), 'w') as f:
        f.write(f'''{json.dumps(post)}\n''')
        for i in range(1,len(lines)):
            f.write(f'''{lines[i]}''')

def get_course(name):
    if os.path.isfile(os.path.join(args.folder, 'catalog', name)):
        with open(os.path.join(args.folder, 'catalog', name), 'r') as f:
            return json.loads(f.readlines()[0])
    else:
        return None

# https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.row_factory
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def statrange(x):
    return max(x) - min(x)

# setup sqlite
conn = sqlite3.connect(args.dbfile)
conn.row_factory = dict_factory
c = conn.cursor()
c.execute('SELECT DISTINCT DEPT, CATALOG_NBR FROM records ORDER BY DEPT, CATALOG_NBR;')
unique_courses = c.fetchall()
c.execute('SELECT COUNT(*) FROM records;')
total_rows = c.fetchone()["COUNT(*)"]

print(f'{len(unique_courses)} distinct courses and {total_rows} total rows in {args.dbfile}')

spinner = Halo(text='Writing collection `catalog_meta` ...', spinner='dots')
spinner.start()
c.execute('SELECT * FROM catalog_meta;')
catalog_meta = c.fetchall() # [{'latestTerm': 201901}]
catalog_meta = catalog_meta[0]
with open(os.path.join(args.folder, 'catalog_meta', 'meta.json'), 'w') as f:
    f.write(f'{json.dumps(catalog_meta)}')
spinner.succeed()

print('Writing collection `catalog/` ...')

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
            "GPA": {
                "minimum": None,
                "maximum": None,
                "average": None,
                "median": None,
                "range": None,
                "standardDeviation": None,
            },
            "sectionCount": 0
        }
        # write the file
        with open(os.path.join(args.folder, 'catalog', row["outfile"]), 'w') as f:
            # write the header line
            f.write(f'{json.dumps(meta)}\n')

            # hold individual sections in memory to be de-duped after the first traversal of `sections`
            cache = []
            # for every section
            for sec in sections:
                # write JSON in the new schema
                data = {
                    "term": sec["TERM_CODE"],
                    "termString": sec["TERM"],
                    "sectionNumber": sec["CLASS_SECTION"],
                    "semesterGPA": sec["AVG_GPA"],
                    "A": sec["A"],
                    "B": sec["B"],
                    "C": sec["C"],
                    "D": sec["D"],
                    "F": sec["F"],
                    "Q": sec["Q"],
                    "instructorNames": []
                }
                # check for multiple instructors teaching this section
                # https://stackoverflow.com/a/25373204
                dups = list(filter(lambda s: s["TERM_CODE"] == sec["TERM_CODE"] and s["CLASS_SECTION"] == sec["CLASS_SECTION"], sections)) # array of complete sqlite dicts
                # lamba function counts number of names in a provided list
                countNames = lambda l, first, last: sum(1 if v["INSTR_FIRST_NAME"] == first and v["INSTR_LAST_NAME"] == last else 0 for v in l)
                # de-dupe dups for instructors that are listed twice for the same section number
                for j in range(len(dups)-1,0,-1):
                    # if this instructor has multiple records for some reason
                    if(countNames(dups, dups[j]["INSTR_FIRST_NAME"], dups[j]["INSTR_LAST_NAME"]) > 1):
                        dups.pop(j)
                # for every instructor that taught this exact section
                for d in dups:
                    # append a new object to "instructors" property
                    data["instructorNames"] += [{
                        "firstName": d["INSTR_FIRST_NAME"],
                        "lastName": d["INSTR_LAST_NAME"],
                        "termGPAmin": d["PROF_MIN"],
                        "termGPAmax": d["PROF_MAX"],
                        "termGPA": d["PROF_AVG"],
                        "termSectionsTaught": d["PROF_COUNT"]
                    }]
                # append to cache
                cache += [data]
            # counts the number of exact section instances in a list
            countSections = lambda l, term, secNum: sum(1 if v["term"] == term and v["sectionNumber"] == secNum else 0 for v in l)
            # for every item in cache (traverse backwards because removing items)
            for j in range(len(cache)-1,0,-1):
                # if this exact section has multiple occurences
                if(countSections(cache, cache[j]["term"], cache[j]["sectionNumber"]) > 1):
                    # remove it
                    cache.pop(j)
                    # update the progress bar for sections that were removed so that it's not 95% when done
                    t.update()
            # for every item in cache
            for p in cache:
                # write to jsonl file
                f.write(f'''{json.dumps(p)}\n''')
                # update progress bar
                t.update()
        # increment the course counter
        i += 1

finished_rows = 0
for x in os.listdir(path=os.path.join(args.folder, 'catalog')):
    finished_rows += file_len(os.path.join(args.folder, 'catalog', x)) # add line length (one row per line)
    finished_rows -= 1 # subtract header
print(f'To account for sections with multiple professors, {total_rows} records were de-duplicated into {finished_rows} ({round(((1 - (total_rows/finished_rows)) * 100), 1)}%).')

spinner = Halo(text=f'Estimating number of entries to be processed ...', spinner='dots')
spinner.start()

# lists all files in FOLDER/catalog/ and prepends their path so operations will resolve
jsonlfiles = [os.path.join(args.folder, 'catalog', x) for x in os.listdir(path=os.path.join(args.folder, 'catalog'))]
jsonlfiles.sort()

sum = 0
for arg in jsonlfiles:
    sum += file_len(arg) # add line length (one row per line)
    sum -= 1 # subtract header
spinner.succeed(text=f'{sum} sections were counted in the provided .jsonl files.')
total_rows = sum

print(f'🔎 Inspecting {total_rows} records to enumerate instructors.')

with tqdm(total=total_rows, unit="rows") as t:
    i = 1
    # for every file (each file is a course)
    for arg in jsonlfiles:
        # open file
        with open(arg, 'r') as f:
            j = 0
            # declare variable
            courseName = None
            courseMeta = {}
            for line in f.readlines():
                # load json line as Dict
                obj = json.loads(line)
                if j == 0:
                    # block for creating course document
                    # update progress bar
                    t.set_description(f'[{i}/{len(jsonlfiles)}] {obj["department"]} {obj["catalogNumber"]}')
                    courseName = f'{obj["department"]} {obj["catalogNumber"]}'
                    # save course details for other part of the code
                    courseMeta = copy.deepcopy(obj)
                else:
                    obj["instructors"] = []
                    # for every intructor in the file
                    for item in obj["instructorNames"]:
                        update_instructor(f'{item["lastName"]}, {item["firstName"]}.json', {
                            "firstName": item["firstName"],
                            "lastName": item["lastName"],
                            "GPA.minimum": None,
                            "GPA.maximum": None,
                            "GPA.average": None,
                            "GPA.median": None,
                            "GPA.range": None,
                            "GPA.standardDeviation": None
                        })
                    t.update()
                j += 1
        i += 1

# lists all files in FOLDER/instructors/
spinner = Halo(text=f'Listing and sorting files in {os.path.join(args.folder, "instructors")}', spinner='dots')
spinner.start()
instructors = os.listdir(path=os.path.join(args.folder, 'instructors'))
instructors.sort()
spinner.succeed()

print(f'📊 Computing statistics for {len(instructors)} instructors.')

i = 1
for item in tqdm(iterable=instructors, total=len(instructors), unit="files"):
    t.set_description(f'[{i}/{len(instructors)}] {item}')
    pre = get_instructor(item)
    c.execute('SELECT PROF_AVG FROM records WHERE INSTR_LAST_NAME=? AND INSTR_FIRST_NAME=?', (pre["lastName"], pre["firstName"]))
    sections = c.fetchall()
    # create an array of floats
    grades = [ x["PROF_AVG"] for x in sections ]
    # filter the None values
    grades = list(filter(lambda x: x != None, grades))
    if len(grades) > 0:
        update_instructor(item, {
            "GPA.minimum": min(grades),
            "GPA.maximum": max(grades),
            "GPA.average": statistics.mean(grades),
            "GPA.median": statistics.median(grades),
            "GPA.range": statrange(grades),
            "GPA.standardDeviation": statistics.stdev(grades) if len(grades) > 1 else 0
        })
    i += 1

# lists all files in FOLDER/instructors/
spinner = Halo(text=f'Listing and sorting files in {os.path.join(args.folder, "catalog")}', spinner='dots')
spinner.start()
catalog = os.listdir(path=os.path.join(args.folder, 'catalog'))
catalog.sort()
spinner.succeed()

print(f'📊 Computing statistics for {len(catalog)} courses.')

i = 1
for item in tqdm(iterable=catalog, total=len(catalog), unit="files"):
    t.set_description(f'[{i}/{len(catalog)}] {item}')
    pre = get_course(item)
    c.execute('SELECT PROF_AVG FROM records WHERE DEPT=? AND CATALOG_NBR=?', (pre["department"], pre["catalogNumber"]))
    sections = c.fetchall()
    # create an array of floats
    grades = [ x["PROF_AVG"] for x in sections ]
    # filter the None values
    grades = list(filter(lambda x: x != None, grades))
    #print(grades)
    if len(grades) > 0:
        update_course(item, {
            "GPA": {
                "minimum": min(grades),
                "maximum": max(grades),
                "average": statistics.mean(grades),
                "median": statistics.median(grades),
                "range": statrange(grades),
                "standardDeviation": statistics.stdev(grades) if len(grades) > 1 else 0
            }
        })
    i += 1