#!/usr/bin/env python3

import os
import os.path
import sys
import json
import argparse
import time
from tqdm import tqdm
from halo import Halo

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin.firestore import ArrayUnion

parser = argparse.ArgumentParser(description='Import formatted JSONL files into Google Firestore')
parser.add_argument('jsonlfiles', metavar='COSC 1430.jsonl', type=str, nargs='+',
                    help='A set of CSV files to source data from')
parser.add_argument('--key', dest='key', default=None,
                    help='Path to Firebase Service account private key (see: README) ')

args = parser.parse_args()
#print(args)

if args.key == None or not os.path.isfile(args.key):
    print(f'{args.key} is not a file')
    exit(1)

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

spinner = Halo(text=f'Estimating number of entries to be processed ...', spinner='dots')
spinner.start()


sum = 0
for arg in args.jsonlfiles:
    sum += file_len(arg) # add line length (one row per line)
    sum -= 1 # subtract header
spinner.succeed(text=f'{sum} entries were counted in the provided .jsonl files')
total_rows = sum

# prepare Firebase, use a service account
cred = credentials.Certificate(args.key)
firebase_admin.initialize_app(cred)
db = firestore.client()

catalog = db.collection(u'catalog')
instructors = db.collection(u'instructors')

print(f'📝 Writing {total_rows} records to Firestore.')

with tqdm(total=total_rows, unit="rows") as t:
    i = 1
    # for every file (each file is a course)
    for arg in args.jsonlfiles:
        # open file
        with open(arg, 'r') as f:
            j = 0
            # declare variable
            sections = {}
            courseRef = {}
            for line in f:
                # load json line as Dict
                obj = json.loads(line)
                if j == 0:
                    # block for creating course document
                    # update progress bar
                    t.set_description(f'[{i}/{len(args.jsonlfiles)}] {obj["department"]} {obj["catalogNumber"]}')
                    # get course reference
                    course = catalog.document(f'{obj["department"]} {obj["catalogNumber"]}')
                    # if course doesn't exist, set it to the default things
                    if not course.get().exists:
                        course.set(obj)
                    courseRef = course
                    sections = catalog.document(f'{obj["department"]} {obj["catalogNumber"]}').collection('sections')
                else:
                    # block for populating sections subcollection
                    # make reference for the instructor
                    instructorRef = instructors.document(f'{obj["instructorLastName"]}, {obj["instructorFirstName"]}')
                    # save reference to section document
                    obj["instructor"] = instructorRef
                    # get the data for this instructor
                    instructor = instructorRef.get()
                    if not instructor.exists:
                        # if he doesn't yet exist, create him
                        instructorRef.set({
                            "firstName": obj["instructorFirstName"],
                            "lastName": obj["instructorLastName"],
                            "courses": [],
                            "sections": []
                        })
                    # add section to course, save reference to document
                    secRef = sections.add(obj)[1]
                    # add course and section to instructor
                    instructorRef.update({
                        "courses": ArrayUnion([courseRef]),
                        "sections": ArrayUnion([secRef])
                    })
                    t.update()
                j += 1
        i += 1

exit(0)

