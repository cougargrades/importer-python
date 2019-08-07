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
from firebase_admin.firestore import Increment

parser = argparse.ArgumentParser(description='Import formatted JSONL files into Google Firestore')
parser.add_argument('jsonlfiles', metavar='COSC 1430.jsonl', type=str, nargs='+',
                    help='A set of catalog JSONL files to source data from')
parser.add_argument('--key', dest='key', default=None,
                    help='Path to Firebase Service account private key (see: README) ')
parser.add_argument('--meta', dest='meta', default=None,
                    help='Path to catalog_meta/meta.json')

args = parser.parse_args()
#print(args)

if args.key == None or not os.path.isfile(args.key):
    print(f'{args.key} is not a file')
    exit(1)
if args.meta == None or not os.path.isfile(args.meta):
    print(f'{args.kmetaey} is not a file')
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

#batch = db.batch()

with tqdm(total=total_rows, unit="rows") as t:
    i = 1
    # for every file (each file is a course)
    for arg in args.jsonlfiles:
        # prepare batch for faster uploading
        # up to 4 operations per iteration, so batch every 100 iterations
        #if((i-1) % 100):
            #batch.commit()

        # open file
        with open(arg, 'r') as f:
            j = 0
            # declare variable
            sections = {}
            courseRef = {}
            courseName = None
            for line in f:
                # load json line as Dict
                obj = json.loads(line)
                if j == 0:
                    # block for creating course document
                    # update progress bar
                    t.set_description(f'[{i}/{len(args.jsonlfiles)}] {obj["department"]} {obj["catalogNumber"]}')
                    # get course reference
                    course = catalog.document(f'{obj["department"]} {obj["catalogNumber"]}')
                    # save course name for other part of the code
                    courseName = f'{obj["department"]} {obj["catalogNumber"]}'
                    # if course doesn't exist, set it to the default things
                    if not course.get().exists:
                        course.set(obj)
                    courseRef = course
                    sections = catalog.document(f'{obj["department"]} {obj["catalogNumber"]}').collection('sections')
                else:
                    # check for existence of section already (https://stackoverflow.com/a/3114640)
                    #t.write(f'{sections.parent.path} -> {sections.id}')
                    secQuery = sections.where('term','==',obj["term"]).where('sectionNumber','==',obj["sectionNumber"])
                    if any(True for _ in secQuery.stream()):
                        t.write(f'{courseRef.id}#{obj["term"]}-{obj["sectionNumber"]} already exists')
                        t.update()
                        continue

                    # block for populating sections subcollection
                    obj["instructors"] = []
                    # for every intructor in the file
                    for item in obj["instructorNames"]:
                        # make reference for the instructor
                        instructorRef = instructors.document(f'{item["lastName"]}, {item["firstName"]}')
                        # save instructor reference to section document
                        obj["instructors"] += [ instructorRef ]
                        # get the data for this instructor
                        instructorSnap = instructorRef.get()
                        if not instructorSnap.exists:
                            # if he doesn't yet exist, create him
                            instructorRef.set({
                                "firstName": item["firstName"],
                                "lastName": item["lastName"],
                                "courses": [ courseRef ],
                                "sections": [],
                                "courses_count": 1,
                                "sections_count": 0
                            })
                        else:
                            # [DocumentReference, ...] => ["COSC 1430", ...]
                            my_courses = [item.id for item in instructorSnap.to_dict()["courses"]]
                            # if the course i'm operating on isn't in listed as a course for this instructor
                            if(courseName not in my_courses):
                                # add it and increment the course count with the snapshot we already had to fetch
                                instructorRef.update({
                                    "courses": ArrayUnion([courseRef]),
                                    "courses_count": Increment(1)
                                })
                    # add section to course, save reference to document as a variable
                    secRef = sections.add(obj)[1]
                    for item in obj["instructorNames"]:
                        # make reference for the instructor
                        instructorRef = instructors.document(f'{item["lastName"]}, {item["firstName"]}')
                        instructorRef.update({
                            "sections": ArrayUnion([secRef]),
                            "sections_count": Increment(1)
                        })
                    t.update()
                j += 1
        i += 1

# Updating metadata

spinner = Halo(text=f'Merging local catalog metadata with Firestore ...', spinner='dots')
spinner.start()

with open(args.meta, 'r') as f:
    metaLocal = json.loads(f.read())
    metaRef =  db.collection('catalog_meta').document('meta')
    metaSnap = metaRef.get()
    if not metaSnap.exists:
        metaRef.set(metaLocal)
    else:
        metaSnap = metaSnap.to_dict()
        # merging logic
        if metaLocal["latestTerm"] > metaSnap["latestTerm"]:
            metaRef.set({
                "latestTerm":  metaLocal["latestTerm"]
            }, merge=True)
        # in the future: merge other values as needed

spinner.succeed(text=f'Catalog metadata written')
