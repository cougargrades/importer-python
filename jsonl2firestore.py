#!/usr/bin/env python3

import os
import os.path
import sys
import json
import argparse
import time
import copy
from tqdm import tqdm
from halo import Halo

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin.firestore import ArrayUnion
from firebase_admin.firestore import Increment

parser = argparse.ArgumentParser(description='Import formatted JSONL files into Google Firestore')
parser.add_argument('folder', metavar='records.db', type=str,
                    help='Folder where .jsonl files are stored.')
parser.add_argument('--key', dest='key', default=None,
                    help='Path to Firebase Service account private key (see: README) ')

args = parser.parse_args()

if args.key == None or not os.path.isfile(args.key):
    print(f'{args.key} is not a file')
    exit(1)

if args.folder == None:
    print(f'[folder] must be a folder name.')
    exit(1)
else:
    # if not a directory and not an existing file
    if not os.path.isdir(args.folder) and not os.path.isfile(args.folder):
        # create the folder
        print(f'A folder was not found at: {args.folder}')
    if(not os.path.isdir(os.path.join(args.folder, 'catalog')) and not os.path.isfile(os.path.join(args.folder, 'catalog'))):
        # create the subfolder
        print(f'A `catalog` folder was not found under: {args.folder}')
    if(not os.path.isdir(os.path.join(args.folder, 'catalog_meta')) and not os.path.isfile(os.path.join(args.folder, 'catalog_meta'))):
        # create the subfolder
        print(f'A `catalog_meta` folder was not found under: {args.folder}')
    if(not os.path.isdir(os.path.join(args.folder, 'instructors')) and not os.path.isfile(os.path.join(args.folder, 'instructors'))):
        # create the subfolder
        print(f'An `instructors` folder was not found under: {args.folder}')

def file_len(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def get_instructor(name):
    if os.path.isfile(os.path.join(args.folder, 'instructors', name)):
        with open(os.path.join(args.folder, 'instructors', name), 'r') as f:
            return json.loads(f.read())
    else:
        return None

spinner = Halo(text=f'Estimating number of entries to be processed ...', spinner='dots')
spinner.start()

# lists all files in FOLDER/catalog/ and prepends their path so operations will resolve
jsonlfiles = [os.path.join(args.folder, 'catalog', x) for x in os.listdir(path=os.path.join(args.folder, 'catalog'))]
jsonlfiles.sort()

sum = 0
for arg in jsonlfiles:
    sum += file_len(arg) # add line length (one row per line)
    sum -= 1 # subtract header
spinner.succeed(text=f'{sum} entries were counted in the provided .jsonl files')
total_rows = sum

# prepare Firebase, use a service account
cred = credentials.Certificate(args.key)
firebase_admin.initialize_app(cred)
db = firestore.client()

catalog = db.collection(u'catalog_test')
instructors = db.collection(u'instructors_test')

print(f'📚 Writing {total_rows} courses to Firestore. Instructors will be populated.')

with tqdm(total=total_rows, unit="rows") as t:
    i = 1
    # for every file (each file is a course)
    for arg in jsonlfiles:
        # open file
        with open(arg, 'r') as f:
            j = 0
            # declare variable
            sectionsRef = {}
            courseRef = {}
            courseName = None
            courseMeta = {}
            for line in f:
                # load json line as Dict
                obj = json.loads(line)
                if j == 0:
                    # block for creating course document
                    # update progress bar
                    t.set_description(f'[{i}/{len(jsonlfiles)}] {obj["department"]} {obj["catalogNumber"]}')
                    # get course reference
                    courseRef = catalog.document(f'{obj["department"]} {obj["catalogNumber"]}')
                    # save course name for other part of the code
                    courseName = f'{obj["department"]} {obj["catalogNumber"]}'
                    # save course details for other part of the code
                    courseMeta = copy.deepcopy(obj)
                    # if course doesn't exist, set it to the default things
                    if not courseRef.get().exists:
                        courseRef.set(obj)
                    sectionsRef = catalog.document(f'{obj["department"]} {obj["catalogNumber"]}').collection('sections')
                else:
                    # check for existence of section already (https://stackoverflow.com/a/3114640)
                    secQuery = sectionsRef.where('term','==',obj["term"]).where('sectionNumber','==',obj["sectionNumber"])
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
                            # grab statistics for this instructor
                            prof = get_instructor(f'{item["lastName"]}, {item["firstName"]}.json')
                            # if he doesn't yet exist, create him
                            # include pre-computed statistics data
                            instructorRef.set({
                                "firstName": item["firstName"],
                                "lastName": item["lastName"],
                                "fullName": prof["fullName"],
                                "courses": [ courseRef ],
                                "keywords": prof["keywords"],
                                "sections": [],
                                "departments": { # initialize the `departments` Map if this instructor does not yet exist
                                    f'{courseMeta["department"]}': 1
                                 },
                                "courses_count": 1,
                                "sections_count": 0,
                                "GPA": {
                                    "minimum": prof["GPA.minimum"],
                                    "maximum": prof["GPA.maximum"],
                                    "average": prof["GPA.average"],
                                    "median": prof["GPA.median"],
                                    "range": prof["GPA.range"],
                                    "standardDeviation": prof["GPA.standardDeviation"]
                                }
                            })
                        else:
                            # [DocumentReference, ...] => ["COSC 1430", ...]
                            my_courses = [item.id for item in instructorSnap.to_dict()["courses"]]
                            # if the course i'm operating on isn't in listed as a course for this instructor
                            if(courseName not in my_courses):
                                #print(f'courseMeta: {courseMeta}')
                                #print(f'instructorSnap.to_dict() : {instructorSnap.to_dict()}')
                                # add it and increment the course count with the snapshot we already had to fetch
                                instructorRef.update({
                                    "courses": ArrayUnion([courseRef]),
                                    "courses_count": Increment(1), # if the `departments` Map does not yet have a property for this department, set it to 1. if it already exists, increment it.
                                    f'departments': {
                                        f'{courseMeta["department"]}': 1 if "departments" in instructorSnap.to_dict().keys() and f'{courseMeta["department"]}' not in instructorSnap.to_dict()["departments"].keys() else Increment(1)
                                    }
                                })
                    # add section to course, save reference to document as a variable
                    secRef = sectionsRef.add(obj)[1]
                    for item in obj["instructorNames"]:
                        # make reference for the instructor
                        instructorRef = instructors.document(f'{item["lastName"]}, {item["firstName"]}')
                        instructorRef.update({
                            "sections": ArrayUnion([secRef]),
                            "sections_count": Increment(1)
                        })
                    # now that the section has been completely written, update the section counter for this course
                    courseRef.update({
                        "sectionCount": Increment(1)
                    })
                    t.update()
                j += 1
        i += 1
    
# Updating metadata
spinner = Halo(text=f'Merging local catalog metadata with Firestore ...', spinner='dots')
spinner.start()

with open(os.path.join(args.folder, 'catalog_meta', 'meta.json'), 'r') as f:
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
