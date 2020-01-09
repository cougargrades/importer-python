
import itertools

# inspired by: https://medium.com/@ken11zer01/firebase-firestore-text-search-and-pagination-91a0df8131ef
def createKeywords(name):
    arrName = []
    curName = ''
    for letter in name:
        curName += letter.lower()
        arrName += [ curName ]
    return arrName

def generateKeywords(firstName, lastName):
    fullName = f'{firstName} {lastName}'
    k = len(fullName.split(' ')) # number of "names", "John Robert Doe" => 3
    permutations = generatePermutations(fullName) if k < 5 else generateConservativePermutations(firstName, lastName)
    result = []
    for p in permutations:
        result += createKeywords(p)
    return sorted(list(set(result)))

# inspired by: https://stackoverflow.com/a/464882
def generatePermutations(fullName):
    names = fullName.split(' ')
    permutations = []
    results = []
    for i in range(1, len(names)+1):
        permutations += list(itertools.permutations(names, i))
    for tup in permutations:
        results += [ ' '.join(tup) ]
    return results

def generateConservativePermutations(firstName, lastName):
    return [
        firstName,
        lastName,
        f'{firstName} {lastName}',
        f'{lastName} {firstName}'
    ]