📚 => collection/subcollection

📔 => document

📁 => array

property => defined in source material

*property* => computed by Cloud Functions

**property** => references another document



📚 catalog

- 📔 MATH 2331
  - department
  - catalogNumber
  - description
  - *cumulativeGPAmin*
  - *cumulativeGPAmax*
  - *cumulativeGPA*
  - *sectionCount*
  - 📚 sections
    - 📔 201303-1 (generated)
      - term
      - termString
      - sectionNumber
      - instructorFirstName
      - instructorLastName
      - semesterGPA
      - A
      - B
      - C
      - D
      - F
      - Q
      - ***instructor***
      - instructorTermGPAmin
      - instructorTermGPAmax
      - instructorTermGPA
        - average of other sections taught by prof this term
      - instructorTermSectionsTaught
        - number of other sections taught by prof this term
    - ...
- 📔 COSC 1430
  - ...



📚 instructors

- 📔 Lovelace, Ada
  - *firstName*
  - *lastName*
  - 📁 courses
    - ***catalog/MATH 2331***
    - ...
  - 📁 sections
    - ***catalog/MATH 2331/sections/abcdef***
    - ...



```javascript
exports.computeSpecialFields = functions.firestore.document('catalog/{courseId}/sections/{sectionId}').onCreate((change, context) => {
    // do thing
})
```

