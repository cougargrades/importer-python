CREATE TABLE IF NOT EXISTS records (
    ID int unsigned not null primary key unique,
    TERM text,
    DEPT text,
    CATALOG_NBR text,
    CLASS_SECTION smallint,
    COURSE_DESCR text,
    INSTR_LAST_NAME text,
    INSTR_FIRST_NAME text,
    A smallint,
    B smallint,
    C smallint,
    D smallint,
    F smallint,
    Q smallint,
    AVG_GPA real,
    PROF_COUNT smallint,
    PROF_AVG real,
    TERM_CODE int,
    GROUP_CODE text,
    FIRESTORE_KEY text
    );
INSERT INTO records VALUES(1,'Spring 2019','AAS','2320',1,'Intro To African American Stdy','Smith','Marlon Antoine',7,12,7,4,6,0,2.3140000000000000568,1,2.3140000000000000568,201901,'201901-AAS2320_SmithMarlonAntoine','201901-AAS2320_SmithMarlonAntoine~1');
INSERT INTO records VALUES(2,'Spring 2019','AAS','2320',2,'Intro To African American Stdy','Horne','Gerald',NULL,NULL,NULL,NULL,NULL,NULL,NULL,1,NULL,201901,'201901-AAS2320_HorneGerald','201901-AAS2320_HorneGerald~2');
INSERT INTO records VALUES(3,'Spring 2019','AAS','2320',3,'Intro To African American Stdy','Walker','Alan',19,10,6,0,5,3,2.9249999999999998223,1,2.9249999999999998223,201901,'201901-AAS2320_WalkerAlan','201901-AAS2320_WalkerAlan~3');
INSERT INTO records VALUES(4,'Spring 2019','AAS','2320',4,'Intro To African American Stdy','Thompson','Kevin Bernard',25,6,1,2,0,1,3.5200000000000000177,2,3.6429999999999997939,201901,'201901-AAS2320_ThompsonKevinBernard','201901-AAS2320_ThompsonKevinBernard~4');