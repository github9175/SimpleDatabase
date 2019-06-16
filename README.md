# Simple Database

## Introduction
This is a basic database management system called SimpleDB, a course project taken from Berkeley CS deparment's Intro to Database System (https://sites.google.com/site/cs186fall2013/homeworks). It provides several basic functions like a buffer pool, a query optimizor and transaction locks.

## Architecture
SimpleDB consists of:

Classes that represent fields, tuples, and tuple schemas;
Classes that apply predicates and conditions to tuples;
One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations;
A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples;
A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions (neither of which you need to worry about for this project); and,
A catalog that stores information about available tables and their schemas.
SimpleDB does not include many things that you may think of as being a part of a "database."
A simple parser.
A Query optimizer.

In particular, SimpleDB does not have:

Views.
Data types except integers and fixed length strings.
Indices.

## Deployment
SimpleDB uses the Ant build tool to compile the code and run tests. The build file is xx.

You can create any .txt file and convert it to a .dat file in SimpleDB's HeapFile format using the command:

$ java -jar dist/simpledb.jar convert file.txt N
where file.txt is the name of the file and N is the number of columns in the file. Notice that file.txt has to be in the following format:
int1,int2,...,intN
int1,int2,...,intN
int1,int2,...,intN
int1,int2,...,intN
...where each intN is a non-negative integer.

To view the contents of a table, use the print command:

$ java -jar dist/simpledb.jar print file.dat N
where file.dat is the name of a table created with the convert command, and N is the number of columns in the file.
## Testing
To run the unit tests use the test build target:

$ cd CS186-proj1
$ # run all unit tests
$ ant test
$ # run a specific unit test
$ ant runtest -Dtest=TupleTest

If you wish to write new unit tests as you code, they should be added to the test/simpledb directory.

For more details about how to use Ant, see the manual(http://ant.apache.org/manual/). The Running Ant(http://ant.apache.org/manual/running.html) section provides details about using the ant command. However, the quick reference table below should be sufficient for working on the projects.

Command	Description
ant	Build the default target (for simpledb, this is dist).
ant -projecthelp	List all the targets in build.xml with descriptions.
ant dist	Compile the code in src and package it in dist/simpledb.jar.
ant test	Compile and run all the unit tests.
ant runtest -Dtest=testname	Run the unit test named testname.
ant systemtest	Compile and run all the system tests.
ant runsystest -Dtest=testname	Compile and run the system test named testname.
ant handin	Generate tarball for submission.

End-to-end tests are structured as JUnit tests that live in the test/simpledb/systemtest directory. 

To run all the system tests, use the systemtest build target:

$ ant systemtest

 When the tests pass, you will see something like the following:

$ ant systemtest

# ... build output ...

    [junit] Testsuite: simpledb.systemtest.ScanTest
    [junit] Tests run: 3, Failures: 0, Errors: 0, Time elapsed: 7.278 sec
    [junit] Tests run: 3, Failures: 0, Errors: 0, Time elapsed: 7.278 sec
    [junit] 
    [junit] Testcase: testSmall took 0.937 sec
    [junit] Testcase: testLarge took 5.276 sec
    [junit] Testcase: testRandom took 1.049 sec

BUILD SUCCESSFUL
Total time: 52 seconds
