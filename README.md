# SQreamTest

This is small test in python for new jow in SQReam company

The code was build with python 3.8

List of Files
buildPostgreDB.py - Build new databse 'sqltasks'
config.py         - Read configuration ini file
sqlTask1.py       - First Task in Sql
sqlTask2.py       - Second Task in Sql
pythonTask1.py    - First Task in python
pythonTask2.py    - Second Task in python

Data Files:
newdatabase.ini - for configuration first time to PostGreSql for building the databse
database.ini    - for configuration to PostGreSql
config.json     - Json file for task 1 in python
logfile.log     - Log file for task 2 in python
new_ountrtries.js - print of json file of parsed data from log file


SQL TASKS

1.	Given table T1 and T2, each table has 1 column of the same datatype.
Write a query that will return only the unique values in T1.
•	Write at least 2 queries (1 of them must not include sub-query).


2.	 Given table T and another table called csv.
Implement an update command (without using update command) and create a new table with the updated values (if exists) in the csv table for each ID in table T.
See the expected results below.

Please notice:
•	Not every ID should be updated
•	Some ID’s in the CSV has the same value
•	the answer must contain 1 query/statement only.
•	The tables contain an example data, the statement should work for any amount of data inside the tables.

Python Tasks

General demands:
1. Please add comments to your programs wherever you think it’s necessary.
2. Try to make your code as simple as possible.
3. Try to avoid unnecessary operations in your code.


Task 1
For this task, please download the config.json file attached in mail.

The config file given to you is in json format.

The json has 4 keys:
    * “runtimeFlags”
    * “compilerFlags”
    * “runtimeGlobalFlags”
    * “server”

Each key refers to another dictionary.

For example:

“server” is a key in the json that refers to a dictionary which contains key "port" and value 5000.

Create a code that parses (reads) the config file that was given to you, and print the following:

1. How many items are under each key of the 4 keys listed above.
   
   Example:
   runtimeGlobalFlags: 20   
   runtimeFlags : 10
   compilerFlags: 7   
   server : 4
   
2. Print the items under each key that their value is true.
    (can not be printed as list)


Example of print for one of the keys:
    runtimeGlobalFlags items that has “true” as their value are:    
    
    useMetadataServer
    useDevelopmentLog
    ..
    ..

   * Notes: you may and should import python libraries for this task and use them.
Task 2

Every statement (a query is also a statement) that is being executed using SQream DB is written to a log file, every column is separated by "|".

The log file columns are:

1) Datetime of statement.
2) Thread number.
3) IP of the user running the statement + port.
4) The database the user ran the statement to.
5) IP of the user running the statement.
6) User name of the user running the statement.
7) Statement id of statement the user executed.
8) Service name.
9) Info column includes - success of statement, the statement itself, connection id, start/end time of the statement, number of rows returned. (changes dynamically)

* Connection id is unique for session - each session can consists of multiple statements
for example - connection id 0 has executed stmt:0, stmt:1, stmt:2, stmt:3, stmt:5, stmt:7, stmt:9, stmt:10, stmt:11.


Create a code that parse the log_file given to you (attached in mail), and print analysis back to the user.

Analyze the following questions:


1) How many successful statements were sent by the user?
	Expected results: “Number of Successful statements: X”

2) For each connection id - how many successful statements were sent by the user?
	For each connection id:
		Expected results: “Connection Number X:  Y Successful statements”

3) How many failed/successful statements sent each user to the server?
          For each user:	
          		Expected result: “user X sent Y successful statements”
			     “user X sent Y Failed statements”

4) Which stat	ement was the slowest successful statement?
          X = statement id
   	Expected results: “statement X was the slowest”

5) How many statements in total were sent by the user according to log:
   Expected results: “the user sent total of X statements”





