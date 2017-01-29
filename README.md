### Title: Jobs on The Muse
### Pre-requisites: 
Python 2.7+ with requests, json, psycopg2, sys, getopt packages.
Local installation of PostgreSQL database system (9.6) with server running
on port 5432, with a database named 'tmj' with db user name 'tmj1' and 
password 'tmj1'

### Brief Description:
The script getAllJobs.py, in src directory, uses the 'Jobs' endpoint from the public API - 
https://www.themuse.com/developers/api/v2, to download specified number of pages of jobs,
and stores them in a local relational store and answers a specific question. 
Detailed description is given below in Description section. 

### Instructions and Usage examples
The following (optional) parameters can be specified: --pages, --dbname, --dbuser, 
--dbPassword, --host. All parameters are optional

With no parameters  all jobs data on all pages, are downloaded, and question is answered as shown below:

##### tmj1@ip-172-31-27-12:~/tmj/tmj/src$ ./getAllJobs.py 

*** Proceeding with default values of all parameters.
*** Usage and the parameter default values are shown below.

Usage: ./getAllJobs.py --Param1=val --Param2=val ...

Parameters: All parameteres are optional, default values are indicated below in [].

  --pages        [All] # pages to download; if more than total available
                 number is specified, then data from all available pages is downloaded.
                 
  --dbName       [tmj] Name of the PostgreSQL database to store the downloaded data.
  
  --dbUser       [tmj1] Name of the authorized user of the specified dbName
  
  --dbPassword   [tmj1] Passowrd for the authorized user of the database
  
  --host         [localhost] Name of the host with the specified PostgreSQL database
  
  
Connecting to database identified by: dbname = tmj user = tmj1 password = tmj1 host = localhost

Database connection established and schema created.

select count(*) from tm_jobs1 where joblocname = 'New York City Metro Area' and jobpubldate >= '09-01-2016' and jobpubldate <= '09-30-2016' 

Number of jobs located in New City Metro Area, and published from September 1st to 30th 2016 are:  1

#### tmj1@ip-172-31-27-12:~/tmj/tmj/src$ ./getAllJobs.py --pages=7

Connecting to database identified by: dbname = tmj user = tmj1 password = tmj1 host = localhost
Database connection established and schema created.

select count(*) from tm_jobs1 where joblocname = 'New York City Metro Area' and jobpubldate >= '09-01-2016' and jobpubldate <= '09-30-2016' 

Number of jobs located in New City Metro Area, and published from September 1st to 30th 2016 are:  0

#### tmj1@ip-172-31-27-12:~/tmj/tmj/src$ ./getAllJobs.py --pages=90

Connecting to database identified by: dbname = tmj user = tmj1 password = tmj1 host = localhost

Database connection established and schema created.

select count(*) from tm_jobs1 where joblocname = 'New York City Metro Area' and jobpubldate >= '09-01-2016' and jobpubldate <= '09-30-2016' 

Number of jobs located in New City Metro Area, and published from September 1st to 30th 2016 are:  1

#### tmj1@ip-172-31-27-12:~/tmj/tmj/src$ ./getAllJobs.py --pages=101

Connecting to database identified by: dbname = tmj user = tmj1 password = tmj1 host = localhost

Database connection established and schema created.

Specified pages [101] is equal or more than available pages [100]; getting all pages.

select count(*) from tm_jobs1 where joblocname = 'New York City Metro Area' and jobpubldate >= '09-01-2016' and jobpubldate <= '09-30-2016' 

Number of jobs located in New City Metro Area, and published from September 1st to 30th 2016 are:  1

#### tmj1@ip-172-31-27-12:~/tmj/tmj/src$ ./getAllJobs.py --pages=All --dbName=tmj --dbUser=tmj1 dbPassword=tmj1 --host=localhost 

Connecting to database identified by: dbname = tmj user = tmj1 password = tmj1 host = localhost

Database connection established and schema created.

select count(*) from tm_jobs1 where joblocname = 'New York City Metro Area' and jobpubldate >= '09-01-2016' and jobpubldate <= '09-30-2016' 

Number of jobs located in New City Metro Area, and published from September 1st to 30th 2016 are:  1

#### tmj1@ip-172-31-27-12:~/tmj/tmj/src$ ./getAllJobs.py --pages=2 --dbName=tmj --host=local

Connecting to database identified by: dbname = tmj user = tmj1 password = tmj1 host = local

Function setupDB: Unable to connect to database. Please check parameters.

could not translate host name "local" to address: Name or service not known

#### tmj1@ip-172-31-27-12:~/tmj/tmj/src$ ./getAllJobs.py --pages=All --dbName=tmj1 --dbUser=tmj1 dbPassword=tmj1 --host=localhost 

Connecting to database identified by: dbname = tmj1 user = tmj1 password = tmj1 host = localhost

Function setupDB: Unable to connect to database. Please check parameters.

FATAL:  database "tmj1" does not exist

### Description 
Using the 'Jobs' endpoint from The Muse public API, the script getAllJobs.py performs 
the following operations:

(1) Downloads publicly available data from the “Jobs” endpoint.
It uses an optional argument '--pages' that specifies how many pages to download.
If '--pages' is not specified, all jobs data pages are downloaded from the endpoint.

(2) Stores the downloaded data in a PostgreSQL database system on local machine.
[It is assumed that database with name 'tmj' with db user name 'tmj1' and password 'tmj1'
pre-exists on localhost and PostgreSQL service is using default port 5432, and requests,
json, psycopg2, sys and getopt Python packages are installed on local machine.]

(3) Creates the schema indicated below, and populates the tables with the downloaded
jobs data pages.

(4) Executes a SQL statement to answer the following question:
How many jobs with the location "New York City Metro Area" were published from
September 1st to 30th 2016?

Schema Design is given below using PostgreSQL DDL statements.
There are four tables: tm_jobs1, tm_companies1, tm_levels1, and tm_tags1.

create table tm_jobs1 (jobId integer primary key, jobName text, jobShortName text,
                      jobLocName text, jobPublDate timestamp,
                      jobType text, jobCategoryName text,
                      jobCompanyName text, jobLevelName text,
                      jobRefsURL text, jobTagsName text, jobModelType text,
                      jobContents text);
                      
create table tm_companies1 (companyId integer primary key, companyName text,
                            companyShortName text);
                            
 create table tm_levels1 (levelName text primary key, levelShortName text);
 
 create table tm_tags1   (tagName text primary key, tagShortName text);





