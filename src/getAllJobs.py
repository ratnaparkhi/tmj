#!/usr/bin/python
# coding: utf-8
# This script performs the following operations
# (1) Downloads publicly available data from the “Jobs” endpoint.
# It uses an optional argument '--pages' that specifies how many pages to download. 
# If '--pages' is not specified, all jobs data pages are downloaded from the endpoint. 
# (2) Stores the downloaded data in a PostgreSQL database system on local machine. 
# [It is assumed that database with name 'tmj' with db user name 'tmj1' and password 'tmj1' 
# pre-exists on localhost and PostgreSQL service is using default port 5432, and requests, 
# json, psycopg2 and getopt Python packages are installed on local machine.] 
# (3) Creates the schema indicated below, and populates the tables with the downloaded 
# jobs data pages. 
# (4) Executes a SQL statement to answer the following question: 
# How many jobs with the location "New York City Metro Area" were published from 
# September 1st to 30th 2016?
#
# Schema Design is given below using PostgreSQL DDL statements. 
# There are four tables: tm_jobs1, tm_companies1, tm_levels1, and tm_tags1. 
# create table tm_jobs1 (jobId integer primary key, jobName text, jobShortName text, 
#                      jobLocName text, jobPublDate timestamp, 
#                      jobType text, jobCategoryName text, 
#                      jobCompanyName text, jobLevelName text, 
#                      jobRefsURL text, jobTagsName text, jobModelType text, 
#                      jobContents text)
# create table tm_companies1 (companyId integer primary key, companyName text, 
#                            companyShortName text) 
# create table tm_levels1 (levelName text primary key, levelShortName text) 
# create table tm_tags1   (tagName text primary key, tagShortName text) 
#

 
import requests
import json
import sys
import psycopg2
import getopt


def prepareAndInsert(rec, dbConnection):
    # Initialize the necessary field values to some reasonable defaults.
    # So that if value is missing, it is initialized to these.
    jobCategoryName = "UNKNOWN"
    jobCompanyId = -1
    jobCompanyName = "UNKNOWN" 
    jobCompanyShortName = "UNKNOWN" 
    jobModelType = "UNKNOWN"
    jobID = -1
    jobShortName = "UNKNOWN"
    jobName = "UNKNOWN"
    jobPublDate = "1970-01-01T00:00:00.0"
    jobType = "UNKNOWN"
    jobContents = "UNKNOWN"
    jobLocName = "UNKNOWN"
    jobLevelName = "UNKNOWN"
    jobLevelShortName = "UNKNOWN"
    jobRefsURL = "UNKNOWN"
    jobTagsName = "UNKNOWN"
    jobTagsShortName = "UNKNOWN"

    if (len(rec["categories"]) > 0):
        jobCategoryName = rec["categories"][0]["name"]
    jobCompanyName = rec["company"]["name"]
    jobCompanyId = rec["company"]["id"]
    jobCompanyShortName = rec["company"]["short_name"]
    jobModelType = rec["model_type"]
    jobId = rec['id']
    jobShortName = rec['short_name']
    jobName = rec['name']
    jobPublDate = rec['publication_date']
    jobType = rec['type']
    jobContents = rec['contents']
    if (len(rec["locations"]) > 0):
        jobLocName = rec["locations"][0]['name']
    if (len(rec["levels"]) > 0):
        jobLevelName = rec["levels"][0]['name']
        jobLevelShortName = rec["levels"][0]['short_name']
    jobRefsURL = rec['refs']["landing_page"]
       
    if (len(rec["tags"]) > 0):
        jobTagsName = rec["tags"][0]['name']
        jobTagsShortName = rec["tags"][0]['short_name']

    jobRec = (jobId, jobName, jobShortName, jobLocName, jobPublDate, jobType,
              jobCategoryName, jobCompanyName, jobLevelName, jobRefsURL,
              jobTagsName, jobModelType, jobContents)
    companyRec = (jobCompanyId, jobCompanyName, jobCompanyShortName)
    levelsRec  = (jobLevelName, jobLevelShortName)
    tagsRec    = (jobTagsName, jobTagsShortName)
    # insert the record in respective tables. 
    recTempl = ','.join(['%s'] * 1)
    dbCursor = dbConnection.cursor()    
    try:
        qry = 'insert into tm_companies1 values {0}'.format(recTempl)
        dbCursor.execute(qry, [companyRec])
    except psycopg2.Error as err:
        if (err.pgcode == '23505'): # Continue as record with this key alredy exists.
            pass
        else:
            print ("Function prepareAndInsert: Unable to execute 'insert into tm_companies1")
            print(err)
            sys.exit(1)
    dbConnection.commit()

    try:
        qry = 'insert into tm_jobs1 values {0}'.format(recTempl)
        dbCursor.execute(qry, [jobRec])
    except psycopg2.Error as err:
        if (err.pgcode == '23505'): # Continue as record with this key alredy exists.
            pass
        else:
            print ("Function prepareAndInsert: Unable to execute 'insert into tm_jobs1")
            print(err)
            sys.exit(1)
    dbConnection.commit()

    try:
        qry = 'insert into tm_levels1 values {0}'.format(recTempl)
        dbCursor.execute(qry, [levelsRec])
    except psycopg2.Error as err:
        if (err.pgcode == '23505'): # Continue as record with this key alredy exists.
            pass
        else:
            print ("Function prepareAndInsert: Unable to execute 'insert into tm_levels1")
            print(err)
            sys.exit(1)
    dbConnection.commit()

    try:
        qry = 'insert into tm_tags1 values {0}'.format(recTempl)
        dbCursor.execute(qry, [tagsRec])
    except psycopg2.Error as err:
        if (err.pgcode == '23505'): # Continue as record with this key alredy exists.
            pass
        else:
            print ("Function prepareAndInsert: Unable to execute 'insert into tm_tags1")
            print(err)
            sys.exit(1)
    dbConnection.commit()


def queryDB(dbConnection):
    # Sample Query:
    # How many jobs with the location "New York City Metro Area" were published
    # from September 1st to 30th 2016?
    qry2 = "select count(*) from tm_jobs1 where joblocname = '{}' and jobpubldate >= '{}' and jobpubldate <= '{}' "
    param1 = 'New York City Metro Area'
    param2 = '09-01-2016'
    param3 = '09-30-2016'
                                
    dbCursor = dbConnection.cursor()    
    try:
        qry21 = qry2.format(param1, param2, param3)
        print(qry21)
        dbCursor.execute(qry21)
    except psycopg2.Error as err:
        print ("Function queryDB: Unable to exeucte {} ".format(qry2))
        print(err)
        sys.exit(1)

    records = dbCursor.fetchall()
    return(records[0][0])


def getAllRemainingPages(url, dbConnection, numPages):
    # get data from the second page onwards and insert into db.
    for i in xrange(1,numPages):
        r = requests.get(url,params={'page':i})
        if r.status_code != 200:       
            print('Status:', r.status_code, 'Problem with the request. Exiting.')
            sys.exit(1)
        jobsData = r.json()
        jData = jobsData['results']
        for record in jData:
            ## From json record, prepare and insert a record into db
            prepareAndInsert(record, dbConnection)


def getFirstPageAndNumPages(url, dbConnection):
# Get and store the first page data in the db. Also, return number of
# available pages
    r = requests.get(url, params={'page':0})
    if r.status_code != 200:
        print('Status:', r.status_code, 'Problem with the request. Exiting.')
        sys.exit(1)
    jobsData = r.json()
    jData = jobsData['results']
    avPages = jobsData['page_count']
    for record in jData:
        ## From json record, prepare and insert a record into db
        prepareAndInsert(record, dbConnection)
    return(avPages)


def setupDB(dbConnStr):
    # Assumption: a PostgreSQL database accessible using dbConnStr exists. 
    # Each run of the script drops (if existing) and recreates the tables. 

    ## Jobs table create statement
    jobsCrTbl1 = "create table tm_jobs1 ("
    jobsCrTbl2 = "jobId int primary key, jobName text, jobShortName text, "
    jobsCrTbl3 = "jobLocName text, jobPublDate timestamp, "
    jobsCrTbl4 = "jobType text, jobCategoryName text, "
    jobsCrTbl5 = "jobCompanyName text, jobLevelName text, "
    jobsCrTbl6 = "jobRefsURL text, jobTagsName text, "
    jobsCrTbl7 = "jobModelType text, jobContents text)"
    jobsCrTbl = jobsCrTbl1 + jobsCrTbl2 + jobsCrTbl3 + jobsCrTbl4 + jobsCrTbl5 + jobsCrTbl6 + jobsCrTbl7
    ## Companies table create statement  
    comCrTbl1 = "create table tm_companies1 ("
    comCrTbl2 = "companyId int primary key, companyName text, companyShortName text)"
    comCrTbl  = comCrTbl1 + comCrTbl2
    ## Levels table create statement  
    lvlsCrTbl1 = "create table tm_levels1 ("
    lvlsCrTbl2 = "levelName text primary key,levelShortName text)"
    lvlsCrTbl  = lvlsCrTbl1 + lvlsCrTbl2
    ## Tags table create statement  
    tagsCrTbl1 = "create table tm_tags1 ("
    tagsCrTbl2 = "tagName text primary key, tagShortName text)"
    tagsCrTbl  = tagsCrTbl1 + tagsCrTbl2

    try: 
        conn1 = psycopg2.connect(dbConnStr)
    except psycopg2.Error as err:
        print ("Function setupDB: Unable to connect to database. Please check parameters.")
        print(err)
        sys.exit(1)
                                
    cur1 = conn1.cursor()

    # drop tables if exist and then create tables.
    try:     
        cur1.execute("drop table if exists tm_jobs1")
        cur1.execute("drop table if exists tm_companies1")
        cur1.execute("drop table if exists tm_levels1")
        cur1.execute("drop table if exists tm_tags1")

    except psycopg2.Error as err:
        print ("Function setupDB: Unable to execute 'drop table if exists <table-name>'")
        print(err)
        sys.exit(1)

    try:        
        res = cur1.execute(jobsCrTbl)
        res = cur1.execute(comCrTbl)
        res = cur1.execute(lvlsCrTbl)
        res = cur1.execute(tagsCrTbl)
    except psycopg2.Error as err:
        print ("Function setupDB: Unable to execute create table <table-name>...")
        print(err)
        sys.exit(1)
               
    conn1.commit()
    return(conn1)


def usage(argv):
    print("""
Usage: """ + argv[0] + """ --Param1=val --Param2=val ...

Parameters: All parameteres are optional, default values are indicated below in [].
  --pages        [All] # pages to download; if more than total available
                 number is specified, then data from all available pages is downloaded.
  --dbName       [tmj] Name of the PostgreSQL database to store the downloaded data.
  --dbUser       [tmj1] Name of the authorized user of the specified dbName
  --dbPassword   [tmj1] Passowrd for the authorized user of the database
  --host         [localhost] Name of the host with the specified PostgreSQL database
  """)


def main(argv):
    pages = 'All'
    dbN = 'tmj'
    dbU = 'tmj1'
    dbUP = 'tmj1'
    host = 'localhost'
    if (len(argv) == 1): # use all default values, get data for all pages and display usage message.
        print ('*** Proceeding with default values of all parameters.')
        print ('*** Usage and the parameter default values are shown below.')
        usage(argv)
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["pages=", "dbName=", "dbUser=", "dbPassword=", "host="])
    except getopt.GetoptError:
        usage(argv)
        return 2
    # Process arguments. 
    for opt, arg in opts:
        if opt == "--pages":
            if (arg != 'All'):
                try:
                    pages = int(arg)
                except:
                    print("Please specify a postive integer value for --pages argument.")
                    usage(argv)
                    return 2
        elif opt == "--dbName":
            dbN = arg
        elif opt == "--dbUser":
            dbU = arg
        elif opt == "--dbPassword":
            dbUP = arg
        elif opt == "--host":
            host = arg

    if (pages != 'All'):
        if (pages <= 0): 
            print("Please specify a postive integer value for --pages argument.")
            usage(argv)
            return 2

    tmjEndpoint = 'https://api-v2.themuse.com/jobs'
    connStr = "dbname = {} user = {} password = {} host = {}".format(dbN, dbU, dbUP, host)
    print("Connecting to database identified by: {}".format(connStr))
    # Connect to DB, create schema and get db connection object.  
    dbConn = setupDB(connStr)        
    print("Database connection established and schema created.")
   
    # Get the first page from The Muse jobs endpoint, and number of available pages. 
    numPages = getFirstPageAndNumPages(tmjEndpoint, dbConn)
 
    if (pages != 'All'):
        if pages >= numPages: # Get data for all availavble pages.
            print("Specified pages [{}] is equal or more than available pages [{}]; getting all pages.".format(pages, numPages))
            pages = numPages    
        getAllRemainingPages(tmjEndpoint, dbConn, pages)
    else:
        getAllRemainingPages(tmjEndpoint, dbConn, numPages)
    # Execute the query to get jobs with location New York Metro Area and posted afer September 30, 2016. 
    numJobs = queryDB(dbConn)
    print("Number of jobs located in New City Metro Area, and published from September 1st to 30th 2016 are:  {}".format(numJobs))

    # Close the database connection
    dbConn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
