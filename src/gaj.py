#!/Library/Frameworks/Python.framework/Versions/2.7/bin/python
# coding: utf-8
# This script performs the following operations
# (1) Downloads publicly available data from the “Jobs” endpoint, and 
# stores only certain fields of the data in an S3 Bucket. 
 
import requests
import json
import sys
import boto3

# Use these list of records and list of primary keys for respective tables (except jobs)
# to batch and then insert into database in one shot. This works because the dataset
# is relatively small and fits in memory. 
jobRecs = []
lvlRecs = []
comRecs = []
tagRecs = []
existingCompanyIDs = []
existingLevelNames = []
existingTagNames = []

def prepareRecord(rec):
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
    jobCompanyName = rec["company"]["name"].encode("utf-8")
    jobCompanyId = rec["company"]["id"]
    jobCompanyShortName = rec["company"]["short_name"]
    jobModelType = rec["model_type"]
    jobId = rec['id']
    jobShortName = rec['short_name'].encode("utf-8")
    jobName = rec['name'].encode("utf-8")
    jobPublDate = rec['publication_date']
    jobType = rec['type'].encode("utf-8")
    jobContents = rec['contents']
    if (len(rec["locations"]) > 0):
        jobLocName = rec["locations"][0]['name'].encode("utf-8")
    if (len(rec["levels"]) > 0):
        jobLevelName = rec["levels"][0]['name'].encode("utf-8")
        jobLevelShortName = rec["levels"][0]['short_name'].encode("utf-8")
    jobRefsURL = rec['refs']["landing_page"]
       
    if (len(rec["tags"]) > 0):
        jobTagsName = rec["tags"][0]['name']
        jobTagsShortName = rec["tags"][0]['short_name']

    #jobRec = (jobId, jobName, jobShortName, jobLocName, jobPublDate, jobType,
    #          jobCategoryName, jobCompanyName, jobLevelName, jobRefsURL,
    #          jobTagsName, jobModelType, jobContents)

    jobRec = (jobId, jobName, jobLocName, jobPublDate, jobType,
              jobCompanyName, jobLevelName)

    companyRec = (jobCompanyId, jobCompanyName, jobCompanyShortName)
    levelsRec  = (jobLevelName, jobLevelShortName)
    tagsRec    = (jobTagsName, jobTagsShortName)
    return((jobRec, companyRec, levelsRec, tagsRec))

def getAllRemainingPages(url, numPages):
    # get data from the second page onwards and insert into db.
    for i in xrange(1,numPages):
        r = requests.get(url,params={'page':i})
        if r.status_code != 200:       
            print('Status:', r.status_code, 'Problem with the request. Exiting.')
            sys.exit(1)

        jobsData = r.json()
        jobsPage = jobsData['results']
        for record in jobsPage:
            ## From json record, create a tuple representing the db record
            jobRec, comRec, lvlRec, tagRec = prepareRecord(record)
            jobRecs.append(jobRec) ## All job records are assumed to be unique with unique IDs
            if comRec[0] not in existingCompanyIDs: 
                existingCompanyIDs.append(comRec[0])
                comRecs.append(comRec)
            if lvlRec[0] not in existingLevelNames: 
                existingLevelNames.append(lvlRec[0])
                lvlRecs.append(lvlRec)
            if tagRec[0] not in existingTagNames: 
                existingTagNames.append(tagRec[0])
                tagRecs.append(tagRec)


def getFirstPageAndNumPages(url):
# Get and store the first page data in the db. Also, return number of
# available pages
    r = requests.get(url, params={'page':0})
    if r.status_code != 200:
        print('Status:', r.status_code, 'Problem with the request. Exiting.')
        exit()
    jobsData = r.json()
    jobsPage = jobsData['results']
    avPages = jobsData['page_count']
    for record in jobsPage:
        ## From json record, create a tuple representing the db record
        jobRec, comRec, lvlRec, tagRec = prepareRecord(record)
        jobRecs.append(jobRec) ## All job records are assumed to be unique with unique IDs
        if comRec[0] not in existingCompanyIDs: 
            existingCompanyIDs.append(comRec[0])
            comRecs.append(comRec)
        if lvlRec[0] not in existingLevelNames: 
            existingLevelNames.append(lvlRec[0])
            lvlRecs.append(lvlRec)
        if tagRec[0] not in existingTagNames: 
            existingTagNames.append(tagRec[0])
            tagRecs.append(tagRec)
    return(avPages)

def lambda_handler(event, context):

    tmjEndpoint = 'https://api-v2.themuse.com/jobs'
    # Get the first page from The Muse jobs endpoint, and number of available pages. 
    numPages = getFirstPageAndNumPages(tmjEndpoint)
    getAllRemainingPages(tmjEndpoint, numPages)
    # Copy the data to S3 bucket also
    s3 = boto3.resource('s3')
    s3.Bucket('themusejobs').put_object(Key='allJobs', Body=bytes(jobRecs))
    return 0
