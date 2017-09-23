"""
This script is used to persist toolchain information based on RabbitMQ events using OTC API
1 - startup: populate the warehouse DB
    - get all toolchain ids ever - from the metrics events DB
    - get all toolchain ids from the warehouse DB
    - fetch all unknown toolchains from OTC API
    - persist in warehouse DB
2 - loop 1: every 1 min
    - get the list of toolchains events from events DB since last run
    - fetch all related toolchains from OTC API
    - persist in warehouse DB
3 - loop 2: every 6hrs
    - get all toolchain ids from the warehouse DB
    - fetch all unknown toolchains from OTC API
    - persist / refresh in warehouse DB
"""

import datetime
import logging
import os
import time
from threading import Thread
import traceback

import cloudantHelpers
from cloudantHelpers import iterview
import otcApiClient

SLEEP_TIME= 60
SIX_HOURS= 360

# cloudant DB
# ys1db= cloudantHelpers.connectToYS1CloudantService()
db= cloudantHelpers.connectToCloudantService()
eventsDB= db['metrics-events']
warehouseDB= db['metrics-warehouse']
toolchainsDB= db['otc-api-toolchains']

NAME= "[TOOLCHAIN UPDATER] - "

# next start key
START_KEY= None

# limit the amount of docs we fetch in memory
MAX_DOCS= 1000

# gets the next check point: will be 1 minute before last run was started (so we have some overlap)
def getNextCheckPoint():
    rawDate= str(datetime.datetime.utcnow() - datetime.timedelta(minutes=1))[:16]
    return rawDate.replace(' ', '  T')

def setNextCheckPoint(aDate):
    global START_KEY
    START_KEY= "[\"" + aDate + "\"]"

def printOtcApiMode():
    if otcApiClient.usingOtcTiamClient():
        print NAME + "using otc-tiam-client as found in vcap_services..."
    else:
        print NAME + "vcap_services not set - not using otc-tiam-client service..."

def getRetainedToolChainsIds():
    print NAME + "Retrieving retained toolchain IDs from metrics-warehouse DB..."
    jobStartTime= time.time()
    matched = 0
    tc_ids = set()
    for hit in warehouseDB.view('toolchains/retainedToolChains', reduce=True, group=True):
        if hit['key'] is not None:
            tc_ids.add(hit['key'])
        matched += 1
    jobElapsed= time.time() - jobStartTime
    print NAME + "%d unique retained toolchain IDs found in metrics-warehouse DB, from %d query matches - elapsed %d secs" % (len(tc_ids), matched, jobElapsed)
    return tc_ids

def getAllToolChainIdsEverFromOTCAPI():
    print NAME + "Retrieving known toolchain IDs from otc-api-toolchains DB..."
    jobStartTime= time.time()
    matched = 0
    tc_ids = set()
    for hit in iterview(toolchainsDB, 'toolchains/org_guids_to_toolchain_guids', batch=2000, reduce= False):
        if hit['value'] is not None:
            tc_ids.add(hit['value'])
        matched += 1
        if matched % 1000 == 0:
            print matched
    jobElapsed= time.time() - jobStartTime
    print NAME + "%d unique known toolchain IDs found in otc-api-toolchains DB, from %d query matches - elapsed %d secs" % (len(tc_ids), matched, jobElapsed)
    return tc_ids

def getAllToolChainIdsEverFromWarehouseDB():
    print NAME + "Retrieving known toolchain IDs from metrics-warehouse DB..."
    jobStartTime= time.time()
    matched = 0
    tc_ids = set()
    for hit in iterview(warehouseDB, 'toolchains/toolchains_by_id', batch=20000, reduce=True, group=True):
        if hit['key'] is not None:
            tc_ids.add(hit['key'])
        matched += 1
        if matched % 10000 == 0:
            print matched
    jobElapsed= time.time() - jobStartTime
    print NAME + "%d unique known toolchain IDs found in metrics-warehouse DB, from %d query matches - elapsed %d secs" % (len(tc_ids), matched, jobElapsed)
    return tc_ids

def getAllToolChainIdsEverFromEventsDB():
    print NAME + "Retrieving all toolchain IDs from metrics-events DB..."
    jobStartTime= time.time()
    tc_ids = set()
    matched = 0
    for hit in iterview(eventsDB, 'allToolChainId/allToolChainIds', batch=20000, reduce=True, group=True):
        if hit['key'] is not None:
            tc_ids.add(hit['key'])
        matched += 1
        if matched % 10000 == 0:
            print matched
    jobElapsed= time.time() - jobStartTime
    print NAME + "%d unique toolchain IDs found in metrics-events DB, from %d query matches - elapsed %d secs" % (len(tc_ids), matched, jobElapsed)
    return tc_ids

def getRecentToolChainFromEventsDB():
    print NAME + "Retrieving recent OTC events from events DB since last run %s" % START_KEY
    jobStartTime= time.time()
    matched = 0
    events = []
    for hit in iterview(eventsDB, 'provisioning/toolchain_id_by_minute', batch=1000, start_key=START_KEY, reduce=False, include_docs=True):
        if hit['doc'] is not None:
            events.append(hit['doc'])
        matched += 1
        if matched % 1000 == 0:
            print matched
    jobElapsed= time.time() - jobStartTime
    print NAME + "%d recent events found in events DB, from %d query matches - elapsed %d secs" % (len(events), matched, jobElapsed)
    return events

# fix old delete dummy records in warehouse DB
# after a successful run, should not retrieve any old delete docs
def fixOldDeleteRecordsFromWarehouseDB():
    while fixOldDeleteRecordsOnePass():
        pass

def fixOldDeleteRecordsOnePass():
    # get olds docs
    print (NAME + "fixing old delete docs from warehouse DB")
    jobStartTime= time.time()
    matched = 0
    oldDocs= []
    for aToolchainDoc in warehouseDB.view('toolchains/oldDeleteDocs', reduce = False, include_docs=True, limit = MAX_DOCS):
        aDoc = aToolchainDoc['doc']
        if (aDoc['name'] == "deleted"): # should not be necessary since the view is supposed to return ad hoc docs
            oldDocs.append(aDoc)
        matched += 1
    jobElapsed= time.time() - jobStartTime
    if len(oldDocs) == 0:
        print (NAME + "all old delete docs fixed - exiting")
        return False

    print (NAME + str(len(oldDocs)) + " old delete records in warehouse DB, from " + str(matched) + " query matches" + " - elapsed " + str(jobElapsed) + " secs")
    print (NAME + "fixing " + str(len(oldDocs)) + " old delete docs")

    # fix them
    jobStartTime= time.time()
    fixed= 0
    for anOldDoc in oldDocs:
        anOtcId= anOldDoc['toolchain_guid']
        fixDeleteDoc(anOtcId, anOldDoc)
        fixed +=1
        if fixed % 100 == 0: # show some progress here
            jobElapsed= time.time() - jobStartTime
            print (NAME + str(fixed) + " old dummy records fixed" + " - elapsed " + str(jobElapsed) + " secs")
    print (NAME + str(fixed) + " old delete docs fixed - continue with next records")

    return True

def getToolchainDetailsFromOtcApi(anOtcId):
    toolChainsList = otcApiClient.getToolChain(anOtcId)
    if (toolChainsList != None):
        for tc in toolChainsList["items"]:
            return tc
    return None

def getToolchainDocFromWarehouseDB(anOtcId):
    try:
        return warehouseDB[anOtcId]
    except:
        pass # not found in warehouse DB - no problem
    return None

def insertToolchain(anOtcId, aToolchain):
    try:
        warehouseDB[anOtcId]= aToolchain
    except:
        pass # ignore failure

def updateToolchain(anOtcId, toolChainDoc, toolChain):
    try:
        lastRev= toolChainDoc['_rev']
        toolChain['_rev']= lastRev
        warehouseDB[anOtcId]= toolChain
    except:
        pass # ignore failure

# fix old dummy delete docs
def fixDeleteDoc(anOtcId, toolChainDoc):
    # search any event corresponding to this toolchain id
    createEvent = None
    lastEvent = None
    for aToolchain in eventsDB.view('allToolChainId/allToolChainIds', key=anOtcId, reduce=False, include_docs=True):
        anEvent = aToolchain['doc']
        # allToolChainIds view includes events from all generators, but we're
        # only interested in ones from otc-core
        if anEvent['generator'] == 'otc-core':
            if (anEvent['verb'] == "create"):
                createEvent = anEvent
                break
            if lastEvent is None or lastEvent['published'] < anEvent['published']:
                lastEvent = anEvent
    if (createEvent == None):
        createEvent = lastEvent # no create event found - take last event found if any
    createDeletedToolchainDoc(anOtcId, createEvent, toolChainDoc)

def deleteToolchain(anOtcId, toolChainDoc):
    #  check if we're dealing with the old dummy anonymous record
    try:
        docName= toolChainDoc["name"]
        if docName == "deleted":
            fixDeleteDoc(anOtcId, toolChainDoc)
            print "fixed old style delete doc, id: %s" % anOtcId
            return
    except:
        # new doc - OK
        pass

    try:
        knowThisGuyAlready= toolChainDoc["deleted"] # doc might be already flagged as deleted
    except:
        # persisted toolchain was deleted: update it
        toolChainDoc["deleted"]= True
        toolChainDoc["date_deleted"]= str(datetime.datetime.utcnow().isoformat()) + 'Z'
        warehouseDB[anOtcId]= toolChainDoc

def populateWarehouseDBFromEventsDB():
    # get the list of recent toolchain events from metrics_events DB since last run
    recentToolchainsEvents= getRecentToolChainFromEventsDB()

    # counters
    toSearch= len(recentToolchainsEvents)
    if (toSearch > 0):
        print (NAME + "Using OTC API to retrieve toolchains information based on recent toolchain metrics events")
        print(NAME + "Number of events to process: " + str(len(recentToolchainsEvents)))
        printOtcApiMode()
    else:
        print(NAME + "No new toolchain event found since last run!")

    inserted = updated = deleted = unknown = apiCalls= 0
    processedToolchainIds= []
    for anEvent in recentToolchainsEvents:
        try:
            if (anEvent['object']['objectType'] == 'service_instance'):
                anOtcId= anEvent['target']['id']
            else:
                anOtcId= anEvent['object']['id']

            if (anOtcId in processedToolchainIds): # we already processed this toolchain - don't call the API again
                continue

            # first query OTC API
            toolChain= getToolchainDetailsFromOtcApi(anOtcId)
            processedToolchainIds.append(anOtcId) # don't call the call more than once for the same otc id
            apiCalls +=1
            if (apiCalls%10 == 0): # show some progress here
                print (NAME + str(apiCalls) + " API calls completed")

            toolChainFoundUsingAPI = False if toolChain == None else True

            # now check if the toolchain is stored in warehouse db
            toolChainDoc= getToolchainDocFromWarehouseDB(anOtcId)
            storedInWarehouseDB= False if toolChainDoc == None else True

            # update the warehouse DB
            if (toolChainFoundUsingAPI):
                if (not storedInWarehouseDB):
                    insertToolchain(anOtcId, toolChain) # found in OTC API but not in warehouse DB: it's a new toolchain
                    inserted +=1
                else :
                    updateToolchain(anOtcId, toolChainDoc, toolChain) # found in both OTC API and warehouse DB:  toolchain was updated
                    updated +=1
            else :
                if (storedInWarehouseDB):
                    deleteToolchain(anOtcId, toolChainDoc) # found in warehouse DB but not using OTC API - it's a deleted toolchain
                    deleted +=1
                else:
                    createDeletedToolchainDoc(anOtcId, anEvent, None) # not found neither in API or warehouse DB: keep track of event details
                    unknown +=1

        except Exception, e:
            # something went wrong - exit for now
            traceback.print_exc()

    if (toSearch > 0):
        print(NAME + "Number of toolchain events to process: " + str(toSearch))
        print(NAME + str(apiCalls) + " OTC API calls completed")
        print(NAME + "    Inserted toolchains: " + str(inserted))
        print(NAME + "    Updated toolchains: " + str(updated))
        print(NAME + "    Deleted toolchains: " + str(deleted))
        print(NAME + "    Unknown toolchains: " + str(unknown))
        print(NAME + "Done...")

def updateWarehouseDB(toolchains):
    toSearch= len(toolchains)

    if (toSearch > 0):
        print (NAME + "Using OTC API to update toolchains information")
        print(NAME + "Number of toolchains to search: " + str(len(toolchains)))
        printOtcApiMode()
    else:
        print(NAME + "No new toolchain found since last run!")
        return

    inserted = updated = deleted = apiCalls = 0

    for anOtcId in toolchains:
        if anOtcId is None:
            continue
        try:
            # first query OTC API
            toolChain= getToolchainDetailsFromOtcApi(anOtcId)
            toolChainFoundUsingAPI = False if toolChain == None else True

            # now check if the toolchain is stored in warehouse db
            toolChainDoc= getToolchainDocFromWarehouseDB(anOtcId)
            apiCalls +=1
            if (apiCalls%100 == 0): # show some progress here
                print (NAME + str(apiCalls) + " API calls completed")

            storedInWarehouseDB= False if toolChainDoc == None else True

            # update the warehouse DB
            if (toolChainFoundUsingAPI):
                if (not storedInWarehouseDB):
                    insertToolchain(anOtcId, toolChain) # found in OTC API but not in warehouse DB: it's a new toolchain
                    inserted +=1
                else :
                    updateToolchain(anOtcId, toolChainDoc, toolChain) # found in both OTC API and warehouse DB:  toolchain was updated
                    updated +=1
            else :
                if (storedInWarehouseDB):
                    deleteToolchain(anOtcId, toolChainDoc) # found in warehouse DB but not using OTC API - it's a deleted toolchain
                else:
                    # create an old dummy record that will be converted right after as we currently don't have corresponding event
                    oldDeleteDoc= {"toolchain_guid": anOtcId, "name": "deleted", "description": "toolchain guid not found using OTC API", "deleted": "true"}
                    # store it
                    warehouseDB[anOtcId]= oldDeleteDoc
                    # fix it
                    deleteToolchain(anOtcId, oldDeleteDoc)
                deleted +=1

        except Exception, e:
            print "Error in updateWarehouseDB while processing toolchain ID: %s" % anOtcId
            # something went wrong - exit for now
            traceback.print_exc()

    if (toSearch > 0):
        print(NAME + "Number of unknown toolchains to process: " + str(toSearch))
        print(NAME + "    Inserted toolchains: " + str(inserted))
        print(NAME + "    Updated toolchains: " + str(updated))
        print(NAME + "    Deleted toolchains: " + str(deleted))
        print(NAME + "Done...")


def getToolchainDeleteDate(anOtcId):
    deleteEvent= None
    for aToolchain in eventsDB.view('allToolChainId/allToolChainIds', key = anOtcId, reduce = False, include_docs=True):
        anEvent= aToolchain['doc']
        if (anEvent['verb'] == "delete"):
            deleteEvent= anEvent
            break
    if (deleteEvent != None):
        return deleteEvent['published']
    # delete date not found: return a value anyway
    return str(datetime.datetime.utcnow().isoformat()) + 'Z'

def getToolchainGuid(anEvent):
    o, t = anEvent.get('object'), anEvent.get('target')
    if o and o.get('objectType') == 'toolchain' and o.get('id'):
        return o.get('id')
    if t and t.get('objectType') == 'toolchain' and t.get('id'):
        return t.get('id')
   
def getGenerator(anEvent):
    try:
        if (anEvent['object']['objectType'] == 'service_instance'):
            return anEvent["generator"]
        else:
            return anEvent["object"]["generator"]
    except:
        # some old delete event have no object
        return anEvent["generator"]

def getOrgId(anEvent):
    try:
        return anEvent["object"]["organization"]["id"]
    except:
        # some old delete event have no object
        return "unknown"

# creates a deleted doc for a toolchain that can not be found using OTC API (deleted toolchain)
# 1 - creates a deleted toolchain doc with additional information as found from events DB
# 2 - if necessary, fixes old delete docs
def createDeletedToolchainDoc(anOtcId, eventDetails, existingDoc):
    # no event found for this toolchain: sometimes happens if events DB was re-created
    if (eventDetails == None):
        if (existingDoc != None):
            # we will never be able to fix this doc: add a new attribute that will be used as a filter by the Cloudant view
            deleteDoc= existingDoc
            deleteDoc["no_event_found"] = True
            updateToolchain(anOtcId, existingDoc, deleteDoc)
            print(NAME + "No event found for toolchain id " + anOtcId)
    # event found: create an augmented delete doc based on event's details
    else:
        toolchain_guid= getToolchainGuid(eventDetails)
        generator= getGenerator(eventDetails)
        organization_guid= getOrgId(eventDetails)

        name= "unknown"
        description= "toolchain guid not found using OTC API"

        creator= eventDetails.get("actor", {}).get("id")
        created= eventDetails["published"]

        date_deleted= str(datetime.datetime.utcnow().isoformat()) + 'Z'
        if (existingDoc != None):
            # keep track of the original delete date
            date_deleted= getToolchainDeleteDate(anOtcId)

        deletedToolChainDoc= {"toolchain_guid": toolchain_guid, "deleted": True, "name": name, "description": description,
                "creator" : creator, "created" : created, "generator" : generator, "organization_guid" : organization_guid, "date_deleted" : date_deleted}

        if (existingDoc == None): # new doc
            insertToolchain(anOtcId, deletedToolChainDoc)
        else:
            # updating old dummy record
            updateToolchain(anOtcId, existingDoc, deletedToolChainDoc)

def sleep():
    print (NAME + "Next startup checkpoint: " + START_KEY + " sleeping for 1 minute")
    time.sleep(SLEEP_TIME)

# performed once at startup
# get all toolchains ids ever, from the events DB
# for each, fetch details from OTC API
# then insert into warehouse DB
def updateAllToolchainsEver():
    print (NAME + "Startup: retrieving known toolchain ids from metrics-warehouse")
    allToolchainIdsFromWarehouse = getAllToolChainIdsEverFromWarehouseDB()

    print (NAME + "Startup: retrieving unknown toolchain ids from otc-api-toolchains")
    allToolchainIdsFromOTC = getAllToolChainIdsEverFromOTCAPI()
    unknownToolchainIds = allToolchainIdsFromOTC - allToolchainIdsFromWarehouse

    print NAME + "%d unknown toolchains found" % len(unknownToolchainIds)
    updateWarehouseDB(unknownToolchainIds)

    print (NAME + "Startup: retrieving unknown toolchain ids from metrics-events")
    allToolchainIdsFromEvents = getAllToolChainIdsEverFromEventsDB()
    unknownToolchainIds= allToolchainIdsFromEvents - allToolchainIdsFromWarehouse

    print NAME + "%d unknown toolchains found" % len(unknownToolchainIds)
    updateWarehouseDB(unknownToolchainIds)

def mainLoop():
    print (NAME + "Entering main loop...")
    timeToRefreshAllToolChains= 0

    while(True):
        # keep track of the next start
        nextCheckPoint= getNextCheckPoint()

        # loop #1: every 5 minutes, get toolchain ids from events DB since the last run (with 1 min overlap)
        # and persist them in warehouse DB
        populateWarehouseDBFromEventsDB()

        # set the next checkpoint
        setNextCheckPoint(nextCheckPoint)

        sleep()

        timeToRefreshAllToolChains += 1

        # loop #2: every 6 hours, refresh all [warehouse DB] retained (not deleted) toolchains using OTC API
        if(timeToRefreshAllToolChains == SIX_HOURS):
            # refresh all tool chains every 6 hours
            print (NAME + "Using OTC API to refresh all known toolchains (every 6 hrs)...")
            # refresh the list of retained toolchain ids
            retainedToolChainIds= getRetainedToolChainsIds()
            # refresh all retained toolchains using OTC API
            updateWarehouseDB(retainedToolChainIds)
            timeToRefreshAllToolChains= 0
            print (NAME + "Done - refreshed " + str(len(retainedToolChainIds)) + " toolchains")

def main():
    logging.getLogger('').setLevel(os.getenv('LOG_LEVEL', logging.INFO))
    print NAME + "root logger level: %s" % logging.getLevelName(logging.getLogger('').level)

    logging.info(NAME + "Toolchain Updater starting....")
    try:
        retainedToolChainIds= getRetainedToolChainsIds()
        # refresh all retained toolchains using OTC API
        print "refreshing " + str(len(retainedToolChainIds)) + " TCS"
        updateWarehouseDB(retainedToolChainIds)

        # 3 - start the main loop
        mainLoop()

    except Exception,e:
        logging.error(NAME + "Unexpected exception: %s", str(e.message))
        traceback.print_exc()

    logging.info(NAME + "Toolchain Updater ended....")

if __name__ == "__main__":
    main()