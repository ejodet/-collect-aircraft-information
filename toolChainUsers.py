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
import json

import usersCloudantHelpers
from usersCloudantHelpers import iterview
import otcApiClient
import csv

SLEEP_TIME= 60
SIX_HOURS= 360

# cloudant DB
# ys1db= cloudantHelpers.connectToYS1CloudantService()
db= usersCloudantHelpers.connectToCloudantService()
usersDB= db['metrics-users-dw']

NAME= "[TOOLCHAIN SCANNER] - "

csv_file = "toolchains.csv"
json_file = "toolchains_to_fix.json"
EXPORT_PATH= "c:/temp"
format_csv= "formatted_toolchains.csv"
format_json_file = "toolchains_to_fix.json"

# next start key
START_KEY= None

# limit the amount of docs we fetch in memory
MAX_DOCS= 1000
allUsers= {}
allToolchainsWithGithub= {}
allToolchainsWithHostedGit= {}
allToolchainsWithGithubPublic= {}
allToolchains= {}

usersWithToolchainsWithNoWebHook= {}

newAllToolchains= []
newToolchainsToFix= []

usersById= {}
orgsById= {}

def fields():
    return [("toolchain_guid", ""),
            ("organization_guid", ""),
            ("creator", ""),
            ("service_id", ""),
            ("instance_id", ""),
            ("webhook_id", ""),            
            ("enable_traceability", "")]

# gets the next check point: will be 1 minute before last run was started (so we have some overlap)
def getNextCheckPoint():
    rawDate= str(datetime.datetime.utcnow() - datetime.timedelta(minutes=1))[:16]
    return rawDate.replace(' ', 'T')

def setNextCheckPoint(aDate):
    global START_KEY
    START_KEY= "[\"" + aDate + "\"]"

def printOtcApiMode():
    if otcApiClient.usingOtcTiamClient():
        print NAME + "using otc-tiam-client as found in vcap_services..."
    else:
        print NAME + "vcap_services not set - not using otc-tiam-client service..."
    
def getToolchainInfo(aDoc):
    toolchainId= aDoc['toolchain_guid']
    orgId= aDoc['organization_guid']
    userId= aDoc['creator']
    services= aDoc['services']
    for aService in services:
        serviceId= aService['service_id']
        if (serviceId == "githubpublic" or serviceId == "hostedgit" or serviceId == "github"):
            serviceInstanceId= aService['instance_id']
            traceAbilityEnabled= "traceability not found"
            try:
                traceAbilityEnabled= aService['parameters']['enable_traceability']
            except:
                pass
            webHookId= "NOT SET"
            try:
                webHookId= aService['toolchain_binding']['webhook_id']
            except Exception,e:
                pass
            toolchainInfo = {'toolchain_guid': toolchainId, 'organization_guid' : orgId, 'creator' : userId,
                            'service_id' : serviceId, 'instance_id' : serviceInstanceId, 'webhook_id' : webHookId, 'enable_traceability' : traceAbilityEnabled}
            return toolchainInfo
    return None


def getRetainedToolchainsWithGithubOrHostedGitWithNoWebhookId():
    print NAME + "Retrieving retained toolchains with Github or Hostedgit with no webhookid..."
    
    githubpublic_tc_noweb= {}
    for aToolChainId, aToolchainDoc in allToolchainsWithGithubPublic.iteritems():
        services= aToolchainDoc['services']
        for aService in services:
            if (aService['service_id'] == "githubpublic"):
                try:
                    webHookId= aService['toolchain_binding']['webhook_id']
                    if (webHookId == None or len(webHookId) == 0):
                        githubpublic_tc_noweb[aToolChainId]= getToolchainInfo(aToolchainDoc)
                        break
                except Exception,e:
                    githubpublic_tc_noweb[aToolChainId]= getToolchainInfo(aToolchainDoc)
        
    hostedgit_tc_noweb= {}  
    for aToolChainId, aToolchainDoc in allToolchainsWithHostedGit.iteritems():
        services= aToolchainDoc['services']
        for aService in services:
            if (aService['service_id'] == "hostedgit"):
                try:
                    webHookId= aService['toolchain_binding']['webhook_id']
                    if (webHookId == None or len(webHookId) == 0):
                        hostedgit_tc_noweb[aToolChainId]= getToolchainInfo(aToolchainDoc)
                        break
                except Exception,e:
                    hostedgit_tc_noweb[aToolChainId]= getToolchainInfo(aToolchainDoc)
                    
    github_tc_noweb= {}  
    for aToolChainId, aToolchainDoc in allToolchainsWithGithub.iteritems():
        services= aToolchainDoc['services']
        for aService in services:
            if (aService['service_id'] == "github"):
                try:
                    webHookId= aService['toolchain_binding']['webhook_id']
                    if (webHookId == None or len(webHookId) == 0):
                        hostedgit_tc_noweb[aToolChainId]= getToolchainInfo(aToolchainDoc)
                        break
                except Exception,e:
                    hostedgit_tc_noweb[aToolChainId]= getToolchainInfo(aToolchainDoc)
                   
    print NAME +"Toolchains with Git or Hostedgit with NO WebhookId"
    print NAME +"    Github: "  + str(len(github_tc_noweb))
    print NAME +"    Githubpublic: "  + str(len(githubpublic_tc_noweb))
    print NAME +"    Hostedgit: "  + str(len(hostedgit_tc_noweb))
    
    
    for aToolChainId, aToolchainDoc in githubpublic_tc_noweb.iteritems():
        aUserId= aToolchainDoc['creator']
        oldCount = usersWithToolchainsWithNoWebHook.get(aUserId, 0)
        newCount = oldCount + 1 
        usersWithToolchainsWithNoWebHook[aUserId]= newCount
    
    print NAME +"Number of unique users impacted by the NO WebhookId issue "  + str(len(usersWithToolchainsWithNoWebHook))
    for aUserId, countOfTc in sorted(usersWithToolchainsWithNoWebHook.iteritems(), key=lambda (k,v): (v,k), reverse=True):
        print aUserId + ": " + str(countOfTc)
    
    print NAME + "Done"
    
def getUserInfo(userId):
    if len(allUsers) == 0:
        for hit in usersDB.view('_all_docs', include_docs=True):
            if hit['key'] is not None:
                aDoc= hit(['value'])
                userId= aDoc["profile"]["user_id"]
                allUsers[userId]= aDoc
    return allUsers.get(userId)


def getToolchainDetailsFromOtcApi(anOtcId):
    toolChainsList = otcApiClient.getToolChain(anOtcId)
    if (toolChainsList != None):
        for tc in toolChainsList["items"]:
            return tc
    return None


def exportToJson():
    outpath = os.path.join(EXPORT_PATH, json_file)
    toolchainsToFix= []
    for aToolchainDoc in newAllToolchains:
        webHookId= aToolchainDoc['webhook_id']
        if (webHookId == "NOT SET"):
            toolchainsToFix.append(aToolchainDoc)
    
    toolchainsAsJson= {"toolchainsToFix" : toolchainsToFix}
    
    out = json.dumps(toolchainsAsJson)
    f = open(outpath, "w")
    f.write(out)
    f.close()
    
    print "done!"            
        
def exportToCSV():
    outpath = os.path.join(EXPORT_PATH, csv_file)
    outputFile = open(outpath,'wb')
    csvFile = csv.writer(outputFile)
    csvFile.writerow([name for name, _ in fields()])
    for aToolchainDoc in newAllToolchains:
        toolchain_guid= aToolchainDoc['toolchain_guid']
        organization_guid= aToolchainDoc['organization_guid']
        creator= aToolchainDoc['creator']
        service_id= aToolchainDoc['service_id']
        serviceInstanceId= aToolchainDoc['instance_id']
        webHookId= aToolchainDoc['webhook_id']
        enable_traceability= aToolchainDoc['enable_traceability']
        
        # write row
        csvFile.writerow([toolchain_guid, organization_guid, creator, service_id, serviceInstanceId, webHookId, enable_traceability])
        outputFile.flush()
    
    outputFile.close()
    print "done! "
    
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

def getAllUsers():
    for hit in usersDB.view('_all_docs', include_docs=True):
        if hit['key'] is not None:
            userDoc= hit['doc']
            try: 
                # user
                userRecord= userDoc['user_record']
                userProfile= userRecord['profile']
                userId= userProfile['user_id']
                registeredUser= usersById.get(userId, None)
                if (registeredUser == None):
                    userName= userProfile['user_name']
                    userEmail= userProfile['email']                    
                    userDetails= {"userName" : userName, "userEmail" : userEmail}
                    usersById[userId]= userDetails
                
                # org
                orgs= userRecord['organizations']
                for anOrg in orgs:
                    anOrgId= anOrg['guid']
                    registeredOrg= orgsById.get(anOrgId, None)
                    if (registeredOrg == None):
                        anOrgName= anOrg['name']
                        orgsById[anOrgId]= anOrgName
            except:
                pass
    print "Done!"
    
def reformatCsvFile():
    outpath = os.path.join(EXPORT_PATH, format_csv)
    outputFile = open(outpath,'wb')
    csvFile = csv.writer(outputFile)
    exceptCount= 0
    input = os.path.join(EXPORT_PATH, csv_file)
    with open(input, "rb") as f:
        reader = csv.reader(f, delimiter="\t")
        for i, line in enumerate(reader):
            if (i == 0):
                lineSegments= line[0].split(',')
                newHeader= [lineSegments[0] , lineSegments[1] , lineSegments[2] , lineSegments[3] , lineSegments[4] , lineSegments[5] , lineSegments[6]  , "IBM"]
                print newHeader
                csvFile.writerow(newHeader)
            else :
                try:
                    lineSegments= line[0].split(',')
                    userId= lineSegments[2]
                    orgId= lineSegments[1]
                    userDetails= usersById.get(userId, None)
                    orgName= orgsById.get(orgId, "Unknown OrgId")
                    userName= "Unknown userId"
                    if (userDetails != None):
                        userName= userDetails['userName']
                    if ("ibm" in userName.lower()):
                        ibmer= "true"
                    else:
                        ibmer= "false"
                    formattedLine= [lineSegments[0] , orgName , userName , lineSegments[3] , lineSegments[4] , lineSegments[5] , lineSegments[6]  , str(ibmer)]    
                    print formattedLine
                    csvFile.writerow([lineSegments[0] , str(orgName) , str(userName).encode('utf-8').strip() , lineSegments[3] , lineSegments[4] , lineSegments[5] , lineSegments[6]  , str(ibmer)])
                 
                except:
                    exceptCount = exceptCount +1
                    pass
            outputFile.flush()
    outputFile.close()
    print "done! "
    
def sleep():
    print (NAME + "Next startup checkpoint: " + START_KEY + " sleeping for 1 minute")
    time.sleep(SLEEP_TIME)



def mainLoop():
    print (NAME + "Entering main loop...")
    getAllUsers()
    reformatCsvFile()
    exportToJson()
    exportToCSV()

def main():
    logging.getLogger('').setLevel(os.getenv('LOG_LEVEL', logging.INFO))
    print NAME + "root logger level: %s" % logging.getLevelName(logging.getLogger('').level)

    logging.info(NAME + "Toolchain Updater starting....")
    try: 
        mainLoop()

    except Exception,e:
        logging.error(NAME + "Unexpected exception: %s", str(e.message))
        traceback.print_exc()

    logging.info(NAME + "Toolchain Updater ended....")

if __name__ == "__main__":
    main()