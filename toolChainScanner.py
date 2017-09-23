"""
This script is used to generate the list of service instances to fix
see https://github.ibm.com/org-ids/otc-setup-issues/issues/507

"""

import logging
import os
import traceback
import json
import csv
from threading import Thread
import couchdb
from urlparse import urlparse

global db
global warehouseDB
global udb
global usersDB
global edb
global eventsDB

NAME= "[TOOLCHAIN SCANNER] - "

# export path: modify it to fit your environment
EXPORT_PATH= "c:/temp"

allServiceInstances= []
usersById= {}
orgsById= {}
activeToolChains= []
invalidTCs= []
noWebhookCreationDates=[]

def fields():
    return [("creation_date", ""),
            ("toolchain_guid", ""),
            ("organization_guid", ""),
            ("organization_name", ""),
            ("creatorId", ""),
            ("creatorName", ""),
            ("service_name", ""),
            ("instance_id", ""),
            ("webhook_id", ""),            
            ("enable_traceability", ""),
            ("IBM", ""),
            ("recent_activity", "")]
                    
def getAllServiceInstances():
    match = 0
    count = 0
    validCount= 0
    deletedCount= 0
    print NAME +"getAllServiceInstances() - retrieving all service instances from the Metrics DW DB"
    # for hit in warehouseDB.view('_all_docs', include_docs=True):
    for hit in warehouseDB.view('toolchains/ejd_retained_toolchains', include_docs=True):
        if hit['key'] is not None:
            count = count +1
            tcDoc= hit['doc']
            
            # filter out deleted docs
            try:
                deleted= tcDoc['deleted']
                deletedCount = deletedCount + 1
                continue # skip deleted doc
            except Exception,e:                
                pass # TC was not deleted
                        
            try:                
                # dealing with a not deleted TC
                toolchainId= tcDoc['toolchain_guid']
                orgId= tcDoc['organization_guid']
                userId= tcDoc['creator']
                services= tcDoc['services']
                validCount = validCount +1
                for aService in services:
                    if (aService['service_id'] == "githubpublic" or aService['service_id'] == "hostedgit" or aService['service_id'] == "github"):
                        serviceId= aService['service_id']
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
                            dateCreated= tcDoc['created'][0:10]
                            # invalidTCs.append(tcDoc)
                            if (dateCreated not in noWebhookCreationDates):
                                noWebhookCreationDates.append(dateCreated)
                            pass
                        
                        active= "false"
                        if (toolchainId in activeToolChains):
                            active= "true"
                            
                        serviceInfo = {"creation_date" : tcDoc['created'][0:10],'toolchain_guid': toolchainId, 'organization_guid' : orgId, 'creator' : userId,
                                    'service_id' : serviceId, 'instance_id' : serviceInstanceId, 'webhook_id' : webHookId, 'enable_traceability' : traceAbilityEnabled, 'recent_activity' : active}
                        
                        allServiceInstances.append(serviceInfo)
                        match = match + 1
                
            except Exception,e:            
                pass
                continue
    
    print NAME + str(count) + " Toolchains processed"
    print NAME + "    " + str(validCount) + " existing toolchains found"
    print NAME + "    " + str(deletedCount) + " deleted toolchains found"
    print NAME + "    " + str(match) + " service instances (githubpublic, hostedgit, github)"
    print ""     
    
    # for debugging only
    """
    print "No webhook id toolchain creation dates"   
    for aDate in sorted(noWebhookCreationDates, reverse=True):
        print aDate 
    print ""
    
    for aDoc in invalidTCs:
        print json.dumps(aDoc, indent= 6)
        print ""
    print ""
    """
    
def exportToJson():
    print NAME +"exportToJson() - exporting service instances to fix to " + str(os.path.join(EXPORT_PATH, getJsonFile()))
    outpath = os.path.join(EXPORT_PATH, getJsonFile())
    instancesToFix= []
    for aToolchainDoc in allServiceInstances:
        webHookId= aToolchainDoc['webhook_id']
        if (webHookId == "NOT SET"):
            organization_guid= aToolchainDoc['organization_guid']
            orgName= orgsById.get(organization_guid, "Unknown OrgId")
            creator= aToolchainDoc['creator']
            userDetails= usersById.get(creator, None)
            if (userDetails != None):
                userName= userDetails['userName']
                if ("ibm" in userName.lower()):
                    ibmer= "true"
                else:
                    ibmer= "false"
                    domainSegs= userName.split('@')
                    if (len(domainSegs) == 2 and domainSegs[1] == "mailinator.com"):
                        ibmer= "true"
            else:
                userName = "Unknown userId"
                
            aToolchainDoc['userName']= userName
            aToolchainDoc['orgName']= orgName
            aToolchainDoc['IBM']= ibmer
            instancesToFix.append(aToolchainDoc)      
    
    toolchainsAsJson= {"toolchainsToFix" : instancesToFix}
    
    out = json.dumps(toolchainsAsJson)
    f = open(outpath, "w")
    f.write(out)
    f.close()
    
    print NAME + "exportToJson() - exported "+ str(len(instancesToFix)) + " service instances"         
        
def exportToCSV():
    print NAME +"exportToCSV() - exporting service instances to " + str(os.path.join(EXPORT_PATH, getCsvFile()))
    outpath = os.path.join(EXPORT_PATH, getCsvFile())
    outputFile = open(outpath,'wb')
    csvFile = csv.writer(outputFile)
    csvFile.writerow([name for name, _ in fields()])
    rows= 0
    for aToolchainDoc in allServiceInstances:
        creation_date= aToolchainDoc['creation_date']
        toolchain_guid= aToolchainDoc['toolchain_guid']
        organization_guid= aToolchainDoc['organization_guid']
        orgName= orgsById.get(organization_guid, "Unknown OrgId")
        creator= aToolchainDoc['creator']
        userDetails= usersById.get(creator, None)
        if (userDetails != None):
            userName= userDetails['userName']
            if ("ibm" in userName.lower()):
                ibmer= "true"
            else:
                ibmer= "false"
                domainSegs= userName.split('@')
                if (len(domainSegs) == 2 and domainSegs[1] == "mailinator.com"):
                    ibmer= "true"
        else:
            userName = "Unknown userId"
            ibmer= "unknown"
                
        service_id= aToolchainDoc['service_id']
        serviceInstanceId= aToolchainDoc['instance_id']
        webHookId= aToolchainDoc['webhook_id']
        enable_traceability= aToolchainDoc['enable_traceability']
        active= aToolchainDoc['recent_activity']
        
        # write row
        csvFile.writerow([creation_date, toolchain_guid, organization_guid, orgName.encode('utf-8').strip(), creator, userName.encode('utf-8').strip(), service_id, serviceInstanceId, webHookId, enable_traceability, ibmer, active])
        outputFile.flush()
        rows= rows +1
    
    outputFile.close()
    print NAME + "exportToCSV() - exported "+ str(rows) + " service instances" 
    
def connectToMetricsDB():
    couchServer = "https://disiffingencelygineirsom:6bab76826160878649d8c07eb16fc5c321224b7a@bmdevops-yp-us-south.cloudant.com"
    # YS1couchServer = "https://bysideredifforeacragiver:805063754b116a6867f295743aa2bf06e8dfc4b4@bmdevops-ys1-us-south.cloudant.com"
    if couchServer:
        # print "log level in cloudantHelpers:", logging.getLevelName(logging.getLogger('').level)
        print NAME + "Connecting to CouchDB or Cloudant at: %s..." % getSanitizedURI(couchServer)
        couch = couchdb.Server(couchServer)
        return couch
    raise 'No CouchDB or Cloudant URL could be determined.'

def connectToUsersDB():
    couchServer = "https://duchicklachistainksootle:bb872e78441d55983a06536bbd149bb6f376e5eb@bmdevops-yp-us-south.cloudant.com"
    # YS1 couchServer = "https://bysideredifforeacragiver:805063754b116a6867f295743aa2bf06e8dfc4b4@bmdevops-ys1-us-south.cloudant.com"
    if couchServer:
        # print "log level in cloudantHelpers:", logging.getLevelName(logging.getLogger('').level)
        print NAME + "Connecting to CouchDB or Cloudant at: %s..." % getSanitizedURI(couchServer)
        couch = couchdb.Server(couchServer)
        return couch
    raise 'No CouchDB or Cloudant URL could be determined.'

def connectToEventsDB():
    couchServer = "https://dsomeriedookepleformseep:eed6419b34ccb748f3dfd25a208fe28a8812f0ce@bmdevops-yp-us-south.cloudant.com"
    # YS1 couchServer = "https://bysideredifforeacragiver:805063754b116a6867f295743aa2bf06e8dfc4b4@bmdevops-ys1-us-south.cloudant.com"
    if couchServer:
        # print "log level in cloudantHelpers:", logging.getLevelName(logging.getLogger('').level)
        print NAME + "Connecting to CouchDB or Cloudant at: %s..." % getSanitizedURI(couchServer)
        couch = couchdb.Server(couchServer)
        return couch
    raise 'No CouchDB or Cloudant URL could be determined.'

def getSanitizedURI(uri):
    uriSplit= uri.split('@')
    if (len(uriSplit) >1):
        p = urlparse(uri)
        return p.geturl().replace(p.password, 'XXXXX')
    # running locally
    return uri

def getAllUsersInfo():
    print NAME +"getAllUsersInfo() - retrieving all users from the Metrics Users DB"
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
    print NAME + "Done - retrieved " + str(len(usersById)) + " users and " + str(len(orgsById)) + " orgs"

def getJsonFile():
    fileName= "srv_instances_to_fix_03_21_1.json"
    return fileName

def getCsvFile():
    fileName= "all_srv_instances_03_21_1.csv"
    return fileName

def ymd_str(dt):
    return dt.strftime('%Y-%m-%d')

def getActiveToolChains():
    count= 0
    for hit in eventsDB.view('pipeline-v2/ejd_job_execs_by_toolchain'):
        if hit['key'] is not None:
            if (not hit['key'] in activeToolChains):
                count= count +1
                activeToolChains.append(hit['key'])
    print NAME + str(count) + " active (in the past 7 days) toolchains retrieved"

def initDB():
    global db
    global udb
    global edb
    global warehouseDB
    global usersDB
    global eventsDB
    db= connectToMetricsDB()
    warehouseDB= db['metrics-warehouse']
    udb= connectToUsersDB()
    usersDB= udb['metrics-users-dw']
    edb= connectToEventsDB()
    eventsDB= edb['metrics-events']
    
def mainLoop():
    
    print NAME + "Initialize DB connections"
    initDB()
    
    getActiveToolChains()
    
    print NAME + "getAllServiceInstances thread starting..."
    getAllServiceInstancesThread = Thread(name = "getAllServiceInstances", target = getAllServiceInstances)
    getAllServiceInstancesThread.start()
    
    print NAME + "getAllUsersInfo thread starting..."
    getAllUsersInfoThread = Thread(name = "getAllUsersInfo", target = getAllUsersInfo)
    getAllUsersInfoThread.start()
    
    getAllServiceInstancesThread.join()
    getAllUsersInfoThread.join()
    
    print NAME + "Both threads completed, generating files"
    
    exportToJson()
    exportToCSV()

def main():
    logging.getLogger('').setLevel(os.getenv('LOG_LEVEL', logging.INFO))
    print NAME + "root logger level: %s" % logging.getLevelName(logging.getLogger('').level)

    logging.info(NAME + "  starting....")
    try: 
        mainLoop()

    except Exception,e:
        logging.error(NAME + "Unexpected exception: %s", str(e.message))
        traceback.print_exc()

    logging.info(NAME + "  ended....")

if __name__ == "__main__":
    main()