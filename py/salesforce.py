# File name: salesforcer.py
# Author: Gunter Fritz
# Copyright: Gunter Fritz
# Lizenz: Apache v 2.0

import requests
import sys
import time
import csv
import re
import getopt

from auth import Auth


class Salesforce():
    def __init__(self, access_token, instance_url):
        self.timeout = 25.000
        self.access_token = access_token
        self.instance_url = instance_url


    """
    simple get request with an url
    e.g. query all accounts: getUrl('/services/data/v20.0/query?q=SELECT+name,BillingPostalCode+from+Account')

    params
    ------
    purl: String, /services/data/v20.0/query?q=SELECT+name,BillingPostalCode+from+Account

    return
    ------
    response
    """
    def getUrl(self, purl):
        headers = { 'Authorization' : 'Bearer ' + self.access_token , 'X-PrettyPrint' : '1' }
        url = self.instance_url + purl
        r = requests.get(url, headers = headers, timeout=self.timeout)

        #print (r.json())

        return r

    """
    creates a duplicate group
        - needed DuplicateRule, DuplicateRecordSet, DuplicateRecordItem (for each data)
    
    params
    ------
    dubs: [] list of ids

    return
    ------
    """
    def deduplicate(self, dubs):
        if self.exists("Account", dubs) == False:
            return None

        rule_id = self.getRuleId('Test_Regel')
        
        #create the group
        headers = { 'Authorization' : 'Bearer ' + self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/data/v42.0/sobjects/DuplicateRecordSet/'
        p = { 'DuplicateRuleId' : rule_id }
        r = requests.post(url, headers = headers, json=p, timeout=self.timeout)
        print (r.json())
        group_id = r.json()['id']
       
        #create DublicateItems
        for dub in dubs:
            url = self.instance_url + '/services/data/v42.0/sobjects/DuplicateRecordItem/'
            p = { 'DuplicateRecordSetId' : group_id, 'RecordId' : dub }
            r = requests.post(url, headers = headers, json=p, timeout=self.timeout)
            print (r.json())
        
        return None

    """
    delete all DuplicateRecordSets from given DuplicateRule

    params
    ------
    rule: String, name of rule 

    return
    ------
    number of deleted objects
    """
    def clean(self, rule):
        retval = 0
        rule_id = self.getRuleId(rule)
        r = self.getUrl("/services/data/v42.0/query?q=SELECT+id+from+DuplicateRecordSet+WHERE+DuplicateRuleId+=+'" + rule_id+ "'")
        
        json = r.json()
        if json['totalSize'] > 0:
            #return existing ID
            for r in json['records']:
                retval = retval + self.delete('DuplicateRecordSet', r['Id'])

        return retval


    """
    delete list of objects

    params
    ------
    sf_type: String, Salesforce sobject 
    fields:  String, comma separated field names

    return
    ------
    number of deleted objects
    """
    def delete(self, sf_type, ids):
        deleted = 0
        headers = { 'Authorization' : 'Bearer ' + self.access_token}
        for i in ids.split(','):
            url = self.instance_url + '/services/data/v42.0/sobjects/' + sf_type + '/' + i
            r = requests.delete(url, headers = headers, timeout=self.timeout)
            if r.status_code != 204:
                print("Error deleting '" + sf_type + "##" + i + "': " + r.json()[0]['message'])
            else:
                print("Deleted: '" + sf_type + "##" + i + "'")
                deleted = deleted + 1
                
        return deleted


    """
    returns the database id from a rule. If no such rule exists it is created
    params
    ------
    rule:  String, DeveloperName of rule (e.g. Standard_Account_Duplicate_Rule)
    label: String, if created, create with that label

    return
    ------
    id:    String, Salesforce Database ID
    """
    def getRuleId(self, rule, label = None):
        #read specific DuplicateRules 
        r = self.getUrl("/services/data/v42.0/query?q=SELECT+id,MasterLabel,DeveloperName+from+DuplicateRule+WHERE+DeveloperName+=+'" + rule + "'")

        json = r.json()
        if json['totalSize'] > 0:
            #return existing ID
            for r in json['records']:
                print("Rule " + rule + " exists" )
                return str(r['Id'])

        if label == None:
            #TODO throw error
            return None
        #DOES NOT WORK!!!
        #create Rule
        print("Creating rule: " + rule)
        headers = { 'Authorization' : 'Bearer ' + self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/data/v37.0/sobjects/DuplicateRule/'
        p = { 'DeveloperName' : rule }
        #p = { 'DeveloperName' : rule, 'MasterLabel' : label, 'SobjectType' : 'Account', 'IsActive' : True }
        r = requests.post(url, headers = headers, json=p, timeout=self.timeout)
        print(r.json()) 
        return str(r.json()['id'])

    """
    list fields from object sf_type

    params
    ------
    sf_type: String, Salesforce sobject 
    fields:  String, comma separated field names if field name is "-" its an empty field

    return
    ------
    list of objects, first line is header
    """
    def listObjects(self, sf_type, fields):
        getfields = fields.replace(",-,", ",").replace("-,", "").replace(",-", "")
        r = self.getUrl('/services/data/v42.0/query?q=SELECT+' + getfields + '+from+' + sf_type)
        #print(r.json())
        header = fields.split(",")
        retval = [header]
        regexp = re.compile('[^a-zA-Z0-9@ßäüö]')
        while True:
            json = r.json()
            for r in json['records']:
                line = []
                for f in header:
                    if f == "-":
                        line.append('')
                        continue
                    #TODO:replace all non alphanumeric chars
                    line.append(regexp.sub(' ', str(r[f])))
                retval.append(line)
            if 'nextRecordsUrl' in json:
                print("read")
                r = self.getUrl(json['nextRecordsUrl'])
                print("ready")
            else:
                break
        
        return retval

    def printCsv(self, out, delimiter):
        for line in out:
            output = line[0]
            for f in line[1:]:
                output = output + delimiter + f
            print(output)

    """
    delete all objects sf_type

    params
    ------
    sf_type: String, Salesforce sobject 
    fields:  String, comma separated field names

    return
    ------
    list of objects, first line is header
    """
    def deleteAll(self, sf_type):
        for ids in self.listObjects(sf_type, 'Id,Name'):
            print("deleting:", ids[1])
            self.delete(sf_type, ids[0])

    """
    wrapper to list all accounts with fiew standard fields in csv format to stdout
    first line is header
    """
    def listAccounts(self):
        accounts = self.listObjects('Account', 'Id,BillingCountry,-,Name,-,BillingStreet,-,BillingPostalCode,BillingCity,-')
        self.printCsv(accounts, ";")

    """
    wrapper to list all accounts with fiew standard fields in csv format to stdout
    first line is header
    
    params
    ------
    sf_type: String, Salesforce sobject 
    fields:  String, comma separated field names
    """
    def listObjectsCsv(self, obj, fields):
        accounts = self.listObjects(obj, fields)
        self.printCsv(accounts, ";")

    """
    reads an outputfile from identity and exports each group to identiyt
    fileformat: header with fields
        out_grp_id: group id from identity
        1         : salesforce Account id

    params
    ------
    filename: String, inputfile
    num:      int, max number of groups to be created
    """
    def createDuplicatesFromFile(self, filename, num):
        grp_id = -1
        count = -1
        idlist = []
        dups = []
        with open(filename, "r") as csvfile:
            reader = csv.DictReader(csvfile, delimiter = ";")
            for row in reader:
                if row['out_grp_id'] != grp_id:
                    if len(idlist)>1:
                        #close group
                        #self.deduplicate(idlist)
                        dups.append(idlist)
                    count = count + 1
                    if count == num:
                        break
                    idlist = [row['1']]
                    grp_id = row['out_grp_id']
                    
                else:
                    #add data id to group
                    idlist.append(row['1'])
            if count < num:
                #close last group
                count = count + 1
                dups.append(idlist)
                #self.deduplicate(idlist)
        self.insertDuplicates(dups)


   

    """
    helper to create json, to create num DuplicateRecordSets

    params
    ------
    num: int, number of DuplicateRecordSets

    return json dict
    """
    def createDuplicateRecordSetJson(self, num):
        rule_id = self.getRuleId('Test_Regel')
        data = []
        for i in range(0, num):
            data.append({'DuplicateRuleId' : rule_id })

        return data
    
    """
    helper to create json, to create DuplicateRecord

    params
    ------
    set_ids: [], list of Salesforce DuplicateRecordSets
    dups:    [], list of duplicates (each element is a list of Record IDs)

    return json dict
    """
    def createDuplicateRecordItemJson(self, set_ids, dups):
        if len(dups) != len(set_ids):
            raise ValueError("length of record sets (" + len(dups) + 
                    ") doesn't match length of recordsetid (" + len(set_ids) + ")")
        data= []
        for group_id, recs in zip(set_ids, dups):
            for rec in recs:
                print({ 'DuplicateRecordSetId' : group_id, 'RecordId' : rec})
                data.append({ 'DuplicateRecordSetId' : group_id, 'RecordId' : rec})

        return data
    
    """
    creates a DuplicateRecordSet in Salesforce

    params
    ------
    num, int number or DuplicateRecordSet to be created

    return
    ------
    list of DublicateRecordSet Ids
    """
    def createDuplicateRecordSet(self, num):
        json = self.createDuplicateRecordSetJson(num)
        res = self.insertBulk("DuplicateRecordSet", json).json()
        retval = []
        for i in res:
            retval.append(i['id'])
        return retval

    """
    creates DuplicateRecordSet and attach DuplicateRecordItem, uses Bulk API

    params
    ------
    dups: [] list of list with Record ids, each line is one duplicate group

    return
    ------
    response
    """
    def insertDuplicates(self, dups):
        set_ids = self.createDuplicateRecordSet(len(dups))
        json = self.createDuplicateRecordItemJson(set_ids, dups)
        
        result = self.insertBulk("DuplicateRecordItem", json)

        print(result.text)
        if result.status_code >= 400:
            raise ValueError(r.text)

    """
    wrapper to handle a complete batch (insert) job
    
    params
    ------
    sf_type, String Salesforce object to be created
    json,    corresponding json object
    """
    def insertBulk(self, sf_type, json):
        bulk = Bulk(self.access_token, self.instance_url)
        bulk.createJob('insert', sf_type)
        bulk.jbatch(json)
        bulk.close()
        return bulk.getSuccessfulResult()

    """
    checks if all requested ids objects are existing
    
    params
    ------
    sf_type: String, sobject type (e.g. Account)
    ids:     String[], list of sf ids

    return
    ------
    True:  all objects do exist
    False: one or more objects do not exist
    """
    def exists(self, sf_type, ids):
        retval = True
        for i in ids:
            r = self.getUrl('/services/data/v42.0/sobjects/' + sf_type + '/' + i)
            if r.status_code != 200:
                retval = False
                print("Error:", r.json()[0]['message'])
            else:
                print("Id ok:", i)
        return retval

    """
    function to evaluate different/ changing calls
    """
    def experimental(self):
        #self.exists('Account', ['0011r00001mj00xAAA', '0011r00001mj00yAAA', '0011r00001mj00zAAA', '0011r00001mj010AAA'])
        bulk = Bulk(self.access_token, self.instance_url)
        #bulk.delete('tbdel.csv')
        bulk.insert()
        #self.exists('Account', ['0011r00001lskeAAAQ', '0011r00001lskeBBBQ', '0011r00001lsOsHAAU', '0011r00001lsadmAAA'])
        #self.exists('Account', ['0011r00001mj00xAAA', '0011r00001mj00yAAA', '0011r00001mj00zAAA', '0011r00001mj010AAA'])
        #read all accounts 
        #getUrl('/services/data/v20.0/query?q=SELECT+name,BillingPostalCode+from+Account')
        #read all contacts from accounts 
        #getUrl('/services/data/v20.0/query?q=SELECT+id,name,(SELECT+id,name+from+Contacts)+from+Account')
        #read all DuplicateRecordSets
        #self.clean("Test_Regel")
        #r = self.getUrl('/services/data/v42.0/query?q=SELECT+id,RecordCount+from+DuplicateRecordSet')
        #read all DuplicateRecordItem 
        #r = self.getUrl('/services/data/v42.0/query?q=SELECT+id+from+DuplicateRecordItem')
        #read all DuplicateRules 
        #r = self.getUrl('/services/data/v42.0/query?q=SELECT+id,MasterLabel,DeveloperName+from+DuplicateRule')
        #read specific DuplicateRules
        #getUrl("/services/data/v42.0/query?q=SELECT+id,MasterLabel,DeveloperName+from+DuplicateRule+WHERE+DeveloperName+=+'Standard_Account_Duplicate_Rule'")
        #print(r.json())

        #getUrl('/services/data/v42.0/query?q=SELECT+id,RecordCount,(SELECT+id+from+DuplicateRecordItem)+from+DuplicateRecordSet')
        #getUrl('/services/data/v42.0/query?q=SELECT+id,(SELECT+id+from+DuplicateRecordSet)+from+DuplicateRecordItem')
        #getUrl('/services/data/v42.0/query?q=SELECT+id+from+DuplicateRecordItem')
        
        #duplicate(['0011r00001lsOsHAAU', '0011r00001lsadmAAA'])
        
        #getUrl('/services/data/v20.0/query?q=SELECT+name,BillingPostalCode+from+Account')
        #getUrl('/services/data/v36.0/sobjects/contacts/0011r00001lsOe5AAE')
        #getUrl('/services/data/v42.0/DuplicateResult/')
        return None

"""
Basic calls to work with Bulk API from Salesforce
Processing data consists of following steps
1 create a job
2 add data to a batch
3 close job
4 check status
5 receive recults
"""

class Bulk():
    def __init__(self, access_token, instance_url):
        self.timeout = 25.000
        self.access_token = access_token
        self.instance_url = instance_url
        self.jobId = None
        self.batchId = None

    """
    creates a bulk job

    params
    ------
    operation, String (delete, insert)
    obj,       String Salesforce Object Type
    """
    def createJob(self, operation, obj):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job'
        p = { 'operation' : operation, 'object' : obj, 'contentType' : 'JSON' }
        r = requests.post(url, headers = headers, json=p, timeout=self.timeout)
        if r.status_code >= 400:
            raise ValueError(r.json())
        self.jobId = r.json()['id']
        print("Created Job with id:", self.jobId)

    """
    add a batch to the job

    params
    ------
    data: json String to be added
    """
    def jbatch(self, data):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job/' + self.jobId + '/batch'
        r = requests.post(url, headers = headers, json=data, timeout=self.timeout)
        
        if r.status_code >= 400:
            raise ValueError(r.text)
        
        self.batchId = r.json()['id']
        print("Job Id", self.jobId)
        print("BatchId", self.batchId)
        print("------")
        
        return None
    
    """
    close the job
    """
    def close(self):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job/' + self.jobId
        p = { 'state' : 'UploadComplete' }
        p = { 'state' : 'Closed' }
        r = requests.post(url, headers = headers, json=p, timeout=self.timeout)
        print("Close", r.status_code)
        print(r.text)

    """
    checkBatch controls if a is completed

    params
    ------
    synch: bool, if True call blocks until job is completed or failed

    return True if batch is completed, False if Failed (sync), False if Queued, InProgress, Failed(async) 
    """
    def checkBatch(self, sync = True):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job/' + self.jobId + '/batch/' + self.batchId
        while True:
            r = requests.get(url, headers = headers, timeout=self.timeout)
            state = r.json()['state']
            if state == 'Completed':
                return True
            if state != 'Queued' and state != 'InProgress':
                raise ValueError("Error in batch with id '" + self.batchId + "': " + state)
            if sync == True:
                time.sleep(1)
            else:
                break

        return False

    """
    returns the response object successful Results, it blocks until job is 
    successully processed or failed
    """
    def getSuccessfulResult(self):
        if not self.checkBatch():
            return None
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job/' + self.jobId + '/batch/' + self.batchId + '/result'
        r = requests.get(url, headers = headers, timeout=self.timeout)
        if r.status_code >= 400:
            raise ValueError(r.text)
        return r

def usage():
        print("""usage: salesforce <options>
                 -a|--accounts:                list all accounts predefined fields, csv output
                 -e|--experimental:            changing usage for experimental code
                 -d|--dedup <list of ids>:     group ids to a duplicate set
                 --filededup <file>             group first 10 groups to salesforce, file is
                                               output from identity
                 --clean:                      deletes all "Test_Regel" DuplicateRecordSet
                 --delete <ids>:               list of object ids to be deleted
                                                 sf_type must be set
                 -l|--list:                    list all objects, csv output
                                                 sf_type must be set
                 -s|--sf_type <sf_object>:     set sf_type
                 -f|--fields <list od fields>: print fields (only available for -l|--list)
                                                 default is id
                """)

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "aed:f:ls:", ['accounts', 'experimental', 'dedup=', \
                'fields=', 'sf_type=', 'list', 'delete=', 'clean', 'filededup='])
    except getopt.GetoptError:
        usage()
        return
    token, url = Auth().auth()
    sf = Salesforce(token, url)
    fields = 'Id'
    mode = 0
    ids = None
    sf_type = None
    for opt, arg in opts:
        if opt in ('--filededup'):
            sf.createDuplicatesFromFile(arg, 10)
        if opt in ('-a', '--accounts'):
            sf.listAccounts()
        if opt in ('-e', '--experimental'):
            sf.experimental()
        if opt in ('-d', '--dedup'):
            objects = arg.split(',')
            sf.deduplicate(objects)
        if opt in ('--delete'):
            ids = arg
        if opt in ('-f', '--fields'):
            fields = arg
        if opt in ('-s', '--sf_type'):
            sf_type = arg
        if opt in (['--clean']):
            sf.clean('Test_Regel')
        if opt in (['--delete']):
            mode = 'delete'
        if opt in ('-l', '--list'):
            mode = 'list'
    
    if mode == 'list':
        if sf_type == None:
            usage()
            return
        print(sf_type, fields)
        sf.listObjectsCsv(sf_type, fields)
    elif mode == 'delete':
        if sf_type == None or ids == None:
            usage()
            return
        sf.delete(sf_type, ids)

#TODO: create DuplicateRule
#      cleanup

if __name__ == "__main__":
    main(sys.argv[1:])
