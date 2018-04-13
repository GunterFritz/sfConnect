# File name: salesforcer.py
# Author: Gunter Fritz
# Copyright: Gunter Fritz
# Lizenz: Apache v 2.0

import requests
import sys
import time
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

        rule_id = self.getRuleId('Test_Rule')

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
        self.exists('Account', ['0011r00001mj00xAAA', '0011r00001mj00yAAA', '0011r00001mj00zAAA', '0011r00001mj010AAA'])
        bulk = Bulk(self.access_token, self.instance_url)
        bulk.run()
        #self.exists('Account', ['0011r00001lskeAAAQ', '0011r00001lskeBBBQ', '0011r00001lsOsHAAU', '0011r00001lsadmAAA'])
        self.exists('Account', ['0011r00001mj00xAAA', '0011r00001mj00yAAA', '0011r00001mj00zAAA', '0011r00001mj010AAA'])
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


class Bulk():
    def __init__(self, access_token, instance_url):
        self.timeout = 25.000
        self.access_token = access_token
        self.instance_url = instance_url

    def createJob(self, operation, obj, contentType = 'CSV', lineEnding = 'LF'):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job'
        p = { 'operation' : operation, 'object' : obj, 'contentType' : contentType, 'lineEnding' : lineEnding }
        r = requests.post(url, headers = headers, json=p, timeout=self.timeout)
        if r.status_code == 400:
            raise ValueError(r.json())
        print(r.json())
        print(r.status_code)
        self.jobId = r.json()['id']

    """
    adds a batch to current job

    params
    ------
    filename: name of csv file with records to be processed
    """
    def batch(self, filename):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'text/csv' }
        url = self.instance_url + '/services/async/42.0/job/' + self.jobId + '/batch'
        f = open(filename, 'r')
        d = f.read()
        print(d)
        r = requests.post(url, headers = headers, data=d, timeout=self.timeout)
        print(r.status_code)
        if r.status_code == 400:
            raise ValueError(r.text)
        print(r.text)
        return None

    def close(self):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job/' + self.jobId
        p = { 'state' : 'UploadComplete' }
        p = { 'state' : 'Closed' }
        r = requests.post(url, headers = headers, json=p, timeout=self.timeout)
        print("Close", r.status_code)
        print(r.text)

    def check(self):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job/ingest/batch/' + self.jobId
        url = self.instance_url + '/services/async/42.0/job/' + self.jobId 
        r = requests.get(url, headers = headers, timeout=self.timeout)
        print("Check", r.status_code)
        print(r.text)

    def result(self):
        headers = { 'X-SFDC-Session' : self.access_token , 'Content-Type' : 'application/json' }
        url = self.instance_url + '/services/async/42.0/job/ingest/batch/' + self.jobId + '/successfulResults/'
        r = requests.get(url, headers = headers, timeout=self.timeout)
        print("Result", r.status_code)
        print(r.text)

    def run(self):
        self.jobId = '7501r00000A1m4CAAR'
        self.createJob('delete', 'Account')
        self.batch('tbdel.csv')
        self.close()
        self.check()
        time.sleep(20)
        self.check()
        self.result()



def usage():
        print("""usage: salesforce <options>
                 -a|--accounts:                list all accounts predefined fields, csv output
                 -e|--experimental:            changing usage for experimental code
                 -d|--dedup <list of ids>:     group ids to a duplicate set
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
                'fields=', 'sf_type=', 'list', 'delete=', 'clean'])
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
