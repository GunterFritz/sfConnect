# sfConnect
python script to read sf_objects

Before you can use it, you need to create a connected App:
https://help.salesforce.com/apex/HTViewHelpDoc?id=connected_app_create.htm

Please insert to your App:
- Name
- Your Contact email
- Activate OAuth
- url (insert: http://localhost)
- access is full

If you would like to create DuplicateRecordSet, add a DuplicateRule "Test_Rule"

For authorization you must edit auth.py, Please insert: 
    self.client_id = 'Oauth client id from your Salesforce App' 
    self.client_secret = 'Oauth client id from your Salesforce App'
    self.username = 'email@example.com'
    self.password = 'top secret'
    self.security_token = 'Security token for that user'

to execute the script:

pyhton3 salesforce.py <options>

