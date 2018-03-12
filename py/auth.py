# File name: auth.py
# Author: Gunter Fritz
# Copyright: Gunter Fritz
# Lizenz: Apache v 2.0


import requests

class Auth():
    def __init__(self):
        self.timeout = 25.000
        self.base = "https://login.salesforce.com/services/oauth2/token"
        self.client_id = 'Oauth client id from your Salesforce App' 
        self.client_secret = 'Oauth client id from your Salesforce App'
        self.username = 'email@example.com'
        self.password = 'top secret'
        self.security_token = 'Security token for that user'

    """
    Oauth authentification with salesforce, as result access_tokon and instance_url is set 
    Needed is initialized:
      - client_id
      - client_secret
      - username
      - password 
      - security Token
    params
    ------
    return
    ------
          access_token, instance_url
    """
    def auth(self):
        p = { 'grant_type'    : 'password', 
              'client_id'     : self.client_id, 
              'client_secret' : self.client_secret, 
              'username'      : self.username, 
              'password'      : self.password + self.security_token }

        r = requests.post(self.base, params=p, timeout=self.timeout)

        self.access_token = r.json()['access_token']
        self.instance_url = r.json()['instance_url']

        return self.access_token, self.instance_url

