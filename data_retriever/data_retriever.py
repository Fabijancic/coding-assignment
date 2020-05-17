import urllib.request
import datetime
import json
import time
import io  # For StringIO

class Data_retriever:
    def __init__(self, url='http://lameapi-env.ptqft8mdpd.us-east-2.elasticbeanstalk.com/'):
        self.data_url = url + 'data'
        self.status_url = url + 'health'

    def get(self):
        """ Gets from url, save to file and returns the fileobject
        """
        for retry in range(30):
            try:
                # Check health of endpoint:
                with urllib.request.urlopen(self.status_url) as response:
                    text = response.read().decode('utf-8')
                    resp = json.loads(text)
                    if resp.get('ok') == True:
                        data_response = self.get_data()
                        if data_response:
                            return data_response
                        print("Endpoint returned empty 'data' field, retrying.")
                    else:
                        print("Endpoint returned status 'ok' = {}.".format(resp.get('ok')))
            except urllib.error.HTTPError as e:
                print("urllib.error.HTTPError happened, retrying.")
            # Backoff:
            backoff = 0.1  # 1*retry
            print("Backoff for {}s.".format(backoff))
            time.sleep(backoff)
        else:
            print("Error, couldn't get data from endpoint {}, health"+\
                  " status 'ok' was never 'True'".format(self.status_url))
            return False


    def get_data(self):
        with urllib.request.urlopen(self.data_url) as response:
            text = response.read().decode('utf-8')
            data = json.loads(text)
            data = data.get('data')
            if len(data)<1:
                return False
            file_object = io.StringIO(data)
            return file_object
