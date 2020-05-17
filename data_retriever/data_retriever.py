import datetime
import io  # For StringIO

class Data_retriever:
    def __init__(self, url='http://lameapi-env.ptqft8mdpd.us-east-2.elasticbeanstalk.com/data'):
        self.url = url

    def get(self):
        """ Gets from url, save to file and returns the fileobject
        """
        print("Getting from url:", self.url)

        data = """1,1
2,100
3,1
4,1"""
        file_object = io.StringIO(data)

        return file_object
