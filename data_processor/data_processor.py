import datetime
import csv

from data_retriever.data_retriever import Data_retriever

class Data_processor:
    def __init__(self, kpi_list=None, start=None, stop=None):
        self.kpi_list = kpi_list
        self.start = start
        self.stop = stop

        self.metrics_dict = dict()

        print("Data processor started with args:")
        print(type(self.kpi_list), self.kpi_list)
        print(type(self.start), self.start)
        print(type(self.stop), self.stop)

    def get_dataset(self):
        """ Gets the dataset in memory,
            for larger dataset should be modified to return a file object
        """
        data_retriever = Data_retriever()
        self.raw_datafile = data_retriever.get()

    def parse_dataset(self):
        csv_reader = csv.reader(self.raw_datafile)
        for line in csv_reader:
            print("Got raw line:", line)
        
        pass

    def metrics_on_dataset(self):
        pass

    def run(self):
        self.get_dataset()
        self.parse_dataset()
        self.metrics_on_dataset()
        return self.metrics_dict


if __name__ == '__main__':
    process = Data_processor()
    
