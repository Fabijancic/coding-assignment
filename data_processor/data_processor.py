import datetime
import copy
import csv

import statistics

from data_retriever.data_retriever import Data_retriever
from utils.date_utils import parse_datetime 

class Data_processor:
    def __init__(self, kpi_list=None, start=None, stop=None):
        self.kpi_list = kpi_list
        self.start = start
        self.stop = stop

        kpi_template = {
            "last_value": float(),
            "first_value": float(),
            "lowest": float(),
            "highest": float(),
            "list_of_values": list()
            }
        self.kpi_dict = dict()
        for kpi in self.kpi_list:
            # pass by value
            self.kpi_dict[kpi] = copy.deepcopy(kpi_template)
        self.processed_data = list()
        self.csv_headers = None

    def get_dataset(self):
        """ Gets the dataset in memory,
            for larger dataset should be modified to return a file object
        """
        data_retriever = Data_retriever()
        self.raw_datafile = data_retriever.get()
        if not self.raw_datafile:
            print("Data_processor couldn't retrieve the data file")
            raise Warning

    def parse_dataset(self):
        """ Turns a csv file into list of dicts
        """
        csv_reader = csv.reader(self.raw_datafile)
        self.csv_headers = next(csv_reader, None)
        for line in csv_reader:
            parsed_line = dict()
            for idx, datapoint in enumerate(line):
                if idx == 0:
                    parsed_line[self.csv_headers[idx]] = parse_datetime(datapoint)
                else:
                    parsed_line[self.csv_headers[idx]] = float(datapoint)
            self.processed_data.append(parsed_line)

    def initialize_kpi(self, kpi, line):
        """ Sets all kpis to a value in line, as a value initialization
            except 'list_of_values'
        """
        for key in self.kpi_dict[kpi].keys():
            if not(key == 'list_of_values'):
                self.kpi_dict[kpi][key] = line[kpi]

    def metrics_on_dataset(self):
        """ Update the metrics during reading
        """
        processed_records = 0
        first_value = True
        for line in self.processed_data:
            if line.get('date') > self.start and line.get('date') < self.stop:
                processed_records+=1
                # Init values for kpi calculation with first member of requested set
                if first_value:
                    first_value = False
                    for kpi in self.kpi_list:
                        self.initialize_kpi(kpi, line)
                        self.kpi_dict[kpi]['first_value'] = line[kpi]
                # Processing code:
                
                for kpi in self.kpi_list:
                    self.update_min_max(kpi, line)
                    self.kpi_dict[kpi]['list_of_values'].append(line[kpi])
                    self.kpi_dict[kpi]['last_value'] = line[kpi]
                
        # END FOR
        print("{} out of {} records were processed.".format(
            processed_records, len(self.processed_data)))

    def update_min_max(self, kpi, line):
        try:
            if line[kpi] < self.kpi_dict[kpi]["lowest"]:
                self.kpi_dict[kpi]["lowest"] = line[kpi]
            if line[kpi] > self.kpi_dict.get(kpi)["highest"]:
                self.kpi_dict[kpi]["highest"] = line[kpi]
        except ValueError as e:
            print("UPDATE ERROR: ", e)
            print("Kpi name: ", type(kpi), kpi)
            print("Line ", line)
            print("Kpi dict for kpi: ", self.kpi_dict.get(kpi))
            print("Whole kpi dict: ", self.kpi_dict)
            print()
            raise e

    def postprocess(self):
        """ Add all kpi-s that require a full dataset for calculation
        """
        for kpi in self.kpi_list:
            self.kpi_dict[kpi]["mode"] = statistics.mode(self.kpi_dict[kpi]['list_of_values'])
            self.kpi_dict[kpi]["median"] = statistics.median(self.kpi_dict[kpi]['list_of_values'])
            self.kpi_dict[kpi]["average"] = statistics.mean(self.kpi_dict[kpi]['list_of_values'])
            # Remove the list of values
            self.kpi_dict[kpi].pop('list_of_values')

            # Calculate percent_change between first and last value
            self.kpi_dict[kpi]["percent_change"] =(
                (
                    self.kpi_dict[kpi]["last_value"] - self.kpi_dict[kpi]["first_value"]
                ) / self.kpi_dict[kpi]["first_value"]
            ) * 100

    def run(self):
        self.get_dataset()
        self.parse_dataset()
        self.metrics_on_dataset()
        self.postprocess()
        return self.kpi_dict


if __name__ == '__main__':
    process = Data_processor()
    
