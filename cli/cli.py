import argparse
from datetime import datetime

from data_processor.data_processor import Data_processor

class Cli_parser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("-k", "--kpi_list",
                                 help="Comma delimited list of KPIs."+\
                                 " (occupancy,light,humidity,temperature,co2)")
        self.parser.add_argument("-s", "--start",
                                 help="Start date of time period."+\
                                 " m/d/y or m/d/y H:M")
        self.parser.add_argument("-e", "--stop",
                                 help="Stop date of time period."+\
                                 " m/d/y or m/d/y H:M")

        self.kpi_list = list()
        self.start = str()
        self.stop = str()

    def parse(self):
        """ This is the main method.
        """
        self.args = self.parser.parse_args()
        if self.process_kwargs():
            self.invoke_data_processor()

    def parse_datetime(self, date_string):
        """ Get the expected time string into datetime object.
        """
        # Unpact date, and time if it exists
        (date, *time) = date_string.split(' ')
        if time:
            return datetime.strptime(date_string, '%m/%d/%y %H:%M')
        else:
            return datetime.strptime(date_string, '%m/%d/%y')

    def process_kwargs(self):
        """ Get data from kwargs into the class.
            If a kwarg is None, show help.
        """
        # If an argument is none, show help
        for item in self.args._get_kwargs():
            if item[1] == None:
                self.parser.print_help()
                return False

        for item in self.args._get_kwargs():
            if item[0] == 'kpi_list':
                self.kpi_list = item[1].split(',')
            if item[0] == 'start':
                self.start = self.parse_datetime(item[1])
            if item[0] == 'stop':
                self.stop = self.parse_datetime(item[1])
        return True

    def invoke_data_processor(self):
        processor = Data_processor(self.kpi_list,
                                   self.start,
                                   self.stop)
        print(processor.run())


if __name__ == '__main__':
    p = Cli_parser()
    p.parse()

