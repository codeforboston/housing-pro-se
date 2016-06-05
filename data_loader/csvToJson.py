import json

""" Utility class to read a CSV file and produce JSON documents """
class CsvToJson(object):

    """ column_line - the first line of the CSV file 
        column_names - optional - name substitutions for the columns
    """

    def __init__(self, columns_line, column_names):
        self.columns =  columns_line.split(',')
        self.column_names = column_names

    """ """
    def transformLine(self, line):
        values =  line.split(',')

        for column, idx in self.columns:
            doc[column] = values[idx]
            
        return json.dumps(doc)

