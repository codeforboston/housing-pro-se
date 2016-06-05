#!/usr/bin/python
import csvToJson
import requests
from elasticsearch import Elasticsearch

""" Loads CSV data into Elasticsearch """

class loadData(object):

    INDEX = 'housingProSe'
    DOC_TYPE = 'eviction'

    def __init__(self, filename, hostname, port):
        self.filename = filename
        self.hostname = hostname
        self.port = port

    """ Clears all data from the index """
    def reset_index(self):
        # TODO: implement
        pass
        

    def load_data(self):
        doc_index = 0

        with open(self.filename, 'r') as f:
            column_line = f.readline()

            converter = csvToJson(column_line)
            doc_id = 'record_{}'.format(doc_index)

            for line in f:

                json_doc = converter.transformLine(line)
                post_entry(json_doc, INDEX, DOC_TYPE, doc_id)
                doc_index += 1

    def post_entry(self, data, index, doc_type, doc_id):
        """ Posts a single entry object to ElasticSearch"""
        post_url = 'http://' + self.hostname + ':' self.port + '/' + index + '/' + doc_type + '/' + doc_id
        try:
            # response =
            requests.request(method='POST', url=post_url, data=data)
            # print 'Response: ', response.read()
            return True
        except(HTTPError) as err:
            print('ERROR {} : {} +\nentry: {}'.format(err.code, err.read(), data))
            return False

def main()
    loader = loadData('', 'localhost', '9300')  

main()
