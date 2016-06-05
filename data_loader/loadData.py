#!/usr/bin/python
import csv
from tqdm import tqdm
import requests
from pprint import pprint
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import RequestError

from pip.utils import cached_property
import re

data_translations = {
    'N/A': None,
    'n/a': None,
    None: None,
    '12/30/1899': 0,
    '12/31/1899': 1
}

currency_match = re.compile(r'^\$[\d\.\,]+$')

""" Loads CSV data into Elasticsearch """
class LoadData(object):

    INDEX = 'housingprose'
    DOC_TYPE = 'eviction'

    def __init__(self, filename, hostname="penguinwrench.com", port="9200"):
        self.filename = filename
        self.hostname = hostname
        self.port = port
        self.es = Elasticsearch([{"host":self.hostname, "port":self.port}])

    @cached_property
    def column_names_map(self):
        """
            Return mapping of {'column_name_in_data_csv':'new_column_name_in_elasticsearch'}, loaded from column_mappings.csv file.
        """
        out = {}
        with open('column_mappings.csv') as csvfile:
            for line in csvfile:
                new_name, old_name = line.strip().split('\t')
                out[old_name] = new_name
        return out

    def reset_index(self):
        """ Clear all data and recreate the index. """
        if self.es.indices.exists(self.INDEX):
            self.es.indices.delete(index=self.INDEX)
        self.es.indices.create(index=self.INDEX, body=open('mapping.json').read())

    def load_data(self):
        with open(self.filename) as csvfile:
            reader = csv.DictReader(csvfile)
            for doc_index, row in tqdm(enumerate(reader)):
                doc_id = 'record_{}'.format(doc_index)

                renamed_row = dict([self.column_names_map[key.strip()], safe_convert(val)] for key, val in row.iteritems())

                try:
                    self.es.index(index=self.INDEX, doc_type=self.DOC_TYPE, id=doc_id, body=renamed_row)
                except RequestError as e:
                    print 'Error posting record:', e
                    pprint(renamed_row)

    # def load_data(self):
    #     def doc_iter():
    #         with open(self.filename) as csvfile:
    #             reader = csv.DictReader(csvfile)
    #             for doc_index, row in tqdm(enumerate(reader)):
    #                 doc_id = 'record_{}'.format(doc_index)

    #                 renamed_row = dict([self.column_names_map[key.strip()], safe_convert(val)] for key, val in row.iteritems())

    #                 yield {
    #                     "_index": self.INDEX,
    #                     "_type": self.DOC_TYPE,
    #                     "_id": doc_id,
    #                     "_source": renamed_row
    #                 }

    #     print bulk(self.es, doc_iter())
               
def safe_convert(in_val):
    if in_val in data_translations:
        return data_translations[in_val]

    if currency_match.match(in_val):
        return float(in_val.replace('$','').replace(',',''))

    try:
        return int(in_val)
    except ValueError:
        try:
            return float(in_val)
        except:
            pass

    return in_val 

def main():
    loader = LoadData('Eviction_Dataset.csv')
    # loader.reset_index()
    loader.load_data()

main()