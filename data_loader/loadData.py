#!/usr/bin/python
import csv
from tqdm import tqdm
import requests
from pprint import pprint
from elasticsearch import Elasticsearch, serializer, compat, exceptions
import json
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


# via https://github.com/elastic/elasticsearch-py/issues/374 -- suppress unicode serialization errors
class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
    """
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)


""" Loads CSV data into Elasticsearch """
class LoadData(object):

    INDEX = 'housingprose'
    DOC_TYPE = 'eviction'

    def __init__(self, filename, hostname="penguinwrench.com", port="9200"):
        self.filename = filename
        self.hostname = hostname
        self.port = port
        self.es = Elasticsearch([{"host":self.hostname, "port":self.port}], serializer=JSONSerializerPython2())

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
        def doc_iter():
            with open(self.filename) as csvfile:
                reader = csv.DictReader(csvfile)
                for doc_index, row in tqdm(enumerate(reader)):
                    doc_id = 'record_{}'.format(doc_index)

                    renamed_row = dict([self.column_names_map[key.strip()], safe_convert(val.strip())] for key, val in row.iteritems())

                    yield {
                        "_index": self.INDEX,
                        "_type": self.DOC_TYPE,
                        "_id": doc_id,
                        "_source": renamed_row
                    }

        print bulk(self.es, doc_iter())
               
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