from hdfs import InsecureClient
from json import dump, dumps


class HDFS:

    def __init__(self):
        self.client = InsecureClient('http://host:port', user='ann') #TODO change port and user when code moved

    def write(self, records):
        # As a context manager:
        with self.client.write('data/records.jsonl', encoding='utf-8') as writer: #TODO change file when code moved
            dump(records, writer)

        # Or, passing in a generator directly:
        self.client.write('data/records.jsonl', data=dumps(records), encoding='utf-8')
