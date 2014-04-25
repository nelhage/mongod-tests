import os
import sys
import optparse
import logging
import time

import pymongo
import mongo

class InsertTest(object):
    def parse_args(self, args):
        parser = optparse.OptionParser("Usage: %prog [args]")
        mongo.add_mongod_options(parser)
        (self.options, args) = parser.parse_args(args)

    def __init__(self, args):
        self.parse_args(args)

    def bulk_insert(self, collection, n, off=0):
        start = time.time()
        for i in xrange(n / 100):
            collection.insert([
                {"i": off + 100*i + j, "junk": "x" * 1024}
                for j in xrange(100)])

    def test_inserts(self):
        client = self.mongo.connect_primary()
        count = 0
        logging.info("creating test.test_insert")
        collection = client['test']['test_insert']
        while True:
            insert = max(2 * count - count, 10000)
            start = time.time()
            self.bulk_insert(collection, insert, count)
            count += insert
            end = time.time()
            logging.info("Inserted n=%d count=%d time=%0.1fs dps=%d", 
                         insert, count, end - start, (insert/(end - start)))

    def run(self):
        with mongo.replset(mongod=self.options.mongod,
                           port=self.options.port,
                           verbose=self.options.verbose,
                           quickstart=False,
                           nodes=1) as self.mongo:
            while True:
                self.test_inserts()

if __name__ == '__main__':
    logging.basicConfig(level='INFO', format='[%(asctime)s %(process)s|%(levelname)s] %(message)s')
    InsertTest(sys.argv).run()
