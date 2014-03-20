import os
import sys
import optparse
import logging
import time

import pymongo
import mongo

class BuildIndexTest(object):
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
        logging.info("Inserted %d objects in %0.1fs...", n, time.time() - start)

    def test_index_builds(self):
        client = self.mongo.connect_primary()
        count = 0
        logging.info("creating test.test_index")
        collection = client['test']['test_index']
        while True:
            insert = max(5 * count - count, 10000)
            logging.info("Inserting %d items...", insert)
            self.bulk_insert(collection, insert, count)
            count += insert
            logging.info("Now contains %d items. Building indexes...", count)

            for t in xrange(5):

                start = time.time()
                collection.ensure_index([["i", 1]], background=False, name="test_index")
                fg = time.time() - start
                collection.drop_index("test_index")

                start = time.time()
                collection.ensure_index([["i", 1]], background=True, name="test_index")
                bg = time.time() - start
                collection.drop_index("test_index")

                logging.info("Done. items=%d fg=%0.1f bg=%0.1f", count, fg, bg)


    def run(self):
        with mongo.replset(mongod=self.options.mongod,
                           port=self.options.port,
                           verbose=self.options.verbose,
                           quickstart=False,
                           nodes=1) as self.mongo:
            while True:
                self.test_index_builds()

if __name__ == '__main__':
    logging.basicConfig(level='INFO', format='[%(asctime)s %(process)s|%(levelname)s] %(message)s')
    BuildIndexTest(sys.argv).run()
