import os
import sys
import optparse
import logging
import time

import pymongo
import mongo

class PushTest(object):
    def parse_args(self, args):
        parser = optparse.OptionParser("Usage: %prog [args]")
        parser.add_option('--count',
                          help='How many $push ops to apply',
                          action='store',
                          default=10000,
                          type='int')
        mongo.add_mongod_options(parser)
        (self.options, args) = parser.parse_args(args)

    def __init__(self, args):
        self.parse_args(args)

    def cmp_ts(self, lhs, rhs):
        return cmp((lhs.time, lhs.inc), (rhs.time, rhs.inc))

    def test_push(self):
        client = self.mongo.connect_primary()

        logging.info("$push'ing %d items, one at a time.", self.options.count)
        collection = client['test']['test_push']
        collection.drop()
        _id = collection.insert({"nums": []})
        for i in xrange(self.options.count):
            collection.update({'_id': _id}, { '$push': { 'nums': i }})

        start = time.time()
        status = client['admin'].command("replSetGetStatus")

        start_optime = [m for m in status['members'] if m['state'] == 1][0]['optimeDate']
        logging.info("Committed %d $push's, optime=%s...", self.options.count, start_optime)

        logging.info("Waiting on secondaries...")
        while len([m for m in status['members'] if m['optimeDate'] < start_optime]):
            time.sleep(0.2)
            status = client['admin'].command("replSetGetStatus")
        logging.info("Secondaries caught up in %0.2fs.", time.time() - start)

    def run(self):
        with mongo.replset(mongod=self.options.mongod,
                           port=self.options.port,
                           verbose=self.options.verbose,
                           quickstart=False) as self.mongo:
            while True:
                self.test_push()

if __name__ == '__main__':
    logging.basicConfig(level='INFO', format='[%(asctime)s %(process)s|%(levelname)s] %(message)s')
    PushTest(sys.argv).run()
