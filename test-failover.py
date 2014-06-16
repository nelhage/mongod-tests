import os
import sys
import optparse
import time
import logging

import pymongo

import mongo

class FailoverTest(object):
    def parse_args(self, args):
        parser = optparse.OptionParser("Usage: %prog [args]")
        mongo.add_mongod_options(parser)
        parser.add_option('--force-sync',
                          dest='force_sync',
                          help='Perform a large number of writes immediately before syncing',
                          action='store_true',
                          default=False)
        (self.options, args) = parser.parse_args(args)

    def __init__(self, args):
        self.parse_args(args)

    def test_failover(self):
        client = self.mongo.connect_primary()
        primary = client['admin'].command({'isMaster': 1})['primary']

        if self.options.force_sync:
            for i in xrange(100):
                client['test']['test-failover'].insert({"i": i}, w=0)

        start = time.time()
        try:
            client['admin'].command({'replSetStepDown': 1})
        except pymongo.errors.ConnectionFailure as e:
            print "Primary stepped down..."
        except pymongo.errors.OperationFailure as e:
            print "Couldn't fail over: %s" % (e,)
            return
        new_primary = self.mongo.wait_primary(client)
        end = time.time()
        print "Failover (%s -> %s) in %.2fs." % (primary, new_primary, end - start)

    def run(self):
        with mongo.replset(mongod=self.options.mongod,
                           port=self.options.port,
                           verbose=self.options.verbose) as self.mongo:
            while True:
                client = self.mongo.connect_primary()
                for i in xrange(30):
                    client['test']['test_failover'].insert({"heartbeat": time.time()})
                    time.sleep(1)
                self.test_failover()

if __name__ == '__main__':
    FailoverTest(sys.argv).run()
