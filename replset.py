import os
import sys
import optparse
import logging
import time

import mongo

class TestReplset(object):
    def parse_args(self, args):
        parser = optparse.OptionParser("Usage: %prog [args]")
        mongo.add_mongod_options(parser)
        parser.add_option('--nodes',
                          help='How many nodes to launch',
                          action='store',
                          default=3,
                          type='int')
        (self.options, args) = parser.parse_args(args)

    def __init__(self, args):
        self.parse_args(args)

    def run(self):
        with mongo.replset(mongod=self.options.mongod,
                           port=self.options.port,
                           verbose=self.options.verbose,
                           quickstart=True,
                           journal=False,
                           nodes=self.options.nodes) as self.mongo:
            c = self.mongo.connect_primary()
            logging.info("replset started, primary is localhost:%d", c.port)
            while True:
                time.sleep(1000)

if __name__ == '__main__':
    logging.basicConfig(level='INFO', format='[%(asctime)s %(process)s|%(levelname)s] %(message)s')
    TestReplset(sys.argv).run()
