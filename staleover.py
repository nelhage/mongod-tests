import os
import sys
import optparse
import time
import logging

import pymongo
import pymongo.errors

import mongo

class FailoverTest(object):
    def parse_args(self, args):
        parser = optparse.OptionParser("Usage: %prog [args]")
        mongo.add_mongod_options(parser)
        (self.options, args) = parser.parse_args(args)

    def __init__(self, args):
        self.parse_args(args)

    def test_failover(self):
        client = self.mongo.connect_primary()

        config = client['local']['system.replset'].find_one()
        for node in config['members']:
            host, port = node['host'].split(":")
            if port != str(client.port):
                node['priority'] = 0
                print "Reconfiguring %s to priority=0" % (node['host'],)
                break
        config['version'] += 1
        try:
            client['admin'].command({'replSetReconfig': config})
        except pymongo.errors.AutoReconnect:
            pass

        # reconnect post-reconfig
        primary = self.mongo.connect_primary()
        secondary, priority0 = None, None

        config = client['local']['system.replset'].find_one()
        for node in config['members']:
            host, port = node['host'].split(":")
            if int(port) == primary.port:
                continue
            if 'priority' in node and int(node['priority']) == 0:
                priority0 = self.mongo.connect(int(port))
            else:
                secondary = self.mongo.connect(int(port))

        print "primary=%s:%s secondary=%s:%s p0=%s:%s" % (
            primary.host, primary.port,
            secondary.host, secondary.port,
            priority0.host, priority0.port)

        secondary['admin'].command('fsync', 1, lock=True)

        print "fsyncLock'd %s:%s" % (secondary.host, secondary.port)

        primary['test']['test-failover'].insert({"i": 1}, w=2)

        time.sleep(1)

        old_primary = "%s:%s" % (primary.host, primary.port)

        start = time.time()
        try:
            for p in self.mongo.processes:
                if p.port == primary.port:
                    p.proc.kill()
                break
            primary['admin'].command('ping')
        except (pymongo.errors.ConnectionFailure, pymongo.errors.OperationFailure) as e:
            print "Primary is dead."

        secondary.unlock()
        time.sleep(1)

        new_primary = self.mongo.wait_primary(secondary)
        end = time.time()
        print "Failover (%s -> %s) in %.2fs." % (
            old_primary, new_primary, end - start)

    def run(self):
        with mongo.replset(mongod=self.options.mongod,
                           port=self.options.port,
                           verbose=self.options.verbose) as self.mongo:
            self.test_failover()

if __name__ == '__main__':
    FailoverTest(sys.argv).run()
