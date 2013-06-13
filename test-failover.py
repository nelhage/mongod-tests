import os
import sys
import subprocess
import optparse
import tempfile
import shutil
import time
from contextlib import contextmanager

import pymongo

class FailoverTest(object):
    def parse_args(self, args):
        parser = optparse.OptionParser("Usage: %prog [args]")
        parser.add_option('--port',
                          help='Base port to run mongod servers on',
                          action='store',
                          type='int',
                          default=9000)
        parser.add_option('--mongod',
                          help='Path to mongod',
                          action='store',
                          type='string',
                          default='/usr/bin/mongod')
        parser.add_option('-v',
                          dest='verbose',
                          help='Make the mongod more verbose',
                          action='count',
                          default=0)
        (self.options, args) = parser.parse_args(args)

    def __init__(self, args):
        self.parse_args(args)

    def start_mongos(self):
        self.tempdir = tempfile.mkdtemp()
        self.processes = []
        self.rs_name = 'rs_%d' % os.getpid()
        for i in xrange(3):
            port = self.options.port + i
            d = os.path.join(self.tempdir, "mongo-%d" % (port,))
            os.mkdir(d)
            extra_args = []
            if self.options.verbose > 0:
                extra_args.append('-' + 'v' * self.options.verbose)
            mongo = subprocess.Popen([
                    self.options.mongod,
                    '--smallfiles', '--noprealloc', '--journal', '--nopreallocj',
                    '--port', str(port),
                    '--replSet', self.rs_name,
                    '--dbpath', d,
                    '--logpath', os.path.join(d, 'mongo.log')]
                                     + extra_args)
            self.processes.append(mongo)

    def connect(self, port):
        while True:
            try:
                return pymongo.MongoClient('localhost', port,
                                           read_preference=pymongo.read_preferences.ReadPreference.PRIMARY_PREFERRED)
            except pymongo.errors.ConnectionFailure as e:
                print "Unable to connect: %s" % (e,)
                print "Retrying..."
                time.sleep(1)

    def wait_primary(self, client):
        while True:
            try:
                primary = client['admin'].command({'isMaster': 1}).get('primary', None)
                if primary:
                    return primary
            except pymongo.errors.OperationFailure as e:
                pass
            time.sleep(0.1)

    def start_replset(self):
        client = self.connect(self.options.port)
        config = {
            '_id': self.rs_name,
            'members': []
            }
        for i in xrange(3):
            config['members'].append({'_id': i, 'host': "127.0.0.1:%d" % (self.options.port + i)})
        client['admin'].command({'replSetInitiate': config})
        print "Waiting for the replset to initialize..."
        self.wait_primary(client)
        while True:
            status = client['admin'].command({'replSetGetStatus': 1})
            states = [m['state'] for m in status['members']]
            if set(states) == set([1,2]):
                return
            time.sleep(1)

    def cleanup(self):
        for p in self.processes:
            p.kill()
        shutil.rmtree(self.tempdir)

    def test_failover(self):
        client = self.connect(self.options.port)
        primary = self.wait_primary(client)
        client = self.connect(int(primary.split(":")[1]))
        start = time.time()
        try:
            client['admin'].command({'replSetStepDown': 1})
        except pymongo.errors.ConnectionFailure as e:
            print "Primary stepped down..."
        except pymongo.errors.OperationFailure as e:
            print "Couldn't fail over: %s" % (e,)
            return
        new_primary = self.wait_primary(client)
        end = time.time()
        print "Failover (%s -> %s) in %.2fs." % (primary, new_primary, end - start)

    def run(self):
        try:
            self.start_mongos()
            self.start_replset()
            while True:
                time.sleep(30)
                self.test_failover()
        finally:
            self.cleanup()

if __name__ == '__main__':
    FailoverTest(sys.argv).run()
