import os
import subprocess
import tempfile
import shutil
import time
import logging
from contextlib import contextmanager

import pymongo

class ReplSet(object):
    def __init__(self, mongod='mongod', port=9000,
                 verbose=0, quickstart=True, nodes=3,
                 journal=True):
        self.mongod = mongod
        self.port = port
        self.verbose = verbose
        self.quickstart = quickstart
        self.nodes = nodes
        self.journal = journal

    def start_mongos(self):
        self.tempdir = tempfile.mkdtemp(prefix='mongo-test')
        self.processes = []
        self.rs_name = 'rs_%d' % os.getpid()
        for i in xrange(self.nodes):
            port = self.port + i
            d = os.path.join(self.tempdir, "mongo-%d" % (port,))
            os.mkdir(d)
            extra_args = []
            if self.verbose > 0:
                extra_args.append('-' + 'v' * self.verbose)
            if self.quickstart:
                extra_args.extend(['--smallfiles', '--noprealloc', '--nopreallocj'])
            if self.journal:
                extra_args.extend(['--journal'])


            mongo = subprocess.Popen([
                    self.mongod,
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
                logging.debug("Unable to connect: %s" % (e,))
                logging.debug("Retrying...")
                time.sleep(1)

    def connect_primary(self):
        client = self.connect(self.port)
        primary_port = int(self.wait_primary(client).split(":")[1])
        return self.connect(primary_port)

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
        client = self.connect(self.port)
        config = {
            '_id': self.rs_name,
            'members': []
            }
        for i in xrange(self.nodes):
            config['members'].append({'_id': i, 'host': "127.0.0.1:%d" % (self.port + i)})
        client['admin'].command({'replSetInitiate': config})
        logging.info("Waiting for the replset to initialize...")
        self.wait_primary(client)
        while True:
            status = client['admin'].command({'replSetGetStatus': 1})
            states = [m['state'] for m in status['members']]
            if set(states) <= set([1,2]):
                return
            time.sleep(1)

    def cleanup(self):
        for p in self.processes:
            p.kill()
        shutil.rmtree(self.tempdir)


@contextmanager
def replset(*args, **kwargs):
    try:
        rs = ReplSet(*args, **kwargs)
        rs.start_mongos()
        rs.start_replset()
        yield rs
    finally:
        rs.cleanup()

def add_mongod_options(parser):
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
