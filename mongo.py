import os
import subprocess
import tempfile
import shutil
import time
import logging
from contextlib import contextmanager

import pymongo

class ReplSet(object):
    def __init__(self, mongod='mongod', port=9000, verbose=0):
        self.mongod = mongod
        self.port = port
        self.verbose = verbose

    def start_mongos(self):
        self.tempdir = tempfile.mkdtemp()
        self.processes = []
        self.rs_name = 'rs_%d' % os.getpid()
        for i in xrange(3):
            port = self.port + i
            d = os.path.join(self.tempdir, "mongo-%d" % (port,))
            os.mkdir(d)
            extra_args = []
            if self.verbose > 0:
                extra_args.append('-' + 'v' * self.verbose)
            mongo = subprocess.Popen([
                    self.mongod,
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
                logging.info("Unable to connect: %s" % (e,))
                logging.info("Retrying...")
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
        for i in xrange(3):
            config['members'].append({'_id': i, 'host': "127.0.0.1:%d" % (self.port + i)})
        client['admin'].command({'replSetInitiate': config})
        logging.info("Waiting for the replset to initialize...")
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


@contextmanager
def replset(*args, **kwargs):
    try:
        rs = ReplSet(*args, **kwargs)
        rs.start_mongos()
        rs.start_replset()
        yield rs
    finally:
        rs.cleanup()
