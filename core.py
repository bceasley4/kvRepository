import logging
import os
import json
import traceback
import time

from filelock import FileLock

class kvDatabase():

    def __init__(self, dbFilePath):
        self.dbFilePath = dbFilePath
        self.lock = '{}.lock'.format(dbFilePath)
        self.processLock = FileLock(self.lock)
        self.numPuts = 0
        self.numGets = 0
        self.numRemoves = 0
        self.maxTimePut = -1
        self.minTimePut = -1
        self.maxTimeGet = -1
        self.minTimeGet = -1
        self.maxTimeRemove = -1
        self.minTimeRemove = -1

        self.start()

    def __repr__(self):
        return str(self.getDatabase())

    def __len__(self):
        return len(self.getDatabase())

    def start(self):
        with self.processLock:
            if not os.path.exists(self.dbFilePath):
                with open(self.dbFilePath, 'w') as dbFile:
                    dbFile.write(json.dumps({}))

    def put(self, key, value): #what errors can happen within put
        start = time.time()
        try:
            db = self.getDatabase()
            db[key] = value
            self.flush(db)
            self.numPuts += 1
            timeTaken = time.time() - start
            if timeTaken > self.maxTimePut:
                self.maxTimePut = timeTaken
            elif timeTaken < self.minTimePut or self.minTimePut == -1:
                self.minTimePut = timeTaken
        except Exception:
            raise Exception("Database closed")

    def get(self, key):
        start = time.time()
        db = self.getDatabase()
        key = str(key)
        if key in db:
            self.numGets += 1
            timeTaken = time.time() - start
            if timeTaken > self.maxTimeGet:
                self.maxTimeGet = timeTaken
            elif timeTaken < self.minTimeGet or self.minTimeGet == -1:
                self.minTimeGet = timeTaken
            return db[key]
        else:
            raise KeyError("This key {} does not exist in the database".format(key))

    def remove(self, key):
        start = time.time()
        db = self.getDatabase()
        key = str(key)
        if key in db:
            del db[key]
        else:
            raise KeyError("This key {} does not exist in the database".format(key))
        try:
            self.flush(db)
            self.numRemoves += 1
            timeTaken = time.time() - start
            if timeTaken > self.maxTimeRemove:
                self.maxTimeRemove = timeTaken
            elif timeTaken < self.minTimeRemove or self.minTimeRemove == -1:
                self.minTimeRemove = timeTaken
        except Exception:
            raise Exception("Database closed")

    def getDatabase(self):
        with self.processLock:
            with open(self.dbFilePath, 'r') as dbFile:
                fileData = dbFile.read()
                if fileData == "":
                    db = {}
                else:
                    db = json.loads(fileData)
                return db

    def flush(self, db):
        with self.processLock:
            with open(self.dbFilePath, 'w') as dbFile:
                dbFile.write(json.dumps(db))

    def stats(self):
        return "Number of puts: {}" \
               "Number of gets: {}" \
               "Number of removes: {}" \
               ""

    def getKeys(self):
        return self.getDatabase().keys()

    def getValues(self):
        return self.getDatabase().values()

    def getItems(self):
        return self.getDatabase().items()

    def getNumPuts(self):
        return self.numPuts

    def getNumGets(self):
        return self.numGets

    def getNumRemoves(self):
        return self.numRemoves

    def close(self):
        self.flush({})

