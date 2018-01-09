import logging
import pickle
import threading
import uuid
from typing import Callable

import pymongo
from bson.binary import Binary
from bson.errors import BSONError

import pyevents_util.mongodb.util as mongoutil


class MongoDBSequenceLog(object):
    """Log events based on accept_event_function criteria"""

    def __init__(self, mongo_collection, accept_for_serialization: Callable, listeners, group_id=None, encoder: Callable = None):

        self.collection = mongo_collection

        self._lock = threading.RLock()

        self.group_id = group_id if group_id is not None else uuid.uuid4()

        self._sequence_id = 0

        if group_id is not None:
            existing_events = self.collection.find({'group_id': group_id}).sort('sequence_id', pymongo.DESCENDING).limit(1)
            if existing_events.count() > 0:
                e = next(existing_events)
                self._sequence_id = e['sequence_id'] + 1

        self.accept_for_serialization = accept_for_serialization

        self._encoder = encoder if encoder is not None else mongoutil.default_encoder

        self.listeners = listeners

        self.listeners += self.onevent

    def store(self, obj):
        with self._lock:
            try:
                self.collection.insert_one({'group_id': self.group_id, 'sequence_id': self._sequence_id, 'obj': obj if self._encoder is None else self._encoder(obj)})
                logging.getLogger(__name__).debug("Log json event")
            except (BSONError, TypeError):
                self.collection.insert_one({'group_id': self.group_id, 'sequence_id': self._sequence_id, 'obj': Binary(pickle.dumps(obj))})
                logging.getLogger(__name__).debug("Failed to serialize json. Falling back to binary serialization")

            self._sequence_id += 1

        self.listeners({'type': 'store_object', 'data': obj})

    def onevent(self, event):
        if self.accept_for_serialization(event):
            self.store(event)


class MongoDBSequenceProvider(object):
    """Fire logged events"""

    def __init__(self, mongo_collection, group_id, listeners, decoder: Callable = None):
        self._mongo_collection = mongo_collection
        self.group_id = group_id
        self.listeners = listeners

        self._decoder = decoder if decoder is not None else mongoutil.default_decoder

    def __call__(self):
        for e in self._mongo_collection.find({'group_id': self.group_id}).sort('sequence_id', pymongo.ASCENDING):
            element = e['obj'] if self._decoder is None else self._decoder(e['obj'])
            logging.debug("Sequence " + str(element))
            self.listeners(element)
