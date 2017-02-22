import pickle
import uuid
from typing import Callable

import pymongo
from bson.binary import Binary
from bson.errors import BSONError

import pyeventsml.mongodb.util as mongoutil
from pyevents.events import *


class MongoDBEventLogger(object):
    """Log events based on accept_event_function criteria"""

    def __init__(self, mongo_collection, accept_for_serialization: Callable, group_id=None, encoder: Callable = None, default_listeners=None):

        self._mongo_collection = mongo_collection

        self.group_id = group_id if group_id is not None else uuid.uuid4()

        self._sequence_id = 0

        if group_id is not None:
            existing_events = self.collection.find({'group_id': group_id}).sort('sequence_id', pymongo.DESCENDING).limit(1)
            if existing_events.count() > 0:
                e = next(existing_events)
                self._sequence_id = e['sequence_id'] + 1

        self.accept_for_serialization = accept_for_serialization

        self._encoder = encoder if encoder is not None else mongoutil.default_encoder

        if default_listeners is not None:
            default_listeners += self.onevent

    @property
    def collection(self):
        if self._mongo_collection is None:
            self._mongo_collection = pymongo.MongoClient().db.events

        return self._mongo_collection

    def store(self, event):
        try:
            self.collection.insert({'group_id': self.group_id, 'sequence_id': self._sequence_id, 'event': event if self._encoder is None else self._encoder(event)})
            logging.getLogger(__name__).debug("Log json event")
        except (BSONError, TypeError):
            self.collection.insert({'group_id': self.group_id, 'sequence_id': self._sequence_id, 'event': Binary(pickle.dumps(event))})
            logging.getLogger(__name__).debug("Failed to serialize json. Falling back to binary serialization")

        self._sequence_id += 1

    def onevent(self, event):
        if self.accept_for_serialization(event):
            self.store(event)


class MongoDBEventProvider(object):
    """Fire logged events"""

    def __init__(self, mongo_collection, group_id, decoder: Callable = None, default_listeners=None):

        self._mongo_collection = mongo_collection

        self.group_id = group_id

        self._decoder = decoder if decoder is not None else mongoutil.default_decoder

        if default_listeners is not None:
            self.fire_event += default_listeners

    @after
    def fire_event(self, event):
        return event

    def __call__(self):
        for e in self._mongo_collection.find({'group_id': self.group_id}).sort('sequence_id', pymongo.ASCENDING):
            self.fire_event(e['event'] if self._decoder is None else self._decoder(e['event']))
