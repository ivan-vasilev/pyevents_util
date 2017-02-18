import uuid
import pymongo
from pyevents.events import *


class MongoDBEventLogger(object):
    """Log events based on accept_event_function criteria"""

    def __init__(self, mongo_collection, group_id=None, accept_event_function=None, default_listeners=None):

        self._mongo_collection = mongo_collection

        self.group_id = group_id if group_id is not None else uuid.uuid4()

        if group_id is not None:
            existing_events = self.collection.find({'group_id': group_id}).sort('sequence_id', pymongo.DESCENDING).limit(1)
            e = next(existing_events)
            if e is not None:
                self._sequence_id = e['sequence_id'] + 1
        else:
            self._sequence_id = 0

        self.accept_for_serialization = accept_event_function

        if default_listeners is not None:
            default_listeners += self.onevent

    @property
    def collection(self):
        if self._mongo_collection is None:
            self._mongo_collection = pymongo.MongoClient().db.events

        return self._mongo_collection

    def onevent(self, event):
        if self.accept_for_serialization is not None and self.accept_for_serialization(event):
            self.collection.insert({'group_id': self.group_id, 'sequence_id': self._sequence_id, 'event': event})
            self._sequence_id += 1


class MongoDBEventProvider(object):
    """Fire logged events"""

    def __init__(self, mongo_collection, group_id, default_listeners=None):

        self._mongo_collection = mongo_collection

        self.group_id = group_id

        if default_listeners is not None:
            self.fire_event += default_listeners

    @after
    def fire_event(self, event):
        return event

    def __call__(self):
        for e in self._mongo_collection.find({'group_id': self.group_id}).sort('sequence_id', pymongo.ASCENDING):
            self.fire_event(e['event'])
