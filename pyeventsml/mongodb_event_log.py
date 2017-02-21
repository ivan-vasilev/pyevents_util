import uuid
import pymongo
from pyevents.events import *
from typing import Callable
import numpy as np
import base64


class MongoDBEventLogger(object):
    """Log events based on accept_event_function criteria"""

    def __init__(self, mongo_collection, accept_for_serialization, group_id=None, encoder: Callable = None, default_listeners=None):

        self._mongo_collection = mongo_collection

        self.group_id = group_id if group_id is not None else uuid.uuid4()

        self._sequence_id = 0

        if group_id is not None:
            existing_events = self.collection.find({'group_id': group_id}).sort('sequence_id', pymongo.DESCENDING).limit(1)
            if existing_events.count() > 0:
                e = next(existing_events)
                self._sequence_id = e['sequence_id'] + 1

        self.accept_for_serialization = accept_for_serialization

        self._encoder = encoder if encoder is not None else default_encoder

        if default_listeners is not None:
            default_listeners += self.onevent

    @property
    def collection(self):
        if self._mongo_collection is None:
            self._mongo_collection = pymongo.MongoClient().db.events

        return self._mongo_collection

    def onevent(self, event):
        if self.accept_for_serialization is not None and self.accept_for_serialization(event):
            self.collection.insert({'group_id': self.group_id, 'sequence_id': self._sequence_id, 'event': event if self._encoder is None else self._encoder(event)})
            self._sequence_id += 1


def default_encoder(obj):
    """
    Encodes object to json serializable types
    :param obj: object to encode
    :return: encoded object
    """

    if isinstance(obj, np.ndarray):
        data_b64 = base64.b64encode(np.ascontiguousarray(obj).data)
        return dict(__ndarray__=data_b64, dtype=str(obj.dtype), shape=obj.shape)

    try:
        obj = obj.__dict__
    except:
        pass

    if isinstance(obj, dict):
        result = dict()
        for k, v in obj.items():
            result[k] = default_encoder(v)

        return result

    return obj


class MongoDBEventProvider(object):
    """Fire logged events"""

    def __init__(self, mongo_collection, group_id, decoder: Callable = None, default_listeners=None):

        self._mongo_collection = mongo_collection

        self.group_id = group_id

        self._decoder = decoder if decoder is not None else default_decoder

        if default_listeners is not None:
            self.fire_event += default_listeners

    @after
    def fire_event(self, event):
        return event

    def __call__(self):
        for e in self._mongo_collection.find({'group_id': self.group_id}).sort('sequence_id', pymongo.ASCENDING):
            self.fire_event(e['event'] if self._decoder is None else self._decoder(e['event']))


def default_decoder(obj):
    """
    Decodes a previously encoded json object, taking care of numpy arrays
    :param obj: object to decode
    :return: decoded object
    """

    if isinstance(obj, dict):
        if '__ndarray__' in obj:
            data = base64.b64decode(obj['__ndarray__'])
            return np.frombuffer(data, obj['dtype']).reshape(obj['shape'])
        else:
            for k, v in obj.items():
                obj[k] = default_decoder(v)

    return obj
