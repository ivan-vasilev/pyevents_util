import logging
from typing import Callable

import pymongo
from bson.binary import Binary
from bson.errors import BSONError

from pyevents_util.mongodb.util import *


class MongoDBStore(object):
    """Save object manager based on accept_event_function criteria"""

    def __init__(self, mongo_collection, accept_for_serialization: Callable, encoder: Callable = None, listeners=None):

        self._mongo_collection = mongo_collection

        self.accept_for_serialization = accept_for_serialization

        self._encoder = encoder if encoder is not None else default_encoder

        self.listeners = listeners
        self.listeners += self.on_event

    @property
    def collection(self):
        if self._mongo_collection is None:
            self._mongo_collection = pymongo.MongoClient().db.events

        return self._mongo_collection

    def on_event(self, event):
        if self.accept_for_serialization(event):
            self.store(event['data'])

    def store(self, obj):
        if isinstance(obj, dict):
            _id = obj['_id']
        else:
            _id = getattr(obj, '_id')

        try:
            self.collection.replace_one({'_id': _id}, obj if self._encoder is None else self._encoder(obj), upsert=True)
            logging.getLogger(__name__).debug("Stored json object")
        except (BSONError, TypeError):
            self.collection.replace_one({'_id': _id}, {'binary_data': Binary(pickle.dumps(obj))}, upsert=True)
            logging.getLogger(__name__).debug("Failed to serialize json. Falling back to binary serialization")

        self.listeners({'type': 'store_object', 'data': obj})

    @staticmethod
    def restore(mongo_collection, _id):
        data = mongo_collection.find_one({'_id': _id})
        return default_decoder(data['binary_data'] if 'binary_data' in data else data)
