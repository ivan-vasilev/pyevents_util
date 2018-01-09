import unittest

from pyevents.events import *
from pyevents_util.mongodb.mongodb_sequence_log import *
from pyevents_util.mongodb.mongodb_store import *


class TestMongoDB(unittest.TestCase):
    """
    MongoDB event log
    """

    def setUp(self):
        self.client = pymongo.MongoClient()
        events.reset()

    def test_event_log_with_dict(self):
        events.use_global_event_bus()

        listeners = AsyncListeners()
        log = MongoDBSequenceLog(self.client.test_db.events, accept_for_serialization=lambda x: True if x['type'] == 'data' else False, listeners=listeners, group_id=None)

        # phase 1
        e1 = threading.Event()
        listeners += lambda x: e1.set() if x['type'] == 'store_object' else None
        listeners({'type': 'data', 'test_data': 'test_value', '_id': 0, 'test_numpy': np.zeros((2, 3))})
        e1.wait()

        e2 = threading.Event()
        listeners += lambda x: e2.set() if x['type'] == 'store_object' else None
        listeners({'type': 'data', 'test_data': 'test_value', '_id': 1, 'test_numpy': np.zeros((2, 3))})
        e2.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            self.assertEqual(e['obj']['_id'], i)
            event = mongoutil.default_decoder(e['obj'])
            self.assertTrue(isinstance(event['test_numpy'], np.ndarray))
            self.assertEqual(event['test_numpy'].shape, (2, 3))

        self.assertEqual(i, 1)

        # phase 2
        listeners -= log.onevent

        log = MongoDBSequenceLog(self.client.test_db.events, accept_for_serialization=lambda x: True if x['type'] == 'data' else False, listeners=listeners, group_id=log.group_id)

        e3 = threading.Event()
        listeners += lambda x: e3.set() if x['type'] == 'store_object' else None
        listeners({'type': 'data', 'test_data': 'test_value', '_id': 2, 'test_numpy': np.zeros((2, 3))})
        e3.wait()

        e4 = threading.Event()
        listeners += lambda x: e4.set() if x['type'] == 'store_object' else None
        listeners({'type': 'data', 'test_data': 'test_value', '_id': 3, 'test_numpy': np.zeros((2, 3))})
        e4.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            self.assertEqual(e['obj']['_id'], i)

        self.assertEqual(i, 3)

        # phase 3
        event_provider = MongoDBSequenceProvider(self.client.test_db.events, listeners=listeners, group_id=log.group_id)

        e5 = threading.Event()

        listener_called = {'called': False}

        def test_event_provider(event):
            if '_id' in event and event['_id'] == 3:
                listener_called['called'] = True
                self.assertEqual(event['_id'], 3)
                e5.set()

        listeners += test_event_provider

        event_provider()

        e5.wait()

        self.assertTrue(listener_called['called'])

    def test_event_log_with_composite_objects(self):
        global_listeners = events.AsyncListeners()

        log = MongoDBSequenceLog(self.client.test_db.events, accept_for_serialization=lambda x: True if isinstance(x, TestMongoDB.TestLogComposite) else False, listeners=global_listeners,  group_id=None)

        # phase 1
        e1 = threading.Event()
        global_listeners += lambda x: e1.set() if isinstance(x, dict) and x['type'] == 'store_object' else None

        global_listeners(TestMongoDB.TestLogComposite(0))
        e1.wait()

        e2 = threading.Event()
        global_listeners += lambda x: e2.set() if isinstance(x, dict) and x['type'] == 'store_object' else None
        global_listeners(TestMongoDB.TestLogComposite(1))
        e2.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            obj = mongoutil.default_decoder(e['obj'])
            self.assertEqual(obj._id, i)
            self.assertEqual(type(obj._test_numpy), np.ndarray)

        self.assertEqual(i, 1)

        # phase 2
        global_listeners -= log.onevent

        log = MongoDBSequenceLog(self.client.test_db.events, accept_for_serialization=lambda x: True if isinstance(x, TestMongoDB.TestLogComposite) else False, listeners=global_listeners, group_id=log.group_id)

        e3 = threading.Event()
        global_listeners += lambda x: e3.set() if isinstance(x, dict) and x['type'] == 'store_object' else None
        global_listeners(TestMongoDB.TestLogComposite(2))
        e3.wait()

        e4 = threading.Event()
        global_listeners += lambda x: e4.set() if isinstance(x, dict) and x['type'] == 'store_object' else None
        global_listeners(TestMongoDB.TestLogComposite(3))
        e4.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            obj = mongoutil.default_decoder(e['obj'])
            self.assertEqual(obj._id, i)
            self.assertEqual(type(obj._test_numpy), np.ndarray)

        self.assertEqual(i, 3)

        # phase 3
        event_provider = MongoDBSequenceProvider(self.client.test_db.events, listeners=global_listeners, group_id=log.group_id)

        e5 = threading.Event()

        listener_called = {'called': False}

        def test_event_provider(event):
            if isinstance(event, TestMongoDB.TestLogComposite):
                obj = mongoutil.default_decoder(event)
                if obj._id == 3:
                    listener_called['called'] = True

                    self.assertEqual(type(obj._test_numpy), np.ndarray)

                    self.assertEqual(type(obj.test_list), list)
                    self.assertEqual(type(obj.test_list[0]), tuple)
                    self.assertEqual(type(obj.test_tuple), tuple)
                    e5.set()

        global_listeners += test_event_provider

        event_provider()

        e5.wait()

        self.assertTrue(listener_called['called'])

    def test_store(self):
        listeners = AsyncListeners()

        store = MongoDBStore(self.client.test_db.store, listeners=listeners, accept_for_serialization=lambda x: True if x['type'] == 'data' else False)

        # phase 1
        e1 = threading.Event()
        listeners += lambda x: e1.set() if x['type'] == 'store_object' else None
        listeners({'type': 'data', 'data': TestMongoDB.TestLogComposite(0)})
        e1.wait()

        e2 = threading.Event()
        listeners += lambda x: e2.set() if x['type'] == 'store_object' else None
        listeners({'type': 'data', 'data': TestMongoDB.TestLogComposite(1)})
        e2.wait()

        obj = store.restore(self.client.test_db.store, 0)
        self.assertEqual(obj._id, 0)
        self.assertEqual(type(obj._test_numpy), np.ndarray)

        obj = store.restore(self.client.test_db.store, 1)
        self.assertEqual(obj._id, 1)
        self.assertEqual(type(obj._test_numpy), np.ndarray)

        obj._test_numpy[0, 0, 0] = 5

        e3 = threading.Event()
        listeners += lambda x: e3.set() if x['type'] == 'store_object' else None
        store.store(obj)
        e3.wait()

        obj_result = store.restore(self.client.test_db.store, 1)
        self.assertEqual(obj_result._id, 1)
        self.assertEqual(type(obj_result._test_numpy), np.ndarray)
        self.assertEqual(obj_result._test_numpy[0, 0, 0], 5)

    def tearDown(self):
        self.client.drop_database('test_db')
        self.client = None

    class TestLogCompositeNested(object):
        def __init__(self):
            self.nested = 'nested'

    class TestLogComposite(object):
        def __init__(self, _id):
            self._id = _id
            self._test_private = 'test private'
            self._test_numpy = np.zeros((3, 4, 5))
            self._test_nested = TestMongoDB.TestLogCompositeNested()
            self.test_tuple = (123, 'abc')
            self.test_list = [(123, 'abc'), (1, 2, 3)]


if __name__ == '__main__':
    unittest.main()
