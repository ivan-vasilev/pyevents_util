import unittest

from pyeventsml.mongodb.mongodb_event_log import *

from pyeventsml.mongodb.mongodb_store import *


class TestMongoDBEventLog(unittest.TestCase):
    """
    MongoDB event log
    """

    def setUp(self):
        self.client = pymongo.MongoClient()

    def test_event_log_with_dict(self):
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        log = MongoDBEventLogger(self.client.test_db.events, lambda x: True, group_id=None, default_listeners=global_listeners)

        @after
        def test_event(_id):
            return {'test_data': 'test_value', '_id': _id, 'test_numpy': np.zeros((2, 3))}

        test_event += global_listeners

        # phase 1
        e1 = threading.Event()
        global_listeners += lambda x: e1.set()
        test_event(0)
        e1.wait()

        e2 = threading.Event()
        global_listeners += lambda x: e2.set()
        test_event(1)
        e2.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            self.assertEqual(e['event']['_id'], i)
            event = mongoutil.default_decoder(e['event'])
            self.assertTrue(isinstance(event['test_numpy'], np.ndarray))
            self.assertEqual(event['test_numpy'].shape, (2, 3))

        self.assertEqual(i, 1)

        # phase 2
        global_listeners -= log.onevent

        log = MongoDBEventLogger(self.client.test_db.events, lambda x: True, group_id=log.group_id, default_listeners=global_listeners)

        e3 = threading.Event()
        global_listeners += lambda x: e3.set()
        test_event(2)
        e3.wait()

        e4 = threading.Event()
        global_listeners += lambda x: e4.set()
        test_event(3)
        e4.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            self.assertEqual(e['event']['_id'], i)

        self.assertEqual(i, 3)

        # phase 3
        event_provider = MongoDBEventProvider(self.client.test_db.events, log.group_id, default_listeners=global_listeners)

        e5 = threading.Event()

        listener_called = {'called': False}

        def test_event_provider(event):
            if event['_id'] == 3:
                listener_called['called'] = True
                self.assertEqual(event['_id'], 3)
                e5.set()

        global_listeners += test_event_provider

        event_provider()

        e5.wait()

        self.assertTrue(listener_called['called'])

    def test_event_log_with_composite_objects(self):
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        log = MongoDBEventLogger(self.client.test_db.events, lambda x: True, group_id=None, default_listeners=global_listeners)

        @after
        def test_event(_id):
            return TestMongoDBEventLog.TestLogComposite(_id)

        test_event += global_listeners

        # phase 1
        e1 = threading.Event()
        global_listeners += lambda x: e1.set()
        test_event(0)
        e1.wait()

        e2 = threading.Event()
        global_listeners += lambda x: e2.set()
        test_event(1)
        e2.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            obj = mongoutil.default_decoder(e['event'])
            self.assertEqual(obj._id, i)
            self.assertEqual(type(obj._test_numpy), np.ndarray)

        self.assertEqual(i, 1)

        # phase 2
        global_listeners -= log.onevent

        log = MongoDBEventLogger(self.client.test_db.events, lambda x: True, group_id=log.group_id, default_listeners=global_listeners)

        e3 = threading.Event()
        global_listeners += lambda x: e3.set()
        test_event(2)
        e3.wait()

        e4 = threading.Event()
        global_listeners += lambda x: e4.set()
        test_event(3)
        e4.wait()

        q_events = self.client.test_db.events.find({'group_id': log.group_id}).sort('sequence_id', pymongo.ASCENDING)
        for i, e in enumerate(q_events):
            self.assertEqual(e['sequence_id'], i)
            obj = mongoutil.default_decoder(e['event'])
            self.assertEqual(obj._id, i)
            self.assertEqual(type(obj._test_numpy), np.ndarray)

        self.assertEqual(i, 3)

        # phase 3
        event_provider = MongoDBEventProvider(self.client.test_db.events, log.group_id, default_listeners=global_listeners)

        e5 = threading.Event()

        listener_called = {'called': False}

        def test_event_provider(event):
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
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        store = MongoDBStore(self.client.test_db.store, lambda x: True, default_listeners=global_listeners)

        @after
        def test_event(_id):
            return {'data': TestMongoDBEventLog.TestLogComposite(_id)}

        test_event += global_listeners

        # phase 1
        e1 = threading.Event()
        store.store += lambda x: e1.set()
        test_event(0)
        e1.wait()

        e2 = threading.Event()
        store.store += lambda x: e2.set()
        test_event(1)
        e2.wait()

        obj = store.restore(self.client.test_db.store, 0)
        self.assertEqual(obj._id, 0)
        self.assertEqual(type(obj._test_numpy), np.ndarray)

        obj = store.restore(self.client.test_db.store, 1)
        self.assertEqual(obj._id, 1)
        self.assertEqual(type(obj._test_numpy), np.ndarray)

        obj._test_numpy[0, 0, 0] = 5

        e3 = threading.Event()
        store.store += lambda x: e3.set()
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
            self._test_nested = TestMongoDBEventLog.TestLogCompositeNested()
            self.test_tuple = (123, 'abc')
            self.test_list = [(123, 'abc'), (1, 2, 3)]


if __name__ == '__main__':
    unittest.main()
