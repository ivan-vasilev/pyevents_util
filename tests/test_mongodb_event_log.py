import unittest
import numpy as np

from pyeventsml.mongodb_event_log import *


class TestMongoDBEventLog(unittest.TestCase):
    """
    MongoDB event log
    """

    def setUp(self):
        self.client = pymongo.MongoClient()

    def test_with_dict(self):
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        log = MongoDBEventLogger(self.client.test_db.events, lambda x: True, group_id=None, default_listeners=global_listeners)

        @after
        def test_event(counter):
            return {'test_data': 'test_value', 'counter': counter}

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
            self.assertEqual(e['event']['counter'], i)

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
            self.assertEqual(e['event']['counter'], i)

        self.assertEqual(i, 3)

        # phase 3
        event_provider = MongoDBEventProvider(self.client.test_db.events, log.group_id, default_listeners=global_listeners)

        e5 = threading.Event()

        listener_called = {'called': False}

        def test_event_provider(event):
            if event['counter'] == 3:
                listener_called['called'] = True
                self.assertEqual(event['counter'], 3)
                e5.set()

        global_listeners += test_event_provider

        event_provider()

        e5.wait()

        self.assertTrue(listener_called['called'])

    def test_with_composite_objects(self):
        # logging.basicConfig(level=logging.DEBUG)

        class TestLogCompositeNested(object):
            def __init__(self):
                self.nested = 'nested'

        class TestLogComposite(object):
            def __init__(self, counter):
                self.counter = counter
                self.__test_private = 'test private'
                self.__test_numpy = np.zeros((3, 4, 5))
                self.__test_nested = TestLogCompositeNested()

        global_listeners = AsyncListeners()

        log = MongoDBEventLogger(self.client.test_db.events, lambda x: True, group_id=None, default_listeners=global_listeners)

        @after
        def test_event(counter):
            return TestLogComposite(counter)

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
            self.assertEqual(e['event']['counter'], i)
            self.assertEqual(type(e['event']['_TestLogComposite__test_numpy']), dict)

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
            self.assertEqual(e['event']['counter'], i)
            self.assertEqual(type(e['event']['_TestLogComposite__test_numpy']), dict)

        self.assertEqual(i, 3)

        # phase 3
        event_provider = MongoDBEventProvider(self.client.test_db.events, log.group_id, default_listeners=global_listeners)

        e5 = threading.Event()

        listener_called = {'called': False}

        def test_event_provider(event):
            if event['counter'] == 3:
                listener_called['called'] = True
                self.assertEqual(event['counter'], 3)
                self.assertEqual(type(event['_TestLogComposite__test_numpy']), np.ndarray)
                e5.set()

        global_listeners += test_event_provider

        event_provider()

        e5.wait()

        self.assertTrue(listener_called['called'])

    def tearDown(self):
        self.client.drop_database('test_db')
        self.client = None


if __name__ == '__main__':
    unittest.main()
