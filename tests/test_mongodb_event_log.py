import unittest

from pyeventsml.mongodb_event_log import *


class TestMongoDBEventLog(unittest.TestCase):
    """
    MongoDB event log
    """

    def setUp(self):
        self.client = pymongo.MongoClient()

    def test_log(self):
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        collection = self.client.test_db.events

        log = MongoDBEventLogger(collection, group_id=None, accept_event_function=lambda x: True, default_listeners=global_listeners)

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

        log = MongoDBEventLogger(group_id=log.group_id, accept_event_function=lambda x: True, default_listeners=global_listeners, mongo_collection=collection)

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
        event_provider = MongoDBEventProvider(collection, log.group_id, global_listeners)

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

    def tearDown(self):
        self.client.drop_database('test_db')
        self.client = None


if __name__ == '__main__':
    unittest.main()
