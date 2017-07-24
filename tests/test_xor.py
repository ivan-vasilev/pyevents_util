import unittest

import tensorflow as tf

from pyevents_util import ml_phase
from pyevents_util.algo_phase import *
from pyevents_util.mongodb.mongodb_sequence_log import *
import numpy as np


class TestXor(unittest.TestCase):
    """
    XOR Test
    """

    def setUp(self):
        self.client = pymongo.MongoClient()

    def test_xor(self):
        events.use_global_event_bus()

        self._test_event_logger()
        events.reset()
        events.use_global_event_bus()

        self._test_event_provider()

    def _test_event_logger(self):
        MongoDBSequenceLog(self.client.test_db.events, group_id='test_xor_group', accept_for_serialization=lambda event: event['type'] == 'data' and 'phase' in event and event['phase'].endswith('_unordered'))

        training_iterations = 50

        AlgoPhaseEventsOrder(phases=[(ml_phase.TRAINING, training_iterations), (ml_phase.TESTING, 1), (None, 0)])

        # network definition
        input_ = tf.placeholder(tf.float32,
                                shape=[None, 2],
                                name="input")
        target = tf.placeholder(tf.float32,
                                shape=[None, 1],
                                name="target")
        nb_hidden_nodes = 4

        dense1 = tf.layers.dense(inputs=input_, units=nb_hidden_nodes, activation=None, kernel_initializer=tf.contrib.layers.xavier_initializer())

        bn1 = tf.layers.batch_normalization(dense1, axis=1)

        a1 = tf.nn.relu(bn1)

        logits = tf.layers.dense(inputs=a1, units=1, activation=None, kernel_initializer=tf.contrib.layers.xavier_initializer())

        loss = tf.nn.sigmoid_cross_entropy_with_logits(labels=target, logits=logits)

        correct_prediction = tf.equal(tf.argmax(tf.nn.softmax(logits), 1), tf.argmax(target, 1))

        accuracy_op = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

        train_step = tf.train.AdagradOptimizer(0.05).minimize(loss)

        # Start training
        init = tf.global_variables_initializer()
        with tf.Session() as sess:
            sess.run(init)

            @events.after
            def xor_data_provider(phase):
                return {'data': {'input:0': [[0, 0], [0, 1], [1, 0], [1, 1]],
                                 'target:0': [[0], [1], [1], [0]]},
                        'phase': phase + '_unordered',
                        'type': 'data'}

            # training phase
            training_phase = AlgoPhase(model=lambda x: sess.run(train_step, feed_dict=x), phase=ml_phase.TRAINING)

            # testing phase
            testing_phase = AlgoPhase(model=lambda x: accuracy_op.eval(feed_dict=x, session=sess), phase=ml_phase.TESTING)

            evaluations = {'accuracy': -1}

            e2 = threading.Event()

            @events.listener
            def end_testing_listener(event):
                if event['type'] == 'after_iteration' and event['phase'] == ml_phase.TESTING:
                    evaluations['testing_iterations'] = event['iteration']
                    evaluations['accuracy'] = event['model_output']
                    e2.set()

            for i in range(training_iterations):
                xor_data_provider(ml_phase.TRAINING)

            xor_data_provider(ml_phase.TESTING)

            e2.wait()

            self.assertEqual(training_phase._iteration, training_iterations)
            self.assertEqual(testing_phase._iteration, 1)
            self.assertEqual(evaluations['accuracy'], 1)

        tf.reset_default_graph()

    def _test_event_provider(self):
        training_iterations = 50

        AlgoPhaseEventsOrder(phases=[(ml_phase.TRAINING, training_iterations), (ml_phase.TESTING, 1), (None, 0)])

        # network definition
        input_ = tf.placeholder(tf.float32,
                                shape=[None, 2],
                                name="input")
        target = tf.placeholder(tf.float32,
                                shape=[None, 1],
                                name="target")
        nb_hidden_nodes = 4

        dense1 = tf.layers.dense(inputs=input_, units=nb_hidden_nodes, activation=tf.nn.relu, kernel_initializer=tf.contrib.layers.xavier_initializer())

        logits = tf.layers.dense(inputs=dense1, units=1, activation=None, kernel_initializer=tf.contrib.layers.xavier_initializer())

        loss = tf.nn.sigmoid_cross_entropy_with_logits(labels=target, logits=logits)

        correct_prediction = tf.equal(tf.argmax(tf.nn.softmax(logits), 1), tf.argmax(target, 1))

        accuracy_op = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

        train_step = tf.train.AdagradOptimizer(0.05).minimize(loss)

        # Start training
        init = tf.global_variables_initializer()
        with tf.Session() as sess:
            sess.run(init)

            e2 = threading.Event()

            # training phase
            training_phase = AlgoPhase(model=lambda x: sess.run(train_step, feed_dict=x), phase=ml_phase.TRAINING)

            testing_phase = AlgoPhase(model=lambda x: accuracy_op.eval(feed_dict=x, session=sess), phase=ml_phase.TESTING)

            evaluations = {'accuracy': -1}

            @events.listener
            def end_testing_listener(event):
                if event['type'] == 'after_iteration' and event['phase'] == ml_phase.TESTING:
                    evaluations['accuracy'] = event['model_output']
                    e2.set()

            MongoDBSequenceProvider(self.client.test_db.events, group_id='test_xor_group')()

            e2.wait()

            self.assertEqual(training_phase._iteration, training_iterations)
            self.assertEqual(testing_phase._iteration, 1)
            self.assertEqual(evaluations['accuracy'], 1)

        tf.reset_default_graph()

    def tearDown(self):
        self.client.drop_database('test_db')
        self.client = None

if __name__ == '__main__':
    unittest.main()
