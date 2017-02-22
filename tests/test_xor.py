import unittest

import tensorflow as tf

from pyeventsml.algo_phase import *
from pyeventsml import ml_phase
from pyeventsml.mongodb.mongodb_event_log import *


class TestXor(unittest.TestCase):
    """
    XOR Test
    """

    def setUp(self):
        self.client = pymongo.MongoClient()

    def test_xor(self):
        self._test_event_logger()
        self._test_event_provider()

    def _test_event_logger(self):
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        MongoDBEventLogger(self.client.test_db.events, group_id='test_xor_group', accept_for_serialization=lambda event: event['type'] == 'data', default_listeners=global_listeners)

        # network definition
        nb_classes = 2
        input_ = tf.placeholder(tf.float32,
                                shape=[None, 2],
                                name="input")
        target = tf.placeholder(tf.float32,
                                shape=[None, nb_classes],
                                name="target")
        nb_hidden_nodes = 4
        # enc = tf.one_hot([0, 1], 2)
        w1 = tf.Variable(tf.random_uniform([2, nb_hidden_nodes], -1, 1, seed=0),
                         name="Weights1")
        w2 = tf.Variable(tf.random_uniform([nb_hidden_nodes, nb_classes], -1, 1,
                                           seed=0),
                         name="Weights2")
        b1 = tf.Variable(tf.zeros([nb_hidden_nodes]), name="Biases1")
        b2 = tf.Variable(tf.zeros([nb_classes]), name="Biases2")
        activation2 = tf.sigmoid(tf.matmul(input_, w1) + b1)
        hypothesis = tf.nn.softmax(tf.matmul(activation2, w2) + b2)
        cross_entropy = -tf.reduce_sum(target * tf.log(hypothesis))
        train_step = tf.train.GradientDescentOptimizer(0.1).minimize(cross_entropy)

        correct_prediction = tf.equal(tf.argmax(hypothesis, 1), tf.argmax(target, 1))
        accuracy_op = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

        # Start training
        init = tf.global_variables_initializer()
        with tf.Session() as sess:
            sess.run(init)

            @after
            def xor_data_provider(phase):
                return {'data': {'input:0': [[0, 0], [0, 1], [1, 0], [1, 1]],
                                 'target:0': [[0, 1], [1, 0], [1, 0], [0, 1]]},
                        'phase': phase,
                        'type': 'data'}

            xor_data_provider += global_listeners
            # training phase
            training_phase = AlgoPhase(model=lambda x: sess.run(train_step, feed_dict=x), phase=ml_phase.TRAINING,
                                       default_listeners=global_listeners)

            # testing phase
            testing_phase = AlgoPhase(model=lambda x: accuracy_op.eval(feed_dict=x, session=sess),
                                      phase=ml_phase.TESTING, default_listeners=global_listeners)

            accuracy = {'accuracy': -1}

            e = threading.Event()

            training_iterations = 1000

            def start_testing_listener(event):
                if event['type'] == 'after_iteration' and event['phase'] == ml_phase.TESTING:
                    accuracy['accuracy'] = event['model_output']
                    e.set()

            global_listeners += start_testing_listener

            for i in range(training_iterations):
                xor_data_provider(ml_phase.TRAINING)

            xor_data_provider(ml_phase.TESTING)

            e.wait()
            self.assertEqual(training_phase._iteration, training_iterations)
            self.assertEqual(testing_phase._iteration, 1)
            self.assertEqual(accuracy['accuracy'], 1)

        tf.reset_default_graph()

    def _test_event_provider(self):
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        # network definition
        nb_classes = 2
        input_ = tf.placeholder(tf.float32,
                                shape=[None, 2],
                                name="input")
        target = tf.placeholder(tf.float32,
                                shape=[None, nb_classes],
                                name="target")
        nb_hidden_nodes = 4
        # enc = tf.one_hot([0, 1], 2)
        w1 = tf.Variable(tf.random_uniform([2, nb_hidden_nodes], -1, 1, seed=0),
                         name="Weights1")
        w2 = tf.Variable(tf.random_uniform([nb_hidden_nodes, nb_classes], -1, 1,
                                           seed=0),
                         name="Weights2")
        b1 = tf.Variable(tf.zeros([nb_hidden_nodes]), name="Biases1")
        b2 = tf.Variable(tf.zeros([nb_classes]), name="Biases2")
        activation2 = tf.sigmoid(tf.matmul(input_, w1) + b1)
        hypothesis = tf.nn.softmax(tf.matmul(activation2, w2) + b2)
        cross_entropy = -tf.reduce_sum(target * tf.log(hypothesis))
        train_step = tf.train.GradientDescentOptimizer(0.1).minimize(cross_entropy)

        correct_prediction = tf.equal(tf.argmax(hypothesis, 1), tf.argmax(target, 1))
        accuracy_op = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

        # Start training
        init = tf.global_variables_initializer()
        with tf.Session() as sess:
            sess.run(init)

            # training phase
            training_phase = AlgoPhase(model=lambda x: sess.run(train_step, feed_dict=x), phase=ml_phase.TRAINING,
                                       default_listeners=global_listeners)

            # testing phase
            testing_phase = AlgoPhase(model=lambda x: accuracy_op.eval(feed_dict=x, session=sess),
                                      phase=ml_phase.TESTING, default_listeners=global_listeners)

            accuracy = {'accuracy': -1}

            e = threading.Event()

            TRAINING_ITERATIONS = 1000

            def start_testing_listener(event):
                if event['type'] == 'after_iteration' and event['phase'] == ml_phase.TESTING:
                    accuracy['accuracy'] = event['model_output']
                    e.set()

            global_listeners += start_testing_listener

            MongoDBEventProvider(self.client.test_db.events, group_id='test_xor_group', default_listeners=global_listeners)()

            e.wait()
            self.assertEqual(training_phase._iteration, TRAINING_ITERATIONS)
            self.assertEqual(testing_phase._iteration, 1)
            self.assertEqual(accuracy['accuracy'], 1)

        tf.reset_default_graph()

    def tearDown(self):
        self.client.drop_database('test_db')
        self.client = None

if __name__ == '__main__':
    unittest.main()
