import unittest

import numpy as np
import tensorflow as tf
from pyeventsml.ml_phase import *

from pyeventsml.algo_phase import *

import threading


class TestXor(unittest.TestCase):
    """
    XOR Test
    """

    def test_xor(self):
        # logging.basicConfig(level=logging.DEBUG)

        global_listeners = AsyncListeners()

        # network definition
        nb_classes = 2
        input_ = tf.placeholder(tf.float32,
                                shape=[None, 2],
                                name="input")
        target = tf.placeholder(tf.float32,
                                shape=[None, nb_classes],
                                name="output")
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
        init = tf.initialize_all_variables()
        with tf.Session() as sess:
            sess.run(init)

            @after
            def xor_data_provider(phase):
                return {'data': {input_: np.array([[0, 0], [0, 1], [1, 0], [1, 1]]),
                                 target: np.array([[0, 1], [1, 0], [1, 0], [0, 1]])},
                        'phase': phase,
                        'type': 'data'}

            xor_data_provider += global_listeners
            # training phase
            training_phase = AlgoPhase(model=lambda x: sess.run(train_step, feed_dict=x), phase=MLPhase.TRAINING,
                                       default_listeners=global_listeners)

            # testing phase
            testing_phase = AlgoPhase(model=lambda x: accuracy_op.eval(feed_dict=x, session=sess),
                                      phase=MLPhase.TESTING, default_listeners=global_listeners)

            accuracy = {'accuracy': -1}

            e = threading.Event()

            TRAINING_ITERATIONS = 1000

            def start_testing_listener(event):
                if event['type'] == 'after_iteration' and event['phase'] == MLPhase.TESTING:
                    accuracy['accuracy'] = event['model_output']
                    e.set()

            global_listeners += start_testing_listener

            for i in range(TRAINING_ITERATIONS):
                xor_data_provider(MLPhase.TRAINING)

            xor_data_provider(MLPhase.TESTING)

            e.wait()
            self.assertEqual(training_phase._iteration, TRAINING_ITERATIONS)
            self.assertEqual(testing_phase._iteration, 1)
            self.assertEqual(accuracy['accuracy'], 1)


if __name__ == '__main__':
    unittest.main()
