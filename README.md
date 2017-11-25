# Utilities for PyEvents

Some utilities for the [pyevents](https://github.com/ivan-vasilev/pyevents)
 library. Currently you can:
 
* Store events sequences in MongoDB. You can also *load* previously stored events from MongoDB and fire them again in the order, in which they were saved. This allows you to exactly replicate a scenario played in the past. For more information check on how to use this check *tests/test_mongodb.py* .
* Train and test **machine learning algorithms**. In *tests/test_xor* you can find an example how to use this by training a simple neural network to solve the XOR task using [TensorFlow](https://github.com/tensorflow/tensorflow).


#### Author
Ivan Vasilev (ivanvasilev [at] gmail (dot) com)

#### License
[MIT License](http://opensource.org/licenses/MIT)