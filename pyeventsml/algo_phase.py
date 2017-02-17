from pyevents.events import *
import logging


class AlgoPhase(object):
    """Simple training/testing/evaluation class"""

    def __init__(self, model, phase=None, default_listeners=None, event_processor=None):
        self._phase = phase
        self._model = model
        self._iteration = 0

        if event_processor is not None:
            self.input_event_processor = event_processor
        else:
            self.input_event_processor = self.onevent

        if default_listeners is not None:
            self.before_iteration += default_listeners
            self.after_iteration += default_listeners
            default_listeners += self.onevent

    def process(self, data):
        self._iteration += 1

        self.before_iteration(data)

        logging.getLogger(__name__).debug("Phase " + str(self._phase) + " iteration " + str(self._iteration))

        model_output = self._model(data)

        self.after_iteration(data, model_output)

    def onevent(self, event):
        if event['type'] == 'data' and event['phase'] == self._phase:
            self.process(event['data'])

    @after
    def before_iteration(self, input_data):
        return {'model': self._model, 'phase': self._phase, 'iteration': self._iteration, 'model_input': input_data, 'type': 'before_iteration'}

    @after
    def after_iteration(self, input_data, model_output):
        return {'model': self._model, 'phase': self._phase, 'iteration': self._iteration, 'model_input': input_data, 'model_output': model_output, 'type': 'after_iteration'}
