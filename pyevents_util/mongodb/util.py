import numpy as np
import base64
import pickle


def default_encoder(obj):
    """
    Encodes object to json serializable types
    :param obj: object to encode
    :return: encoded object
    """

    if isinstance(obj, np.ndarray):
        data_b64 = base64.b64encode(np.ascontiguousarray(obj).data)
        return dict(__ndarray__=data_b64, dtype=str(obj.dtype), shape=obj.shape)

    if isinstance(obj, dict):
        result = dict()
        for k, v in obj.items():
            result[k] = default_encoder(v)

        return result

    return obj


def default_decoder(obj):
    """
    Decodes a previously encoded json object, taking care of numpy arrays
    :param obj: object to decode
    :return: decoded object
    """

    if isinstance(obj, dict):
        if '__ndarray__' in obj:
            data = base64.b64decode(obj['__ndarray__'])
            return np.frombuffer(data, obj['dtype']).reshape(obj['shape'])
        else:
            for k, v in obj.items():
                obj[k] = default_decoder(v)
    elif isinstance(obj, bytes):
        return pickle.loads(obj)

    return obj
