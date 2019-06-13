import ast
import base64
import json
import logging
import os
import random
import string


def get_env_vars(prefix):
    return {k.replace(prefix, ''): v for (k, v) in os.environ.items() if prefix in k}


def get_var(var, env_vars, args, default=None):
    val = env_vars.pop(var, None)
    if val is None:
        return args.pop(var, default)
    return val


def id_generator(size=8, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def get_base64_decoded_str(encoded_str):
    decoded_str = base64.b64decode(bytes(encoded_str, 'utf-8')).decode('utf-8')
    logging.debug('Base64 decoded str: ' + decoded_str)
    return decoded_str


def get_base64_encoded_str(s):
    encoded_s = base64.b64encode(bytes(s, 'utf-8')).decode('utf-8')
    logging.debug('Base64 encoded str: ' + encoded_s)
    return encoded_s


def get_config_dict_decoded(config_str):
    if os.path.isfile(config_str):
        logging.info('Using config path')
        with open(config_str, 'r') as f:
            return json.load(f)

    decoded_str = get_base64_decoded_str(config_str)
    config_eval = ast.literal_eval(decoded_str)
    if isinstance(config_eval, dict):
        logging.info('Using config content')
        return config_eval
    else:
        logging.error('Unable to load config from content')
        raise CurwDockerRainfallException('Unknown config: ' + config_str)


# Deprecated method
def get_config_dict(config_str):
    try:
        wrf_config_eval = ast.literal_eval(config_str)
        if isinstance(wrf_config_eval, dict):
            logging.info('Using config content')
            wrf_config_dict = wrf_config_eval
        else:
            logging.error('Unable to load config from content')
            raise Exception
    except (SyntaxError, ValueError):
        if os.path.isfile(config_str):
            logging.info('Using config path')
            with open(config_str, 'r') as f:
                wrf_config_dict = json.load(f)
        else:
            logging.error('Unable to load config from path')
            raise Exception
    except Exception:
        raise CurwDockerRainfallException('Unknown config: ' + config_str)

    return wrf_config_dict


class CurwDockerRainfallException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)