import base64


def read_file(path):
    with open(path, 'r') as f:
        return f.read()


def get_base64_encoded_str(s):
    return base64.b64encode(s.encode()).decode()


def sanitize_name(name):
    return name.replace('_', '--').replace(':', '-').lower()
