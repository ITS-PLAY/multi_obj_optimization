import uuid


def gen_uuid():
    return str(uuid.uuid4()).replace('-', '')
