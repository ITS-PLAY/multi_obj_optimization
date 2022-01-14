import os


class Meta(object):
    def __init__(self, name, default_value=None, type_=str):
        self.name = name
        self.type = type_
        self.default_value = default_value

    def parse_from_environ(self):
        raw = os.getenv(self.name)
        if raw is None:
            return self.default_value
        else:
            return self.type(raw)

    def get(self):
        return self.parse_from_environ()


DATA_PATH = Meta('data.path', 'data').get()
