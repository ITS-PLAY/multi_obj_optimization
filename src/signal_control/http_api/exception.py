class ServerError(Exception):
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg


class BadRequestError(Exception):
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg
