from collections import namedtuple
class Disconnect(Exception): pass
class CommandError(Exception): pass

Error = namedtuple("Error", ("message",))