from io import BytesIO
from error import Disconnect, CommandError, Error

class ProtocolHandler(object):
    # redis protocal: https://redis.io/topics/protocol
    def __init__(self):
        self.handler_map = {
            "+": self.handle_simple_string,
            "-": self.handle_errors,
            ":": self.handle_integers,
            "$": self.handle_bulk_strings,
            "*": self.handle_arrays
        }

    def recv(self, socket_file):
        handler_key = socket_file.read(1)
        if not handler_key:
            raise Disconnect()

        if handler_key not in self.handler_map:
            raise CommandError("Bad request")

        return self.handler_map[handler_key](socket_file)


    def send(self, socket_file, data):
        buf = BytesIO()
        self._write(data, buf)
        # buf.seek(0)
        v = buf.getvalue()
        socket_file.write(v)
        socket_file.flush()

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip()

    def handle_errors(self, socket_file):
        return Error(socket_file.readline().rstrip())

    def handle_integers(self, socket_file):
        return int(socket_file.readline().rstrip())

    # for a single binary safe string
    def handle_bulk_strings(self, socket_file):
        num = int(socket_file.readline().rstrip())
        if num <= 0:
            return None
        bulk_string = socket_file.read(num + 2)
        return bulk_string[:-2]

    def handle_arrays(self, socket_file):
        array_length = int(socket_file.readline().rstrip())
        result = []
        for dummy_i in range(array_length):
            result.append(self.recv(socket_file))
        return result

    def _write(self, data, buf):
        # convert string to bytes first
        if isinstance(data, str):
            data = data.encode("utf-8")

        if isinstance(data, str):
            buf.write("+%s\r\n" % data)
        elif isinstance(data, Error):
            buf.write("-%s\r\n" % (data.message))
        elif isinstance(data, bytes):
            buf.write("$%s\r\n%s\r\n" % len(data), data)
        elif isinstance(data, int):
            buf.write(":%s\r\n" % data)
        elif isinstance(data, list):
            buf.write("*%s\r\n" % len(data))
            for item in data:
                self._write(item, buf)
        elif data is None:
            buf.write("$-1\r\n")
        else:
            raise CommandError("Unrecognized type: %s" % type(data))

