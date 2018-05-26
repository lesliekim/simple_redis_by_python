from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from collections import namedtuple
from io import BytesIO

class Disconnect(Exception): pass
class CommandError(Exception): pass

Error = namedtuple("Error", ("message",))

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

    def handle_request(self, socket_file):
        handler_key = socket_file.read(1)
        if not handler_key:
            raise Disconnect()

        if handler_key not in self.handler_map:
            raise CommandError("Bad request")

        return self.handler_map[handler_key](socket_file)


    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(data, buf)
        # buf.seek(0)
        socket_file.write(buf.getvalue())
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
        bulk_string = socket_file.read(num + 2)
        return bulk_string[:-2]

    def handle_arrays(self, socket_file):
        array_length = int(socket_file.readline().rstrip())
        result = []
        for dummy_i in range(array_length):
            result.append(self.handle_request(socket_file))
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
            for item in range(data):
                self._write(item, buf)
        elif data is None:
            buf.write("$-1\r\n")
        else:
            raise CommandError("Unrecognized type: %s" % type(data))


class Server(object):
    def __init__(self, host="127.0.0.1", port="3000", max_client=64):
        self._pool = Pool(max_client)
        self._server = StreamServer(
            (host, port),
            self.connect_handler,
            self._pool
        )
        self._protocol = ProtocolHandler()
        self._kv = {}
        # Full redis command set is too large: https://redis.io/commands
        # Here just implement CRUD
        # Redis command example: https://redis.io/topics/protocol
        self.command_map = {
            "GET": self._get, #R
            "SET": self._set, #U & C
            "DELETE": self._delete, #D
            "MGET": self._mget, #batch get
            "MSET": self._mset, #batch set
        }

    def connect_handler(self, conn):
        socket_file = conn.makefile('rwb')

        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                # data: a list, data[0] is the command type
                resp = self.get_response(data)
            except CommandError:
                break

            self._protocol.write_response(socket_file, resp)

    def get_response(self, data):
        if type(data) is not list or len(data) < 1:
            raise CommandError("Bad request")

        command = data[0].upper()
        if command not in self.command_map:
            raise CommandError("Unrecognized command")

        return self.command_map[command](*data[1:])

    # request example: *2\r\n $3\r\n GET\r\n $5\r\n mykey\r\n (GET mykey)
    def _get(self, key):
        return self._kv[key] if key in self._kv else None

    def _set(self, key, value):
        self._kv[key] = value
        return ("Set %s = %s successfully" % key, value)

    def _delete(self, key):
        if key in self._kv:
            self._kv.pop(key)
            return ("Delete %s successfully" % key)
        return ("%s not in redis" % key)

    def _mget(self, args):
        return [self._get(key) for key in range(args)]

    # request example: *2\r\n $4\r\n MSET\r\n *2\r\n *2\r\n $4\r\n key1\r\n $6\r\n value1\r\n $4\r\n key2\r\n $6\r\n value2\r\n
    # (MSET [[key1, value1], [key2, value2]])
    def _mset(self, args):
        for item in range(args):
            self._set(item[0], item[1])
        return "Set multi-value successfully"

    def run(self):
        self._server.serve_forever()

