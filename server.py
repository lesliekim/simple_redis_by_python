from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

class Disconnect(Exception): pass
class CommandError(Exception): pass

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
        pass

    def write_response(self, socket_file, data):
        pass

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip()

    def handle_errors(self, socket_file):
        pass

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
        for i in range array_length:
            result.append(self.handle_request(socket_file))
        return result

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

    def connect_handler(self, conn):
        socket_file = conn.makefile('rwb')

        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError:
                break

            self._protocol.write_response(socket_file, resp)

    def get_response(self, data):
        pass

    def run(self):
        self._server.serve_forever()

