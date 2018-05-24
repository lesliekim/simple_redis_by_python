from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

class Disconnect(Exception): pass
class CommandError(Exception): pass

class ProtocolHandler(object):
    def handle_request(self, socket_file):
        pass

    def write_response(self, socket_file, data):
        pass

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
        
