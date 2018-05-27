from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from error import Disconnect, CommandError, Error
from protocol import ProtocolHandler
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Server(object):
    def __init__(self, host="127.0.0.1", port=4567, max_client=64):
        self._pool = Pool(max_client)
        self._server = StreamServer(
            (host, port),
            self.connect_handler,
            spawn=self._pool
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

    def connect_handler(self, conn, address):
        socket_file = conn.makefile('rwb')

        while True:
            try:
                data = self._protocol.recv(socket_file)
                logger.debug("Server recieve data: %s" % data)
            except Disconnect:
                break

            try:
                # data: a list, data[0] is the command type
                resp = self.get_response(data)
            except CommandError as exc:
                logger.error("CommandError: %s" % exc.args[0])
                resp = Error(exc.args[0])

            logger.info("Server send response: %s" % resp)
            self._protocol.send(socket_file, resp)

    def get_response(self, data):
        if type(data) is not list or len(data) < 1:
            raise CommandError("Bad request")

        command = data[0].upper()
        logger.info("Command: %s, Data as follow:" % command)
        logger.info(data[1:])
        if command not in self.command_map:
            raise CommandError("Unrecognized command")

        return self.command_map[command](*data[1:])

    # request example: *2\r\n $3\r\n GET\r\n $5\r\n mykey\r\n (GET mykey)
    def _get(self, key):
        return self._kv[key] if key in self._kv else None

    def _set(self, key, value):
        self._kv[key] = value
        return ("Set %s = %s successfully" % (key, value))

    def _delete(self, key):
        if key in self._kv:
            self._kv.pop(key)
            return ("Delete %s successfully" % key)
        return ("%s not in redis" % key)

    def _mget(self, args):
        return [self._get(key) for key in args]

    # request example: *2\r\n $4\r\n MSET\r\n *2\r\n *2\r\n $4\r\n key1\r\n $6\r\n value1\r\n $4\r\n key2\r\n $6\r\n value2\r\n
    # (MSET [[key1, value1], [key2, value2]])
    def _mset(self, args):
        for key, val in args:
            self._set(key, val)
        return "Set multi-value successfully"

    def run(self):
        self._server.serve_forever()

if __name__ == "__main__":
    s = Server()
    s.run()