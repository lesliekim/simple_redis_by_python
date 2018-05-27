import socket
from io import BytesIO
from protocol import ProtocolHandler
from error import Error, CommandError
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Client(object):
    def __init__(self, host="127.0.0.1", port=4567):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._socket_file = self._socket.makefile("rwb")

    def excute(self, data):
        logger.info("Client send data: %s" % data)
        self._protocol.send(self._socket_file, data)
        resp = self._protocol.recv(self._socket_file)
        if isinstance(resp, Error):
            logger.error("Client recieve error: %s" % resp.message)
            raise CommandError(resp.message)

        logger.info("Client recieve response: ")
        logger.info(resp)
        return resp

    def get(self, key):
        return self.excute(["GET", key])

    def update(self, key, value):
        return self.excute(["SET", key, value])

    def delete(self, key):
        return self.excute(["DELETE", key])

    def mset(self, args):
        args.insert(0, "MSET")
        return self.excute(args)

    def mget(self, args):
        args.insert(0, "MGET")
        return self.excute(args)