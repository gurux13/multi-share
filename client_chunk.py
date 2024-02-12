from connection_factory import ConnectionFactory
import socket
from zlib import decompress
class ClientChunk:
    def __init__(self, conn_factory: ConnectionFactory, offset: int, chunk_size: int, compress: bool):
        self.conn_factory = conn_factory
        self.offset = offset
        self.chunk_size = chunk_size
        self.compress = compress
    
    def process(self):
        connection = None
        result = None
        try:
            connection = self.conn_factory.connect(self.offset)
            connection.send(self.chunk_size.to_bytes(8, 'little'))
            size = int.from_bytes(connection.recv(8), 'little')
            result = connection.recv(size, socket.MSG_WAITALL)
            if self.compress:
                result = decompress(result)
        finally:
            if connection is not None:
                connection.close()
        return result
        
