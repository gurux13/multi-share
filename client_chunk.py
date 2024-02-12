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
        data = None
        try:
            connection = self.conn_factory.connect(self.offset)
            connection.send(self.chunk_size.to_bytes(8, 'little'))
            size = int.from_bytes(connection.recv(8), 'little')
            data = bytearray()
            while len(data) < size:
                data.extend(connection.recv(size - len(data)))
            connection.send(b'\x00')
            connection.shutdown(socket.SHUT_WR)
            if self.compress:
                data = decompress(data)
            
        finally:
            if connection is not None:
                connection.close()
        return data
        
