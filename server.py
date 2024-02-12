from connection_factory import ConnectionFactory
import pathlib
from zlib import compress
import socket
class Server:
    def __init__(self, connection_factory: ConnectionFactory, file_path: str, compress: bool):
        self.connection_factory = connection_factory
        self.file_path = file_path
        self.compress = compress
        
    def _on_connection(self, conn, addr, offset, size):
        try:
            conn.settimeout(5)
            if offset == -1:
                conn.send(pathlib.Path(self.file_path).stat().st_size.to_bytes(8, 'little'))
                conn.close()
                return
            with open(self.file_path, 'rb') as file:
                file.seek(offset)
                data = file.read(size)
                if self.compress:
                    data = compress(data, 6)
            conn.sendall(len(data).to_bytes(8, 'little'))
            conn.sendall(data)
            conn.recv(1)
        except Exception as e:
            print(f"Error processing connection from {addr}: {e}")
        finally:
            conn.close()
        
    def run(self):
        self.connection_factory.bind()
        print("Accepting connections...")
        self.connection_factory.loop(self._on_connection)