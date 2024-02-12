import socket

class ConnectionFactory:
    def __init__(self, port: int, host: str = "0.0.0.0"):
        self.host = host
        self.port = port
        self.server_socket = None
    
    def connect(self, offset: int):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        if offset == -1:
            offset = 0xFFFFFFFFFFFFFFFF
        s.send(offset.to_bytes(8, "little"))
        return s
    
    def bind(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
    
    def loop(self, callback):
        while True:
            conn, addr = self.server_socket.accept()
            offset = int.from_bytes(conn.recv(8), "little")
            size = 0
            if offset == 0xFFFFFFFFFFFFFFFF:
                offset = -1
            else:
                size = int.from_bytes(conn.recv(8), "little")
            callback(conn, addr, offset, size)