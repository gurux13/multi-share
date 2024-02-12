from concurrent.futures import ThreadPoolExecutor, as_completed
from connection_factory import ConnectionFactory
from client_chunk import ClientChunk
from dataclasses import dataclass
import json
import pathlib

class Client:
    @dataclass
    class BitmapData:
        size: int
        chunk_size: int
        threads: int
        processed: dict[int, int]

    def __init__(self, connection_factory, chunk_size: int, write_to: str, overwrite: bool, bitmap_path: str, threads: int, compress: bool):
        self.connection_factory = connection_factory
        self.write_to = write_to
        self.threads = threads
        self.chunk_size = chunk_size
        self.bitmap_path = bitmap_path
        self.overwrite = overwrite
        self.compress = compress
     
    def _thread_proc(self, offset, size):
        try:
            chunk = ClientChunk(self.connection_factory, offset, size, self.compress)
            data = chunk.process()
            return offset, size, data
        except Exception as e:
            return offset, size, None
        
    def run(self, progress_callback):
        if pathlib.Path(self.write_to).exists() and not self.overwrite:
            print(f"File {self.write_to} already exists, not overwriting")
            return
        info_connection = self.connection_factory.connect(-1)
        total_size = int.from_bytes(info_connection.recv(8), 'little')
        progress_callback(0, total_size)
        info_connection.close()
        status_from_bitmap = None
        
        if pathlib.Path(self.bitmap_path).exists():
            with open(self.bitmap_path, 'rb') as bitmap:
                status_from_bitmap = json.loads(bitmap.read())
        elif pathlib.Path(self.bitmap_path + "~").exists():
            with open(self.bitmap_path + "~", 'rb') as bitmap:
                status_from_bitmap = json.loads(bitmap.read())
        if status_from_bitmap is not None:
            if status_from_bitmap['size'] != total_size or status_from_bitmap['chunk_size'] != self.chunk_size or status_from_bitmap['threads'] != self.threads:
                print(f"Bitmap does not match invocation, discarding bitmap")
                status_from_bitmap = None
        if status_from_bitmap is None:    
            status_from_bitmap = self.BitmapData(total_size, self.chunk_size, self.threads, {}).__dict__
        futures = []
        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            with open(self.write_to, 'w+b') as file:
                for offset in range(0, total_size, self.chunk_size):
                    if str(offset) in status_from_bitmap['processed']:
                        progress_callback(status_from_bitmap['processed'][str(offset)], total_size)
                        continue
                    size = min(total_size - offset, self.chunk_size)
                    futures.append(executor.submit(self._thread_proc, offset, size))
                for future in as_completed(futures):

                    offset, size, data = future.result()
                    if data is None:
                        print(f"Failed to download chunk at offset {offset}, retrying")
                        futures.append(executor.submit(self._thread_proc, offset, size))
                        continue
                    file.seek(offset)
                    file.write(data)
                    progress_callback(len(data), total_size)
                    status_from_bitmap['processed'][offset] = len(data)
                    with open(self.bitmap_path + "~", 'wb') as bitmap:
                        bitmap.write(json.dumps(status_from_bitmap).encode())
                    pathlib.Path(self.bitmap_path + "~").replace(self.bitmap_path)
        