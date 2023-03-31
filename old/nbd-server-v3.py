"""
nbd server, remote storage, via HTTP
"""
import socket
import struct
import threading

import requests

DISK_SIZE = 1024 * 1024 * 1024 * 1  # 磁盘大小 1G
BLOCK_SIZE = 1024 * 1024 * 4  # 分片大小 4M
STORAGE_PATH = "./storage"
DEBUG = False
STORAGE_NODES = ["10.229.65.12:8080"]
STORAGE_NODE_COUNT = len(STORAGE_NODES)

READ_REQUEST = 0
WRITE_REQUEST = 1
DISCONNECT_REQUEST = 2
NBD_REQUEST_STRUCT = '>LL8sQL'


class BuffSock:
    """Buffered socket wrapper; always returns the amount of data you want."""

    def __init__(self, sock):
        self.sock = sock

    def recv(self, nbytes):
        if DEBUG:
            print("recv(%d) =" % nbytes, end="")
        receive_bytes = b''
        while len(receive_bytes) < nbytes:
            more = self.sock.recv(nbytes - len(receive_bytes))
            if more == b'':
                raise Exception("connection error")
            receive_bytes += more
        if DEBUG:
            print(receive_bytes)
        return receive_bytes

    def send(self, send_bytes):
        if DEBUG:
            print("send(%r) =" % len(send_bytes), end="")
            print(send_bytes)
        self.sock.send(send_bytes)

    def close(self):
        self.sock.close()


class NbdRequest:
    def __init__(self, data):
        self.magic, self.type, self.handle, self.offset, self.len = struct.unpack(NBD_REQUEST_STRUCT, data)
        if self.magic != 0x25609513:
            raise Exception(self.magic)

    def range(self):
        return slice(self.offset, self.offset + self.len)


class NbdServer:

    def __init__(self, conn):
        self.conn = BuffSock(conn)

    def handshake(self):
        self.conn.send(b'NBDMAGIC' + b'IHAVEOPT' + b'\0\0')
        client_flags = self.conn.recv(4)
        client_magic = self.conn.recv(8)
        client_option = self.conn.recv(4)
        client_option_data_length = self.conn.recv(4)
        # print(client_flags, client_magic, client_option, client_option_data_length)
        # TODO: handle option data
        self.conn.send(struct.pack('>Q', DISK_SIZE) + b'\0\x01' + b'\0' * 124)

    def reply(self, error=0, handle=b'', data=b''):
        """Construct an NBD reply."""
        assert type(handle) is type(b'') and len(handle) == 8
        return b'\x67\x44\x66\x98' + struct.pack('>L', error) + handle + data

    def get_request(self):
        header = self.conn.recv(struct.calcsize(NBD_REQUEST_STRUCT))
        return NbdRequest(header)

    def handle_request(self):
        while 1:
            req = self.get_request()
            if DEBUG:
                print("type: %d, length: %d" % (req.type, req.len))
                # print(req.__dict__)
            if req.type == READ_REQUEST:
                self.conn.send(
                    self.reply(
                        error=0, handle=req.handle, data=self.read(req.offset, req.len)
                    )
                )
            elif req.type == WRITE_REQUEST:
                write_data = self.conn.recv(req.len)
                assert len(write_data) == req.len
                self.write(req.offset, write_data)
                self.conn.send(self.reply(error=0, handle=req.handle))
            elif req.type == DISCONNECT_REQUEST:
                self.conn.close()
                return
            else:
                print("Unknown req type:", req.type)
                self.conn.close()
                return

    def read(self, offset, length):
        # print("read offset=%d length=%d(%.1fKB)" % (offset, length, length / 1024))
        block_number = offset // BLOCK_SIZE  # 块的序号
        node = STORAGE_NODES[block_number % STORAGE_NODE_COUNT]
        if offset % BLOCK_SIZE + length > BLOCK_SIZE:
            # print("读取内容跨块：", block_number, offset % BLOCK_SIZE, length)
            req = requests.get("http://{server}/{block}/{offset}/{length}".format(server=node, block=block_number, offset=offset % BLOCK_SIZE,
                                                                                  length=BLOCK_SIZE - offset % BLOCK_SIZE))
            content = req.content
            block_number += 1
            node = STORAGE_NODES[block_number % STORAGE_NODE_COUNT]
            req = requests.get("http://{server}/{block}/{offset}/{length}".format(server=node, block=block_number, offset=0,
                                                                                  length=length - (BLOCK_SIZE - offset % BLOCK_SIZE)))
            content += req.content
        else:
            req = requests.get(
                "http://{server}/{block}/{offset}/{length}".format(server=node, block=block_number, offset=offset % BLOCK_SIZE, length=length))
            content = req.content
        return content

    def write(self, offset, data):
        block_number = offset // BLOCK_SIZE  # 块的序号
        node = STORAGE_NODES[block_number % STORAGE_NODE_COUNT]
        if offset % BLOCK_SIZE + len(data) > BLOCK_SIZE:
            # print("写入内容跨块：", block_number, offset % BLOCK_SIZE, len(data))
            tmp_data = data[slice(0, BLOCK_SIZE - offset % BLOCK_SIZE)]
            req = requests.post("http://{server}/{block}/{offset}".format(server=node, block=block_number, offset=offset % BLOCK_SIZE), data=tmp_data)
            block_number += 1
            node = STORAGE_NODES[block_number % STORAGE_NODE_COUNT]
            tmp_data = data[slice(BLOCK_SIZE - offset % BLOCK_SIZE, len(data))]
            req = requests.post("http://{server}/{block}/{offset}".format(server=node, block=block_number, offset=0), data=tmp_data)
        else:
            req = requests.post("http://{server}/{block}/{offset}".format(server=node, block=block_number, offset=offset % BLOCK_SIZE), data=data)


def serve_client(conn, addr):
    """Serves a single client until it exits."""
    print("Client connected:", addr)
    nbd_server = NbdServer(conn)
    nbd_server.handshake()
    nbd_server.handle_request()


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.settimeout(None)  # block
sock.bind(('', 10809))  # modprobe nbd; nbd-client -no-optgo 127.0.0.1 10809 /dev/nbd0 不加-no-optgo会出现 invalid negotiation magic 报错
sock.listen(5)
while 1:
    connection, address = sock.accept()
    t = threading.Thread(target=serve_client, args=(connection, address))
    t.start()
