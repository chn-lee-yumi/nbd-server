"""
nbd server, local storage
"""
import os
import socket
import struct
import threading

# nbd protocol document: https://sourceforge.net/p/nbd/code/ci/master/tree/doc/proto.md
"""
nbd-client -no-optgo 10.214.4.215 10809 /dev/nbd0
nbd-client -no-optgo 10.214.4.216 10809 /dev/nbd1
nbd-client -no-optgo 10.214.4.217 10809 /dev/nbd2
nbd-client -no-optgo 10.214.4.218 10809 /dev/nbd3
"""

DISK_SIZE = 1024 * 1024 * 1024 * 4  # 磁盘大小 4G
BLOCK_SIZE = 1024 * 1024 * 4  # 分片大小 4M
STORAGE_PATH = "./storage"
DEBUG = False

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
        print("read offset=%d length=%d(%.1fKB)" % (offset, length, length / 1024))
        data = bytearray(length)
        block_number = offset // BLOCK_SIZE  # 第一个块的序号
        block_count = length // BLOCK_SIZE + 1  # 需要连续读取几个块
        for count in range(block_count):
            block_path = "%s/%d" % (STORAGE_PATH, block_number + count)
            if os.path.exists(block_path):
                with open(block_path, 'rb') as f:
                    if count == 0:  # 第一个块，需要先偏移
                        # 先偏移
                        this_offset = offset % BLOCK_SIZE
                        f.seek(this_offset)
                        # 判断是只要读这个块还是需要读多个块
                        if length <= BLOCK_SIZE - this_offset:  # 只需要读这一个块
                            data = f.read(length)
                            return data
                        else:  # 还需要读别的块，这个块就直接全部读取
                            this_length = BLOCK_SIZE - this_offset
                            data[slice(0, this_length)] = f.read(this_length)
                            if f.read():
                                raise Exception("块后面还有内容，这不正常！")
                    else:  # 读非第一个块，无需偏移
                        # 判断是只要读这个块还是需要读多个块
                        this_offset = offset % BLOCK_SIZE + BLOCK_SIZE * (count - 1)
                        if length - this_offset <= BLOCK_SIZE:  # 只要读这一个块
                            this_length = length - this_offset
                            data[slice(this_offset, this_offset + this_length)] = f.read(this_length)
                        else:
                            data[slice(this_offset, this_offset + BLOCK_SIZE)] = f.read(BLOCK_SIZE)
        return data

    def write(self, offset, data):
        length = len(data)
        print("write offset=%d length=%d(%.1fKB)" % (offset, length, length / 1024))
        block_number = offset // BLOCK_SIZE  # 第一个块的序号
        block_count = length // BLOCK_SIZE + 1  # 需要连续写几个块
        for count in range(block_count):
            block_path = "%s/%d" % (STORAGE_PATH, block_number + count)
            if os.path.exists(block_path):
                print("打开文件")
                with open(block_path, 'r+b') as f:  # 必须用r+，而不是w，w会清空文件，r+是覆写文件
                    if count == 0:  # 第一个块，需要先偏移
                        # 先偏移
                        this_offset = offset % BLOCK_SIZE
                        f.seek(this_offset)
                        # 判断是只要写这个块还是需要读多个块
                        if length <= BLOCK_SIZE - this_offset:  # 只需要写这一个块
                            print("只写这一个块")
                            f.write(data)
                            return
                        else:  # 还需要写别的块，这个块就直接写到末尾
                            this_length = BLOCK_SIZE - this_offset
                            f.write(data[slice(0, this_length)])
                    else:  # 读非第一个块，无需偏移
                        # 判断是只要读这个块还是需要读多个块
                        this_offset = offset % BLOCK_SIZE + BLOCK_SIZE * (count - 1)
                        if length - this_offset <= BLOCK_SIZE:  # 只要读这一个块
                            this_length = length - this_offset
                            f.write(data[slice(this_offset, this_offset + this_length)])
                        else:
                            f.write(data[slice(this_offset, this_offset + BLOCK_SIZE)])
            else:
                # 创建文件
                print("创建文件")
                file_data = bytearray(BLOCK_SIZE)
                if count == 0:  # 第一个块，需要先偏移
                    # 先偏移
                    this_offset = offset % BLOCK_SIZE
                    # 判断是只要写这个块还是需要读多个块
                    if length <= BLOCK_SIZE - this_offset:  # 只需要写这一个块
                        file_data[slice(this_offset, this_offset + length)] = data
                        print(len(file_data))
                        print("第一个块，只要写这个块")
                    else:  # 还需要写别的块，这个块就直接写到末尾
                        this_length = BLOCK_SIZE - this_offset
                        file_data[slice(this_offset, this_offset + this_length)] = data[slice(0, this_length)]
                        print("第一个块，还需要写别的块")
                else:  # 非第一个块，无需偏移
                    # 判断是只要写这个块还是需要写多个块
                    this_offset = offset % BLOCK_SIZE + BLOCK_SIZE * (count - 1)
                    if length - this_offset <= BLOCK_SIZE:  # 只要写这一个块
                        print("非第一个块，只要写这个块")
                        this_length = length - this_offset
                        file_data[slice(0, length)] = data[slice(this_offset, this_offset + this_length)]
                    else:
                        print("非第一个块，还需要写别的块")
                        file_data = data[slice(this_offset, this_offset + BLOCK_SIZE)]
                with open(block_path, 'wb') as f:
                    count = f.write(file_data)
                    print("写入完毕，共写入%d字节" % count)


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
