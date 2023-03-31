"""
nbd server, remote storage, s3
"""
# TODO: 统计读写字节数和放大倍数
import io
import socket
import struct
import threading
import time
from typing import Union, Optional

import boto3
import botocore

# hfc20-liyumintest
# access_key = "RJ1M4VV3HP3P90MNC60G"
# secret_key = "9HolxLxKID7hw8opFq9WOcoKVjdl3VxOo7zJRaMz"
# bucket_name = "hfc20-liyumintest"
access_key = "07FN23P1QCOCQQQJH167"
secret_key = "p84d9J2B9orCRsSGgFoITxnUl4hLJT4FUw5Cb3zN"
bucket_name = "testbucket"
s3 = boto3.client(
    service_name="s3",
    region_name=None,
    use_ssl=False,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url="http://10.0.1.1:9001/"
)

TOTAL_READ_BYTES_CLIENT = 0
TOTAL_WRITE_BYTES_CLIENT = 0
TOTAL_READ_BYTES_S3 = 0
TOTAL_WRITE_BYTES_S3 = 0

DISK_SIZE = 1024 * 1024 * 1024 * 8  # 磁盘大小 8G
BLOCK_SIZE = 256 * 1024  # 分片大小 256KB。分片大小太小会影响持续读取，太大会影响随机写入。最小不能小于128KB。推荐在1MB/512KB/256KB中选取。
STORAGE_PATH = "./storage"  # 没用……后续可以考虑磁盘缓存
USE_MEM_BUFFER = True  # 使用memory性能会快2%左右……如不使用，则产生一个临时文件作为读写，当ENABLE_CACHE时此项无效
ENABLE_CACHE = True  # 是否启用内存缓存
CACHE_SIZE = 1024 * 4  # 1G 内存缓存的块数量
COMPRESS = False  # TODO: 压缩功能
# TODO: 上面测参数如何影响性能，待测试
# TODO：试试 内存-磁盘-S3 三层架构
"""
BLOCK_SIZE越大，缓存外随机写性能越差
如果ENABLE_CACHE，则BLOCK_SIZE大一点，缓存内性能会更好
256KB约8k的随机写，18k随机读。512KB约16k的随机写，20k随机读。1MB的约26k随机写，26k随机读。
"""

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


class BufferBlock(io.RawIOBase):
    def __init__(self, size):
        self.size = size
        self.data = bytearray(size)  # 数据
        self.cursor = 0  # 光标
        self.debug = False

    def seek(self, __offset: int, __whence: int = 0) -> int:
        if self.debug:
            print("seek", __offset, __whence)
        if __whence == 0:
            self.cursor = __offset
        elif __whence == 1:
            self.cursor += __offset
        elif __whence == 2:
            self.cursor = self.size + __offset
        if self.cursor < 0:
            self.cursor = 0
        elif self.cursor > self.size:
            self.cursor = self.size
        return self.cursor

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.cursor

    def writable(self) -> bool:
        return True

    def write(self, __b: Union[bytes, bytearray]) -> Optional[int]:
        if self.debug:
            print("write", len(__b))
        self.data[slice(self.cursor, self.cursor + len(__b))] = __b
        return len(__b)

    def readable(self) -> bool:
        return True

    def read(self, __size: int = ...) -> Optional[bytes]:
        if self.debug:
            print("read", __size)
        if self.cursor + __size >= self.size:
            data = self.data[slice(self.cursor, self.size)]
            self.cursor = self.size
        else:
            data = self.data[slice(self.cursor, self.cursor + __size)]
            self.cursor += __size
        return data

    def readall(self) -> bytes:
        if self.debug:
            print("readall")
        data = self.data[slice(self.cursor, self.size)]
        self.cursor = self.size
        return data


class LruCache:
    def __init__(self, size):
        self.size = size
        self.lru_list = []
        self.dirty_blocks = set()
        self.data = {}
        self.write_lock = threading.Lock()
        self.last_write_time = 0
        threading.Thread(target=self.auto_writeback).start()

    def auto_writeback(self):
        while True:
            if time.time() - self.last_write_time > 2 and self.dirty_blocks:
                # print("自动回写", self.dirty_blocks)
                self.write_lock.acquire()
                for key in self.lru_list[::-1]:
                    if key in self.dirty_blocks:
                        self.flush(key, clean_cache=False)
                        break
                self.write_lock.release()
                if not self.dirty_blocks:
                    print("所有脏数据已回写完毕")
            else:
                time.sleep(1)

    def has_block(self, block):
        if block in self.data:
            return True
        return False

    def flush(self, key=None, clean_cache=False):
        """
        回写脏数据
        """
        # print("触发回写", key)
        global TOTAL_WRITE_BYTES_S3
        if key is not None:
            if key in self.dirty_blocks:  # 如果key不在dirty_blocks，说明已经被自动回写了
                self.data[key].seek(0)
                s3.upload_fileobj(self.data[key], bucket_name, str(key))
                TOTAL_WRITE_BYTES_S3 += BLOCK_SIZE
                self.dirty_blocks.remove(key)
            if clean_cache:
                self.lru_list.remove(key)
                del self.data[key]
        else:
            for key in self.dirty_blocks.copy():
                self.data[key].seek(0)
                s3.upload_fileobj(self.data[key], bucket_name, str(key))
                TOTAL_WRITE_BYTES_S3 += BLOCK_SIZE
                self.dirty_blocks.remove(key)
                if clean_cache:
                    self.lru_list.remove(key)
                    del self.data[key]

    def write(self, block, offset, data):
        global TOTAL_READ_BYTES_S3
        self.last_write_time = time.time()
        self.write_lock.acquire()
        self.dirty_blocks.add(block)
        if self.has_block(block):
            # print("修改", block, self.lru_list, self.data.keys())
            self.data[block].data[offset:offset + len(data)] = data
            self.lru_list.remove(block)
            self.lru_list.insert(0, block)
        else:
            f = BufferBlock(BLOCK_SIZE)
            if offset != 0 or len(data) != BLOCK_SIZE:  # 如果不是覆写，则需要下载下来再修改
                try:
                    s3.download_fileobj(bucket_name, str(block), f)
                    TOTAL_READ_BYTES_S3 += BLOCK_SIZE
                    # res = s3.get_object(Bucket=bucket_name, Key=str(block), Range="bytes=%d-%d" % (0, offset - 1))  # 不能这样写，如果data很小，后面的部分就没了
                    # TOTAL_READ_BYTES_S3 += offset
                    # f.write(res['Body'].read())
                except botocore.exceptions.ClientError as e:
                    pass
            f.seek(offset)
            f.write(data)
            f.seek(0)
            self.data[block] = f
            # print("写入", block, self.lru_list, self.data.keys())
            self.lru_list.insert(0, block)
            if len(self.lru_list) > self.size:
                self.flush(self.lru_list[-1], clean_cache=True)
        self.write_lock.release()

    def read(self, block, offset, length):
        return self.data[block].data[slice(offset, offset + length)]


if ENABLE_CACHE:
    LRU_Cache = LruCache(CACHE_SIZE)


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
        global TOTAL_READ_BYTES_CLIENT
        TOTAL_READ_BYTES_CLIENT += length
        # print("read offset=%d length=%d(%.1fKB)" % (offset, length, length / 1024))
        block_number = offset // BLOCK_SIZE  # 块的序号
        if offset % BLOCK_SIZE + length > BLOCK_SIZE:
            if DEBUG:
                print("读取内容跨块：", block_number, offset % BLOCK_SIZE, length)
            content = remote_read(block=block_number, offset=offset % BLOCK_SIZE, length=BLOCK_SIZE - offset % BLOCK_SIZE)
            block_number += 1
            content += remote_read(block=block_number, offset=0, length=length - (BLOCK_SIZE - offset % BLOCK_SIZE))
        else:
            content = remote_read(block=block_number, offset=offset % BLOCK_SIZE, length=length)
        # if len(content) != length:
        #     print("ERROR! 长度不一样:", block_number, offset, length, len(content))
        return content

    def write(self, offset, data):
        global TOTAL_WRITE_BYTES_CLIENT
        TOTAL_WRITE_BYTES_CLIENT += len(data)
        block_number = offset // BLOCK_SIZE  # 块的序号
        if offset % BLOCK_SIZE + len(data) > BLOCK_SIZE:
            if DEBUG:
                print("写入内容跨块：", block_number, offset % BLOCK_SIZE, len(data))
            tmp_data = data[slice(0, BLOCK_SIZE - offset % BLOCK_SIZE)]
            remote_write(block=block_number, offset=offset % BLOCK_SIZE, data=tmp_data)
            block_number += 1
            tmp_data = data[slice(BLOCK_SIZE - offset % BLOCK_SIZE, len(data))]
            remote_write(block=block_number, offset=0, data=tmp_data)
        else:
            remote_write(block=block_number, offset=offset % BLOCK_SIZE, data=data)


def remote_read(block=0, offset=0, length=0):
    # 远程读取，可以直接采用range请求大大减少损耗
    global ENABLE_CACHE, TOTAL_READ_BYTES_S3, LRU_Cache
    if ENABLE_CACHE and LRU_Cache.has_block(block):
        return LRU_Cache.read(block, offset, length)
    # TODO: 先判断块在不在s3
    try:
        res = s3.get_object(Bucket=bucket_name, Key=str(block), Range="bytes=%d-%d" % (offset, offset + length - 1))
        TOTAL_READ_BYTES_S3 += length
        data = res['Body'].read()
    except Exception as e:
        data = bytearray(length)
    # if USE_MEM_BUFFER:
    #     f = BufferBlock(BLOCK_SIZE)
    #     try:
    #         s3.download_fileobj(bucket_name, str(block), f)
    #     except botocore.exceptions.ClientError as e:
    #         pass
    #     f.seek(offset)
    #     data = f.read(length)
    # else:
    #     try:
    #         s3.download_file(bucket_name, str(block), "./tmp.file")
    #         TOTAL_READ_BYTES_S3 += BLOCK_SIZE
    #     except botocore.exceptions.ClientError as e:
    #         return bytearray(length)
    #     else:
    #         with open("./tmp.file", "rb") as f:
    #             f.seek(offset)
    #             data = f.read(length)
    return data


def remote_write(block=0, offset=0, data=b''):
    # 远程写入，需下载整个块，修改对应地方后再上传
    global ENABLE_CACHE, TOTAL_READ_BYTES_S3, TOTAL_WRITE_BYTES_S3, LRU_Cache
    if ENABLE_CACHE:
        LRU_Cache.write(block, offset, data)
        return
    if USE_MEM_BUFFER:
        f = BufferBlock(BLOCK_SIZE)
        if offset != 0 or len(data) != BLOCK_SIZE:  # 部分覆写，需要先下载，再修改
            try:
                s3.download_fileobj(bucket_name, str(block), f)
                TOTAL_READ_BYTES_S3 += BLOCK_SIZE
                # res = s3.get_object(Bucket=bucket_name, Key=str(block), Range="bytes=%d-%d" % (0, offset - 1))  # 不能这样写，如果data很小，后面的部分就没了
                # TOTAL_READ_BYTES_S3 += offset
                # f.write(res['Body'].read())
            except botocore.exceptions.ClientError as e:
                pass
        f.seek(offset)
        f.write(data)
        f.seek(0)
        s3.upload_fileobj(f, bucket_name, str(block))
        TOTAL_WRITE_BYTES_S3 += BLOCK_SIZE
    else:
        if offset != 0 or len(data) != BLOCK_SIZE:  # 部分覆写，需要先下载，再修改
            try:
                s3.download_file(bucket_name, str(block), "./tmp.file")
                TOTAL_READ_BYTES_S3 += BLOCK_SIZE
                with open("../../tmp.file", "r+b") as f:
                    f.seek(offset)
                    f.write(data)
            except botocore.exceptions.ClientError as e:
                file_data = bytearray(BLOCK_SIZE)
                file_data[slice(offset, offset + len(data))] = data
                with open("../../tmp.file", "wb") as f:
                    f.write(file_data)
        else:
            with open("../../tmp.file", "wb") as f:
                f.write(data)
        s3.upload_file("./tmp.file", bucket_name, str(block))
        TOTAL_WRITE_BYTES_S3 += BLOCK_SIZE
    return


# print(remote_read(1, 0, 10))
# remote_write(1, 2, b'12345')
# print(remote_read(1, 0, 10))
# remote_write(2, 10, b'12345')
# remote_write(3, 2, b'12345')
# remote_write(5, 2, b'12345')
# remote_write(9, 2, b'12345')
# print(remote_read(2, 0, 20))
# time.sleep(10)
# exit()


# t = time.time()
# for i in range(100):
#     remote_write(0, 999, b"12345lashfjsagfuiwqoefgbc")
#     remote_read(0, 1000, 15)
# print(time.time() - t)
# exit()

def print_stat():
    # TODO: 这个数据好像不大对，增加读写缓存命中率
    global TOTAL_READ_BYTES_CLIENT, TOTAL_WRITE_BYTES_CLIENT, TOTAL_READ_BYTES_S3, TOTAL_WRITE_BYTES_S3
    while True:
        time.sleep(2)
        print()
        print("客户端读写(MB/s):\t%.2f\t%.2f" % (TOTAL_READ_BYTES_CLIENT / 1024 / 1024 / 10, TOTAL_WRITE_BYTES_CLIENT / 1024 / 1024 / 10))
        print("S3读写(MB/s):   \t%.2f\t%.2f" % (TOTAL_READ_BYTES_S3 / 1024 / 1024 / 10, TOTAL_WRITE_BYTES_S3 / 1024 / 1024 / 10))
        TOTAL_READ_BYTES_CLIENT = 0
        TOTAL_WRITE_BYTES_CLIENT = 0
        TOTAL_READ_BYTES_S3 = 0
        TOTAL_WRITE_BYTES_S3 = 0


def serve_client(conn, addr):
    """Serves a single client until it exits."""
    print("Client connected:", addr)
    nbd_server = NbdServer(conn)
    nbd_server.handshake()
    nbd_server.handle_request()


threading.Thread(target=print_stat).start()

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.settimeout(None)  # block
sock.bind(('', 10809))  # modprobe nbd; nbd-client -no-optgo 127.0.0.1 10809 /dev/nbd0 不加-no-optgo会出现 invalid negotiation magic 报错
sock.listen(3)
while 1:
    connection, address = sock.accept()
    t = threading.Thread(target=serve_client, args=(connection, address))
    t.start()
