"""
nbd server
"""
# TODO: 统计读写字节数和放大倍数
# TODO: NBD相关代码单独提出去一个文件
# TODO: 支持协议参数
# TODO: 支持多层LRU缓存（内存/zram裸盘？/SSD/HDD/S3）
# TODO: 支持压缩
# TODO: 全0的块直接删除
# TODO: 支持合并相同块
import io
import socket
import struct
import threading
import time
from typing import Union, Optional

import boto3
import botocore

# =====NBD配置START=====TODO：改成通过conf文件配置
DISK_SIZE = 1024 * 1024 * 1024 * 8  # 磁盘大小 8G
BLOCK_SIZE = 256 * 1024  # 分片大小 256KB。分片大小太小会影响持续读取，太大会影响随机写入。最小不能小于128KB。推荐在1MB/512KB/256KB中选取。
STORAGE_PATH = "./storage"  # 没用……后续可以考虑磁盘缓存
USE_MEM_BUFFER = True  # 使用memory性能会快2%左右……如不使用，则产生一个临时文件作为读写，当ENABLE_CACHE时此项无效
ENABLE_CACHE = True  # 是否启用内存缓存
CACHE_SIZE = 1024 * 1  # 256MB 内存缓存的块数量（总共消耗内存大小为BLOCK_SIZE*CACHE_SIZE）
COMPRESS = False  # TODO: 压缩功能
# TODO: 上面测参数如何影响性能，待测试
# TODO：试试 内存-磁盘-S3 三层架构
BACKEND = "s3"  # s3/disk
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
# =====NBD配置END=====


# =====性能统计变量START=====
TOTAL_READ_BYTES_CLIENT = 0
TOTAL_WRITE_BYTES_CLIENT = 0
TOTAL_READ_BYTES_S3 = 0
TOTAL_WRITE_BYTES_S3 = 0
# =====性能统计变量END=====


"""
BLOCK_SIZE越大，缓存外随机写性能越差
如果ENABLE_CACHE，则BLOCK_SIZE大一点，缓存内性能会更好【可能和没用链表有关，需要重测！】
256KB约8k的随机写，18k随机读。512KB约16k的随机写，20k随机读。1MB的约26k随机写，26k随机读。
"""

DEBUG = False

READ_REQUEST = 0
WRITE_REQUEST = 1
DISCONNECT_REQUEST = 2
NBD_REQUEST_STRUCT = '>LL8sQL'
NBD_OPT_MAGIC = b"\0\x03\xe8\x89\x04\x55\x65\xa9"

# https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#option-types
NBD_OPT_EXPORT_NAME = 1
NBD_OPT_ABORT = 2
NBD_OPT_LIST = 3
NBD_OPT_PEEK_EXPORT = 4
NBD_OPT_STARTTLS = 5
NBD_OPT_INFO = 6
NBD_OPT_GO = 7
NBD_OPT_STRUCTURED_REPLY = 8
NBD_OPT_LIST_META_CONTEXT = 9
NBD_OPT_SET_META_CONTEXT = 10
NBD_REP_ACK = 1
NBD_REP_SERVER = 2
NBD_REP_INFO = 3
NBD_REP_META_CONTEXT = 4
NBD_REP_ERR_UNSUP = 2 ^ 31 + 1
NBD_INFO_EXPORT = 0
NBD_INFO_NAME = 1
NBD_INFO_DESCRIPTION = 2
NBD_INFO_BLOCK_SIZE = 3
NBD_EINVAL = 22


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


class LinkNode:
    def __init__(self, key=None, val=0, prev=None, next=None):
        self.key = key
        self.val = val
        self.prev = prev
        self.next = next


class LruCache:
    def __init__(self, capacity):
        self.head = LinkNode()
        self.tail = LinkNode()
        self.capacity = capacity
        self.head.next = self.tail
        self.tail.prev = self.head
        self.size = 0
        self.dirty_blocks = set()
        self.dict = {}
        self.write_lock = set()
        self.last_write_time = 0
        threading.Thread(target=self.auto_writeback).start()

    # def remove_last(self) -> None:
    #     """
    #     删除最后一个元素
    #     """
    #     print("remove_last", self.tail.prev.key)
    #     del self.dict[self.tail.prev.key]
    #     self.tail.prev.prev.next = self.tail
    #     self.tail.prev = self.tail.prev.prev
    #     self.size -= 1

    def insert(self, key: int, val) -> None:
        """
        在最前方插入节点
        """
        self.size += 1
        # print("insert(%d)" % self.size, key)
        node = LinkNode(key=key, val=val)
        self.dict[key] = node
        self.move_to_head(key)
        if self.size > self.capacity:
            key = self.tail.prev.key
            while key in self.write_lock:
                continue
            self.write_lock.add(key)
            self.flush(key, clean_cache=True)
            self.write_lock.remove(key)

    def move_to_head(self, key: int) -> None:
        """
        将节点移到最前面
        """
        # print("move_to_head", key)
        node = self.dict[key]
        if node.prev and node.next:
            node.prev.next = node.next
            node.next.prev = node.prev
        node.next = self.head.next
        node.next.prev = node
        node.prev = self.head
        self.head.next = node

    def delete(self, key: int) -> None:
        """
        删除节点
        """
        self.size -= 1
        # print("delete(%d)" % self.size, key)
        node = self.dict[key]
        node.prev.next = node.next
        node.next.prev = node.prev
        del self.dict[key]

    def auto_writeback(self):
        while True:
            if (time.time() - self.last_write_time > 2 and self.dirty_blocks) or len(self.dirty_blocks) > CACHE_SIZE * 0.2:
                # print("自动回写", self.dirty_blocks)
                thread_list = []
                for key in self.dirty_blocks.copy():
                    # self.flush(key, clean_cache=False)
                    thread = threading.Thread(target=self.flush, args=(key, False))
                    thread.start()
                    thread_list.append(thread)
                for thread in thread_list:
                    thread.join()
                if not self.dirty_blocks:
                    print("所有脏数据已回写完毕")
            else:
                time.sleep(1)

    def has_block(self, block):
        if block in self.dict:
            return True
        return False

    def flush(self, key=None, clean_cache=False):
        """
        回写脏数据
        """
        # print("触发回写(%d>%d)" % (self.size, self.capacity), key)
        global TOTAL_WRITE_BYTES_S3
        if key is not None:
            while key in self.write_lock:
                continue
            self.write_lock.add(key)
            node = self.dict[key]
            if key in self.dirty_blocks:  # 如果key不在dirty_blocks，说明已经被自动回写了
                node.val.seek(0)
                s3.upload_fileobj(node.val, bucket_name, str(key))
                TOTAL_WRITE_BYTES_S3 += BLOCK_SIZE
                self.dirty_blocks.remove(key)
            if clean_cache:
                self.delete(key)
            self.write_lock.remove(key)
        else:
            for key in self.dirty_blocks.copy():
                while key in self.write_lock:
                    continue
                self.write_lock.add(key)
                node = self.dict[key]
                node.val.seek(0)
                s3.upload_fileobj(node.val, bucket_name, str(key))
                TOTAL_WRITE_BYTES_S3 += BLOCK_SIZE
                self.dirty_blocks.remove(key)
                if clean_cache:
                    self.delete(key)
                self.write_lock.remove(key)

    def write(self, block, offset, data):
        global TOTAL_READ_BYTES_S3
        self.last_write_time = time.time()
        while block in self.write_lock:
            continue
        self.write_lock.add(block)
        self.dirty_blocks.add(block)
        if self.has_block(block):
            # print("修改缓存", block)
            self.dict[block].val.data[offset:offset + len(data)] = data
            self.move_to_head(block)
        else:
            # print("新增缓存", block)
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
            self.insert(block, f)
        self.write_lock.remove(block)

    def read(self, block, offset, length):
        self.move_to_head(block)
        return self.dict[block].val.data[slice(offset, offset + length)]


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
    # TODO：handle these request: NBD_OPT_INFO NBD_OPT_ABORT NBD_OPT_LIST

    def __init__(self, conn):
        self.conn = BuffSock(conn)

    def handshake(self):
        """
        Handshake with the client.
        :return:
        """
        """
        The initial few exchanges in newstyle negotiation look as follows:
        S: 64 bits, 0x4e42444d41474943 (ASCII 'NBDMAGIC') (as in the old style handshake)
        S: 64 bits, 0x49484156454F5054 (ASCII 'IHAVEOPT') (note different magic number)
        S: 16 bits, handshake flags
        C: 32 bits, client flags
        """
        self.conn.send(b'NBDMAGIC')  # 64 bits, 0x4e42444d41474943 (ASCII 'NBDMAGIC')
        self.conn.send(b'IHAVEOPT')  # 64 bits, 0x49484156454F5054 (ASCII 'IHAVEOPT')
        self.conn.send(b'\0\x01')  # 16 bits, handshake flags https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#handshake-flags
        client_flags = self.conn.recv(4)  # 32 bits, client flags
        print("client_flags:", client_flags)  # should be b'\x00\x00\x00\x01'
        """
        Fixed newstyle negotiation
        """
        client_magic = self.conn.recv(8)  # 64 bits, 0x49484156454F5054 (ASCII 'IHAVEOPT')
        if client_magic != b'IHAVEOPT':
            self.conn.close()
        while True:
            client_option = self.conn.recv(4)  # 32 bits, option
            client_option_int = struct.unpack('>I', client_option)[0]
            print("client_option:", client_option_int)
            client_option_data_length = struct.unpack('>I', self.conn.recv(4))[0]  # 32 bits, length of option data
            print("client_option_data_length:", client_option_data_length)
            if client_option_data_length:
                client_option_data = self.conn.recv(client_option_data_length)  # any data needed for the chosen option, of length as specified above.
                print("client_option_data:", client_option_data)
            if client_option_int == NBD_OPT_EXPORT_NAME:
                """
                The presence of the option length in every option allows the server to skip any options presented by the client that it does not understand.
                If the value of the option field is NBD_OPT_EXPORT_NAME and the server is willing to allow the export, 
                the server replies with information about the used export:
                S: 64 bits, size of the export in bytes (unsigned)
                S: 16 bits, transmission flags
                S: 124 bytes, zeroes (reserved) (unless NBD_FLAG_C_NO_ZEROES was negotiated by the client)
                """
                self.conn.send(struct.pack('>Q', DISK_SIZE))  # 64 bits, size of the export in bytes (unsigned)
                self.conn.send(b'\0\x01')  # 16 bits, transmission flags
                self.conn.send(b'\0' * 124)  # 124 bytes, zeroes (reserved) (unless NBD_FLAG_C_NO_ZEROES was negotiated by the client)
                """
                If the server is unwilling to allow the export, it MUST terminate the session.
                """
                break
            elif client_option_int == NBD_OPT_GO:
                export_name_length = struct.unpack('>I', client_option_data[:4])[0]  # 32 bits, length of name
                client_option_data = client_option_data[4:]
                if export_name_length:
                    export_name = client_option_data[:export_name_length]
                    print("export_name:", export_name)
                    client_option_data = client_option_data[export_name_length:]
                info_req_num = struct.unpack('>H', client_option_data[:2])[0]  # 16 bits, number of information requests
                if info_req_num:  # TODO
                    client_option_data = client_option_data[2:]  # 16 bits x n - list of NBD_INFO information requests
                # send disk size https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md#option-reply-types
                self.conn.send(NBD_OPT_MAGIC)  # 64 bits, 0x3e889045565a9 (magic number for replies)
                self.conn.send(client_option)  # 32 bits, the option as sent by the client to which this is a reply
                self.conn.send(struct.pack('>I', NBD_REP_INFO))  # 32 bits, reply type
                self.conn.send(b'\0\0\0\x0c')  # [12] 32 bits, length of the reply.
                self.conn.send(struct.pack('>H', NBD_INFO_EXPORT))  # 16 bits, information type (e.g. NBD_INFO_EXPORT)
                self.conn.send(struct.pack('>Q', len(DISK_SIZE)))  # 64 bits, size of the export in bytes (unsigned)
                self.conn.send(b'\0\x01')  # 16 bits, transmission flags
                # send ack
                self.conn.send(NBD_OPT_MAGIC)  # 64 bits, 0x3e889045565a9 (magic number for replies)
                self.conn.send(client_option)  # 32 bits, the option as sent by the client to which this is a reply
                self.conn.send(struct.pack('>I', NBD_REP_ACK))  # 32 bits, reply type
                self.conn.send(b'\0\0\0\0')  # 32 bits, length of the reply.
                break
            self.conn.send(NBD_OPT_MAGIC)  # 64 bits, 0x3e889045565a9 (magic number for replies)
            self.conn.send(client_option)  # 32 bits, the option as sent by the client to which this is a reply
            self.conn.send(struct.pack('>I', NBD_REP_ERR_UNSUP))  # 32 bits, reply type
            self.conn.send(b'\0\0\0\0')  # 32 bits, length of the reply.
            # self.conn.send()  # any data as required by the reply

    def reply(self, error=0, handle=b'', data=b''):
        """Construct an NBD reply."""
        """
        The simple reply message MUST be sent by the server in response to all requests if structured replies have not been negotiated using NBD_OPT_STRUCTURED_REPLY. If structured replies have been negotiated, a simple reply MAY be used as a reply to any request other than NBD_CMD_READ, but only if the reply has no data payload. The message looks as follows:
        S: 32 bits, 0x67446698, magic (NBD_SIMPLE_REPLY_MAGIC; used to be NBD_REPLY_MAGIC)
        S: 32 bits, error (MAY be zero)
        S: 64 bits, handle
        S: (length bytes of data if the request is of type NBD_CMD_READ and error is zero)
        """
        assert type(handle) is type(b'') and len(handle) == 8
        return b'\x67\x44\x66\x98' + struct.pack('>L', error) + handle + data

    def get_request(self):
        header = self.conn.recv(struct.calcsize(NBD_REQUEST_STRUCT))
        return NbdRequest(header)

    def handle_request(self):
        """
        Handle a request.
        :return:
        """
        """
        The request message, sent by the client, looks as follows:
        C: 32 bits, 0x25609513, magic (NBD_REQUEST_MAGIC)
        C: 16 bits, command flags
        C: 16 bits, type
        C: 64 bits, handle
        C: 64 bits, offset (unsigned)
        C: 32 bits, length (unsigned)
        C: (length bytes of data if the request is of type NBD_CMD_WRITE)
        """
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
                self.conn.send(self.reply(error=NBD_EINVAL, handle=req.handle))
                return

    def read(self, offset, length):  # TODO：封装，后面直接提供单个请求
        global TOTAL_READ_BYTES_CLIENT
        TOTAL_READ_BYTES_CLIENT += length
        block_number = offset // BLOCK_SIZE  # 块的序号
        # print("read block=%d offset=%d length=%d(%.1fKB)" % (block_number, offset, length, length / 1024))
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

    def write(self, offset, data):  # TODO：封装，后面直接提供单个请求
        global TOTAL_WRITE_BYTES_CLIENT
        TOTAL_WRITE_BYTES_CLIENT += len(data)
        block_number = offset // BLOCK_SIZE  # 块的序号
        # print("write block=%d offset=%d length=%d(%.1fKB)" % (block_number, offset, len(data), len(data) / 1024))
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
                with open("../tmp.file", "r+b") as f:
                    f.seek(offset)
                    f.write(data)
            except botocore.exceptions.ClientError as e:
                file_data = bytearray(BLOCK_SIZE)
                file_data[slice(offset, offset + len(data))] = data
                with open("../tmp.file", "wb") as f:
                    f.write(file_data)
        else:
            with open("../tmp.file", "wb") as f:
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
    # TODO: 增加读写缓存命中率
    global TOTAL_READ_BYTES_CLIENT, TOTAL_WRITE_BYTES_CLIENT, TOTAL_READ_BYTES_S3, TOTAL_WRITE_BYTES_S3
    while True:
        time.sleep(5)
        print()
        print("客户端读写(MB/s):\t%.2f\t%.2f" % (TOTAL_READ_BYTES_CLIENT / 1024 / 1024 / 5, TOTAL_WRITE_BYTES_CLIENT / 1024 / 1024 / 5))
        print("S3读写(MB/s):   \t%.2f\t%.2f" % (TOTAL_READ_BYTES_S3 / 1024 / 1024 / 5, TOTAL_WRITE_BYTES_S3 / 1024 / 1024 / 5))
        print("脏块数量：%d" % len(LRU_Cache.dirty_blocks))
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
