"""
nbd server, ramdisk
"""
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
# NBD读写length最大为128KB，按4K对齐。也就是说，如果块大小大于128KB，需考虑读写横跨一个块的情况；如果块大小小于128KB，需考虑读写横跨多个块的情况。

mem_disk = bytearray(1024 * 1024 * 1024 * 1)  # 1GB
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


class NbdRequest:
    def __init__(self, data):
        self.magic, self.type, self.handle, self.offset, self.len = struct.unpack(NBD_REQUEST_STRUCT, data)
        if self.magic != 0x25609513:
            raise Exception(self.magic)

    def range(self):
        return slice(self.offset, self.offset + self.len)


class NbdServer:
    global mem_disk

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
                self.conn.send(struct.pack('>Q', len(mem_disk)))  # 64 bits, size of the export in bytes (unsigned)
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
                self.conn.send(struct.pack('>Q', len(mem_disk)))  # 64 bits, size of the export in bytes (unsigned)
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

    def read(self, offset, length):
        return mem_disk[offset:offset + length]

    def write(self, offset, data):
        mem_disk[offset:offset + len(data)] = data


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
