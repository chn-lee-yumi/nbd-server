import os
import socket
import struct
import time

PORT = 8080
STORAGE_PATH = "./storage"

BLOCK_SIZE = 1024 * 1024 * 4  # 分片大小 4M


def read(block_num: int, offset: int, length: int):
    data = bytearray(length)
    block_path = "%s/%d" % (STORAGE_PATH, block_num)
    if os.path.exists(block_path):
        with open(block_path, "rb") as f:
            f.seek(offset)
            data = f.read(length)
    return data


def write(block_num: int, offset: int, length: int, data: bytearray):
    block_path = "%s/%d" % (STORAGE_PATH, block_num)
    if os.path.exists(block_path):
        with open(block_path, "r+b") as f:
            f.seek(offset)
            f.write(data)
    else:
        file_data = bytearray(BLOCK_SIZE)
        file_data[slice(offset, offset + length)] = data
        with open(block_path, "wb") as f:
            f.write(file_data)
    return "OK"


def serve_client(conn, addr):
    # print("Client connected:", addr)
    data = conn.recv(struct.calcsize("!sLQL"))
    method, block, offset, length = struct.unpack("!sLQL", data)
    print(method, block, offset, length)
    if method == b'R':
        _data = read(block, offset, length)
        conn.send(read(block, offset, length))
    elif method == b'W':
        write_data = conn.recv(length)
        write(block, offset, length, write_data)
    conn.close()


if __name__ == '__main__':
    # 启动存储节点
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(None)  # block
    sock.bind(('', 8080))  # modprobe nbd; nbd-client -no-optgo 127.0.0.1 10809 /dev/nbd0 不加-no-optgo会出现 invalid negotiation magic 报错
    sock.listen(1000)  # 这个需要大一点，可能回收需要时间，如果改成个位数，会导致write的时候一卡一卡（read不会），不知道为什么
    """
    内核调优：
    sysctl -w net.ipv4.tcp_tw_reuse=1
    sysctl -w net.ipv4.tcp_max_syn_backlog=65536
    sysctl -w net.ipv4.tcp_max_tw_buckets=65536
    """
    while 1:
        connection, address = sock.accept()
        # t = threading.Thread(target=serve_client, args=(connection, address))
        # t.start()
        t0 = time.time()
        serve_client(connection, address)
        print("%.5f" % (time.time() - t0))
