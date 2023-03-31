import os

from flask import Flask, request

PORT = 8080
STORAGE_PATH = "./storage"
app = Flask(__name__)
BLOCK_SIZE = 1024 * 1024 * 4  # 分片大小 4M


@app.route("/<int:block_num>/<int:offset>/<int:length>", methods=['GET'])
def read(block_num: int, offset: int, length: int):
    data = bytearray(length)
    block_path = "%s/%d" % (STORAGE_PATH, block_num)
    if os.path.exists(block_path):
        with open(block_path, "rb") as f:
            f.seek(offset)
            data = f.read(length)
    return data, 200


@app.route("/<int:block_num>/<int:offset>", methods=['POST'])
def write(block_num: int, offset: int):
    data = request.data
    length = len(data)
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
    return "OK", 200


if __name__ == '__main__':
    # 启动存储节点
    app.run(host="0.0.0.0", port=PORT)
