import requests

SERVER = "127.0.0.1:8080"

req = requests.post("http://{server}/0/0".format(server=SERVER), data=b'1123')
print(req.status_code)
req = requests.get("http://{server}/0/0/4".format(server=SERVER))
print(req.status_code, len(req.text), req.content)
