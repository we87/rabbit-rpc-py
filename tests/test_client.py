from rabbit_rpc_py import Client
import pika
import time

rpc_client = Client("hello", pika.ConnectionParameters(host='localhost'))

payload = {
    "args": "this is a rpc call"
}

start = time.time()
for x in range(10):
    result = rpc_client.invoke("boo", payload)
    print(result)
end = time.time()
print("total time: {}".format(end-start))
