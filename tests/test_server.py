from rabbit_rpc_py import Server
import pika

rpc_server = Server("hello", pika.ConnectionParameters(host='localhost'))


@rpc_server.bind("boo")
def boo(payload):
    print(payload)
    return "this is rpc response"


rpc_server.start()
