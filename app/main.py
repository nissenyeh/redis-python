# Uncomment this to pass the first stage
import socket
import threading
import re
import time
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--port", type=int, default=6380)
parser.add_argument("--replicaof", type=str, default='')
parser.add_argument("--local", type=str, default='False')



# ping 
def to_redis_protocol(command: str) -> str:
    parts = command.split()
    proto = f"*{len(parts)}\r\n"
    for part in parts:
        proto += f"${len(part)}\r\n{part}\r\n"
    return proto


def handle_connection(client_socket):
    
    while True:
        request: bytes = client_socket.recv(1024) # 獲取客戶端發送的訊息
        if not request:
            break;
        print(f'http_request: {request}')
        parser_request: list =  parse_request(request)
        parse_command(client_socket, parser_request)

def parse_request(request) ->list:
    request_str: str = request.decode()
    print(f'http_request_decode: {request_str}')
    parse_request: list = []

    # run only in local 
    if parser.parse_args().local == 'True':
        request_str = to_redis_protocol(request_str)
    
    # e.g: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    # ['*2', '$4', 'ECHO', '$3', 'hey', '']
    # e.g: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    # ['*3', 'SET', '3', 'bar', '']
    if '\r\n' in request_str:
        parse_request = re.split(r'\r\n|\\r\\n', request_str)[2:-1:2]
    else: # e.g: ping 
        parse_request = [request_str]

    print(f'parser_request: {parse_request}')

    return parse_request

replicaof = parser.parse_args().replicaof
role = "slave" if replicaof else "master"  
cache_dict = {}
expire_time_dict ={}
replicas = []

def parse_command(client_socket, parser_request) -> bytes:
    response = b'+No\r\n'

    if not parser_request:
        response = b'+No\r\n'
    elif "ping" in parser_request[0].lower():
        response = b'+PONG\r\n'
    elif "echo" in parser_request[0].lower():
        response = f'+{parser_request[1]}\r\n'.encode()
    elif 'set' in parser_request[0].lower():
        key_name = parser_request[1]
        value = parser_request[2]
        cache_dict[key_name] = value

        if len(parser_request) > 4 and 'px' in parser_request[3]:
            expire_time = parser_request[4]
            current_time_ms = int(time.time() * 1000)
            expire_time_dict[key_name] = current_time_ms + int(expire_time)

        if role == 'master':  # master
            for rep in replicas:
                try:
                    rep.send(to_redis_protocol(f"SET {key_name} {value}").encode())
                except Exception as e:
                    print(f"Failed to send to replica: {e}")
        
    elif 'get' in parser_request[0].lower():
        key_name = parser_request[1]

        if key_name in expire_time_dict:
            expire_time = expire_time_dict[key_name]
            current_time_ms = int(time.time() * 1000)
            print(f'expire_time:{expire_time}')
            print(f'current_time_ms:{current_time_ms}')
            if current_time_ms > expire_time:
                response = f'$-1\r\n'.encode()
            else:
                res = cache_dict[key_name]
                response = f'+{res}\r\n'.encode()
        else:
            res = cache_dict[key_name]
            response = f'+{res}\r\n'.encode()
    elif 'info' in parser_request[0].lower():

        res = f'role:{role}\n'
        res += 'master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n'
        res += 'master_repl_offset:0'
        response = f'+{res}\r\n'.encode()

    # Connect by Replica
    elif 'replconf' in parser_request[0].lower():
        response = f'+OK\r\n'.encode()
    elif 'psync' in parser_request[0].lower():
        REPL_ID = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        response = f'+FULLRESYNC {REPL_ID} 0\r\n'.encode()
        client_socket.send(response)
        # client_response = client_socket.recv(1024)
        # print(f"Client response: {client_response}")
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb_content = bytes.fromhex(rdb_hex)
        rdb_length = len(rdb_content)
        print(f"${rdb_length}\r\n".encode()+rdb_content)
        client_socket.send(f"${rdb_length}\r\n".encode()+rdb_content)
        
        replicas.append(client_socket)
        return 

    client_socket.send(response)

def connect_to_master() -> None:
    host , port = parser.parse_args().replicaof.split(' ')
    replica_to_master_socket = socket.create_connection((host, port))
    try:
        replica_to_master_socket.send(to_redis_protocol('ping').encode())
        response = replica_to_master_socket.recv(1024)
        print(f"[handshake(1/3)] Successfully connected to the master server {host}:{port}, response: {response}")
        # 在这里可以添加更多的逻辑来处理与主服务器的通信
    except Exception as e:
        print(f"fail: {e}")

    try:
        replica_to_master_socket.send(to_redis_protocol(f'REPLCONF listening-port 6380').encode())
        response = replica_to_master_socket.recv(1024)
        print(f"[handshake(2/3)] Successfully send 'REPLCONF' to the master server {host}:{port}, response: {response}")
        # 在这里可以添加更多的逻辑来处理与主服务器的通信
    except Exception as e:
        print(f"fail: {e}")

    try:
        replica_to_master_socket.send(to_redis_protocol('REPLCONF capa psync').encode())
        response = replica_to_master_socket.recv(1024)
        print(f"[handshake(2/3)] Successfully send 'REPLCONF' to the master server {host}:{port}, response: {response}")
        # 在这里可以添加更多的逻辑来处理与主服务器的通信
    except Exception as e:
        print(f"fail: {e}")

    try:
        replica_to_master_socket.send(to_redis_protocol('PSYNC ? -1').encode())
        response = replica_to_master_socket.recv(1024)
        print(f"[handshake(3/3)] Successfully send 'PSYNC' to the master server {host}:{port}, response: {response}")

    except Exception as e:
        print(f"fail: {e}")


def start_server():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    port = parser.parse_args().port

    print(f"Redis server is ready to connect to port: {port}!")
    server_socket = socket.create_server(("localhost", port), reuse_port=True)

    if role == 'slave':
        threading.Thread(target=connect_to_master).start()

    # Uncomment this to pass the first stage
    while True:
        try:
            client_socket, _ = server_socket.accept()  # 等待客戶端連接
            threading.Thread(
                target=handle_connection, args=[client_socket]
            ).start()
        except Exception as e:
            print(f"Error accepting connection: {e}")


if __name__ == "__main__":
    start_server()
