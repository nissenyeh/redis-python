# Uncomment this to pass the first stage
import socket
import threading
import re
import time
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--port", type=int, default=6379)
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
        response = parse_command(parser_request)
        client_socket.send(response)

def parse_request(request) ->list:
    request_str: str = request.decode()
    print(f'http_request_decode: {request_str}')
    parse_request: list = []

    # run only in local 
    if  bool(parser.parse_args().local):
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
        
cache_dict = {}
expire_time_dict ={}

def parse_command(parser_request)-> bytes:

     if not parser_request:
        return b'+No\r\n'
     # *1\r\n$4\r\nPING\r\n
     if "ping" in parser_request[0].lower():
         return b'+PONG\r\n'
     # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
     if "echo" in parser_request[0].lower():
         return f'+{parser_request[1]}\r\n'.encode()
    # e.g: SET foo bar
    # *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n

     if 'set' in parser_request[0].lower():
         key_name = parser_request[1]
         value = parser_request[2]
         cache_dict[key_name] = value

         if len(parser_request) > 4 and 'px' in parser_request[3]:
            expire_time = parser_request[4]
            current_time_ms = int(time.time() * 1000)
            expire_time_dict[key_name] = current_time_ms + int(expire_time)

         return b'+OK\r\n'
     
     # e.g: get foo  => bar
     if 'get' in parser_request[0].lower():
         key_name = parser_request[1]

         # if expire
         if key_name in expire_time_dict:
            expire_time = expire_time_dict[key_name]
            current_time_ms = int(time.time() * 1000)
            print(f'expire_time:{expire_time}')
            print(f'current_time_ms:{current_time_ms}')
            if(current_time_ms>expire_time):
                return f'$-1\r\n'.encode()
            
         res = cache_dict[key_name]
         return f'+{res}\r\n'.encode()
    # $ redis-cli INFO replication
    # # Replication
    # role:master
    # connected_slaves:0
    # master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
    # master_repl_offset:0
    # second_repl_offset:-1
    # repl_backlog_active:0
    # repl_backlog_size:1048576
    # repl_backlog_first_byte_offset:0
    # repl_backlog_histlen:
     if 'info' in parser_request[0].lower():
         replicaof = parser.parse_args().replicaof
         role = "master" if not replicaof else "slave"
         res = f'role:{role}'
         return f'+{res}\r\n'.encode()
    
     return b'+No\r\n'



def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    port = parser.parse_args().port

    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    print(f"Redis server is running in port: {port}!")
    print('cool')

    # Uncomment this to pass the first stage
    while True:
        client_socket, _ = server_socket.accept() # 等待客戶端連接
        threading.Thread(
            target=handle_connection, args=[client_socket]
        ).start()


if __name__ == "__main__":
    main()
