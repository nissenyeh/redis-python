# Uncomment this to pass the first stage
import socket
import threading
import re
import time
from argparse import ArgumentParser

from app.parser import redis_protocol_encoder, redis_protocol_parser
from app.command_handler import handle_echo, handel_ping, handle_config, handle_info, handle_keys
    

parser = ArgumentParser()
parser.add_argument("--port", type=int, default=6379)
parser.add_argument("--replicaof", type=str, default='')
parser.add_argument("--local", type=str, default='False')
parser.add_argument("--dir", type=str, default='')
parser.add_argument("--dbfilename", type=str, default='')
offset = 0
start_record_offset = False

# ping 
# def to_redis_protocol(command: str) -> str:
#     parts = command.split()
#     proto = f"*{len(parts)}\r\n"
#     for part in parts:
#         proto += f"${len(part)}\r\n{part}\r\n"
#     return proto

def to_redis_protocol(command: str) -> str: # local
    parts = command.split()
    if parts[0].lower() == 'replconf' or parts[0].lower() == 'psync':
        return f"*{len(parts)}\r\n" + "\r\n".join(parts) + "\r\n"
    proto = f"*{len(parts)}\r\n"
    for part in parts:
        proto += f"${len(part)}\r\n{part}\r\n"
    return proto



def handle_connection(client_socket):
    global offset

    while True:
        try: 
            request: bytes = client_socket.recv(1024) # 獲取客戶端發送的訊息
            if not request:
                break;
            if start_record_offset:
                offset += len(request)
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} -  redis server get a request: {request}")
            commands: list = parse_request(request)
            # for parser_request in parser_requests:
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - redis command running: {commands}")
            parse_command(client_socket, commands)
                
        except Exception as e:
            print(f"An error occurred: {e}")


def parse_request(request) ->list:
    request_str: str = request.decode(errors='ignore')
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - http_request_decode: {request_str}")
    parse_requests: list = []

    # run only in local 
    if parser.parse_args().local == 'True':
        if ' ' in request_str:
            request_str :list = redis_protocol_encoder('array', request_str.split())
            # *2$4echo$5hello -> ['echo', 'hello]
        else:
            # $5\r\nhello\r\n -> hello
            request_str :str = redis_protocol_encoder('str', request_str)
        print(f"request_str:{request_str}")
        # print(f"redis_protocol_encoder_request_str:{redis_protocol_encoder('array', request_str.split())}")
    


    parsed = redis_protocol_parser(request_str)
    commands = parsed if isinstance(parsed, list) else [parsed]

   


    print(f'commands: {commands}')

    return commands

replicaof = parser.parse_args().replicaof
get_first_getack = False
role = "slave" if replicaof else "master"  
cache_dict = {}
expire_time_dict ={}
replicas = []
updated_replicas = []
updated_replicas_number = 0



def respond(client_socket, response):
    if parser.parse_args().local == 'True':
        parses: list | str = redis_protocol_parser(response.decode())
        client_socket.send(parses.encode() if isinstance(parses,str) else str(parses).encode())
    else:
        client_socket.send(response)


def parse_command(client_socket, commands) -> bytes:
    global offset
    global start_record_offset
    global updated_replicas_number

    commands = [command.lower() for command in commands]

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - commands: {commands}")
    if not commands:
        return 
    
    elif "ping" in commands[0]:
        args = {
            'role' : "slave" if replicaof else "master" 
        }
        response = handel_ping(commands, args)
        respond(client_socket, response)

    elif "config" in commands[0]:
        args = {
            'dir': parser.parse_args().dir,
            'dbfilename':parser.parse_args().dbfilename
        }
        response = handle_config(commands, args)
        client_socket.send(response)
       
    elif "echo" in commands[0]:
        response = handle_echo(commands)
        respond(client_socket, response)
        # client_socket.send(response)

    elif "keys" in commands[0]:
        args = {
            'dir': parser.parse_args().dir,
            'dbfilename':parser.parse_args().dbfilename
        }
        response = handle_keys(commands, args)
        respond(client_socket, response)
        # client_socket.send(response)       
    elif 'set' in commands[0].lower():
        
        key_name = commands[1]
        value = commands[2]
        cache_dict[key_name] = value
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - SET key: {key_name} -  VALUE: {value}")

        if len(commands) > 4 and 'px' in commands[3]:
            expire_time = commands[4]
            current_time_ms = int(time.time() * 1000)
            expire_time_dict[key_name] = current_time_ms + int(expire_time)

        print(f'role:{role}')
        if role == 'master':  # master
            updated_replicas_number = 0
            # rep.send(redis_protocol('array',res))
            response = redis_protocol_encoder('str','OK').encode()
        
            # client_socket.send(response)
            for index, replica in enumerate(replicas):
                try:
                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Sync data to slave - {index+1} / total {len(replicas)} ")

                    if parser.parse_args().local == 'True':
                        replica.send((f"SET {key_name} {value}").encode())
                    else:
                        res = ["SET", key_name , value]
                        # replica.send(redis_protocol('array',res).encode())
                        replica.send(to_redis_protocol(f"SET {key_name} {value}").encode())

        
                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - command: REPLCONF GETACK *")

                except Exception as e:
                    print(f"Failed to send to replica: {e}")
            client_socket.send(response)
        if role == 'slave':
            return 
        
    elif 'get' in commands[0].lower():
        response = ''
        key_name = commands[1]

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
        client_socket.send(response)

    elif 'info' in commands[0]:
        args = {
            'role': role
        }
        response = handle_info(commands, args)
        client_socket.send(response)


    # Connect by Replica
    elif 'replconf' in commands[0].lower():
        print(f'{role} get {commands[1].lower()}')
        response = ''

        if role == 'master' and 'ack' in commands[1].lower():
            updated_replicas_number += 1
            print(f"replconf updated_replicas_number: {updated_replicas_number}")
            return 

        if role == 'slave' and 'getack' in commands[1].lower():
            print(f'send REPLCONF ACK {offset}\r\n')
            # response = to_redis_protocol(f'REPLCONF ACK {offset}\r\n').encode()  
            res = ["REPLCONF", "ACK", str(offset)]
            print(f"REPLCONF:{redis_protocol_encoder('array',res)}")
            response = redis_protocol_encoder('array',res).encode()
            start_record_offset = True
            return client_socket.send(response)

        if role == 'master':
            response = f'+OK\r\n'.encode()
            return client_socket.send(response)

        

    # Send RDB file to create A replica & Add to replicas
    elif 'psync' in commands[0].lower():
        REPL_ID = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        response = f'+FULLRESYNC {REPL_ID} 0\r\n'.encode()
        client_socket.send(response)
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb_content = bytes.fromhex(rdb_hex)
        rdb_length = len(rdb_content)
        print(f"${rdb_length}\r\n".encode()+rdb_content)
        client_socket.send(f"${rdb_length}\r\n".encode()+rdb_content)
        replicas.append(client_socket)
        updated_replicas_number +=1 
        # time.sleep(1)  # 延遲 1 秒

    
    # WAIT 1 500
    elif 'wait' in commands[0].lower():
        num_replicas = int(commands[1])
        timeout = int(commands[2]) / 1000  # Convert to seconds
        start_time = time.time()

        for index, replica in enumerate(replicas):
            try:
                if parser.parse_args().local == 'True':
                    replica.send((f'REPLCONF GETACK *').encode())
                else:
                    res = ["REPLCONF", "GETACK", "*"]
                    replica.send(redis_protocol_encoder('array', res).encode())
            except Exception as e:
                print(f"Failed to send REPLCONF GETACK to replica: {e}")

        while time.time() - start_time < timeout:
            if updated_replicas_number >= num_replicas:
                break
       
        response = f':{updated_replicas_number}\r\n'.encode()
        client_socket.send(response)


    

def connect_to_master() -> None:
    host , port = parser.parse_args().replicaof.split(' ')
    replica_to_master_socket = socket.create_connection((host, port))
    try:
        if parser.parse_args().local == 'True':
            replica_to_master_socket.send(b'+PING\r\n') # local
        else:
            replica_to_master_socket.send(to_redis_protocol('PING').encode())
        response = replica_to_master_socket.recv(1024)
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [handshake(1/3)] Successfully connected to the master server {host}:{port}, response: {response}")
        # 在这里可以添加更多的逻辑来处理与主服务器的通信
    except Exception as e:
        print(f"fail: {e}")

    try:
        # replica_to_master_socket.send(to_redis_protocol(f'REPLCONF listening-port 6380').encode())
        response = redis_protocol_encoder('array',['REPLCONF','listening-port','6380']).encode()
        replica_to_master_socket.send(response)
        response = replica_to_master_socket.recv(1024)
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [handshake(2/3)] Successfully send 'REPLCONF' to the master server {host}:{port}, response: {response}")
        # 在这里可以添加更多的逻辑来处理与主服务器的通信

    except Exception as e:
        print(f"fail: {e}")

    try:
        response = redis_protocol_encoder('array',['REPLCONF','capa','psync']).encode()
        replica_to_master_socket.send(response)
        # replica_to_master_socket.send(to_redis_protocol('REPLCONF capa psync').encode())
        # response = replica_to_master_socket.recv(1024)
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [handshake(3/3)] Successfully send 'REPLCONF' to the master server {host}:{port}, response: {response}")
        # 在这里可以添加更多的逻辑来处理与主服务器的通信
    except Exception as e:
        print(f"fail: {e}")

    try:
        response = redis_protocol_encoder('array',['PSYNC','?','-1']).encode()
        replica_to_master_socket.send(response)

        # replica_to_master_socket.send(to_redis_protocol('PSYNC ? -1').encode())

        response = replica_to_master_socket.recv(1024)
        thread =  threading.Thread(
            target=handle_connection, args=[replica_to_master_socket]
        )
        thread.start()
        # print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [handshake(3/3)] Successfully send 'PSYNC' to the master server {host}:{port}, response: {response}")
        # handle_connection(replica_to_master_socket)

    except Exception as e:
        print(f"fail: {e}")

import time



def start_server():
    port = parser.parse_args().port
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Redis {role} server is running at port: {port}!")
    server_socket = socket.create_server(("localhost", port), reuse_port=True)


    if role == 'slave':
        thread = threading.Thread(target=connect_to_master)
        thread.start()
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Starting thread {thread.name} for connect_to_master")

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Entering the main loop to accept connections...")
    while True:
        try:
            client_socket, _ = server_socket.accept()  # 等待客戶端連接
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} -  {port} is ready to accept a new connection")
            thread =  threading.Thread(
                target=handle_connection, args=[client_socket]
            )
            thread.start()
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Starting thread {thread.name} for start listening to connection")
        except Exception as e:
            print(f"Error accepting connection: {e}")



if __name__ == "__main__":
    start_server()
