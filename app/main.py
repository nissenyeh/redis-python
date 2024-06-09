import socket
import threading
import time

from argparse import ArgumentParser
from app.package.command_parser import (
    parse_request,
    redis_protocol_encoder,
    redis_protocol_parser
)
from app.package.command_handler import (
    handle_echo,
    handel_ping,
    handle_config,
    handle_info,
    handle_keys
)
# argument
parser = ArgumentParser()
parser.add_argument("--port", type=int, default=6379)
parser.add_argument("--replicaof", type=str, default='')
parser.add_argument("--local", type=str, default='False')
parser.add_argument("--dir", type=str, default='')
parser.add_argument("--dbfilename", type=str, default='')

replicaof = parser.parse_args().replicaof
role = "slave" if replicaof else "master"  

start_record_offset = False
offset = 0
replicas_offsets = {}
get_first_getack = False

cache_dict = {}

expire_time_dict ={}
replicas = []

updated_replicas = []
updated_to_date_replicas_number = 0

def handle_connection(client_socket, thread_name):
    global offset

    while True:
        try: 
            request: bytes = client_socket.recv(1024)
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Redis server get a request: {request} at {thread_name}")

            if not request:
                break;

            # start_record_offset:    
            if role == 'slave':
                offset += len(request)
                print(f'offset: {offset}')

            is_local_command = True if parser.parse_args().local == 'True' else False

            commands: list = parse_request(request, is_local_command)
            if commands:
               print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Redis server get commands: {commands}")

            for command in commands:
                parse_command(client_socket, command)
                
        except Exception as e:
            print(f"An error occurred: {e}")


def respond(client_socket, response):
    global offset

    respond = b''
    if parser.parse_args().local == 'True': # local need to parse to string 
        parses: list | str = redis_protocol_parser(response.decode())
        if isinstance(parses,str):
            respond = parses.encode()
        elif isinstance(parses,list):
            respond = ' '.join(parses).encode()
        else:
            respond = str(parses).encode()
        client_socket.send(respond)
    else:
        respond = response
        client_socket.send(respond)

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Redis server sent command: {respond}")


def parse_command(client_socket, commands) -> bytes:
    global offset
    global start_record_offset
    global updated_to_date_replicas_number

    if not commands:
        return 

    commands = [command.lower() for command in commands]
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Command is running: {commands}")

    if "ping" in commands[0]:
        args = {
            'role' : "slave" if replicaof else "master" 
        }
        response = handel_ping(commands, args)
        respond(client_socket, response)

    elif "echo" in commands[0]:
        response = handle_echo(commands)
        respond(client_socket, response)

    elif "config" in commands[0]:
        args = {
            'dir': parser.parse_args().dir,
            'dbfilename':parser.parse_args().dbfilename
        }
        response = handle_config(commands, args)
        respond(client_socket, response)
  
    elif "set" in commands[0]:
        key_name = commands[1]
        value = commands[2]
        cache_dict[key_name] = value
        
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Command - SET | KEY: {key_name} -  VALUE: {value}")

        # 1. Expire command
        if len(commands) > 4 and 'px' in commands[3]:
            expire_time = commands[4]
            current_time_ms = int(time.time() * 1000)
            expire_time_dict[key_name] = current_time_ms + int(expire_time)

        # 2. Forward Commands to Replicas
        if role == 'master': 
            print(f'Forward Commands to Replicas:{replicas}')
            for index, replica in enumerate(replicas):
                try:
                    command = ["SET", key_name, value]
                    replica_message = redis_protocol_encoder('array',command).encode()
                    
                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Sync data to slave - {index+1} / total {len(replicas)} ")
                    respond(replica, replica_message)
                except Exception as e:
                    print(f"Failed to send to replica: {e}")
            if len(replicas) > 0:
                offset += len(replica_message) # Record command send to replica
            response = redis_protocol_encoder('str','OK').encode()
            respond(client_socket, response)
        if role == 'slave':
            return 
        
    elif "get" in commands[0].lower():
        key_name = commands[1]

        if key_name in expire_time_dict:
            expire_time = expire_time_dict[key_name]
            current_time_ms = int(time.time() * 1000)
            if current_time_ms > expire_time:
                print(f'{key_name} is expired')
                response = redis_protocol_encoder('str','-1').encode()
                return respond(client_socket, response)  
        
        res = cache_dict[key_name]
        response = f'+{res}\r\n'.encode()
        return respond(client_socket, response)    


    elif "info" in commands[0]:
        args = {
            'role': role
        }
        response = handle_info(commands, args)
        respond(client_socket, response) 

    elif "keys" in commands[0]:
        args = {
            'dir': parser.parse_args().dir,
            'dbfilename':parser.parse_args().dbfilename
        }
        response = handle_keys(commands, args)
        respond(client_socket, response)

    # Connect by Replica
    elif 'replconf' in commands[0]:

        if role == 'master' and 'ack' in commands[1]:
            replica_offset = int(commands[2])
            print(f"offset: {offset}, replica_offset: {replica_offset}")
            replicas_offsets[client_socket] = replica_offset
            if replica_offset == offset: # if offset 
                updated_to_date_replicas_number += 1
            return 
        
        if role == 'master':
            response = f'+OK\r\n'.encode()
            return respond(client_socket, response)
        
        if role == 'slave' and 'getack' in commands[1]:

            remove_offset = redis_protocol_encoder('array', ["REPLCONF", "GETACK", "*"]).encode()
            res = ["REPLCONF", "ACK", str(offset - len(remove_offset))]
            response = redis_protocol_encoder('array',res).encode()
            # start_record_offset = True
            return respond(client_socket, response)


    # Send RDB file to create A replica & Add to replicas
    elif 'psync' in commands[0]:
        REPL_ID = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        response = f'+FULLRESYNC {REPL_ID} 0\r\n'.encode()
        respond(client_socket, response)
        
        # Send RDB file
        rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        rdb_content = bytes.fromhex(rdb_hex)
        rdb_length = len(rdb_content)
        response = f"${rdb_length}\r\n".encode()+ rdb_content  # + b"\r\n"
        print(f'response: {response}')
        client_socket.send(response) # local needed

        print(f'psync - updated_to_date_replicas_number:{updated_to_date_replicas_number}')
        replicas.append(client_socket)
        updated_to_date_replicas_number += 1
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - replicas : {replicas}")
        time.sleep(1)  # 延遲 1 秒，避免後面指令，會跟 RDB file 黏再一起



    # WAIT 1 500
    elif 'wait' in commands[0]:
        num_replicas = int(commands[1])
        timeout = int(commands[2]) / 1000  # Convert to seconds
        start_time = time.time()

        # Check how many replicas are up-to-date
        all_synced_replicas = [replica for replica, replica_offset in replicas_offsets.items() if offset == replica_offset]

        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {len(all_synced_replicas)} replicas are up-to-date.")
        if len(all_synced_replicas) >= num_replicas:
            response = redis_protocol_encoder('int', len(replicas)).encode()
            return client_socket.send(f":{len(all_synced_replicas)}\r\n".encode())
            # return respond(client_socket, response)

        # If not enough, send GETACK to check all replicas
        updated_to_date_replicas_number = 0
        for index, replica in enumerate(replicas):
            try:
                command = ["REPLCONF", "GETACK", "*"]
                replica_message = redis_protocol_encoder('array', command).encode()
                respond(replica, replica_message)
            except Exception as e:
                print(f"Failed to send REPLCONF GETACK to replica: {e}")

        # 測試遇到的問題： 才把 GETACK 送出去， ACK 還沒回來就時間到了，所以 return 0
        while time.time() - start_time <= timeout:
            if updated_to_date_replicas_number >= num_replicas:
                break
        # time.sleep(1)

        response = redis_protocol_encoder('int', updated_to_date_replicas_number).encode()
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - response: {response}")
        client_socket.send(f":{updated_to_date_replicas_number}\r\n".encode())
        # After sending ["REPLCONF", "GETACK", "*"] to all replicas, updated the offset
        offset += len(replica_message)
        # respond(client_socket,response)
    

def connect_to_master() -> None:
    host , port = parser.parse_args().replicaof.split(' ')
    replica_to_master_socket = socket.create_connection((host, port))

    # PING
    try:
        command = b'+PING\r\n'
        respond(replica_to_master_socket, command)
        response = replica_to_master_socket.recv(1024)
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [handshake(1/3)] Successfully connected to the master server {host}:{port}, response: {response}")
    except Exception as e:
        print(f"fail: {e}")

    # Talk to Master with port 
    try:
        command = ['REPLCONF','listening-port','6380']
        response = redis_protocol_encoder('array', command).encode()
        respond(replica_to_master_socket,response)
        response = replica_to_master_socket.recv(1024)
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [handshake(2/3)] Successfully send 'REPLCONF' to the master server {host}:{port}, response: {response}")
    except Exception as e:
        print(f"fail: {e}")

    try:
        command = ['REPLCONF','capa','psync']
        response = redis_protocol_encoder('array', command).encode()
        respond(replica_to_master_socket, response)
        response = replica_to_master_socket.recv(1024)
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - [handshake(3/3)] Successfully send 'REPLCONF' to the master server {host}:{port}, response: {response}")
    except Exception as e:
        print(f"fail: {e}")

    try:
        # Ready to receive RDB file
        command = ['PSYNC','?','-1']
        response = redis_protocol_encoder('array', command).encode()
        respond(replica_to_master_socket,response)

        # Ready to receive RDB file
        # request = replica_to_master_socket.recv(1024)
        # print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Redis server get a request: {request}")
        thread =  threading.Thread(
            target=handle_connection, args=[replica_to_master_socket, threading.current_thread().name]
        )
        thread.start()


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
                target=handle_connection, args=[client_socket, threading.current_thread().name]
            )
            thread.start()
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Starting thread {thread.name} for start listening to connection")
        except Exception as e:
            print(f"Error accepting connection: {e}")



if __name__ == "__main__":
    start_server()
