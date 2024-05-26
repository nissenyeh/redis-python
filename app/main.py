# Uncomment this to pass the first stage
import socket
import threading
import re


def handle_connection(client_socket):
    while True:
        request: bytes = client_socket.recv(1024) # 獲取客戶端發送的訊息
        if not request:
            break;
        print(f'request: {request}')
        parser_request: list =  parse_request(request)
        response = parse_command(parser_request)
        client_socket.send(response)

def parse_request(request) ->list:
    request_str: str = request.decode()
    parse_request: list = []
    print(f'request_str: {request_str}')
    # e.g: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    # ['*2', '$4', 'ECHO', '$3', 'hey', '']
    # e.g: *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    # ['*3', 'SET', '3', 'bar', '']
    if '\r\n' in request_str:
        parse_request = re.split(r'\r\n|\\r\\n', request_str)[2:-1:2]
        print(f'parser_request: {parse_request}')
    else: # e.g: ping 
        parse_request = [request_str]

    print(f'parser_request: {parse_request}')

    return parse_request
        
cache = {}

def parse_command(parser_request)-> bytes:
    

     if not parser_request:
        return b'+No\r\n'

     if "ping" in parser_request[0].lower():
         return b'+PONG\r\n'
     # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
     if "echo" in parser_request[0].lower():
         return f'+{parser_request[1]}\r\n'.encode()
    # e.g: SET foo bar
    # *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n

     if 'set' in parser_request[0].lower():
         cache[parser_request[1]] = parser_request[2]
         return b'+OK\r\n'
     
     # e.g: get foo  => bar
     if 'get' in parser_request[0].lower():
         res = cache[parser_request[1]]
         return f'+{res}\r\n'.encode()



def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Uncomment this to pass the first stage
    while True:
        client_socket, _ = server_socket.accept() # 等待客戶端連接
        threading.Thread(
            target=handle_connection, args=[client_socket]
        ).start()


if __name__ == "__main__":
    main()
