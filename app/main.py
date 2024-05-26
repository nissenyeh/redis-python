# Uncomment this to pass the first stage
import socket
import threading


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
    print(f'request_str: {request_str}')
    # e.g: *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
    # ['*2', '$4', 'ECHO', '$3', 'hey', '']
    if '\r\n' in request_str:
        parse_request = [item for item in request_str.split('\r\n')  if (item and not '*' in item and  not '$' in item) ]
    else: # e.g: ping 
        parse_request = [request_str]
    print(parse_request)
    return parse_request
        

def parse_command(parser_request)-> bytes:
     print(f'parser_request: {parser_request}')

     if "ping" in parser_request[0].lower():
         return b'+PONG\r\n'
     # *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
     if "echo" in parser_request[0].lower():
         return f'+{parser_request[1]}\r\n'.encode()



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
