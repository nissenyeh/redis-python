# Uncomment this to pass the first stage
import socket
import threading


def handle_connection(client_socket):
    while True:
        request: bytes = client_socket.recv(1024) # 獲取客戶端發送的訊息
        message: str = request.decode()
        if "ping" in message.lower():
            client_socket.sendall(b'+PONG\r\n')
  

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    client_socket, _ = server_socket.accept() # 等待客戶端連接

    threading.Thread(
            target=handle_connection, args=[client_socket]
    ).start()


if __name__ == "__main__":
    main()
