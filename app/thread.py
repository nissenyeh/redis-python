import threading
import time

count = 0

def update_number():
    global count
    print(f'update_number - count:{count}') # 0
    time.sleep(1)  
    for _ in range(10):
        count += 1
        print(f'update_number - count:{count}') # 0 -> 1 -> 2
        time.sleep(1)  
        
def read_number():
    global count
    print(f'read_number - count:{count}') # 0
    time.sleep(1)  
    for _ in range(10):
        time.sleep(1)  
        print(f'read_number - count:{count}') # 0 -> 0 or 0->1->2
        

thread1 = threading.Thread(target=update_number)
thread2 = threading.Thread(target=read_number)

thread1.start()
thread2.start()

#