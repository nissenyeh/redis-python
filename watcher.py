import subprocess
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("--port", type=int, default=6379)
parser.add_argument("--replicaof", type=str, default='')
parser.add_argument("--dir", type=str, default='')
parser.add_argument("--dbfilename", type=str, default='')


port = parser.parse_args().port
replicaof = parser.parse_args().replicaof
dir = parser.parse_args().dir
dbfilename = parser.parse_args().dbfilename


class ChangeHandler(FileSystemEventHandler):
    def __init__(self, script):
        self.script = script
        self.process = None
        self.start_script()

    def start_script(self):
        if self.process:
            self.process.terminate()
        commands = ['sh', self.script, '--local', str(False) ,'--port', str(port), '--replicaof', replicaof]
        if dir:
            commands.extend(['--dir', dir])
        if dbfilename:
            commands.extend(['--dbfilename', dbfilename])
        self.process = subprocess.Popen(commands)

    def on_modified(self, event):
        if event.src_path.endswith('app/main.py'):
            print(f'{event.src_path} has been modified, restarting script...')
            self.start_script()

if __name__ == "__main__":
    path = "app/main.py"
    event_handler = ChangeHandler('spawn_redis_server.sh')
    observer = Observer()
    observer.schedule(event_handler, path='app', recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()