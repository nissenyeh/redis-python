# Redis Server


This is a Python-based  Redis server.

- Redis is an in-memory database that persists on disk.
- 


## Support command


- Support the following commands
    - PING: Send a greeting to the server
    - ECHO:
    - SET & GET: Store and retrieve data
    - INFO
    - CONFIG
- Support multiple concurrent commands
- Support synchronization with Replica
- Data persistence

## DEMO

1. Basic  Command
  - PING: Send a greeting to the server
  - ECHO:
  - SET & GET: Store and retrieve data
  - INFO
  - CONFIG



2. (Master - Replica) Partial Resynchronization


## testing in local

```
./spawn_redis_server.sh --port 6379
or
python watcher.py --port 6379 # hot load

```

- Run a replica server
`./spawn_redis_server.sh --port <port> --replicaof "localhost <master-prot>"`

```

./spawn_redis_server.sh --port 6380 --replicaof "localhost 6379"
or
python watcher.py --port 6380 --replicaof "localhost 6379"
```

```
telnet localhost 6379
```
