from app.package.command_parser import redis_protocol_encoder
from app.package.rdb_reader import RDB_PARSER

def handel_ping(commands, args):
    # commands: ['ping']
    role = args['role']
    if role == 'master':
        return redis_protocol_encoder('str', 'PONG').encode()
    if role == 'slave':
        return

def handle_echo(commands):
    # commands: ['echo'. 'hello']
    return redis_protocol_encoder('str', commands[1]).encode()


def handle_config(commands, args):
    if commands[1] == 'get':
        if commands[2] == 'dir':
            result = ['dir', args['dir']]
            return redis_protocol_encoder('array', result).encode()
        elif commands[2] == 'dbfilename':
            result = ['dbfilename', args['dbfilename']]
            return redis_protocol_encoder('array', result).encode()
    

def handle_info(commands, args):
    role = args['role']
    res = f'role:{role}\r\n'
    res += 'master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n'
    res += 'master_repl_offset:0\r\n'
    return redis_protocol_encoder('bulk', res).encode()

def handle_keys(commands, args):
    if commands[1] == '*':
        data = RDB_PARSER(args['dir'], args['dbfilename'])
        keys = data.getKeys()
        print(f"keys:{keys}")
        print(f"redis_protocol_encoder('array', keys):{redis_protocol_encoder('array', keys)}")
        
        return redis_protocol_encoder('array', keys).encode()