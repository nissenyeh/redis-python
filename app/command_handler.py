from app.parser import redis_protocol_encoder

def handel_ping(role, args):
    role = args['role']
    if role == 'master':
        return redis_protocol_encoder('str', 'PONG').encode()
    if role == 'slave':
        return

def handle_echo(commands):
    
    return redis_protocol_encoder('str', commands[1]).encode()

def handle_config(commands, args):
    if commands[1] == 'get':
        if commands[2] == 'dir':
            result = ['dir', args['dir']]
        elif commands[2] == 'dbfilename':
            result = ['dbfilename', args['dbfilename']]
        return redis_protocol_encoder('array', result).encode()