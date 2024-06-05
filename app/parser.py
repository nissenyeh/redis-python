

class Split:

  def is_simple_str(str):
    return str.startswith('+')
  
  def is_bulk_string(str):
    return str.startswith('$')
  
  def is_array(str):
    return str.startswith('*')

  def split_redis_protocol(input: str):  

    result = []
    elements = input.split('\r\n')

    index = 0
    while index < len(elements):
      element = elements[index]

      if Split.is_simple_str(element):
          split = elements[index]+'\r\n'
          result.append(split)
          index+=1

      elif Split.is_bulk_string(element):
        split = elements[index]+'\r\n'+elements[index+1]+'\r\n'
        result.append(split)
        index+=2

      elif Split.is_array(element):
        num = element.lstrip('*')
        
        array_length = int(element.lstrip('*')) # e.g: *3
        element_number = array_length * 2 # e.g: 
        num = 0
        re = ''
        while num <= element_number:
            re += elements[index]+'\r\n'
            index +=1
            num +=1
        result.append(re)
      else:
        break

    return result
     

  def parse_str_to_command_list(request_str: str) -> list:
    if ';' in request_str: # mutiple command
        requests = request_str.split(';')
    else:
        requests = [request_str]
    return requests


def split_commands(request_str, is_local_command=False):
  if is_local_command:
    requests = Split.parse_str_to_command_list(request_str)
  else:
    requests = Split.split_redis_protocol(request_str)
  return requests
   

def encode_for_local_command(str_command):
  if ' ' in str_command:
      str_command :list = redis_protocol_encoder('array', str_command.split())
  else:
      str_command :str = redis_protocol_encoder('str', str_command)
  return str_command

def decode_commands_by_redis_protocol(str_commands: list, is_local_command=False) -> list:
  commands = []
  for str_command in str_commands:
    if is_local_command:
      str_command = encode_for_local_command(str_command)
    parsed = redis_protocol_parser(str_command) # => ['echo', 'hello'] or ping
    command = parsed if isinstance(parsed, list) else [parsed]
    commands.append(command)
  return commands

# Sync as Main
def parse_request(request: bytes, is_local_command=False) ->list:
    
    request_str: str = request.decode(errors='ignore') 
    str_commands = split_commands(request_str, is_local_command)
    commands = decode_commands_by_redis_protocol(str_commands, is_local_command)

    return commands

def redis_protocol_parser(input): 

  if input.startswith('+'):
    return input.lstrip('+').rstrip('\r\n')
  
  if input.startswith(':'):
    return int(input.lstrip(':').rstrip('\r\n'))

  if input.startswith('$'):
    return input.split('\r\n')[1]
  
  if input.startswith('*'):
    array_len = int(input.split('\r\n')[0].lstrip('*'))
    array = []
    lines = input.split('\r\n')
    index = 1
    for _ in range(array_len):
      code = lines[index]
      if code.startswith(':'):
        char = redis_protocol_parser(code)
        array.append(char)
        index += 1
      elif code.startswith('$'):
        length = int(code.lstrip('$'))
        next_char = lines[index + 1]
        if len(next_char) == length:
          char = next_char
          array.append(char)
        index += 2
    return array



def redis_protocol_encoder(type , input) -> str: # local
  if type == 'str':
    symbol = '+'
    return f"{symbol}{input}\r\n"

  if type == 'int':
    symbol = ':'
    return f"{symbol}{input}\r\n"
  
  if type == 'bulk':
    symbol = '$'
    res = str(len(input)) + "\r\n" + input + "\r\n"
    return f"{symbol}{res}"
  
  if type == 'array' and len(input) >= 1:
    symbol = '*'
    res = f"{len(input)}\r\n"
    for i in input:
      if isinstance(i, str):
        res += redis_protocol_encoder('bulk', i)
      if isinstance(i, int):
        res += redis_protocol_encoder('int', i)
 
    return f"{symbol}{res}"
    



