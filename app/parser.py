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
    



