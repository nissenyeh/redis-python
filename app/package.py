
def your_function():
  return 1


def redis_protocol(type , input) -> str: # local
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
  
  if type == 'array' and len(input) > 1:
    symbol = '*'
    res = f"{len(input)}\r\n"
    for i in input:
      if isinstance(i, str):
        res += redis_protocol('bulk', i)
      if isinstance(i, int):
        res += redis_protocol('int', i)
    print(f'res:{res}')
 
    return f"{symbol}{res}"
    



