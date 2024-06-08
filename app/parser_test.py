import pytest
# from parser import redis_protocol_encoder, redis_protocol_parser
from app.parser import (
    encode_for_local_command,
    Split,
    parse_request,
    redis_protocol_encoder,
    redis_protocol_parser
)

class TestRedisProtocolEncoder:
    def test_to_redis_protocol_for_simple_strings(self) -> str:
        data = 'ok'
        result = redis_protocol_encoder('str', data)
        assert result == "+ok\r\n"

    def test_redis_protocol_for_integer(self) -> str:
        data = 2
        result = redis_protocol_encoder('int', data)
        assert result == ":2\r\n"

    def test_to_redis_protocol_for_bulk_string(self) -> str:
        data = "hello"
        result = redis_protocol_encoder('bulk', data)
        assert result == "$5\r\nhello\r\n"


    def test_to_redis_protocol_for_array(self) -> str:
        data =  ['array']
        result = redis_protocol_encoder('array', data)
        assert result == "*1\r\n$5\r\narray\r\n"

        data =  [1,2,3]
        result = redis_protocol_encoder('array', data)
        assert result == "*3\r\n:1\r\n:2\r\n:3\r\n"

        data =  [1,2,'hello']
        result = redis_protocol_encoder('array', data)
        assert result == "*3\r\n:1\r\n:2\r\n$5\r\nhello\r\n"
    

class TestRedisProtocolParser:
    def test_parse_redis_protocol_for_integer(self):
        str = "+ok\r\n"
        result = redis_protocol_parser(str)
        assert result == 'ok'


    def test_parse_redis_protocol_for_simple_string(self):
        str =  ":2\r\n"
        result = redis_protocol_parser(str)
        assert result == 2

        str =  ":100\r\n"
        result = redis_protocol_parser(str)
        assert result == 100

    def test_parse_redis_protocol_for_bulk_string(self):
        str =  "$5\r\nhello\r\n"
        result = redis_protocol_parser(str)
        assert result == 'hello'



    def test_split_redis_protocol(self):

        str =  "+ping\r\n"
        result = Split.split_redis_protocol(str)
        assert result[0] == ['ping']

        str =  "$5\r\nhello\r\n$6\r\nNissen\r\n"
        result = Split.split_redis_protocol(str)
        assert result[0] == ['hello']
        assert result[1] == ['Nissen']

        str =  "+ping\r\n$5\r\nhello\r\n$6\r\nNissen\r\n"
        result = Split.split_redis_protocol(str)
        assert result[0] == ['ping']
        assert result[1] == ['hello']
        assert result[2] == ['Nissen']

        str = '*3\r\n$3\r\nSET\r\n$6\r\norange\r\n$9\r\npineapple\r\n*3\r\n$3\r\nSET\r\n$4\r\npear\r\n$9\r\nraspberry\r\n*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n'
        result = Split.split_redis_protocol(str)
        assert result[0] == ['SET', 'orange', 'pineapple']
        assert result[1] == ['SET', 'pear', 'raspberry']
        assert result[2] == ['REPLCONF', 'GETACK', '*']


    def test_parse_redis_protocol_for_array(self):
        str =  "*3\r\n:1\r\n:2\r\n:3\r\n"
        result = redis_protocol_parser(str)
        assert result == [1,2,3]

        str =  "*3\r\n:1\r\n:2\r\n$5\r\nhello\r\n"
        result = redis_protocol_parser(str)
        assert result == [1,2,'hello']

        str =  "*4\r\n:1\r\n$3\r\nten\r\n:2\r\n$5\r\nhello\r\n"
        result = redis_protocol_parser(str)
        assert result == [1,'ten',2,'hello']

        str =  "*2\r\n$4\r\nkeys\r\n$1\r\n*\r\n"
        result = redis_protocol_parser(str)
        assert result == ['keys', '*']


class TestCommandParser:

    def test_parse_request(self):
        data = b'+ping\r\n'
        result = parse_request(data, is_local_command=False)
        assert result == [['ping']]


        data = b'pingg' # local allowed only
        result = parse_request(data, is_local_command=True)
        assert result == [['pingg']]

        data = b'echo hello' # local allowed only
        result = parse_request(data, is_local_command=True)
        assert result == [['echo', 'hello']]

        data = b'*3\r\n$3\r\nSET\r\n$9\r\npineapple\r\n$6\r\nbanana\r\n*3\r\n$3\r\nSET\r\n$4\r\npear\r\n$9\r\npineapple\r\n*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n' # local allowed only
        result = parse_request(data, is_local_command=False)
        assert result[0] == ['SET', 'pineapple', 'banana']
        assert result[1] == ['SET', 'pear', 'pineapple']
        assert result[2] == ['REPLCONF', 'GETACK', '*']


    def test_parse_str_to_local_commands(self):
        data = 'ping'
        result = Split.parse_str_to_command_list(data)
        assert result == [['ping']]

        data = 'ping ping'
        result = Split.parse_str_to_command_list(data)
        assert result == [['ping','ping']]



    def test_encode_for_local_command(self):
        data = "echo hello"
        result = encode_for_local_command(data)
        assert result == "*2\r\n$4\r\necho\r\n$5\r\nhello\r\n"

        data = "ping"
        result = encode_for_local_command(data)
        assert result == "+ping\r\n"


if __name__ == "__main__":
    pytest.main()