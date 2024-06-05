import pytest
# from parser import redis_protocol_encoder, redis_protocol_parser
from app.parser import redis_protocol_encoder, redis_protocol_parser

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
    

if __name__ == "__main__":
    pytest.main()