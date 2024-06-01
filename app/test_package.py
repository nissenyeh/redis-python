import pytest
from package import redis_protocol

def test_to_redis_protocol_for_simple_strings() -> str:
    data = 'ok'
    result = redis_protocol('str', data)
    assert result == "+ok\r\n"

def test_redis_protocol_for_integer() -> str:
    data = 2
    result = redis_protocol('int', data)
    assert result == ":2\r\n"

def test_to_redis_protocol_for_bulk_string() -> str:
    data = "hello"
    result = redis_protocol('bulk', data)
    assert result == "$5\r\nhello\r\n"


def test_to_redis_protocol_for_array() -> str:
    data =  [1,2,3]
    result = redis_protocol('array', data)
    assert result == "*3\r\n:1\r\n:2\r\n:3\r\n"

    data =  [1,2,'hello']
    result = redis_protocol('array', data)
    assert result == "*3\r\n:1\r\n:2\r\n$5\r\nhello\r\n"
    
    data = ["REPLCONF", "ACK", "0"]
    result = redis_protocol('array', data)
    assert result == "*3\r\n$5\r\n:2\r\n$5\r\nhello\r\n"
    print(result)


if __name__ == "__main__":
    pytest.main()