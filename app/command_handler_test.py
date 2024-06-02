import pytest
from command_handler import handle_echo, handel_ping, handle_config

class TestRedisProtocolEncoder:
    def test_handle_echo(self) -> str:
        args = ['echo', 'Nissen']
        result = handle_echo(args)
        assert result == b"+Nissen\r\n"

    def test_handle_ping(self) -> str:
      args = {
        'role' : 'master'
      }
      commands = ['ping']
      result = handel_ping(commands, args)
      assert result == b"+PONG\r\n"

    def test_handle_config(self) -> str:
      args = {
          'dir': '/tmp/redis-files',
          'filename': 'rdbfile', 
      }
      commands = ['config', 'get', 'dir']
      result = handle_config(commands, args)
      assert result == b"*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n"

      commands = ['config', 'get', 'filename']
      result = handle_config(commands, args)
      assert result == b"*2\r\n$8\r\nfilename\r\n$7\r\nrdbfile\r\n"
      
