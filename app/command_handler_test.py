import pytest
from command_handler import handle_echo, handel_ping, handle_config, handle_info

class TestRedisProtocolEncoder:
    def test_handle_echo(self) -> str:
        commands = ['echo', 'Nissen']
        result = handle_echo(commands)
        assert result == b"+Nissen\r\n"

    def test_handle_ping(self) -> str:
      commands = ['ping']
      args = {
        'role' : 'master'
      }
      result = handel_ping(commands, args)
      assert result == b"+PONG\r\n"

    def test_handle_config(self) -> str:
      args = {
          'dir': '/tmp/redis-files',
          'dbfilename': 'rdbfile', 
      }

      commands = ['config', 'get', 'dir']
      result = handle_config(commands, args)
      assert result == b"*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n"

      commands = ['config', 'get', 'dbfilename']
      result = handle_config(commands, args)
      assert result == b"*2\r\n$10\r\ndbfilename\r\n$7\r\nrdbfile\r\n"

    def test_handle_info(self) -> str:
      args = {
          'role': 'master',
      }
      commands = ['info']
      result = handle_info(commands, args)
      assert result == b"$91\r\nrole:master\r\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\nmaster_repl_offset:0\r\n\r\n"

