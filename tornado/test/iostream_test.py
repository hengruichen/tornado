from tornado.concurrent import Future
from tornado import gen
from tornado import netutil
from tornado.ioloop import IOLoop
from tornado.iostream import (
    IOStream,
    SSLIOStream,
    PipeIOStream,
    StreamClosedError,
    _StreamBuffer,
)
from tornado.httputil import HTTPHeaders
from tornado.locks import Condition, Event
from tornado.log import gen_log
from tornado.netutil import ssl_options_to_context, ssl_wrap_socket
from tornado.tcpserver import TCPServer
from tornado.testing import (
    AsyncHTTPTestCase,
    AsyncHTTPSTestCase,
    AsyncTestCase,
    bind_unused_port,
    ExpectLog,
    gen_test,
)
from tornado.test.util import (
    skipIfNonUnix,
    refusing_port,
    skipPypy3V58,
    ignore_deprecation,
)
from tornado.web import RequestHandler, Application
import asyncio
import errno
import hashlib
import logging
import os
import platform
import random
import socket
import ssl
import typing
from unittest import mock
import unittest


def _server_ssl_options():
    return dict(
        certfile=os.path.join(os.path.dirname(__file__), "test.crt"),
        keyfile=os.path.join(os.path.dirname(__file__), "test.key"),
    )


class HelloHandler(RequestHandler):
    def get(self):
        self.write("Hello")


class TestIOStreamWebMixin(object):
    def _make_client_iostream(self):
        raise NotImplementedError()

    def get_app(self):
        return Application([("/", HelloHandler)])

    def test_connection_closed(self: typing.Any):
        # When a server sends a response and then closes the connection,
        # the client must be allowed to read the data before the IOStream
        # closes itself.  Epoll reports closed connections with a separate
        # EPOLLRDHUP event delivered at the same time as the read event,
        # while kqueue reports them as a second read/write event with an EOF
        # flag.
        response = self.fetch("/", headers={"Connection": "close"})
        response.rethrow()

    @gen_test
    def test_read_until_close(self: typing.Any):
        stream = self._make_client_iostream()
        yield stream.connect(("127.0.0.1", self.get_http_port()))
        stream.write(b"GET / HTTP/1.0\r
\r
")

        data = yield stream.read_until_close()
        self.assertTrue(data.startswith(b"HTTP/1.1 200"))
        self.assertTrue(data.endswith(b"Hello"))

    @gen_test
    def test_read_zero_bytes(self: typing.Any):
        self.stream = self._make_client_iostream()
        yield self.stream.connect(("127.0.0.1", self.get_http_port()))
        self.stream.write(b"GET / HTTP/1.0\r
\r
")

        # normal read
        data = yield self.stream.read_bytes(9)
        self.assertEqual(data, b"HTTP/1.1 ")

        # zero bytes
        data = yield self.stream.read_bytes(0)
        self.assertEqual(data, b"")

        # another normal read
        data = yield self.stream.read_bytes(3)
        self.assertEqual(data, b"200")

        self.stream.close()

    @gen_test
    def test_write_while_connecting(self: typing.Any):
        stream = self._make_client_iostream()
        connect_fut = stream.connect(("127.0.0.1", self.get_http_port()))
        # unlike the previous tests, try to write before the connection
        # is complete.
        write_fut = stream.write(b"GET / HTTP/1.0\r
Connection: close\r
\r
")
        self.assertFalse(connect_fut.done())

        # connect will always complete before write.
        it = gen.WaitIterator(connect_fut, write_fut)
        resolved_order = []
        while not it.done():
            yield it.next()
            resolved_order.append(it.current_future)
        self.assertEqual(resolved_order, [connect_fut, write_fut])

        data = yield stream.read_until_close()
        self.assertTrue(data.endswith(b"Hello"))

        stream.close()

    @gen_test
    def test_future_interface(self: typing.Any):
        """Basic test of IOStream's ability to return Futures."""
        stream = 
# ... [truncated] ...
f.assertEqual(data, b"wor")

        ws.close()

        data = yield rs.read_until_close()
        self.assertEqual(data, b"ld")

        rs.close()

    @gen_test
    def test_pipe_iostream_big_write(self):
        rs, ws = yield self.make_iostream_pair()

        NUM_BYTES = 1048576

        # Write 1MB of data, which should fill the buffer
        ws.write(b"1" * NUM_BYTES)

        data = yield rs.read_bytes(NUM_BYTES)
        self.assertEqual(data, b"1" * NUM_BYTES)

        ws.close()
        rs.close()

    @gen_test
    def test_stream_buffer(self):
        """Unit tests for the private _StreamBuffer class."""
        from tornado.iostream import _StreamBuffer

        buf = _StreamBuffer()
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 4)
        buf.advance(33)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(2)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 1)
        for i in range(9):
            buf.append(b"x")
        self.assertEqual(len(buf._buffers), 2)
        buf.advance(10)
        self.assertEqual(len(buf._buffers), 1)
        buf.advance(8)
        self.assertEqual(len(buf._buffers), 0)
        self.assertEqual(len(buf), 0)

        # Test internal algorithm
        buf = self.make_streambuffer(10)
        for i in range(3):
            buf.append(b"x" * 11)
        self.assertEqual(len(buf._buffers), 3)
        buf.append(b"y")
        self.assertEqual(len(buf._buffers), 4)
        buf.append(b"z")
        self.assertEqual(len(buf._buffers), 