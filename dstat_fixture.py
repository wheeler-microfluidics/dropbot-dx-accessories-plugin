"""
Copyright 2015 Christian Fobel

This file is part of dropbot_dx_plugin.

dropbot_dx_plugin is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

dropbot_dx_plugin is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with dropbot_dx_plugin.  If not, see <http://www.gnu.org/licenses/>.
"""
from collections import OrderedDict
from datetime import datetime
import time
import logging

import zmq
from zmq.eventloop.ioloop import ZMQIOLoop
from zmq.eventloop.zmqstream import ZMQStream


class DstatFixture(object):
    def __init__(self, rep_uri):
        self.context = zmq.Context.instance()
        self.socks = OrderedDict()
        self.rep_uri = rep_uri
        self._start_time = datetime.now()

    def start(self, delay_s=0):
        self.reset_socks()
        loop = ZMQIOLoop()
        stream = ZMQStream(self.socks['rep'], io_loop=loop)
        def respond(msg):
            print 'got %s' % msg
            request = msg[0]
            if request == 'start':
                stream.send('started')
                self._start_time = datetime.now()
            elif request == 'notify_completion':
                time.sleep(delay_s)
                stream.send('completed')

        stream.on_recv(respond)
        loop.start()

    def close_socks(self):
        # Close any currently open sockets.
        for name, sock in self.socks.iteritems():
            sock.close()
        self.socks = OrderedDict()

    def reset_socks(self):
        self.close_socks()
        # Service address is available
        self.socks['rep'] = zmq.Socket(self.context, zmq.REP)
        self.socks['rep'].bind(self.rep_uri)

    def __del__(self):
        self.close_socks()


def parse_args(args=None):
    """Parses arguments, returns (options, args)."""
    import sys
    from argparse import ArgumentParser

    if args is None:
        args = sys.argv

    parser = ArgumentParser(description='D-Stat test fixture.  Implements '
                            'D-Stat ZeroMQ remote interface for testing.')
    parser.add_argument('bind_uri', default=r'tcp://*:12345')
    parser.add_argument('delay_s', nargs='?', type=float, default=0)

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()

    fixture = DstatFixture(args.bind_uri)
    fixture.start(args.delay_s)
