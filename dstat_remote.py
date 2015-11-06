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
import logging

import zmq


class DstatRemote(object):
    def __init__(self, req_uri):
        self.context = zmq.Context.instance()
        self.socks = OrderedDict()
        self.timeout_id = None
        self._start_time = None
        self.req_uri = req_uri

    def close_socks(self):
        # Close any currently open sockets.
        for name, sock in self.socks.iteritems():
            sock.close()
        self.socks = OrderedDict()

    def reset_socks(self):
        self.close_socks()
        if self.timeout_id is not None:
            gtk.timeout_remove(self.timeout_id)
            self.timeout_id = None
        # Service address is available
        self.socks['req'] = zmq.Socket(self.context, zmq.REQ)
        self.socks['req'].connect(self.req_uri)

    def __del__(self):
        self.close_socks()

    def _command(self, command, timeout_ms=None):
        self.reset_socks()
        # Request DStat to begin acquisition.
        self.socks['req'].send(command)
        if timeout_ms is not None and (not self.socks['req']
                                       .poll(timeout=timeout_ms)):
            self.reset_socks()
            raise IOError('[DstatRemote] Timed-out waiting for a response.')
        else:
            # Response is ready.
            return self.socks['req'].recv()

    def start_acquisition(self, timeout_ms=None):
        response = self._command('start', timeout_ms)
        if response != 'started':
            raise IOError('[DstatRemote] Acquisition not started (got: "%s")' %
                          response)
        else:
            logging.info('[DstatRemote] Service started successfully.')

    def acquisition_complete(self, timeout_ms=None):
        try:
            response = self._command('notify_completion', timeout_ms)
        except IOError:
            return False
        else:
            if response == 'completed':
                logging.info('[DstatRemote] Service completed task '
                             'successfully.')
                return True
            else:
                logging.error('[DstatRemote] Unexpected response: %s' %
                              response)
                return False

    def acquire(self):
        self.start_acquire()
        return self.completed()

    def reset(self):
        self.reset_socks()
