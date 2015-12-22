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
import sys, traceback
from functools import wraps
import subprocess
import logging

import gtk
from path_helpers import path
from flatland import Boolean, Form, String
from microdrop.plugin_helpers import (AppDataController, StepOptionsController,
                                      get_plugin_info)
from microdrop.plugin_manager import (PluginGlobals, Plugin, IPlugin,
                                      implements, emit_signal)
from microdrop.app_context import get_app
import dropbot_dx as dx
from dstat_remote import DstatRemote
import gobject
from pandas_helpers import series_to_gtk_form

logger = logging.getLogger(__name__)


PluginGlobals.push_env('microdrop.managed')


def is_connected(_lambda):
    def wrapper(f):
        @wraps(f)
        def wrapped(self, *f_args, **f_kwargs):
            if not self.connected():
                logger.warning('Dropbot DX not connected.')
            else:
                f(self, *f_args, **f_kwargs)
        return wrapped
    return wrapper(_lambda)


class DropbotDxPlugin(Plugin, AppDataController, StepOptionsController):
    """
    This class is automatically registered with the PluginManager.
    """
    implements(IPlugin)
    version = get_plugin_info(path(__file__).parent).version
    plugin_name = get_plugin_info(path(__file__).parent).plugin_name

    '''
    AppFields
    ---------

    A flatland Form specifying application options for the current plugin.
    Note that nested Form objects are not supported.

    Since we subclassed AppDataController, an API is available to access and
    modify these attributes.  This API also provides some nice features
    automatically:
        -all fields listed here will be included in the app options dialog
            (unless properties=dict(show_in_gui=False) is used)
        -the values of these fields will be stored persistently in the microdrop
            config file, in a section named after this plugin's name attribute
    '''
    AppFields = Form.of(
        String.named('dstat_uri').using(default='', optional=True),
    )

    '''
    StepFields
    ---------

    A flatland Form specifying the per step options for the current plugin.
    Note that nested Form objects are not supported.

    Since we subclassed StepOptionsController, an API is available to access and
    modify these attributes.  This API also provides some nice features
    automatically:
        -all fields listed here will be included in the protocol grid view
            (unless properties=dict(show_in_gui=False) is used)
        -the values of these fields will be stored persistently for each step
    '''
    StepFields = Form.of(
        Boolean.named('magnet_engaged').using(default=False, optional=True),
        Boolean.named('dstat_enabled').using(default=False, optional=True),
    )

    def __init__(self):
        self.name = self.plugin_name
        self.timeout_id = None
        self.dstat_remote = None
        self.dropbot_dx_remote = None
        self.initialized = False

    def connected(self):
        return (self.dropbot_dx_remote is not None)

    def on_plugin_enable(self):
        try:
            self.dropbot_dx_remote = dx.SerialProxy()
        except IOError:
            logger.warning('Could not connect to Dropbot DX.')

        if not self.initialized:
            app = get_app()
            self.tools_menu_item = gtk.MenuItem("DropBot DX")
            app.main_window_controller.menu_tools.append(
                self.tools_menu_item)
            self.tools_menu = gtk.Menu()
            self.tools_menu.show()
            self.tools_menu_item.set_submenu(self.tools_menu)
            menu_item = gtk.MenuItem("Launch Dstat interface")
            self.tools_menu.append(menu_item)
            menu_item.connect("activate", self.on_launch_dstat_inteface)
            menu_item.show()
            self.edit_config_menu_item = \
                gtk.MenuItem("Edit configuration settings")
            self.tools_menu.append(self.edit_config_menu_item)
            self.edit_config_menu_item.connect("activate",
                                               self.on_edit_configuration)
            self.initialized = True
            
        self.tools_menu_item.show()
        if self.connected():
            self.edit_config_menu_item.show()

    def on_launch_dstat_inteface(self, widget, data=None):
        subprocess.Popen([sys.executable, '-m', 'dstat_interface'])

    def on_plugin_disable(self):
        if self.connected():
            self.dropbot_dx_remote.terminate()
        self.tools_menu_item.hide()

    @is_connected
    def on_protocol_run(self):
        pass

    def on_step_run(self):
        """
        Handler called whenever a step is executed. Note that this signal
        is only emitted in realtime mode or if a protocol is running.

        Plugins that handle this signal must emit the on_step_complete
        signal once they have completed the step. The protocol controller
        will wait until all plugins have completed the current step before
        proceeding.

        return_value can be one of:
            None
            'Repeat' - repeat the step
            or 'Fail' - unrecoverable error (stop the protocol)
        """
        app = get_app()
        logger.info('[DropbotDxPlugin] on_step_run(): step #%d',
                    app.protocol.current_step_number)
        # If `acquire` is `True`, start acquisition
        options = self.get_step_options()
        if self.connected():
            if not (self.dropbot_dx_remote
                    .update_state(light_enabled=not options['dstat_enabled'],
                                  magnet_engaged=options['magnet_engaged'])):
                logger.error('Could not set state of Dropbot DX board.')
                emit_signal('on_step_complete', [self.name, 'Fail'])
            if options['dstat_enabled']:
                app_values = self.get_app_values()
                try:
                    if self.timeout_id is not None:
                        # Timer was already set, so cancel previous timer.
                        gobject.source_remove(self.timeout_id)
                    self.dstat_remote = DstatRemote(app_values['dstat_uri'])
                    self.dstat_remote.start_acquisition()
                    # Check every 100ms to see if remote command has completed.
                    self.timeout_id = gobject.timeout_add(100,
                                                          self
                                                          .remote_check_tick)
                except:
                    print "Exception in user code:"
                    print '-'*60
                    traceback.print_exc(file=sys.stdout)
                    print '-'*60
                    # An error occurred while initializing Analyst remote
                    # control.
                    emit_signal('on_step_complete', [self.name, 'Fail'])
        else:
            emit_signal('on_step_complete', [self.name, None])

    def remote_check_tick(self):
        '''
         1. Check if there is a D-Stat acquisition has been started.
         2. If (1), check to see if acquisition is finished.
         3. If (2), emit `on_step_complete` signal.
        '''
        if self.dstat_remote is not None:
            try:
                if self.dstat_remote.acquisition_complete():
                    # Acquisition is complete so notify step complete.
                    self.dstat_remote.reset()
                    if not (self.dropbot_dx_remote
                            .update_state(light_enabled=True)):
                        raise IOError('Could not enable light.')
                    emit_signal('on_step_complete', [self.name, None])
                    self.timeout_id = None
                    self.dstat_remote = None
                    return False
                else:
                    print "Waiting for acquisition to complete..."
            except:
                print "Exception in user code:"
                print '-'*60
                traceback.print_exc(file=sys.stdout)
                print '-'*60
                emit_signal('on_step_complete', [self.name, 'Fail'])
                self.timeout_id = None
                self.dstat_remote = None
                return False
        return True

    def on_step_options_swapped(self, plugin, old_step_number, step_number):
        """
        Handler called when the step options are changed for a particular
        plugin.  This will, for example, allow for GUI elements to be
        updated based on step specified.

        Parameters:
            plugin : plugin instance for which the step options changed
            step_number : step number that the options changed for
        """
        pass

    def on_step_swapped(self, old_step_number, step_number):
        """
        Handler called when the current step is swapped.
        """

    def on_edit_configuration(self, widget=None, data=None):
        '''
        Display a dialog to manually edit the configuration settings.
        '''
        config = self.dropbot_dx_remote.config
        valid, response = series_to_gtk_form(config,
            title='Edit configuration settings'
        )
        if valid:
            self.dropbot_dx_remote.update_config(**response)


PluginGlobals.pop_env()
