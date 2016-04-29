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
from functools import wraps
import itertools
import logging
import re
import subprocess
import sys, traceback

import gtk
from path_helpers import path
from flatland import Boolean, Form, String, Float
from flatland.validation import ValueAtLeast, ValueAtMost, Validator
from pygtkhelpers.ui.extra_widgets import Filepath
from microdrop.plugin_helpers import (StepOptionsController,
                                      get_plugin_info, hub_execute)
from microdrop.plugin_manager import (PluginGlobals, Plugin, IPlugin,
                                      implements, emit_signal,
                                      ScheduleRequest, get_service_instance_by_name)
from microdrop.app_context import get_app
import dropbot_dx as dx
import gobject
from pygtkhelpers.ui.extra_dialogs import yesno, FormViewDialog
from pygtkhelpers.utils import dict_to_form
from arduino_helpers.upload import upload_firmware

logger = logging.getLogger(__name__)


PluginGlobals.push_env('microdrop.managed')


def is_connected(_lambda):
    '''
    Decorator to check if DropBot DX instrument is connected.

    If not connected, warning is logged, but wrapped function is not called.
    '''
    def wrapper(f):
        @wraps(f)
        def wrapped(self, *f_args, **f_kwargs):
            if not self.connected():
                logger.warning('DropBot DX not connected.')
            else:
                f(self, *f_args, **f_kwargs)
        return wrapped
    return wrapper(_lambda)


def get_unique_path(filepath):
    '''
    Append `-###` to the base name of a file until a file path is found that
    does not exist.

    Args
    ----

        filepath (str) : Full file path to target file.

    Returns
    -------

        (path) : Full path where no file exists.
    '''
    filepath = path(filepath)
    cre_incremental = re.compile(r'^(?P<namebase>.*)-(?P<index>\d+)$')
    while filepath.isfile():
        # Output path exists.
        parent_i = filepath.parent
        namebase_i = filepath.namebase
        ext_i = filepath.ext
        match = cre_incremental.search(namebase_i)
        if match:
            # File name ends with `-##`.  Increment and retry.
            index_i = int(match.group('index')) + 1
            namebase_i = match.group('namebase')
        else:
            index_i = 0
        filepath = parent_i.joinpath(namebase_i + '-%02d%s' % (index_i, ext_i))
    return filepath


class DropBotDxAccessoriesPlugin(Plugin, StepOptionsController):
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

    # `StepFields`
    # ------------

    # A `flatland` `Form` specifying the per step options for the current
    # plugin. Note that nested `Form` objects are not supported.

    # Since we subclassed `StepOptionsController`, an API is available to
    # access and modify these attributes.  This API also provides some nice
    # features automatically:

    #  - All fields listed here will be included in the protocol grid view
    #    (unless `properties=dict(show_in_gui=False`) is used).
    #  - The values of these fields will be stored persistently for each step.
    StepFields = Form.of(Boolean.named('magnet_engaged').using(default=False,
                                                               optional=True),
                         Boolean.named('dstat_enabled').using(default=False,
                                                              optional=True))

    def __init__(self):
        self.name = self.plugin_name
        self.dstat_timeout_id = None  # Periodic Dstat status check timeout id
        self.dstat_experiment_id = None  # UUID of active Dstat experiment
        self.dropbot_dx_remote = None  # `dropbot_dx.SerialProxy` instance
        self.initialized = False  # Latch to, e.g., config menus, only once

    def connect(self):
        '''
        Connect to dropbot-dx instrument.
        '''
        # if the dropbot dx plugin is installed and enabled, try getting its
        # reference
        try:
            service = get_service_instance_by_name('wheelerlab.dropbot_dx')
            if service.enabled():
                self.dropbot_dx_remote = service.control_board
                return
        except:
            pass

        # if we couldn't get a reference, try finding a DropBot DX connected to
        # a serial port
        try:
            self.dropbot_dx_remote = dx.SerialProxy()
            host_version = self.dropbot_dx_remote.host_software_version
            remote_version = self.dropbot_dx_remote.remote_software_version

            if remote_version != host_version:
                response = yesno('The DropBot DX firmware version (%s) '
                                 'does not match the driver version (%s). '
                                 'Update firmware?' % (remote_version,
                                                       host_version))
                if response == gtk.RESPONSE_YES:
                    self.on_flash_firmware()
            
            # turn on the light by default
            self.dropbot_dx_remote.light_enabled = True
        except IOError:
            logger.warning('Could not connect to DropBot DX.')

    def connected(self):
        '''
        Returns
        -------

            (bool) : `True` if dropbot-dx instrument is connected.
        '''
        return (self.dropbot_dx_remote is not None)

    ###########################################################################
    # # Menu callbacks #
    def on_edit_configuration(self, widget=None, data=None):
        '''
        Display a dialog to manually edit the configuration settings.
        '''
        config = self.dropbot_dx_remote.config
        form = dict_to_form(config)
        dialog = FormViewDialog(form, 'Edit configuration settings')
        valid, response = dialog.run()
        if valid:
            self.dropbot_dx_remote.update_config(**response)

    def on_flash_firmware(self):
        board = dx.get_firmwares().keys()[0]
        firmware_path = dx.get_firmwares()[board][0]
        port = self.dropbot_dx_remote.stream.serial_device.port

        # disconnect from DropBot DX so that we can flash it
        del self.dropbot_dx_remote
        self.dropbot_dx_remote = None

        logger.info(upload_firmware(firmware_path, board, port=port))

        # reconnect
        self.connect()

    def on_launch_dstat_interface(self, widget, data=None):
        subprocess.Popen([sys.executable, '-m', 'dstat_interface.main'])

    def on_set_dstat_params_file(self, widget, data=None):
        options = self.get_step_options()
        form = Form.of(Filepath.named('dstat_params_file')
                       .using(default=options.get('dstat_params_file', ''),
                              optional=True,
                              properties={'patterns':
                                          [('Dstat parameters file (*.yml)',
                                            ('*.yml', ))]}))
        dialog = FormViewDialog()
        valid, response = dialog.run(form)

        if valid:
            options['dstat_params_file'] = response['dstat_params_file']
            self.set_step_values(options)

    ###########################################################################
    # # Plugin signal handlers #
    def on_plugin_enable(self):
        self.connect()
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
            menu_item.connect("activate", self.on_launch_dstat_interface)
            menu_item.show()
            menu_item = gtk.MenuItem("Set step Dstat parameters file...")
            self.tools_menu.append(menu_item)
            menu_item.connect("activate", self.on_set_dstat_params_file)
            menu_item.show()
            self.edit_config_menu_item = \
                gtk.MenuItem("Edit configuration settings...")
            self.tools_menu.append(self.edit_config_menu_item)
            self.edit_config_menu_item.connect("activate",
                                               self.on_edit_configuration)
            self.initialized = True

        self.tools_menu_item.show()
        if self.connected():
            self.edit_config_menu_item.show()

    def on_plugin_disable(self):
        if self.connected():
            self.dropbot_dx_remote.terminate()
        self.tools_menu_item.hide()

    @is_connected
    def on_protocol_run(self):
        pass

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

    def on_step_run(self):
        '''
        Handler called whenever a step is executed. Note that this signal is
        only emitted in realtime mode or if a protocol is running.

        Plugins that handle this signal must emit the `on_step_complete` signal
        once they have completed the step. The protocol controller will wait
        until all plugins have completed the current step before proceeding.

        The `on_step_complete` signal is emitted with following signature:

            emit_signal('on_step_complete', [plugin_name, return_value])

        where `plugin_name` is the name of the emitting plugin, and
        `return_value` can be one of:

         - `None`: Step completed successfully.
         - `'Repeat'`: Repeat the step.
         - `'Fail'`: Unrecoverable error (stop the protocol).
        '''
        app = get_app()
        logger.info('[DropBotDxAccessoriesPlugin] on_step_run(): step #%d',
                    app.protocol.current_step_number)
        # If `acquire` is `True`, start acquisition
        options = self.get_step_options()
        if self.connected():
            self.dropbot_dx_remote.light_enabled = not options['dstat_enabled']
            self.dropbot_dx_remote.magnet_engaged=options['magnet_engaged']
            if options['dstat_enabled']:
                try:
                    if 'dstat_params_file' in options:
                        # Load Dstat parameters.
                        hub_execute('dstat-interface', 'load_params',
                                    params_path=options['dstat_params_file'])

                    if self.dstat_timeout_id is not None:
                        # Timer was already set, so cancel previous timer.
                        gobject.source_remove(self.dstat_timeout_id)
                    self.dstat_experiment_id = \
                        hub_execute('dstat-interface', 'run_active_experiment')
                    self._dstat_spinner = itertools.cycle(r'-\|/')
                    print ''
                    # Check every 100ms to see if dstat acquisition has
                    # completed.
                    self.dstat_timeout_id = gobject.timeout_add(100, self
                                                          .check_dstat_status)
                except:
                    print "Exception in user code:"
                    print '-'*60
                    traceback.print_exc(file=sys.stdout)
                    print '-'*60
                    # An error occurred while initializing Analyst remote
                    # control.
                    emit_signal('on_step_complete', [self.name, 'Fail'])
            else:
                # D-State is not enabled, so step is complete.
                emit_signal('on_step_complete', [self.name, None])
        else:
            # DropBox-DX device is not connected, but allow protocol to
            # continue.
            #
            # N.B., A warning message is display once at the *start* of the
            # protocol if no DropBot-DX connection has been established, but
            # *not* on each step.
            emit_signal('on_step_complete', [self.name, None])

    ###########################################################################
    # # Periodic callbacks #
    def check_dstat_status(self):
        '''
         1. Check to see if acquisition is finished.
         2. If (1), emit `on_step_complete` signal.
        '''
        try:
            completed_timestamp = hub_execute('dstat-interface',
                                              'acquisition_complete',
                                              experiment_id=
                                              self.dstat_experiment_id)
            if completed_timestamp is not None:
                # ## Acquisition is complete ##

                # ### Save results data and plot ###
                app = get_app()
                output_directory = (path(app.experiment_log.get_log_path())
                                    .abspath())
                output_namebase = str(app.protocol.current_step_number)

                try:
                    step_label_plugin =\
                        get_service_instance_by_name('wheelerlab'
                                                     '.step_label_plugin')
                    label = step_label_plugin.get_step_options().get('label')
                    if label:
                        output_namebase = label
                except:
                    pass

                # Save results to a text file in the experiment log directory.
                output_txt_path = get_unique_path(output_directory
                                                  .joinpath(output_namebase +
                                                            '.txt'))
                logger.info('Save results to: %s', output_txt_path)
                hub_execute('dstat-interface', 'save_text',
                            save_data_path=output_txt_path)

                # Save results plot to a PDF in the experiment log directory.
                output_pdf_path = get_unique_path(output_directory
                                                  .joinpath(output_namebase +
                                                            '.pdf'))
                logger.info('Save plot to: %s', output_pdf_path)
                hub_execute('dstat-interface', 'save_plot',
                            save_plot_path=output_pdf_path)

                # Turn light back on after photomultiplier tube (PMT)
                # measurement.
                self.dropbot_dx_remote.light_enabled = True

                # notify step complete.
                emit_signal('on_step_complete', [self.name, None])
                self.dstat_timeout_id = None
                return False
            else:
                print '\rWaiting for Dstat...', self._dstat_spinner.next(),
        except:
            print "Exception in user code:"
            print '-'*60
            traceback.print_exc(file=sys.stdout)
            print '-'*60
            emit_signal('on_step_complete', [self.name, 'Fail'])
            self.dstat_timeout_id = None
            return False
        return True

    def get_schedule_requests(self, function_name):
        """
        Returns a list of scheduling requests (i.e., ScheduleRequest
        instances) for the function specified by function_name.
        """
        if function_name in ['on_plugin_enable']:
            return [ScheduleRequest('wheelerlab.dropbot_dx',
                                    self.name),
                    ]
        return []


PluginGlobals.pop_env()
