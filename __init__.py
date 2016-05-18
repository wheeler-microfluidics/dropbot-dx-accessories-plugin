"""
Copyright 2015-2016 Christian Fobel and Ryan Fobel

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
from datetime import timedelta
from functools import wraps
import itertools
import logging
import re
import subprocess
import sys, traceback
import time
import types

import gtk
from path_helpers import path
from flatland import Boolean, Float, Form
from pygtkhelpers.ui.extra_widgets import Filepath
from microdrop.plugin_helpers import (AppDataController, StepOptionsController,
                                      get_plugin_info, hub_execute)
from microdrop.plugin_manager import (PluginGlobals, Plugin, IPlugin,
                                      ScheduleRequest, implements, emit_signal,
                                      get_service_instance_by_name)
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


class DropBotDxAccessoriesPlugin(Plugin, AppDataController, StepOptionsController):
    """
    This class is automatically registered with the PluginManager.
    """
    implements(IPlugin)
    version = get_plugin_info(path(__file__).parent).version
    plugin_name = get_plugin_info(path(__file__).parent).plugin_name

    AppFields = Form.of(Float.named('dstat_delay_s')
                        .using(default=2., optional=True,
                               properties={'title': 'Delay before D-stat '
                                           'measurement (seconds)'}))
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
        self._metadata = None
        self.has_environment_data = False
        # Number of completed DStat experiments for each step.
        self.dstat_experiment_count_by_step = {}

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
        except:
            pass

        if self.dropbot_dx_remote is None:
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

        try:
            env = self.dropbot_dx_remote.get_environment_state().to_dict()
            logger.info('temp=%.1fC, Rel. humidity=%.1f%%' %
                        (env['temperature_celsius'],
                         100 * env['relative_humidity']))
            self.has_environment_data = True
        except:
            self.has_environment_data = False

    def connected(self):
        '''
        Returns
        -------

            (bool) : `True` if dropbot-dx instrument is connected.
        '''
        return (self.dropbot_dx_remote is not None)

    def get_schedule_requests(self, function_name):
        """
        Returns a list of scheduling requests (i.e., ScheduleRequest
        instances) for the function specified by function_name.
        """
        if function_name in ['on_plugin_enable']:
            return [ScheduleRequest('wheelerlab.dropbot_dx', self.name),
                    ScheduleRequest('wheelerlab.dmf_control_board_plugin',
                                    self.name)]
        elif function_name == 'on_step_run':
            return [ScheduleRequest('wheelerlab.dmf_device_ui_plugin',
                                    self.name)]
        return []

    ###########################################################################
    # # Accessor methods #
    def get_step_label(self):
        try:
            step_label_plugin =\
                get_service_instance_by_name('wheelerlab.step_label_plugin')
            return step_label_plugin.get_step_options().get('label')
        except:
            return None

    @property
    def metadata(self):
        '''
        Add experiment index and experiment UUID to metadata.
        '''
        metadata = self._metadata.copy() if self._metadata else {}
        app = get_app()
        metadata['experiment_id'] = app.experiment_log.experiment_id
        metadata['experiment_uuid'] = app.experiment_log.uuid
        return metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

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
        dialog = FormViewDialog(form, 'Set DStat parameters file')
        valid, response = dialog.run()

        if valid:
            options['dstat_params_file'] = response['dstat_params_file']
            self.set_step_values(options)

    ###########################################################################
    # # Plugin signal handlers #
    def on_metadata_changed(self, original_metadata, metadata):
        '''
        Notify DStat interface of updates to the experiment metadata.
        '''
        self.metadata = metadata

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
        super(DropBotDxAccessoriesPlugin, self).on_plugin_enable()

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
        options = self.get_step_options()
        app_values = self.get_app_values()
        if self.connected():
            self.dropbot_dx_remote.light_enabled = not options['dstat_enabled']
            self.dropbot_dx_remote.magnet_engaged=options['magnet_engaged']
            try:
                if self.has_environment_data:
                    env = self.dropbot_dx_remote.get_environment_state().to_dict()
                    logger.info('temp=%.1fC, Rel. humidity=%.1f%%' %
                                (env['temperature_celsius'],
                                 100 * env['relative_humidity']))
                    app.experiment_log.add_data({"environment state": env},
                                                self.name)
            except ValueError:
                self.has_environment_data = False

            if options['dstat_enabled']:
                # D-stat is enabled for step.  Request acquisition.
                try:
                    if 'dstat_params_file' in options:
                        # Load Dstat parameters.
                        hub_execute('dstat-interface', 'load_params',
                                    params_path=options['dstat_params_file'])
                    if self.dstat_timeout_id is not None:
                        # Timer was already set, so cancel previous timer.
                        gobject.source_remove(self.dstat_timeout_id)
                    # Delay before D-stat measurement (e.g., to allow webcam
                    # light to turn off).
                    dstat_delay_s = app_values.get('dstat_delay_s', 0)
                    time.sleep(max(0, dstat_delay_s))
                    step_label = self.get_step_label()
                    # Send Microdrop step label (if available) to provide name
                    # for DStat experiment.
                    metadata = self.metadata.copy()
                    metadata['name'] = (step_label if step_label else
                                        str(app.protocol.current_step_number +
                                            1))
                    metadata['patient_id'] = metadata.get('sample_id', 'None')

                    # Get target path for DStat database directory.
                    dstat_database_path = (path(app.config['data_dir'])
                                           .realpath().joinpath('dstat-db'))
                    self.dstat_experiment_id = \
                        hub_execute('dstat-interface', 'run_active_experiment',
                                    metadata=metadata,
                                    params={'db_path_entry':
                                            str(dstat_database_path),
                                            'db_enable_checkbutton': True})
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

                app = get_app()

                # Increment the number of completed DStat experiments for
                # current step.
                step_i = app.protocol.current_step_number
                count_i = 1 + self.dstat_experiment_count_by_step.get(step_i,
                                                                      0)
                self.dstat_experiment_count_by_step[step_i] = count_i

                # ### Save results data and plot ###
                output_directory = (path(app.experiment_log.get_log_path())
                                    .abspath())
                output_namebase = str(app.protocol.current_step_number)

                step_label = self.get_step_label()
                if step_label is not None:
                    output_namebase = step_label

                # Save results to a text file in the experiment log directory.
                output_txt_path = get_unique_path(output_directory
                                                  .joinpath(output_namebase +
                                                            '.txt'))
                logger.info('Save results to: %s', output_txt_path)
                hub_execute('dstat-interface', 'save_text',
                            save_data_path=output_txt_path)
                data_i = hub_execute('dstat-interface', 'get_experiment_data',
                                     experiment_id=self.dstat_experiment_id)
                metadata_i = self.get_step_metadata()
                # Compute (approximate) `utc_timestamp` for each DStat
                # measurement.
                max_time_s = data_i.time_s.max()
                metadata_i['utc_timestamp'] = (completed_timestamp -
                                               data_i.time_s
                                               .map(lambda t:
                                                    timedelta(seconds=
                                                              max_time_s - t)))

                # Step label from step label plugin.
                metadata_i['step_label'] = step_label

                # Cast metadata `unicode` fields as `str` to enable HDF export.
                for k, v in metadata_i.iteritems():
                    if isinstance(v, types.StringTypes):
                        metadata_i[k] = str(v)

                data_md_i = data_i.copy()

                for i, (k, v) in enumerate(metadata_i.iteritems()):
                    data_md_i.insert(i, k, v)

                # Set order for known columns.  Unknown columns are ordered
                # last, alphabetically.
                column_order = ['experiment_id', 'experiment_uuid',
                                'utc_timestamp', 'device_id', 'batch_id',
                                'sample_id', 'step_label', 'step_number',
                                'attempt_number', 'temperature_celsius',
                                'relative_humidity', 'time_s', 'current_amps']
                column_index = dict([(k, i) for i, k in
                                     enumerate(column_order)])
                ordered_columns = sorted(data_md_i.columns, key=lambda k:
                                         (column_index
                                          .get(k, len(column_order)), k))
                data_md_i = data_md_i[ordered_columns]

                namebase_i = ('e[{}]-d[{}]-s[{}]'
                              .format(metadata_i['experiment_uuid'],
                                      metadata_i.get('device_id'),
                                      metadata_i.get('sample_id')))

                # Append DStat experiment data to HDF file.
                hdf_output_path = (app.experiment_log.get_log_path()
                                   .joinpath(namebase_i + '.h5'))
                data_md_i.to_hdf(str(hdf_output_path),
                                 '/dstat_experiment_data', format='t',
                                 data_columns=True, append=True)

                # Append DStat experiment data to CSV file.
                csv_output_path = (app.experiment_log.get_log_path()
                                   .joinpath(namebase_i + '.csv'))
                # Only include header if the file does not exist or is empty.
                include_header = not (csv_output_path.isfile() and
                                      (csv_output_path.size > 0))
                with csv_output_path.open('a') as output:
                    data_md_i.to_csv(output, index=False,
                                     header=include_header)

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
                    ScheduleRequest('wheelerlab.dmf_control_board_plugin',
                                    self.name)]
        return []

    def get_step_metadata(self):
        '''
        Returns
        -------

            (OrderedDict) : Contents of `self.metadata` dictionary, updated
                with the additional fields `batch_id`, `step_number`,
                `attempt_number`, `temperature_celsius`, `relative_humidity`.
        '''
        app = get_app()

        # Construct dictionary of metadata for extra columns in the `pandas.DataFrame`.
        metadata = self.metadata.copy()

        cre_device_id = re.compile(r'#(?P<batch_id>[a-fA-F0-9]+)'
                                r'%(?P<device_id>[a-fA-F0-9]+)$')
        device_id = metadata.get('device_id', '')

        # If `device_id` is in the form '#<batch-id>%<device-id>', extract batch and
        # device identifiers separately.
        match = cre_device_id.match(device_id)
        if match:
            metadata['device_id'] = str(match.group('device_id'))
            metadata['batch_id'] = str(match.group('batch_id'))
        else:
            metadata['device_id'] = None
            metadata['batch_id'] = None
        metadata['step_number'] = app.protocol.current_step_number + 1
        # Number of times the DStat experiment has been run for the current step.
        metadata['attempt_number'] = (self.dstat_experiment_count_by_step
                                      [app.protocol.current_step_number])
        # Current temperature and humidity.
        if self.has_environment_data:
            metadata.update(self.dropbot_dx_remote.get_environment_state())
        return metadata

PluginGlobals.pop_env()
