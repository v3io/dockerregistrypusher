import sys
import errno
import logging
import logging.handlers
import simplejson
import datetime
import textwrap
import os

import colorama
import pygments
import pygments.formatters
import pygments.lexers


def make_dir_recursively(path):
    """
    Create a directory in a location if it doesn't exist

    :param path: The path to create
    """
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise


class Record(logging.LogRecord):
    pass


class Severity(object):

    Verbose = 5
    Debug = logging.DEBUG
    Info = logging.INFO
    Warning = logging.WARNING
    Error = logging.ERROR

    string_enum_dict = {
        'verbose': Verbose,
        'debug': Debug,
        'info': Info,
        'warn': Warning,
        'warning': Warning,
        'error': Error,
        # Allow abbreviations
        # Also provides backwards compatibility with log-console/file-severity syntax
        'V': Verbose,
        'D': Debug,
        'I': Info,
        'W': Warning,
        'E': Error,
    }

    user_presentable_severities = ['verbose', 'debug', 'info', 'warn', 'error']

    @staticmethod
    def get_level_by_string(severity_string):
        return Severity.string_enum_dict.get(severity_string, 0)


class _VariableLogging(logging.Logger):

    get_child = logging.Logger.getChild

    def __init__(self, name, level=logging.NOTSET):
        logging.Logger.__init__(self, name, level)
        self._bound_variables = {}

        # each time Logger.get_child is called, the Logger manager creates
        # a new Logger instance and adds it to his list
        # so we need to add the first error to the manager attributes
        # so we can keep the first error in the whole application
        if not hasattr(self.manager, 'first_error'):
            setattr(self.manager, 'first_error', None)

    @property
    def first_error(self):
        return self.manager.first_error

    def clear_first_error(self):
        if hasattr(self.manager, 'first_error'):
            self.manager.first_error = None

    def _check_and_log(self, level, msg, args, kw_args):
        if self.isEnabledFor(level):
            kw_args.update(self._bound_variables)
            self._log(level, msg, args, extra={'vars': kw_args})

    def error(self, msg, *args, **kw_args):
        if self.manager.first_error is None:
            self.manager.first_error = {'msg': msg, 'args': args, 'kw_args': kw_args}

        self._check_and_log(Severity.Error, msg, args, kw_args)

    def warn(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Warning, msg, args, kw_args)

    def info(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Info, msg, args, kw_args)

    def debug(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Debug, msg, args, kw_args)

    def verbose(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Verbose, msg, args, kw_args)

    def log_and_raise(self, severity, error_msg, *args, **kwargs):
        getattr(self, severity)(error_msg, *args, **kwargs)

        # format the exception into the raised error message if we got one
        if 'exc' in kwargs:
            error_msg = '{0}: {1}'.format(error_msg, kwargs['exc'].lower())

        exception_type = kwargs.get('exc_type', RuntimeError)

        raise exception_type(error_msg)

    def bind(self, **kw_args):
        self._bound_variables.update(kw_args)


class ObjectEncoder(simplejson.JSONEncoder):
    def default(self, obj):
        try:
            return obj.__log__()
        except:
            try:
                return obj.__repr__()
            except:
                return str(obj)


class _JsonFormatter(logging.Formatter):
    @staticmethod
    def format_to_json_str(params):
        try:

            # default encoding is utf8
            return simplejson.dumps(params, cls=ObjectEncoder)
        except:

            # this is the widest complementary encoding found
            return simplejson.dumps(
                params, cls=ObjectEncoder, encoding='raw_unicode_escape'
            )

    def format(self, record):
        params = {
            'datetime': self.formatTime(record, self.datefmt),
            'name': record.name,
            'level': record.levelname.lower(),
            'message': record.getMessage(),
        }

        params.update(record.vars)

        return _JsonFormatter.format_to_json_str(params)


class HumanReadableFormatter(logging.Formatter):
    def __init__(self, enable_colors, *args, **kwargs):
        super(logging.Formatter, self).__init__(*args, **kwargs)
        self._enable_colors = enable_colors

    # Maps severity to its letter representation
    _level_to_short_name = {
        Severity.Verbose: 'V',
        Severity.Debug: 'D',
        Severity.Info: 'I',
        Severity.Warning: 'W',
        Severity.Error: 'E',
    }

    # Maps severity to its color representation
    _level_to_color = {
        Severity.Info: colorama.Fore.LIGHTGREEN_EX,
        Severity.Warning: colorama.Fore.LIGHTYELLOW_EX,
        Severity.Error: colorama.Fore.LIGHTRED_EX,
    }

    def format(self, record):
        def _get_what_color():
            return {
                Severity.Verbose: colorama.Fore.LIGHTCYAN_EX,
                Severity.Debug: colorama.Fore.LIGHTCYAN_EX,
                Severity.Info: colorama.Fore.CYAN,
                Severity.Warning: colorama.Fore.LIGHTCYAN_EX,
                Severity.Error: colorama.Fore.LIGHTCYAN_EX,
            }.get(record.levelno, colorama.Fore.LIGHTCYAN_EX)

        # coloured using pygments
        if self._enable_colors:
            more = self._prettify_output(record.vars) if len(record.vars) else ''
        else:
            try:
                more = simplejson.dumps(record.vars) if len(record.vars) else ''

            # defensive
            except Exception as exc:
                more = simplejson.dumps({'Log formatting error': str(exc)})

        output = {
            'reset_color': colorama.Fore.RESET,
            'when': datetime.datetime.fromtimestamp(record.created).strftime(
                '%d.%m.%y %H:%M:%S.%f'
            ),
            'when_color': colorama.Fore.LIGHTYELLOW_EX,
            'who': record.name[-15:],
            'who_color': colorama.Fore.WHITE,
            'severity': HumanReadableFormatter._level_to_short_name[record.levelno],
            'severity_color': HumanReadableFormatter._level_to_color.get(
                record.levelno, colorama.Fore.RESET
            ),
            'what': record.getMessage(),
            'what_color': _get_what_color(),
            'more': more,
        }

        # Slice ms to be at maximum of 3 digits
        try:
            time_parts = output['when'].split('.')
            time_parts[-1] = time_parts[-1][:-3]
            output['when'] = '.'.join(time_parts)
        except:
            pass

        # Disable coloring if requested
        if not self._enable_colors:
            for ansi_color in [f for f in output.keys() if 'color' in f]:
                output[ansi_color] = ''

        return (
            '{when_color}{when}{reset_color} {who_color}{who:>15}{reset_color} '
            '{severity_color}({severity}){reset_color} {what_color}{what}{reset_color} '
            '{more}'.format(**output)
        )

    def _prettify_output(self, vars_dict):
        """
        Creates a string formatted version according to the length of the values in the
        dictionary, if the string value is larger than 40 chars, wrap the string using textwrap and
        output it last.

        :param vars_dict: dictionary containing the message vars
        :type vars_dict: dict(str: str)
        :rtype: str
        """
        short_values = []

        # some params for the long texts
        long_values = []
        content_indent = '   '
        wrap_width = 80

        for var_name, var_value in vars_dict.items():

            # if the value is a string over 40 chars long,
            if isinstance(var_value, dict):
                long_values.append(
                    (var_name, simplejson.dumps(var_value, indent=4, cls=ObjectEncoder))
                )
            elif isinstance(var_value, str) and len(var_value) > 40:
                wrapped_text = textwrap.fill(
                    '"{0}"'.format(var_value),
                    width=wrap_width,
                    break_long_words=False,
                    initial_indent=content_indent,
                    subsequent_indent=content_indent,
                    replace_whitespace=False,
                )
                long_values.append((var_name, wrapped_text))
            else:
                short_values.append((var_name, str(var_value)))

        # this will return the following
        # {a: b, c: d} (short stuff in the form of json dictionary)
        # {"some value":
        #                 "very long text for debugging purposes"}

        # The long text is not a full json string, but a raw string (not escaped), as to keep it human readable,
        # but it is surrounded by double-quotes so the coloring lexer will eat it up
        values_str = ''
        if short_values:
            values_str = _JsonFormatter.format_to_json_str(
                {k: v for k, v in short_values}
            )
        if long_values:
            values_str += '\n'

            for lv_name, lv_value in long_values:
                values_str += '{{{0}:\n{1}}}\n'.format(
                    _JsonFormatter.format_to_json_str(lv_name), lv_value.rstrip('\n')
                )

        colorized_output = pygments.highlight(
            values_str,
            pygments.lexers.JsonLexer(),
            pygments.formatters.TerminalTrueColorFormatter(style='paraiso-dark'),
        )

        return colorized_output


class FilebeatJsonFormatter(logging.Formatter):
    def format(self, record):

        # handle non-json-parsable vars:
        try:

            # we can't delete from record.vars because of other handlers
            more = dict(record.vars) if len(record.vars) else {}
            try:
                del more['ctx']
            except:
                pass
        except Exception as exc:
            more = 'Record vars are not parsable: {0}'.format(str(exc))

        try:
            what = record.getMessage()
        except Exception as exc:
            what = 'Log message is not parsable: {0}'.format(str(exc))

        output = {
            'when': datetime.datetime.fromtimestamp(record.created).isoformat(),
            'who': record.name,
            'severity': logging.getLevelName(record.levelno),
            'what': what,
            'more': more,
            'ctx': record.vars.get('ctx', ''),
            'lang': 'py',
        }

        return _JsonFormatter.format_to_json_str(output)


class Client(object):
    def __init__(
        self,
        name,
        initial_severity,
        initial_console_severity=None,
        initial_file_severity=None,
        output_dir=None,
        output_stdout=True,
        max_log_size_mb=5,
        max_num_log_files=3,
        log_file_name=None,
        log_colors='on',
    ):

        initial_console_severity = (
            initial_console_severity
            if initial_console_severity is not None
            else initial_severity
        )
        initial_file_severity = (
            initial_file_severity
            if initial_file_severity is not None
            else initial_severity
        )

        colorama.init()

        # initialize root logger
        logging.setLoggerClass(_VariableLogging)
        self.logger = logging.getLogger(name)

        # the logger's log level must be set with lowest severity of its handlers
        # since it is the logging gateway to the handlers, otherwise they won't get the message.
        # ignore unset None / 0 which will disable logging altogether
        initial_severities = [
            initial_severity,
            initial_console_severity,
            initial_file_severity,
        ]
        lowest_severity = min(
            [
                Severity.get_level_by_string(severity)
                for severity in initial_severities
                if severity
            ]
        )

        self.logger.setLevel(lowest_severity)

        if output_stdout:

            # tty friendliness:
            # on - disable colors if stdout is not a tty
            # always - never disable colors
            # off - always disable colors
            if log_colors == 'off':
                enable_colors = False
            elif log_colors == 'always':
                enable_colors = True
            else:  # on - colors when stdout is a tty
                enable_colors = sys.stdout.isatty()

            human_stdout_handler = logging.StreamHandler(sys.__stdout__)
            human_stdout_handler.setFormatter(HumanReadableFormatter(enable_colors))
            human_stdout_handler.setLevel(
                Severity.get_level_by_string(initial_console_severity)
            )
            self.logger.addHandler(human_stdout_handler)

        if output_dir is not None:
            log_file_name = (
                name.replace('-', '.')
                if log_file_name is None
                else log_file_name.replace('.log', '')
            )
            self.enable_log_file_writing(
                output_dir,
                max_log_size_mb,
                max_num_log_files,
                log_file_name,
                initial_file_severity,
            )

    def enable_log_file_writing(
        self,
        output_dir,
        max_log_size_mb,
        max_num_log_files,
        log_file_name,
        initial_file_severity,
    ):
        """
        Adding a rotating file handler to the logger if it doesn't already have one
        and creating a log directory if it doesn't exist.
        :param output_dir: The path to the logs directory.
        :param max_log_size_mb: The max size of the log (for rotation purposes).
        :param max_num_log_files: The max number of log files to keep as archive.
        :param log_file_name: The name of the log file.
        :param initial_file_severity: full string or abbreviation of severity for formatter.
        """

        # Checks if the logger already have a RotatingFileHandler
        if not any(
            isinstance(h, logging.handlers.RotatingFileHandler)
            for h in self.logger.handlers
        ):
            make_dir_recursively(output_dir)
            log_path = os.path.join(output_dir, '{0}.log'.format(log_file_name))

            # Creates the log file if it doesn't already exist.
            rotating_file_handler = logging.handlers.RotatingFileHandler(
                log_path,
                mode='a+',
                maxBytes=max_log_size_mb * 1024 * 1024,
                backupCount=max_num_log_files,
            )

            rotating_file_handler.setFormatter(FilebeatJsonFormatter())
            rotating_file_handler.setLevel(
                Severity.get_level_by_string(initial_file_severity)
            )
            self.logger.addHandler(rotating_file_handler)

    @staticmethod
    def register_arguments(parser):
        """
        Adds the logger args to the args list.
        :param parser: The argparser
        """
        parser.add_argument(
            '--log-severity',
            help='Set log severity',
            choices=Severity.user_presentable_severities,
            default='info',
        )

        # old-style abbreviation log-level for backwards compatibility
        parser.add_argument(
            '--log-console-severity',
            help='Defines severity of logs printed to console',
            choices=Severity.user_presentable_severities,
        )

        # old-style abbreviation log-level for backwards compatibility
        parser.add_argument(
            '--log-file-severity',
            help='Defines severity of logs printed to file',
            choices=Severity.user_presentable_severities,
        )

        parser.add_argument(
            '--log-disable-stdout',
            help='Disable logging to stdout',
            action='store_true',
        )
        parser.add_argument('--log-output-dir', help='Log files directory path')
        parser.add_argument(
            '--log-file-rotate-max-file-size', help='Max log file size', default=5
        )
        parser.add_argument(
            '--log-file-rotate-num-files', help='Num of log files to keep', default=5
        )
        parser.add_argument(
            '--log-file-name',
            help='Override to filename (instead of deriving it from the logger name. '
            'e.g. [node_name].[service_name].[service_instance].log',
        )
        parser.add_argument(
            '--log-colors',
            help='CLI friendly color control. default is on (color when stdout+tty). '
            'You can also force always/off.',
            choices=['on', 'off', 'always'],
            default='on',
        )
