import argparse
import sys

import core
import clients.logging


def run(args):
    retval = 1

    # plug in verbosity shorthands
    if args.v:
        args.log_severity = 'debug'

    logger = clients.logging.Client(
        'pusher',
        initial_severity=args.log_severity,
        initial_console_severity=args.log_console_severity,
        initial_file_severity=args.log_file_severity,
        output_stdout=not args.log_disable_stdout,
        output_dir=args.log_output_dir,
        max_log_size_mb=args.log_file_rotate_max_file_size,
        max_num_log_files=args.log_file_rotate_num_files,
        log_file_name=args.log_file_name,
        log_colors=args.log_colors,
    ).logger

    # start root logger with kwargs and create manof
    reg_client = core.Registry(
        logger=logger,
        archive_path=args.archive_path,
        registry_url=args.registry_url,
        stream=args.stream,
        login=args.login,
        password=args.password,
        ssl_verify=args.ssl_verify,
        replace_tags_match=args.replace_tags_match,
        replace_tags_target=args.replace_tags_target,
    )
    reg_client.process_archive()
    if logger.first_error is None:
        retval = 0

    return retval


def register_arguments(parser):
    # global options for manof
    clients.logging.Client.register_arguments(parser)

    # verbosity shorthands
    parser.add_argument(
        '-v',
        help='Set log level to debug (same as --log-severity=debug)',
        action='store_true',
        default=False,
    )

    parser.add_argument(
        'archive_path',
        metavar='ARCHIVE_PATH',
        type=str,
        help='The url of the target registry to push to',
    )

    parser.add_argument(
        'registry_url',
        metavar='REGISTRY_URL',
        type=str,
        help='The url of the target registry to push to',
    )
    parser.add_argument(
        '-l',
        '--login',
        help='Basic-auth login name for registry',
        required=False,
    )

    parser.add_argument(
        '-p',
        '--password',
        help='Basic-auth login password for registry',
        required=False,
    )

    parser.add_argument(
        '--ssl-verify',
        help='Skip SSL verification of the registry',
        type=bool,
        default=True,
    )

    parser.add_argument(
        '--stream',
        help='Add some streaming logging during push',
        type=bool,
        default=True,
    )

    parser.add_argument(
        '--replace-tags-match',
        help='A regex string to match on tags. If matches will be replaces with --replace-tags-target',
        type=str,
        required=False,
    )

    parser.add_argument(
        '--replace-tags-target',
        help='If an image tag matches value of --replace-tags-match value, replace it with this tag',
        type=str,
        required=False,
    )


if __name__ == '__main__':
    # create an argument parser
    arg_parser = argparse.ArgumentParser()

    # register all arguments and sub-commands
    register_arguments(arg_parser)

    parsed_args = arg_parser.parse_args()

    # parse the known args, seeing how the targets may add arguments of their own and re-parse
    ret_val = run(parsed_args)

    # return value
    sys.exit(ret_val)
