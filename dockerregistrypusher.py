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

    # initialize and start processing
    processor = core.Processor(
        logger=logger,
        parallel=args.parallel,
        registry_url=args.registry_url,
        archive_path=args.archive_path,
        stream=args.stream,
        login=args.login,
        password=args.password,
        ssl_verify=args.ssl_verify,
        replace_tags_match=args.replace_tags_match,
        replace_tags_target=args.replace_tags_target,
    )
    processor.process()

    if logger.first_error is None:
        retval = 0

    return retval


def register_arguments(parser):

    # logger options
    clients.logging.Client.register_arguments(parser)

    # verbosity shorthands
    parser.add_argument(
        '-v',
        '-verbose',
        help='Set log level to debug (same as --log-severity=debug)',
        action='store_true',
        default=False,
    )

    parser.add_argument(
        '-p',
        '--parallel',
        help='Control parallelism (multi-processing)',
        type=int,
        default=1,
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
        '--login',
        help='Basic-auth login name for registry',
        required=False,
    )

    parser.add_argument(
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
    arg_parser = argparse.ArgumentParser()

    register_arguments(arg_parser)

    parsed_args = arg_parser.parse_args()

    ret_val = run(parsed_args)

    sys.exit(ret_val)
