#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys

from typing import Callable

from . logging import logger

from . import __version__

def parse_and_run(
    parser: argparse.ArgumentParser,
):
    if os.environ.get('DEBUG', '').lower() in ['1', 'true', 'yes', 'on']:
        logger.setLevel('DEBUG')
    args = parser.parse_args()
    if args.verbose:
        logger.setLevel('DEBUG')
    logger.debug('args: %s', args)
    if args.handler:
        args.handler(args)
    else:
        parser.print_help()
        sys.exit(1)

def setup_command(
    subparsers: argparse._SubParsersAction | None,
    setup_parser: Callable[[argparse.ArgumentParser], None],
    command_name: str,
    description: str,
):
    if subparsers is None:
        command_parser = argparse.ArgumentParser(
            description=description
        )
    else:
        command_parser = subparsers.add_parser(command_name, help=description)
    setup_common_args(command_parser)
    setup_parser(command_parser)
    if subparsers is None:
        parse_and_run(command_parser)

def command_aggregate_tables(
    subparsers: argparse._SubParsersAction | None,
):
    from . commands.aggregate_tables import setup_parser
    setup_command(
        subparsers,
        setup_parser,
        'aggregate',
        'Aggregate tables.',
    )

def command_compare_tables(
    subparsers: argparse._SubParsersAction | None,
):
    from . commands.compare_tables import setup_parser
    setup_command(
        subparsers,
        setup_parser,
        'compare',
        'Compare tables.',
    )

def command_convert_tables(
    subparsers: argparse._SubParsersAction | None,
):
    from . commands.convert_tables import setup_parser
    setup_command(
        subparsers,
        setup_parser,
        'convert',
        'Convert a table to a different format.',
    )

def command_merge_tables(
    subparsers: argparse._SubParsersAction | None,
):
    from . commands.merge_tables import setup_parser
    setup_command(
        subparsers,
        setup_parser,
        'merge',
        'Merge tables.',
    )

def command_sort_tables(
    subparsers: argparse._SubParsersAction | None,
):
    from . commands.sort_tables import setup_parser
    setup_command(
        subparsers,
        setup_parser,
        'sort',
        'Sort tables.',
    )

def setup_common_args(
    parser: argparse.ArgumentParser,
):
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
    )
    parser.add_argument(
        '--version', '-V',
        action='store_true',
    )
    # Only check for version flag without processing other arguments
    if '--version' in sys.argv or '-V' in sys.argv:
        print(f'tabpro v{__version__}')
        sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description='Table Data Converter')
    setup_common_args(parser)
    parser.set_defaults(handler=None)
    subparsers = parser.add_subparsers(title='command')

    command_aggregate_tables(subparsers)
    command_compare_tables(subparsers)
    command_convert_tables(subparsers)
    command_merge_tables(subparsers)
    command_sort_tables(subparsers)

    parse_and_run(parser)

if __name__ == '__main__':
    main()
