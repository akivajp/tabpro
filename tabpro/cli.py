#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys

from typing import Callable

from icecream import ic

def parse_and_run(
    parser: argparse.ArgumentParser,
):
    if os.environ.get('DEBUG', '').lower() in ['1', 'true', 'yes', 'on']:
        pass
    else:
        ic.disable()
    ic()
    args = parser.parse_args()
    if args.verbose:
        ic.enable()
    ic(args)
    if args.handler:
        args.handler(args)
    else:
        parser.print_help()
        sys.exit(1)

def setup_command(
    subparsers: argparse._SubParsersAction,
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
    setup_parser(command_parser)
    if subparsers is None:
        setup_common_args(command_parser)
        parse_and_run(command_parser)

def command_aggregate_tables(
    parser: argparse.ArgumentParser|None = None,
):
    from . commands.aggregate_tables import setup_parser
    setup_command(
        parser,
        setup_parser,
        'aggregate',
        'Aggregate tables.',
    )

def command_convert_tables(
    parser: argparse.ArgumentParser|None = None,
):
    from . commands.convert_tables import setup_parser
    setup_command(
        parser,
        setup_parser,
        'convert',
        'Convert a table to a different format.',
    )

def command_merge_tables(
    parser: argparse.ArgumentParser|None = None,
):
    from . commands.merge_tables import setup_parser
    setup_command(
        parser,
        setup_parser,
        'merge',
        'Merge tables.',
    )

def setup_common_args(
    parser: argparse.ArgumentParser,
):
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
    )

def main():
    parser = argparse.ArgumentParser(description='Table Data Converter')
    setup_common_args(parser)
    parser.set_defaults(handler=None)
    subparsers = parser.add_subparsers(dest='command')

    command_aggregate_tables(subparsers)

    #parser_convert_tables = subparsers.add_parser(
    #    'convert',
    #    help='Convert a table to a different format.'
    #)
    #setup_common_args(parser_convert_tables)
    #command_convert_tables(parser_convert_tables)
    command_convert_tables(subparsers)

    #parser_merge_tables = subparsers.add_parser(
    #    'merge',
    #    help='Merge tables.'
    #)
    #setup_common_args(parser_merge_tables)
    #command_merge_tables(parser_merge_tables)
    command_merge_tables(subparsers)

    parse_and_run(parser)

if __name__ == '__main__':
    main()
