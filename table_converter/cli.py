#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import sys

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

def command_convert_tables(
    parser: argparse.ArgumentParser|None = None,
):
    if parser is None:
        command_parser = argparse.ArgumentParser(
            description='Convert a table to a different format.'
        )
    else:
        command_parser = parser
    from table_converter.commands.convert_tables import setup_parser
    setup_parser(command_parser)
    if parser is None:
        parse_and_run(command_parser)

def main():
    parser = argparse.ArgumentParser(description='Table Data Converter')
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
    )
    parser.set_defaults(handler=None)
    subparsers = parser.add_subparsers(dest='command')

    parser_convert_tables = subparsers.add_parser(
        'convert',
        help='Convert a table to a different format.'
    )
    command_convert_tables(parser_convert_tables)

    parse_and_run(parser)

if __name__ == '__main__':
    main()
