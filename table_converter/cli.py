#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys

from icecream import ic

def command_convert_tables(
    parser: argparse.ArgumentParser|None = None,
):
    if parser is None:
        parser = argparse.ArgumentParser(description='Convert a table to a different format.')
    from commands.convert_tables import setup_parser
    setup_parser(parser)

def main():
    parser = argparse.ArgumentParser(description='Table Data Converter')
    subparsers = parser.add_subparsers(dest='command')

    parser_convert_tables = subparsers.add_parser(
        'convert',
        help='Convert a table to a different format.'
    )
    command_convert_tables(parser_convert_tables)

    args = parser.parse_args()
    ic()
    ic(args)
    if args.command:
        args.handler(args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    main()
