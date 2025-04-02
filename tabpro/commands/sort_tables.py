# -*- coding: utf-8 -*-

import argparse

from .. core.sort import sort

def run(
    args: argparse.Namespace,
):
    sort(
        input_files=args.input_files,
        output_file=args.output_file,
        sort_keys=args.sort_keys,
        reverse=args.reverse,
        verbose=args.verbose,
    )

def setup_parser(
    parser: argparse.ArgumentParser,
):
    parser.add_argument(
        'input_files',
        metavar='input-file',
        nargs='+',
        help='Input files to aggregate',
    )
    parser.add_argument(
        '--sort-keys', '--sort-key', '-K',
        required=True,
        default=None,
        nargs='+',
        help='Keys to sort by',
    )
    parser.add_argument(
        '--output-file', '--output', '-O',
        required=False,
        help='Path to output file',
    )
    parser.add_argument(
        '--reverse', '-R',
        action='store_true',
        help='Reverse the sort order',
    )
    parser.set_defaults(handler=run)
