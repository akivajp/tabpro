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
        numeric=args.numeric,
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
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action='extend',
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
    parser.add_argument(
        '--numeric', '-N',
        action='store_true',
        help=(
            'Sort numeric values in numeric order instead of '
            'the default lexicographic order'
        ),
    )
    parser.set_defaults(handler=run)
