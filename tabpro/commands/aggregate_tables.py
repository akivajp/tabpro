# -*- coding: utf-8 -*-

import argparse

from .. core.aggregate import aggregate

def run(
    args: argparse.Namespace,
):
    aggregate(
        input_files=args.input_files,
        output_file=args.output_file,
        verbose=args.verbose,
        list_keys_to_show_duplicates=args.keys_to_show_duplicates,
        list_keys_to_show_all_count=args.keys_to_show_all_count,
        list_keys_to_expand=args.keys_to_expand,
        show_count_threshold=args.show_count_threshold,
        show_count_max_length=args.show_count_max_length,
        compare_columns=args.compare_columns,
        sheet=args.sheet,
        all_sheets=args.all_sheets,
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
        '--output-file', '--output', '-O',
        required=False,
        help='Path to output file',
    )
    parser.add_argument(
        '--keys-to-show-duplicates',
        required=False,
        default=None,
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action='extend',
        nargs='+',
        help='Keys to show duplicates',
    )
    parser.add_argument(
        '--keys-to-show-all-count',
        required=False,
        default=None,
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action='extend',
        nargs='+',
        help='Keys to show all count',
    )
    parser.add_argument(
        '--keys-to-expand', '--expand',
        required=False,
        default=None,
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action='extend',
        nargs='+',
        help='Keys to expand',
    )
    parser.add_argument(
        '--show-count-threshold', '--count-threshold', '-C',
        required=False,
        default=50,
        type=int,
        help=(
            'Above this many distinct values, show only a summary '
            '(default 50)'
        ),
    )
    parser.add_argument(
        '--show-count-max-length', '--count-max-length', '-L',
        required=False,
        default=100,
        type=int,
        help='Show count max length',
    )
    parser.add_argument(
        '--compare-columns', '--compare-column', '--compare',
        action='store_true',
        help='Compare the column sets of the input files with each other',
    )
    parser.add_argument(
        '--sheet',
        type=str,
        default=None,
        help='Name of the Excel sheet to read (default: the first visible one)',
    )
    parser.add_argument(
        '--all-sheets',
        action='store_true',
        help='Read every visible sheet of an Excel workbook',
    )
    parser.set_defaults(handler=run)
