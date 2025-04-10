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
        nargs='+',
        help='Keys to show duplicates',
    )
    parser.add_argument(
        '--keys-to-show-all-count',
        required=False,
        default=None,
        nargs='+',
        help='Keys to show all count',
    )
    parser.add_argument(
        '--keys-to-expand', '--expand',
        required=False,
        default=None,
        nargs='+',
        help='Keys to expand',
    )
    parser.add_argument(
        '--show-count-threshold', '--count-threshold', '-C',
        required=False,
        default=50,
        type=int,
        help='Show count threshold',
    )
    parser.add_argument(
        '--show-count-max-length', '--count-max-length', '-L',
        required=False,
        default=100,
        type=int,
        help='Show count max length',
    )
    parser.set_defaults(handler=run)
