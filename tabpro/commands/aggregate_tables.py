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
        '--output-file', '--output',
        required=False,
        help='Path to output file',
    )
    parser.set_defaults(handler=run)
