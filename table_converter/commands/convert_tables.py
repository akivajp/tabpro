# -*- coding: utf-8 -*-

import argparse

from icecream import ic

from core.convert import convert

def run(
    args: argparse.Namespace,
):
    convert(
        input_files = args.input_files,
        output_file = args.output_file,
        pickup_columns= args.pickup_columns,
    )

def setup_parser(
    parser: argparse.ArgumentParser,
):
    parser.add_argument(
        'input_files',
        metavar='INPUT_FILE',
        nargs='+',
        help='Path to the input file.'
    )
    parser.add_argument(
        '-o', '--output-file',
        metavar='OUTPUT_FILE',
        required=True,
        help='Path to the output file.'
    )
    parser.add_argument(
        '--pickup-columns',
        type=str,
        help='Pickup column map',
    )
    parser.set_defaults(handler=run)
