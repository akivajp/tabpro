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
        assign_constants = args.assign_constants,
        assign_formats = args.assign_formats,
        pickup_columns= args.pickup_columns,
        fields_to_split_by_newline = args.split_by_newline,
        fields_to_assign_ids = args.assign_ids,
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
    parser.add_argument(
        '--split-by-newline',
        type=str,
        help='Fields to split by newline',
    )
    parser.add_argument(
        '--assign-ids',
        type=str,
        help='Field to assign ids',
    )
    parser.add_argument(
        '--assign-constants',
        type=str,
        help='Field to assign constants',
    )
    parser.add_argument(
        '--assign-formats',
        type=str,
        help='Field to assign formats',
    )
    parser.set_defaults(handler=run)
