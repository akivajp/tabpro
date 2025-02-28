# -*- coding: utf-8 -*-

import argparse

from icecream import ic

from .. core.merge import merge

def run(
    args: argparse.Namespace,
):
    merge(
        previous_files=args.previous_files,
        modification_files=args.modification_files,
        keys=args.keys,
        allow_duplicate_keys=args.allow_duplicate_keys,
        ignore_not_found=args.ignore_not_found,
        output_base_data_file=args.output_base_data_file,
        output_modified_data_file=args.output_modified_data_file,
    )

def setup_parser(
    parser: argparse.ArgumentParser,
):
    parser.add_argument(
        '--previous-files', '--previous-file', '--previous', '--old', '-P',
        nargs='+',
        required=True,
        help='Previous files to merge',
    )
    parser.add_argument(
        '--modification-files', '--modification', '--modify', '--modified', '--new', '-M',
        nargs='+',
        required=True,
        help='Modification files to merge',
    )
    parser.add_argument(
        '--keys', '-K',
        nargs='+',
        required=True,
        help='Primary keys',
    )
    parser.add_argument(
        '--allow-duplicate-keys', '--allow-duplicate',
        action='store_true',
        help='Allow duplicate keys',
    )
    parser.add_argument(
        '--ignore-not-found',
        action='store_true',
        help='Ignore not found',
    )
    parser.add_argument(
        '--output-base-data-file', '--output-base',
        required=False,
        help='Path to output base data file',
    )
    parser.add_argument(
        '--output-modified-data-file', '--output-modified',
        required=False,
        help='Path to output modified data file',
    )
    parser.set_defaults(handler=run)
