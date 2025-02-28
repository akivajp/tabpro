# -*- coding: utf-8 -*-

import argparse

from icecream import ic

from .. core.convert import convert

def run(
    args: argparse.Namespace,
):
    convert(
        input_files = args.input_files,
        output_file = args.output_file,
        output_file_filtered_out = args.output_file_filtered_out,
        config_path = args.config,
        list_actions = args.do_actions,
        list_pick_columns = args.pick_columns,
        action_delimiter = args.action_delimiter,
        output_debug = args.output_debug,
        verbose = args.verbose,
        ignore_file_rows = args.ignore_file_rows,
        skip_header = args.skip_header,
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
        '--output-file', '--output', '-o',
        metavar='OUTPUT_FILE',
        required=False,
        help='Path to the output file.'
    )
    parser.add_argument(
        '--output-file-filtered-out', '--output-filtered-out', '-f',
        metavar='OUTPUT_FILE_FILTERED_OUT',
        required=False,
        help='Path to the output file for filtered out rows.',
    )
    parser.add_argument(
        '--config', '-c',
        type=str,
        help='Path to the configuration file.',
    )
    parser.add_argument(
        '--pick-columns', '--pick',
        type=str,
        nargs='+',
        help='Pick column map',
    )
    parser.add_argument(
        '--action-delimiter', '--do-delimiter', '--do-delim',
        type=str,
        default=':',
        help='Action delimiter',
    )
    parser.add_argument(
        '--do-actions', '--actions', '--do',
        nargs='+',
        type=str,
        help='Actions to do',
    )
    parser.add_argument(
        '--ignore-file-rows', '--ignore-rows', '--ignore',
        nargs='+',
        type=str,
        help='Ignore tuples of file name and row index',
    )
    parser.add_argument(
        '--output-debug',
        action='store_true',
        help='Output debug information',
    )
    parser.add_argument(
        '--skip-header',
        action='store_true',
        help='Skip header',
    )
    parser.set_defaults(handler=run)
