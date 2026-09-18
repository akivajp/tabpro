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
        no_header = args.no_header,
        sheet = args.sheet,
        all_sheets = args.all_sheets,
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
        '--output-file', '--output', '-O',
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
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action='extend',
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
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action='extend',
        nargs='+',
        type=str,
        help='Actions to do',
    )
    parser.add_argument(
        '--ignore-file-rows', '--ignore-rows', '--ignore',
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action='extend',
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
        '--no-header',
        action='store_true',
        help='CSV/TSV like data without header row',
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
