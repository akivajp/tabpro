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
        config_path = args.config,
        list_actions = args.do_actions,
        list_pick_columns = args.pick_columns,
        output_debug = args.output_debug,
        verbose = args.verbose,
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
        '--output-file', '-o',
        metavar='OUTPUT_FILE',
        required=True,
        help='Path to the output file.'
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
        '--do-actions', '--actions', '--do',
        nargs='+',
        type=str,
        help='Actions to do',
    )
    parser.add_argument(
        '--output-debug',
        action='store_true',
        help='Output debug information',
    )
    parser.set_defaults(handler=run)
