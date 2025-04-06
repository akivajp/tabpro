# -*- coding: utf-8 -*-

import argparse

from icecream import ic

from ..core.compare import compare

def run(
    args: argparse.Namespace,
):
    compare(
        path1=args.path1,
        path2=args.path2,
        output_path=args.output_path,
        query_keys=args.query_keys,
        compare_keys=args.compare_keys,
        verbose=args.verbose,
    )

def setup_parser(
    parser: argparse.ArgumentParser,
):
    parser.add_argument(
        "path1",
        type=str,
        help="Path to the first table",
    )
    parser.add_argument(
        "path2",
        type=str,
        help="Path to the second table",
    )
    parser.add_argument(
        "--output-path", "--output-file", "--output", "-O",
        required=False,
        default=None,
        type=str,
        help="Path to the output table",
    )
    parser.add_argument(
        "--query-keys", "--query", '-Q',
        required=True,
        type=str,
        nargs="+",
        help="primary keys for query",
    )
    parser.add_argument(
        "--compare-keys", "--compare", '-C',
        type=str,
        nargs="+",
        help="keys for comparison",
    )
    parser.set_defaults(handler=run)
