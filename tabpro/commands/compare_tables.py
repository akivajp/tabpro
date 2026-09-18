# -*- coding: utf-8 -*-

import argparse

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
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action="extend",
        nargs="+",
        help="primary keys for query",
    )
    parser.add_argument(
        "--compare-keys", "--compare", '-C',
        type=str,
        # NOTE:
        #   action を指定しないと、オプションを複数回書いたときに
        #   後の指定が前の指定を上書きして捨ててしまう (警告も出ない)。
        #   extend にすることで繰り返し指定が累積される。
        action="extend",
        nargs="+",
        help="keys for comparison",
    )
    parser.set_defaults(handler=run)
