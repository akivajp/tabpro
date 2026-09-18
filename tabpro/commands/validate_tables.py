# -*- coding: utf-8 -*-

import argparse
import sys

from .. core.validate import (
    SchemaError,
    validate,
)

# 終了コード
EXIT_OK = 0
EXIT_VIOLATIONS_FOUND = 1
EXIT_CANNOT_RUN = 2

def run(
    args: argparse.Namespace,
):
    try:
        result = validate(
            input_files=args.input_files,
            schema_path=args.schema,
            output_valid=args.output_valid,
            output_invalid=args.output_invalid,
            report_file=args.report,
            verbose=args.verbose,
        )
    except (SchemaError, FileNotFoundError, ValueError) as e:
        # NOTE:
        #   検査を実行できなかった場合と、実行できたが違反があった場合を
        #   終了コードで区別する。シェルや CI から扱えるようにするため。
        print(f'error: {e}', file=sys.stderr)
        sys.exit(EXIT_CANNOT_RUN)
    if not result.ok:
        sys.exit(EXIT_VIOLATIONS_FOUND)
    sys.exit(EXIT_OK)

def setup_parser(
    parser: argparse.ArgumentParser,
):
    parser.add_argument(
        'input_files',
        metavar='INPUT_FILE',
        nargs='+',
        help='Input files to validate',
    )
    parser.add_argument(
        '--schema', '-S',
        required=True,
        help='Path to the schema file (YAML)',
    )
    parser.add_argument(
        '--output-valid', '--output-valid-file', '--valid',
        required=False,
        help='Path to write the rows that satisfy the schema',
    )
    parser.add_argument(
        '--output-invalid', '--output-invalid-file', '--invalid',
        required=False,
        help='Path to write the rows that violate the schema',
    )
    parser.add_argument(
        '--report', '--report-file',
        required=False,
        help='Path to write the list of violations',
    )
    parser.set_defaults(handler=run)
