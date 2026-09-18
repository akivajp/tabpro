# TabPro — Table Data Processor

TabPro is a command-line toolkit for reshaping tabular data, built for a
specific situation: **data that people produced by hand.** Spreadsheets come
back with columns renamed or added on a whim, rows that do not follow the
spec, and values typed in the wrong format. TabPro is designed to detect
those anomalies, separate them from the rest, transform what is left, and
deliver it as JSON or JSON Lines.

If your input is already clean and machine-generated, tools like
[Miller](https://github.com/johnkerl/miller), [csvkit](https://csvkit.readthedocs.io/)
or [DuckDB](https://duckdb.org/) will serve you better and faster. TabPro
does not try to compete with them on throughput or on breadth of features.

## Installation

```bash
pip install tabpro
```

Requires Python 3.10 or later.

## Supported formats

| Format | Extension | Read | Write |
|---|---|:-:|:-:|
| CSV | `.csv` | ✓ | ✓ |
| TSV | `.tsv` | ✓ | ✓ |
| Excel | `.xlsx` | ✓ | ✓ |
| JSON | `.json` | ✓ | ✓ |
| JSON Lines | `.jsonl` | ✓ | ✓ |

The format is chosen from the file extension, so conversion is just a matter
of naming the output file:

```bash
tabpro convert annotations.xlsx --output delivery.jsonl
```

Excel cells are read as text so that dates and long numbers are not silently
reinterpreted. Nested JSON values are flattened to dot-separated columns when
written to CSV/TSV/Excel (`user.name`), and rebuilt into nested objects when
written back to JSON/JSON Lines.

## The staging area

This is the one concept worth reading before anything else, because it
explains why an action may appear to do nothing.

Every row carries a hidden namespace called `__staging__`. **Actions write
their results into staging rather than into the row itself, and staging is
discarded just before the row is written out.**

So this command produces output identical to its input:

```bash
# The cast happens, but its result is discarded with the staging area.
tabpro convert input.csv --do 'cast:score=score:as=int' --output out.jsonl
```

You promote staged values into the output with `--pick`, which also declares
the output schema and its column order:

```bash
tabpro convert input.csv \
  --do 'cast:score=score:as=int' \
  --pick id score \
  --output out.jsonl
```

```json
{"id": "1", "score": 10}
```

The point is that intermediate values never pollute what you deliver. You
can build up as many working values as a transformation needs and then state
exactly which of them are part of the result.

`--pick` can also rename columns, with the new name on the left:

```bash
tabpro convert input.csv --pick 'user_id=id' 'display_name=name' --output out.jsonl
```

When an action looks up a field, it searches three places in order:

1. `__staging__.<name>` — a value produced by an earlier action
2. `<name>` — a column of the row itself
3. `__staging__.__input__.<name>` — the original input value, before any action

That ordering is what lets actions be chained: each one sees what the
previous one produced.

To see the staging area instead of discarding it, pass `--output-debug`.

## Commands

Every command is available both as a subcommand and as a standalone
executable (`tabpro convert` = `tabpro-convert` = `convert-tables`).
All of them accept `--help`, `--verbose` / `-v` and `--version` / `-V`.

### convert — transform and change format

```bash
tabpro convert [options] <input_file>... --output <output_file>
```

| Option | Description |
|---|---|
| `--output-file`, `--output`, `-O` | Output file path |
| `--output-file-filtered-out`, `-f` | Write rows removed by filters here, instead of dropping them |
| `--config`, `-c` | YAML configuration file |
| `--do-actions`, `--actions`, `--do` | Actions to apply (see below) |
| `--pick-columns`, `--pick` | Columns to emit, optionally renamed as `new=old` |
| `--action-delimiter` | Separator inside action strings (default `:`) |
| `--ignore-file-rows`, `--ignore` | Skip specific rows, given as `file:index` |
| `--no-header` | Treat CSV/TSV input as having no header row; columns become `0`, `1`, … |
| `--output-debug` | Keep the staging area in the output |

Multiple input files are concatenated. `--output-file-filtered-out` is the
one to reach for when triaging bad data: the rows a filter rejects are
written to a separate file for inspection rather than quietly disappearing.

### merge — apply hand-made corrections to a base table

```bash
tabpro merge --previous <base>... --new <corrections>... --keys <key>... [options]
```

Rows are matched on `--keys`. Every field present in a correction file
overwrites the corresponding field of the matching base row.

| Option | Description |
|---|---|
| `--previous-files`, `--previous`, `--old`, `-P` | Base files |
| `--modification-files`, `--new`, `-M` | Files holding the corrections |
| `--keys`, `-K` | Primary key columns |
| `--merge-fields` | Limit the merge to these fields (default: every field of the correction file) |
| `--output-base-data-file`, `--output-base` | All base rows, with corrections applied |
| `--output-modified-data-file`, `--output-modified` | Only the rows that were corrected |
| `--output-remaining-data-file`, `--output-remaining` | Only the rows that were **not** corrected |
| `--ignore-not-found` | Skip correction rows whose key is absent from the base, instead of failing |
| `--allow-duplicate-conventional-keys` | Permit duplicate keys in the base files |
| `--allow-duplicate-modification-keys` | Permit duplicate keys in the correction files |
| `--merge-staging`, `--use-staging` | Carry the staging area across the merge |

`--output-remaining-data-file` is what tells you which rows are still
waiting for someone to look at them.

### aggregate — profile a table before trusting it

```bash
tabpro aggregate [options] <input_file>... --output <report.json>
```

Reports, per column: how many distinct values there are, which types
appeared, the minimum and maximum length, and a value/count breakdown.
Useful as a first pass over a batch of submissions, to see which columns
hold something unexpected.

| Option | Description |
|---|---|
| `--output-file`, `--output`, `-O` | Report path (`.json`), printed to the terminal when omitted |
| `--keys-to-show-duplicates` | List every duplicated value for these columns |
| `--keys-to-show-all-count` | List all value counts for these columns, not just the top ones |
| `--keys-to-expand`, `--expand` | Also aggregate array elements individually, by index |
| `--show-count-threshold`, `-C` | Above this many distinct values, show only a summary (default 50) |
| `--show-count-max-length`, `-L` | Truncate displayed values to this length (default 100) |
| `--compare-columns`, `--compare` | Compare the column sets of the input files against each other |

`--compare-columns` answers a different question: not what is in the data,
but whether the files agree on their shape. When fifty people each send back
a spreadsheet, some of them will have renamed a column, dropped one, or
added their own.

```bash
tabpro aggregate submissions/*.xlsx --compare-columns --output report.json
```

```
columns per file
┃ file      ┃ id ┃ label ┃ comment ┃ Comment ┃ memo ┃
│ alice.csv │ o  │   o   │    o    │    -    │  -   │
│ bob.csv   │ o  │   o   │    -    │    o    │  -   │
│ carol.csv │ o  │   o   │    o    │    -    │  o   │

files that differ from the expected columns
┃ file      ┃ missing ┃ extra   ┃
│ bob.csv   │ comment │ Comment │
│ carol.csv │         │ memo    │

column names that differ only in case, width or spacing
┃ variants          ┃
│ comment / Comment │
```

Columns present in at least half the files are taken as expected; the rest
are reported per file as missing or extra. The last table catches the common
case of a column that only looks different — `Comment` for `comment`, or a
full-width `ＩＤ` for `id`. Names are compared after NFKC normalization,
case folding and trimming, and nothing fuzzier than that, so a genuinely
different name is never guessed at.

The full file-by-column matrix is always written to the JSON report, even
when the terminal falls back to a summary because there are too many files
or columns to lay out.

### sort

```bash
tabpro sort [options] <input_file>... --sort-keys <key>... --output <output_file>
```

| Option | Description |
|---|---|
| `--sort-keys`, `--sort-key`, `-K` | Columns to sort by |
| `--output-file`, `--output`, `-O` | Output file path |
| `--reverse`, `-R` | Descending order |

Values are compared as they are read, so numeric columns coming from CSV
sort as strings. Use `convert` with `cast` first if that matters.

### compare

```bash
tabpro compare [options] <file1> <file2> --query <key>... --output <diff_file>
```

Reports rows present in only one of the files, and per-field differences for
rows present in both. Removed values are prefixed with `-`, added ones
with `+`.

| Option | Description |
|---|---|
| `--query-keys`, `--query`, `-Q` | Columns identifying a row (required) |
| `--compare-keys`, `--compare`, `-C` | Columns to compare (default: all) |
| `--output-path`, `--output`, `-O` | Output file path |

## Actions

Actions are given to `convert` via `--do`, one string per action:

```
<action-name>:<fields>[:<options>]
```

`<fields>` is a comma-separated list of `target=source` pairs — a bare name
means target and source are the same. `<options>` is a comma-separated list
of `key=value` pairs or bare flags. Remember that results land in staging,
so `--pick` decides what actually reaches the output.

Actions run in the order given. `--do` can be repeated or take several
values at once; both accumulate, so these two are equivalent:

```bash
tabpro convert in.csv --do 'cast:score=score:as=int' 'filter:score!=0' ...
tabpro convert in.csv --do 'cast:score=score:as=int' --do 'filter:score!=0' ...
```

The same holds for `--pick` and the other options that take several values.

| Action | Form | Options | Description |
|---|---|---|---|
| `assign` | `assign:t=s` | `default`, `required`, `ignore-empty` | Copy a value |
| `assign-constant` | `assign-constant:t=value` | `type` (`str`/`int`/`float`/`bool`) | Set a fixed value |
| `assign-format` | `assign-format:t={a}-{b}` | — | Build a string from other fields |
| `assign-id` | `assign-id:t=s` | `context`, `reverse` | Assign sequential integer ids per distinct value |
| `assign-length` | `assign-length:t=s` | — | Length of the value |
| `cast` | `cast:t=s` | `as` (`bool`/`int`/`float`/`str`), `required`, `default` | Convert the type |
| `filter` | `filter:field==value` | — | Keep matching rows (`==`, `!=`, `=~`) |
| `filter-empty` | `filter-empty:field` | — | Keep rows where the field is empty or absent |
| `filter-not-empty` | `filter-not-empty:field` | — | Keep rows where the field has a value |
| `join` | `join:t=s` | `delimiter` (default `;`) | Join an array into a string |
| `omit` | `omit:field` | `purge` | Remove a column; `purge` discards it instead of staging it |
| `parse` | `parse:t=s` | `as` (`bool`/`json`/`literal`), `required`, `default` | Parse a string into a value |
| `parse-json` | `parse-json:t=s` | `required` | Shorthand for `parse` with `as=json` |
| `push` | `push:t=s` | `condition` | Append a value to an array |
| `replace` | `replace:t=s` | `old`, `new`, `count`, `recursive` | Replace a substring |
| `split` | `split:t=s` | `delimiter` | Split a string into an array |

Rows rejected by a filter are dropped unless `--output-file-filtered-out`
is given.

```bash
# Separate rows whose id does not look like a 4-digit number,
# rather than losing them.
tabpro convert submissions.xlsx \
  --do 'filter:id=~^[0-9]{4}$' \
  --output valid.jsonl \
  --output-filtered-out rejected.jsonl
```

## Configuration file

Anything repeated often belongs in a YAML file passed with `--config`.
Command-line `--do` and `--pick` are applied in addition to it.

```yaml
# Columns to emit, in this order.
# Use a list to keep the names as they are...
pick:
  - id
  - name

# ...or a mapping to rename them, with the new name as the key.
# (a list and a mapping cannot be mixed in one `pick` block)
#
# pick:
#   id: id
#   score: raw_score

process:
  # Fixed values
  assign_constants:
    source_batch: 2026-04

  # Strings built from other fields
  assign_formats:
    label: '{name} ({id})'

  # Length of a field
  assign_length:
    name_length: name

  # Sequential ids per distinct value.
  # A list uses several columns as the key; `context` numbers
  # independently within each group.
  assign_ids:
    speaker_id: speaker_name
    utterance_id:
      primary: [speaker_name, utterance]
      context: [document_id]

  # Collect several fields into an array
  assign_array:
    choices:
      - choice_a
      - field: choice_b
        optional: true

  # Keep only matching rows.
  # operator: == != =~ not-in empty not-empty
  filter:
    - field: status
      operator: '=='
      value: done

  # Append to an array, optionally only when another field is truthy
  push:
    - target: notes
      source: comment
      condition: has_comment

  # Split a string into an array
  split:
    tags:
      field: raw_tags
      delimiter: ';'
```

## Known limitations

- **Memory.** The whole input is held in memory; a 30 MB JSON Lines file
  needs roughly 500 MB. Fine for spreadsheets, not for very large exports.
- **Almost no stdout.** Only `aggregate` writes its report to standard
  output when `--output` is omitted. The other commands write to the file
  named by `--output` and cannot be used in the middle of a shell pipeline.
- **Action options cannot contain a comma**, because options are themselves
  comma-separated. `split:t=s:delimiter=,` does not work.
- **`sort` compares values as they are read**, so CSV columns sort as text.

## License

MIT. See [LICENSE](LICENSE).
