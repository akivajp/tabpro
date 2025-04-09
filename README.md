# TabPro - Table Data Processor

TabPro is a Python-based tool for efficient processing of tabular data.

## Features

### Data Format Support
- CSV
- TSV
- Excel
- JSON
- JSON Lines
- Bidirectional conversion between all supported formats

### Table Operations
1. **Table Conversion**
   - Convert between different formats
   - Customize output format settings
   - Filter and transform data

2. **Table Merging**
   - Merge tables based on common columns
   - Handle multiple table merging
   - Support for staging and version control

3. **Table Aggregation**
   - Data aggregation based on grouping
   - Statistical calculations
   - Duplicate detection

4. **Table Sorting**
   - Sort by multiple columns
   - Custom sort order

5. **Table Comparison**
   - Detect differences between tables
   - Data consistency checking
   - Detailed comparison reports

## Installation

### Prerequisites
- Python 3.10 or higher
- pip (Python package installer)

### Installation
```bash
pip install tabpro
```

## CLI Usage

### Basic Command
```bash
tabpro [command] [options]
```

### Available Commands

#### Table Conversion (convert)
```bash
tabpro convert [options] <input_file1> [<input_file2>...] --output <output_file>
# or
tabpro-convert ...
convert-tables ...
```

Options:
- `--output-file-filtered-out`, `--output-filtered-out`, `-f`: Path to the output file for filtered out rows
- `--config`, `-c`: Path to the configuration file
- `--pick-columns`, `--pick`: Pick specific columns
- `--do-actions`, `--actions`, `--do`: Actions to perform on the data
- `--ignore-file-rows`, `--ignore-rows`, `--ignore`: Ignore specific rows
- `--no-header`: Treat CSV/TSV data as having no header row

#### Table Merging (merge)
```bash
tabpro merge [options] --previous <previous_file1> [<previous_file2> ...] --new <modification_file1> [<modification_file2> ...] --keys <key1> [<key2> ...]
# or 
tabpro-merge ...
merge-tables ...
```

Options:
- `--allow-duplicate-conventional-keys`: Allow duplicate keys in previous files
- `--allow-duplicate-modification-keys`: Allow duplicate keys in modification files
- `--output-base-data-file`: Path to output base data file
- `--output-modified-data-file`: Path to output modified data file
- `--output-remaining-data-file`: Path to output remaining data file
- `--merge-fields`: Fields to merge
- `--merge-staging`: Merge staging fields from modification files
- `--use-staging`: Use staging fields files

#### Table Aggregation (aggregate)
```bash
tabpro aggregate [options] <input_file> --output <aggregated_json_path>
# or
tabpro-aggregate ...
aggregate-tables ...
```

Options:
- `--keys-to-show-duplicates`: Keys to show duplicates
- `--keys-to-show-all-count`: Keys to show all count
- `--keys-to-expand`: Keys to expand
- `--show-count-threshold`, `--count-threshold`, `-C`: Show count threshold (default: 50)
- `--show-count-max-length`, `--count-max-length`, `-L`: Show count max length (default: 100)

#### Table Sorting (sort)
```bash
tabpro sort [options] <input_file1> [<input_file2> ...] --sort-keys <key1> [<key2> ...] --output <output_file>
# or
tabpro-sort ...
sort-tables ...
```

Options:
- `--output-file`, `--output`, `-O`: Path to output file
- `--reverse`, `-R`: Reverse the sort order

#### Table Comparison (compare)
```bash
tabpro compare [options] <input_file1> <input_file2> --query <query_key1> [<query_key2> ...] --output <output_file>
# or
tabpro-compare ...
tabpro-diff ...
compare-tables ...
```

Options:
- `--compare-keys`, `--compare`, `-C`: Keys for comparison

### Common Options
- `--verbose`, `-v`: Enable verbose logging
- `--version`, `-V`: Show version information

## Features
- Simple and user-friendly command-line interface
- Flexible data processing options
- Handles large datasets efficiently
- Extensible design
