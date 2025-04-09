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
tabpro convert [options] <input_file> <output_file>
# or
convert-tables [options] <input_file> <output_file>
```

Options:
- `--output-file`, `--output`, `-O`: Path to the output file
- `--output-file-filtered-out`, `--output-filtered-out`, `-f`: Path to the output file for filtered out rows
- `--config`, `-c`: Path to the configuration file
- `--pick-columns`, `--pick`: Pick specific columns
- `--do-actions`, `--actions`, `--do`: Actions to perform on the data
- `--ignore-file-rows`, `--ignore-rows`, `--ignore`: Ignore specific rows
- `--no-header`: Treat CSV/TSV data as having no header row

#### Table Merging (merge)
```bash
tabpro merge [options] <input_file1> <input_file2> [<input_file3> ...]
# or
merge-tables [options] <input_file1> <input_file2> [<input_file3> ...]
```

Options:
- `--previous-files`, `--previous`, `--old`, `-P`: Previous files to merge
- `--modification-files`, `--modification`, `--new`, `-M`: Modification files to merge
- `--keys`, `-K`: Primary keys for merging
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
tabpro aggregate [options] <input_file>
# or
aggregate-tables [options] <input_file>
```

Options:
- `--output-file`, `--output`, `-O`: Path to output file
- `--keys-to-show-duplicates`: Keys to show duplicates
- `--keys-to-show-all-count`: Keys to show all count
- `--keys-to-expand`: Keys to expand
- `--show-count-threshold`, `--count-threshold`, `-C`: Show count threshold (default: 50)
- `--show-count-max-length`, `--count-max-length`, `-L`: Show count max length (default: 100)

#### Table Sorting (sort)
```bash
tabpro sort [options] <input_file>
# or
sort-tables [options] <input_file>
# or
tabpro-diff [options] <input_file>
```

Options:
- `--sort-keys`, `--sort-key`, `-K`: Keys to sort by
- `--output-file`, `--output`, `-O`: Path to output file
- `--reverse`, `-R`: Reverse the sort order

#### Table Comparison (compare)
```bash
tabpro compare [options] <input_file1> <input_file2>
# or
compare-tables [options] <input_file1> <input_file2>
```

Options:
- `--output-path`, `--output-file`, `--output`, `-O`: Path to the output table
- `--query-keys`, `--query`, `-Q`: Primary keys for query
- `--compare-keys`, `--compare`, `-C`: Keys for comparison

### Common Options
- `--verbose`, `-v`: Enable verbose logging
- `--version`, `-V`: Show version information

## Features
- Simple and user-friendly command-line interface
- Flexible data processing options
- Handles large datasets efficiently
- Extensible design
