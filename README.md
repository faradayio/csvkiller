csvkiller
=========

Segment CSV files by any column

#Dependencies

nodejs, npm, and bash

#Installation

Depending on your environment you may need to run this as root

```bash
npm install -g csvkiller
```

#Usage

```
Usage: csvkiller -c [column] [options] [file ...]

Options:

  -h, --help                           output usage information
  -V, --version                        output the version number
  -c, --column [name]                  Which column to segment by
  -d, --delimiter [delimiter]          How to split up lines in the input file (use TAB for tab-delimited) [,]
  -o, --output-directory [path]        Output directory [./output]
  -od, --output-delimiter [delimiter]  How to split up lines in the output files (use TAB for tab-delimited) [,]
  -b, --buffer-size [characters]       Max characters in the output buffer [1000000]
  -u, --uppercase                      Case insensitive column matching, write to OUTPUT.csv instead of Output.csv
  -l, --lowercase                      Case insensitive column matching, write to output.csv instead of Output.csv
  -v, --verbose                        Verbose output
```

#Examples

Turn `people.csv` into `output/male.csv` and `output/female.csv`

```bash
csvkiller -d TAB -c gender people.tsv
```

Combine `*.csv` into `output/Alabama.csv`, `output/Alaska.csv`, etc

```bash
csvkiller -c state *.csv
```
