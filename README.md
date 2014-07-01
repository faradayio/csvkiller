csvkiller
=========

Segment CSV files by any column

#Dependencies

nodejs and npm

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
  -od, --output-delimiter [delimiter]  How to split up lines in the output files (use TAB for tab-delimited) [,]
  -b, --buffer-size [characters]       Number of characters can be in the in-memory file buffer before it's written to the disk [1000000]
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