# SRA-Fetcher: A high-speed CLI tool for batch fetching SRA metadata based on pysradb.
## Description
A multi-threaded utility for batch retrieval of metadata from the NCBI Sequence Read Archive (SRA). Built on top of **pysradb**, it processes lists of SRR accessions and outputs structured CSV reports. The tool is intended to be flexible, working either as a standalone **Command Line Interface** (CLI) or as an importable **Python module**. It includes practical features such as rate limiting that follows NCBI usage guidelines and automatic retries to improve robustness under unstable network conditions.

---

## Installation
### 1. Get the Source Code
You can clone the repository using Git or download the source code directly from GitHub.

**Clone via Git:**
    
    $bash
    git clone https://github.com/TritonX101/SRA-Fetcher.git
    cd SRA-Fetcher


**Or Download:**
Click the **Code** button on the GitHub repository page and select **Download ZIP**.

### 2. Set Up Environment
It is highly recommended to run this program in an isolated **Conda** environment to avoid dependency conflicts.

#### Option A: Automatic Setup (Recommended)
You can automatically create the environment using the provided 
[environment.yml](/environment.yml)

    $bash
    conda env create -f environment.yml
    conda activate SRA-Fetcher

#### Option B: Manual Setup
If you prefer to configure the environment manually, please ensure you have **Python 3.10** installed along with the following libraries:

* pandas
* requests
* typer
* rich
* pysradb

You can install these dependencies via pip:

    $bash
    pip install pandas requests typer rich pysradb

---

## CLI Usage

You can run the tool directly from the command line using [SRA-Fetcher.py](/SRA-Fetcher.py). This mode allows for batch processing of SRR accessions with various configuration options.

    Usage:
        SRA-Fetcher.py [OPTIONS]

    Options:

    -f, --file PATH         Path to file containing SRR accessions (one per line).

    -o, --output PATH       Output file path or directory. If a directory is provided,
                          output files will be written there. Defaults to the input
                          file's directory with a timestamped filename.

    -t, --threads INT       Number of concurrent worker threads. Default: 1

    -d, --detailed          Fetch detailed metadata (may include download URLs and
                          additional fields; some fields can be malformed for certain
                          accessions).

    -a, --api TEXT          NCBI API key (also honored from environment variable
                          NCBI_API_KEY). When provided, increases request quota.

    -v, --verbose           Enable verbose logging (progress and debug information).

    -q, --quiet             Quiet mode — only show warnings and errors.

    -r, --replace           Replace existing output files (overwrite). By default the
                          tool will not overwrite to avoid accidental data loss.

    -h, --help              Show this help message and exit.

### Examples:

Basic usage (single thread):
```SRA-Fetcher.py -f srr_list.txt```

Save output to a specific directory and use 3 threads:
```SRA-Fetcher.py -f srr_list.txt -o ./reports/ -t 3```

Output using detailed mode：
```SRA-Fetcher.py -f srr_list.txt -d```

Use an API key from env (The program will automatically read the NCBI_API_KEY from environment variables.):

    $bash
    conda env config vars set NCBI_API_KEY=[YOUR NCBI API KEY]
    conda deactivate
    conda activate [YOUR CONDA ENVIRONMENT]
    SRA-Fetcher.py -f srr_list.txt -d -o ./reports/

    Or just use

    $bash
    SRA-Fetcher.py -f srr_list.txt -a [YOUR NCBI API KEY]

Overwrite existing output and run quietly:
```SRA-Fetcher.py -f srr_list.txt -o ./reports/report.csv -r -q```





