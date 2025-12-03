#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import concurrent.futures
import os
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests
import typer
from pysradb import SRAweb
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn


# =========================
# Rate limiter (thread lock)
# =========================
class RateLimiter:
    """
    Thread-safe rate limiter that ensures requests are spaced by at least
    a specified minimum interval.
    """
    def __init__(self, min_interval: float = 1.0):
        """
        Parameters:
            min_interval: Minimum interval between requests (seconds).
        """
        self.min_interval = min_interval
        self.last_request_time = 0
        self.lock = threading.Lock()
    
    def acquire(self):
        """
        Wait until the next request is allowed. Guarantees that adjacent
        requests are at least min_interval seconds apart.
        """
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request_time = time.time()


# =========================
# File handling helpers
# =========================
def get_safe_output_path(file_path: str, replace_mode: bool = False) -> str:
    """
    Return a safe output path for writing files.

    If a file already exists:
    - replace_mode=True: return the original path (will overwrite)
    - replace_mode=False: append a timestamp suffix to create a unique name

    Parameters:
        file_path: target file path
        replace_mode: whether to allow replacing existing files

    Returns:
        A safe file path for output
    """
    if replace_mode or not os.path.exists(file_path):
        return file_path
    
    # File exists and replace mode is disabled; append a timestamp suffix
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Split filename and extension
    base_path = file_path
    if "." in os.path.basename(file_path):
        base_path, ext = file_path.rsplit(".", 1)
        new_path = f"{base_path}_{timestamp}.{ext}"
    else:
        new_path = f"{file_path}_{timestamp}"
    
    return new_path


# =========================
# API validation helpers
# =========================
def validate_ncbi_api_key(api_key: str, timeout: int = 8) -> Tuple[bool, str]:
    """
    Validate an NCBI API key.

    Parameters:
        api_key (str): API key to validate.
        timeout (int): network request timeout in seconds (default: 8)

    Returns:
        Tuple[bool, str]: A tuple (is_valid, message) where is_valid is a
        boolean and message explains the outcome.
    """

    # Format checks: empty / None
    if not api_key or not isinstance(api_key, str):
        return False, "API key is empty or not a string."

    # Length check
    if not (30 <= len(api_key) <= 60):
        return False, f"API key length ({len(api_key)}) is invalid."

    # Character validity check
    if not re.fullmatch(r"[A-Za-z0-9\-]+", api_key):
        return False, "API key contains invalid characters."

    # Send validation request
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {"db": "sra", "term": "SRP000001", "retmode": "json", "api_key": api_key}

    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        return False, f"Network error: {str(e)}"

    # Check whether returned JSON contains an 'error' field
    try:
        j = r.json()
    except Exception:
        return False, "API returned a non-JSON response."

    if "error" in j:
        message = j.get("error", "unknown error")
        return False, f"NCBI rejected API key: {message}"

    return True, "API key is valid."


# =========================
# Fetch metadata using the pysradb Python API
# =========================
# Global rate limiter instance (shared across threads)
_rate_limiter = RateLimiter(min_interval=1.0)

def run_pysradb_get_srr_metadata(
    srr: str,
    detailed: bool = False,
    api_key: Optional[str] = None,
    max_retries: int = 4,
    base_sleep: float = 1.0,
):
    """
    Use the pysradb Python API to retrieve metadata for a single SRR.
    Retries on network/SSL errors.

    Parameters:
        srr: SRR accession
        detailed: whether to retrieve detailed metadata
        api_key: NCBI API Key
        max_retries: maximum number of retries (default: 4)
        base_sleep: base sleep time in seconds for backoff (default: 1.0)

    Returns:
        (df, error_msg)
        If successful, df is a pandas.DataFrame and error_msg is None.
        If failed, df is None and error_msg contains a description.
    """
    try:
        client = SRAweb(api_key=api_key)
    except Exception as e:
        return None, f"[ERROR] Failed to initialize SRAweb: {e}"

    for attempt in range(1, max_retries + 1):
        try:
            # Apply rate limiting: ensure requests are spaced by at least the configured interval
            _rate_limiter.acquire()
            
            df = client.sra_metadata(srr, detailed=detailed)

            if df is None or df.empty:
                return None, f"[WARNING] pysradb returned no metadata for {srr}"

            return df.copy(), None

        except (
            requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            # Network errors: retry
            if attempt < max_retries:
                sleep_t = base_sleep * (2 ** (attempt - 1))  # exponential backoff
                time.sleep(sleep_t)
            else:
                return None, f"[ERROR] Network/SSL error: {e}"  # final retry failed

        except requests.exceptions.RequestException as e:
            # Other request-layer errors
            return None, f"[ERROR] Request error: {e}"  # other request-layer errors
        except Exception as e:
            # Non-network exceptions (e.g., pysradb internal errors): return immediately
            return None, f"[ERROR] pysradb.sra_metadata call failed: {e}"

    return None, f"[ERROR] Unknown error after {max_retries} retries"


# =========================
# Parse pysradb TSV output
# =========================
def parse_pysradb_output(stdout: str):
    """
    Parse the TSV output from pysradb.
    Returns: (header_list, data_rows_list)
    """
    lines = stdout.splitlines()
    if not lines:
        return None, []

    header = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        rows.append(line.split("\t"))

    return header, rows


# =========================
# Main processing logic
# =========================
def process_srr_file(
    file_path,
    output_path=None,
    verbose=False,
    quiet=False,
    threads=1,
    detailed=False,
    api_key=None,
    replace_mode=False,
):
    """
    Main API: process a file containing SRR accession IDs and export a CSV.

    Parameters:
        file_path: path to an input file containing SRR accessions (e.g., GEO Accession List)
        output_path: output path or directory (defaults to input file directory if None)
        verbose: enable detailed debug output
        quiet: quiet mode (only show errors)
        threads: number of concurrent threads to use
        detailed: retrieve detailed metadata
        api_key: NCBI API Key
        replace_mode: whether to overwrite existing output files (True to overwrite)
    """

    # Path and environment preparation
    if not os.path.exists(file_path):
        error_msg = f"[ERROR] Input file not found: {file_path}. Please check the path."
        typer.secho(error_msg, fg=typer.colors.RED, err=True)
        return None

    # Validate output path
    if output_path:
        # Convert to string to handle Path objects
        out_str = str(output_path)

        # When a specific .csv file is provided, use it
        if out_str.lower().endswith(".csv"):
            output_csv = out_str
            # Derive parent directory to ensure it exists
            target_dir = os.path.dirname(os.path.abspath(output_csv))
        
        # When only an output directory is provided
        else:
            target_dir = out_str
            output_csv = os.path.join(target_dir, "SRR_info.csv")
        
        # Try to create the target directory
        if target_dir and not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
            except OSError as e:
                typer.secho(
                    f"[ERROR] Unable to create output directory {target_dir}: {e}",
                    fg=typer.colors.RED,
                    err=True,
                )
                return None
    else:
        # If no output path is provided, default to input file directory
        target_dir = os.path.dirname(os.path.abspath(file_path))
        output_csv = os.path.join(target_dir, "SRR_info.csv")

    # Handle output file conflicts by renaming unless overwrite/replace is enabled
    original_output_csv = output_csv
    output_csv = get_safe_output_path(output_csv, replace_mode=replace_mode)
    if not quiet and output_csv != original_output_csv:
        typer.secho(
            f"[INFO] A file with the same name exists. Output has been renamed to: {os.path.basename(output_csv)}",
            fg=typer.colors.CYAN,
        )

    # Read input
    srr_list = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                srr = line.strip()  # strip leading/trailing whitespace
                if srr:
                    srr_list.append(srr)
    except Exception as e:
        typer.secho(f"[ERROR] Failed to read input file: {e}", fg=typer.colors.RED, err=True)
        return None

    if not srr_list:
        if not quiet:
            typer.secho(
                "[WARNING] Input file is empty or contains no valid SRR entries.", fg=typer.colors.YELLOW
            )
        return None

    # Build mapping of SRR -> original order for stable sorting
    srr_order = {srr: idx for idx, srr in enumerate(srr_list)}

    stats = {"total": len(srr_list), "success": 0, "fail": 0}

    dfs = []  # store DataFrames for each SRR

    # Before starting multithreaded processing, adjust the rate limiter based on provided api_key
    if api_key:
        # With a key: allow ~10 requests/sec (interval ~0.12s)
        _rate_limiter.min_interval = 0.12
    else:
        # Without a key: allow ~3 requests/sec (interval ~0.34s)
        _rate_limiter.min_interval = 0.34
    
    # If no API Key is present, limit the number of threads to avoid wasted threads
    if not api_key and threads > 3:
        if not quiet:
            typer.secho("[INFO] No API Key detected; threads limited to 3.", fg=typer.colors.YELLOW)
        threads = 3

    max_workers = threads if threads > 0 else 1
    if not quiet:
        typer.secho(f"[INFO] Starting processing; using thread count: {max_workers}")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_srr = {
                executor.submit(
                    run_pysradb_get_srr_metadata, srr, detailed, api_key
                ): srr
                for srr in srr_list
            }

            if quiet:
                # Quiet mode: do not show the progress bar
                for i, future in enumerate(
                    concurrent.futures.as_completed(future_to_srr)
                ):
                    srr = future_to_srr[future]
                    try:
                        df, error_msg = future.result()
                        if df is not None and not df.empty:
                            df["__input_srr__"] = srr
                            df["__input_order__"] = srr_order[srr]
                            dfs.append(df)
                            stats["success"] += 1
                        else:
                            stats["fail"] += 1
                            placeholder = pd.DataFrame(
                                {
                                    "run_accession": [srr],
                                    "__input_srr__": [srr],
                                    "__input_order__": [srr_order[srr]],
                                    "error": [error_msg or "no metadata returned"],
                                }
                            )
                            dfs.append(placeholder)
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        stats["fail"] += 1
                        placeholder = pd.DataFrame(
                            {
                                "run_accession": [srr],
                                "__input_srr__": [srr],
                                "__input_order__": [srr_order[srr]],
                                "error": [str(e)],
                            }
                        )
                        dfs.append(placeholder)
            else:
                # Show a progress bar
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("• {task.completed}/{task.total}"),
                    transient=True,
                ) as progress:
                    task = progress.add_task(
                        "[cyan]Processing data...", total=stats["total"]
                    )

                    # Process results
                    for i, future in enumerate(
                        concurrent.futures.as_completed(future_to_srr)
                    ):
                        srr = future_to_srr[future]
                        try:
                            df, error_msg = future.result()
                            if df is not None and not df.empty:
                                df["__input_srr__"] = srr
                                df["__input_order__"] = srr_order[srr]
                                dfs.append(df)
                                stats["success"] += 1
                            else:
                                stats["fail"] += 1

                                if not quiet:
                                    typer.echo()
                                
                                # Output warning message
                                typer.secho(
                                    f"[WARNING] Failed to process {srr}: {error_msg}",
                                    fg=typer.colors.YELLOW,
                                )

                                # Placeholder row for failed SRR to keep alignment
                                placeholder = pd.DataFrame(
                                    {
                                        "run_accession": [srr],
                                        "__input_srr__": [srr],
                                        "__input_order__": [srr_order[srr]],
                                        "error": [error_msg or "no metadata returned"],
                                    }
                                )
                                dfs.append(placeholder)

                        except KeyboardInterrupt:
                            raise
                        except Exception as e:
                            stats["fail"] += 1

                            # Print a blank line to avoid progress bar artifacts
                            if not quiet:
                                typer.echo()
                            
                            # Output error message
                            typer.secho(
                                f"[ERROR] Exception while processing {srr}: {e}",
                                fg=typer.colors.RED,
                                err=True,
                            )

                            # Error placeholder row
                            placeholder = pd.DataFrame(
                                {
                                    "run_accession": [srr],
                                    "__input_srr__": [srr],
                                    "__input_order__": [srr_order[srr]],
                                    "error": [str(e)],
                                }
                            )
                            dfs.append(placeholder)
                        finally:
                            progress.update(task, advance=1)

    except KeyboardInterrupt:
        typer.secho("\n[ERROR] Operation interrupted by user.", fg=typer.colors.RED, err=True)
        return None

    if not quiet and not verbose:
        typer.echo()

    if not dfs:
        if not quiet:
            typer.secho(
                "[WARNING] No results were obtained; no CSV generated.", fg=typer.colors.YELLOW
            )
        return None

    # Merge all DataFrames
    merged = pd.concat(dfs, axis=0, join="outer", ignore_index=True)

    # Sort by the original SRR list order; keep original order for identical SRR rows
    sort_cols = ["__input_order__"]

    # Sort by run_accession to tidy output
    if "run_accession" in merged.columns:
        sort_cols.append("run_accession")

    # Use a stable sort
    merged = merged.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    # Filter: remove rows where run_accession != __input_srr__
    if "run_accession" in merged.columns and "__input_srr__" in merged.columns:
        initial_count = len(merged)
        # Keep only rows where run_accession matches __input_srr__ or run_accession is NaN/None
        merged = merged[
            (merged["run_accession"] == merged["__input_srr__"]) | 
            (merged["run_accession"].isna())
        ].reset_index(drop=True)
        filtered_count = len(merged)
        if not quiet and filtered_count < initial_count:
            typer.secho(
                f"[INFO] Filtered out {initial_count - filtered_count} mismatched rows "
                f"(run_accession != __input_srr__)",
                fg=typer.colors.CYAN,
            )

    # Remove internal sort helper column
    merged = merged.drop(columns=["__input_order__"], errors="ignore")

    # Write CSV
    try:
        merged.to_csv(output_csv, index=False)
    except IOError as e:
        typer.secho(f"[ERROR] Failed to write CSV file: {e}", fg=typer.colors.RED, err=True)
        return None
    if not quiet:
        typer.echo()
    if verbose:
        typer.echo("=" * 60)
        typer.echo("Debug information:")
        typer.echo(f"Mode           : {'Detailed' if detailed else 'Basic'}")
        typer.echo(f"API Key        : {'Enabled' if api_key else 'Disabled'}")
        typer.echo(f"Total SRR count: {stats['total']}")
        typer.echo(f"Successful     : {stats['success']}")
        typer.echo(f"Failed         : {stats['fail']}")
        typer.echo(f"Threads        : {max_workers}")
        typer.echo(f"Output file    : {output_csv}")
        typer.echo("=" * 60)
    elif not quiet:
        typer.secho(f"Processing complete. Results saved to: {output_csv}")

    return output_csv


# =========================
# Environment variable initialization helper
# =========================
def setup_ncbi_api_key(
    cli_apikey: Optional[str] = None, quiet: bool = False
) -> Optional[str]:
    """
    Initialize and validate the NCBI API Key.

    If an API key is provided on the command line, validate it first.
    If the command-line key fails or is not provided, check and validate the
    NCBI_API_KEY environment variable.
    If neither validates, return None and operate in rate-limited mode.
    """
    # First try API key passed via command-line
    if cli_apikey:
        if not quiet:
            masked = (
                cli_apikey[:6] + "..." + cli_apikey[-6:]
                if len(cli_apikey) > 12
                else cli_apikey
            )
            typer.secho(f"[INFO] Detected command-line API Key: {masked}")
            typer.secho("[INFO] Validating command-line API key...")
        is_valid, message = validate_ncbi_api_key(cli_apikey)
        if is_valid:
            if not quiet:
                typer.secho(f"✓ {message}", fg=typer.colors.GREEN)
            return cli_apikey
        else:
            typer.secho(
                f"[WARNING] Command-line API key validation failed: {message}",
                fg=typer.colors.YELLOW,
                err=True,
            )

    # Next, try to find the API Key in environment variables
    ncbi_api_key = os.environ.get("NCBI_API_KEY")
    if ncbi_api_key:
        if not quiet:
            masked = (
                ncbi_api_key[:6] + "..." + ncbi_api_key[-6:]
                if len(ncbi_api_key) > 12
                else ncbi_api_key
            )
            typer.secho(f"[INFO] Detected environment variable NCBI_API_KEY: {masked}")
            typer.secho("[INFO] Validating environment variable NCBI_API key...")
        is_valid, message = validate_ncbi_api_key(ncbi_api_key)
        if is_valid:
            if not quiet:
                typer.secho(f"✓ {message}", fg=typer.colors.GREEN)
            return ncbi_api_key
        else:
            typer.secho(
                f"[WARNING] Environment variable NCBI_API_KEY validation failed: {message}",
                fg=typer.colors.YELLOW,
                err=True,
            )
    else:
        if not quiet:
            typer.secho("[INFO] No environment variable NCBI_API_KEY detected.", err=True)

    # No valid API key found; operate in rate-limited mode
    if not quiet:
        typer.secho(
            "[WARNING] No valid NCBI API Key found; running in rate-limited mode.",
            fg=typer.colors.YELLOW,
            err=True,
        )
    return None


# =========================
# CLI entrypoint
# =========================
def cli(
    file: Path = typer.Option(
        ...,
        "--file",
        "-f",
        help="Path to SRR accession list file (e.g., GEO Accession List)",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output path or directory (defaults to input file directory)",
        file_okay=True,
        dir_okay=True,
        writable=True,
    ),
    threads: int = typer.Option(
        1,
        "--threads",
        "-t",
        help="Number of concurrent threads",
    ),
    detailed: bool = typer.Option(
        False,
        "--detailed",
        "-d",
        help="Fetch detailed metadata including download URLs (some entries may return malformed fields)",
    ),
    apikey: Optional[str] = typer.Option(
        None, "--api", "-a", help="NCBI API Key"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose debug output"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Quiet mode (only show errors)"),
    replace: bool = typer.Option(False, "--replace", "-r", help="Enable replace mode (overwrite existing files)"),
):
    """
    Retrieve information using pysradb for a list of SRR accessions and export to CSV.
    """
    if verbose and quiet:
        typer.secho("Error: --verbose and --quiet cannot be used together.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # Initialize & validate API Key (check CLI first, then environment; return None if none valid)
    final_api_key = setup_ncbi_api_key(apikey, quiet=quiet)

    # Limit the maximum number of threads depending on whether an API Key is present
    max_threads = 9 if final_api_key else 3
    if threads > max_threads:
        warning_msg = f"[WARNING] Thread count {threads} exceeds the limit of {max_threads}; adjusted to {max_threads}"
        if not quiet:
            typer.secho(warning_msg, fg=typer.colors.YELLOW)
        else:
            typer.secho(warning_msg, fg=typer.colors.YELLOW, err=True)
        threads = max_threads

    process_srr_file(
        file_path=str(file),
        output_path=output,
        verbose=verbose,
        quiet=quiet,
        threads=threads,
        detailed=detailed,
        api_key=final_api_key,
        replace_mode=replace,
    )


if __name__ == "__main__":
    # Treat -h as --help for compatibility
    if "-h" in sys.argv and "--help" not in sys.argv:
        sys.argv[sys.argv.index("-h")] = "--help"

    typer.run(cli)
