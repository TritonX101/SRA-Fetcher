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
# 线程安全速率限制器
# =========================
class RateLimiter:
    """
    线程安全速率限制器，确保请求间隔至少为指定的最小间隔时间。
    """
    def __init__(self, min_interval: float = 1.0):
        """
        参数:
            min_interval: 最小请求间隔时间，每个请求之间至少等待该时间（秒）
        """
        self.min_interval = min_interval
        self.last_request_time = 0
        self.lock = threading.Lock()
    
    def acquire(self):
        """
        等待直到可以进行下一个请求。
        保证相邻请求间隔至少为 min_interval 秒。
        """
        with self.lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request_time = time.time()


# =========================
# 文件处理函数
# =========================
def get_safe_output_path(file_path: str, replace_mode: bool = False) -> str:
    """
    获取安全的输出文件路径。
    若文件已存在：
    - 替换模式 (replace_mode=True)：返回原路径（覆盖）
    - 保留模式 (replace_mode=False)：添加时间戳后缀
    
    参数:
        file_path: 目标文件路径
        replace_mode: 是否启用替换模式
    
    返回:
        安全的输出文件路径
    """
    if replace_mode or not os.path.exists(file_path):
        return file_path
    
    # 文件存在且非替换模式，添加时间戳后缀
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # 分离文件名和扩展名
    base_path = file_path
    if "." in os.path.basename(file_path):
        base_path, ext = file_path.rsplit(".", 1)
        new_path = f"{base_path}_{timestamp}.{ext}"
    else:
        new_path = f"{file_path}_{timestamp}"
    
    return new_path


# =========================
# API验证函数
# =========================
def validate_ncbi_api_key(api_key: str, timeout: int = 8) -> Tuple[bool, str]:
    """
    验证 NCBI API 密钥的正确性。

    参数:
        api_key (str): 要验证的 API 密钥。
        timeout (int): 网络请求超时时间，默认为 8 秒。

    返回:
        Tuple[bool, str]: 验证结果和消息。
    """

    # 格式检查：空 / None
    if not api_key or not isinstance(api_key, str):
        return False, "API 密钥为空或不是字符串。"

    # 长度检查
    if not (30 <= len(api_key) <= 60):
        return False, f"API 密钥长度 ({len(api_key)}) 异常。"

    # 字符合法性检查
    if not re.fullmatch(r"[A-Za-z0-9\-]+", api_key):
        return False, "API 密钥包含非法字符。"

    # 发送验证请求
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {"db": "sra", "term": "SRP000001", "retmode": "json", "api_key": api_key}

    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        return False, f"网络错误: {str(e)}"

    # 判断返回的 JSON 是否包含 error 字段
    try:
        j = r.json()
    except Exception:
        return False, "API 返回非 JSON 格式响应。"

    if "error" in j:
        message = j.get("error", "未知错误")
        return False, f"NCBI 拒绝了 API 密钥: {message}"

    return True, "API 密钥有效。"


# =========================
# 使用 pysradb Python API 获取 metadata
# =========================
# 全局速率限制器实例（所有线程共享）
_rate_limiter = RateLimiter(min_interval=1.0)

def run_pysradb_get_srr_metadata(
    srr: str,
    detailed: bool = False,
    api_key: Optional[str] = None,
    max_retries: int = 4,
    base_sleep: float = 1.0,
):
    """
    使用 pysradb 的 Python API 获取单个 SRR 的 metadata，
    对网络/SSL 错误做多次重试。

    参数:
        srr: SRR 编号
        detailed: 是否获取详细元数据
        api_key: NCBI API Key
        max_retries: 最大重试次数（默认 4）
        base_sleep: 基础睡眠时间，单位秒（默认 1.0）

    返回:
        (df, error_msg)
        成功时 df 为 pandas.DataFrame，error_msg 为 None
        失败时 df 为 None，error_msg 为错误描述
    """
    try:
        client = SRAweb(api_key=api_key)
    except Exception as e:
        return None, f"[ERROR] 初始化 SRAweb 失败: {e}"

    for attempt in range(1, max_retries + 1):
        try:
            # 应用速率限制：确保请求间隔至少 1 秒
            _rate_limiter.acquire()
            
            df = client.sra_metadata(srr, detailed=detailed)

            if df is None or df.empty:
                return None, f"[WARNING] pysradb 未返回 {srr} 的任何元数据"

            return df.copy(), None

        except (
            requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            # 网络类错误：重试
            if attempt < max_retries:
                sleep_t = base_sleep * (2 ** (attempt - 1))  # 指数退避
                time.sleep(sleep_t)
            else:
                return None, f"[ERROR] 网络/SSL错误: {e}"  # 最后重试依然失败

        except requests.exceptions.RequestException as e:
            # 其他请求层错误
            return None, f"[ERROR] 请求错误: {e}"  # 其他请求层错误
        except Exception as e:
            # 非网络类异常（比如 pysradb 内部bug）直接返回
            return None, f"[ERROR] pysradb.sra_metadata 调用失败: {e}"

    return None, f"[ERROR] 未知错误，已尝试 {max_retries} 次重试"


# =========================
# 解析 pysradb TSV 输出
# =========================
def parse_pysradb_output(stdout: str):
    """
    解析 pysradb 的 TSV 输出
    返回: (header_list, data_rows_list)
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
# 主处理逻辑
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
    API 接口：处理包含 SRR 编号的文件并导出 CSV。
    参数:
        file_path: 输入文件路径，包含 SRR 编号列表，直接对接 GEO 下载的 Accession List
        output_path: 输出路径，可以是文件夹(使用默认文件名) 或 具体的 .csv 文件路径，默认与输入文件同目录
        verbose: 是否输出调试信息
        quiet: 是否静默模式（仅输出错误）
        threads: 并发线程数
        detailed: 是否获取详细元数据
        api_key: NCBI API Key
        replace_mode: 是否启用替换模式（True=覆盖同名文件，False=添加时间戳后缀）
    """

    # 路径与环境准备
    if not os.path.exists(file_path):
        error_msg = f"[ERROR] 找不到输入文件 {file_path}，请检查路径是否正确。"
        typer.secho(error_msg, fg=typer.colors.RED, err=True)
        return None

    # 输出路径检测
    if output_path:
        # 转换为字符串以防传入的是 Path 对象
        out_str = str(output_path)

        # 当指定了具体的 .csv 文件名，获取文件名称并以此保存
        if out_str.lower().endswith(".csv"):
            output_csv = out_str
            # 获取该文件的父目录，以便创建
            target_dir = os.path.dirname(os.path.abspath(output_csv))
        
        # 当只指定了输出目录 
        else:
            target_dir = out_str
            output_csv = os.path.join(target_dir, "SRR_info.csv")
        
        # 尝试创建目标文件夹
        if target_dir and not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
            except OSError as e:
                typer.secho(
                    f"[ERROR] 无法创建输出目录 {target_dir}: {e}",
                    fg=typer.colors.RED,
                    err=True,
                )
                return None
    else:
        # 若没有传递参数，默认保存在输入文件同级目录
        target_dir = os.path.dirname(os.path.abspath(file_path))
        output_csv = os.path.join(target_dir, "SRR_info.csv")

    # 处理文件冲突：若同名文件存在，根据模式决定是否添加时间戳后缀
    original_output_csv = output_csv
    output_csv = get_safe_output_path(output_csv, replace_mode=replace_mode)
    if not quiet and output_csv != original_output_csv:
        typer.secho(
            f"[INFO] 同名文件已存在，输出文件已更名为: {os.path.basename(output_csv)}",
            fg=typer.colors.CYAN,
        )

    # 读取输入
    srr_list = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                srr = line.strip()  # 去除首尾空白字符
                if srr:
                    srr_list.append(srr)
    except Exception as e:
        typer.secho(f"[ERROR] 读取文件失败: {e}", fg=typer.colors.RED, err=True)
        return None

    if not srr_list:
        if not quiet:
            typer.secho(
                "[WARNING] 输入文件为空或没有有效的 SRR 编号。", fg=typer.colors.YELLOW
            )
        return None

    # 建一个 SRR 原始顺序索引的映射用于排序
    srr_order = {srr: idx for idx, srr in enumerate(srr_list)}

    stats = {"total": len(srr_list), "success": 0, "fail": 0}

    dfs = []  # 用来存每个 SRR 对应的 DataFrame

    # 在开始多线程处理之前，根据传入的 api_key 动态调整限速器
    if api_key:
        # 有 Key：允许每秒 10 次请求，间隔 0.12 秒
        _rate_limiter.min_interval = 0.12
    else:
        # 无 Key：允许每秒 3 次请求，间隔 0.34 秒
        _rate_limiter.min_interval = 0.34
    
    # 如果没有 API Key，强制限制线程数，防止过多线程空转
    if not api_key and threads > 3:
        if not quiet:
            typer.secho("[INFO] 未检测到 API Key，线程数已限制为 3 。", fg=typer.colors.YELLOW)
        threads = 3

    max_workers = threads if threads > 0 else 1
    if not quiet:
        typer.secho(f"[INFO] 开始处理，使用线程数: {max_workers}")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_srr = {
                executor.submit(
                    run_pysradb_get_srr_metadata, srr, detailed, api_key
                ): srr
                for srr in srr_list
            }

            if quiet:
                # 静默模式：不显示进度条
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
                # 使用进度条显示
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("• {task.completed}/{task.total}"),
                    transient=True,
                ) as progress:
                    task = progress.add_task(
                        "[cyan]正在处理数据...", total=stats["total"]
                    )

                    # 处理结果
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
                                
                                # 输出警告信息
                                typer.secho(
                                    f"[WARNING] 处理 {srr} 失败: {error_msg}",
                                    fg=typer.colors.YELLOW,
                                )

                                # 错误占位行，防止不知道哪个 SRR 失败
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

                            # 输出空白行，防止进度条bug
                            if not quiet:
                                typer.echo()
                            
                            # 输出报错信息
                            typer.secho(
                                f"[ERROR] 处理 {srr} 时发生异常: {e}",
                                fg=typer.colors.RED,
                                err=True,
                            )

                            # 错误占位行
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
        typer.secho("\n[ERROR] 用户中断操作。", fg=typer.colors.RED, err=True)
        return None

    if not quiet and not verbose:
        typer.echo()

    if not dfs:
        if not quiet:
            typer.secho(
                "[WARNING] 未获取到任何结果，未生成 CSV。", fg=typer.colors.YELLOW
            )
        return None

    # 合并所有 DataFrame
    merged = pd.concat(dfs, axis=0, join="outer", ignore_index=True)

    # 按原始 SRR 列表顺序排序；同一个 SRR 有多行则保持原有顺序
    sort_cols = ["__input_order__"]

    # 按 run_accession 排一下，输出更整齐
    if "run_accession" in merged.columns:
        sort_cols.append("run_accession")

    # 使用稳定
    merged = merged.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    # 删掉内部使用的排序列
    merged = merged.drop(columns=["__input_order__"], errors="ignore")

    # 写 CSV
    try:
        merged.to_csv(output_csv, index=False)
    except IOError as e:
        typer.secho(f"[ERROR] 写入 CSV 文件失败: {e}", fg=typer.colors.RED, err=True)
        return None
    if not quiet:
        typer.echo()
    if verbose:
        typer.echo("=" * 60)
        typer.echo("调试信息 (Debug Info):")
        typer.echo(f"模式          : {'Detailed' if detailed else 'Basic'}")
        typer.echo(f"API Key       : {'已启用' if api_key else '未启用'}")
        typer.echo(f"总 SRR 编号数 : {stats['total']}")
        typer.echo(f"匹配成功数    : {stats['success']}")
        typer.echo(f"匹配失败数    : {stats['fail']}")
        typer.echo(f"线程数        : {max_workers}")
        typer.echo(f"输出文件路径  : {output_csv}")
        typer.echo("=" * 60)
    elif not quiet:
        typer.secho(f"处理完成，结果已保存到: {output_csv}")

    return output_csv


# =========================
# 环境变量初始化函数
# =========================
def setup_ncbi_api_key(
    cli_apikey: Optional[str] = None, quiet: bool = False
) -> Optional[str]:
    """
    初始化 NCBI API Key：
    若提供了命令行 apikey，优先验证该 key；
    若命令行 key 验证失败或未提供，则尝试读取并验证环境变量 NCBI_API_KEY；
    若两者都未通过验证，则返回 None（限速模式）。
    """
    # 首先尝试命令行传入的 apikey
    if cli_apikey:
        if not quiet:
            masked = (
                cli_apikey[:6] + "..." + cli_apikey[-6:]
                if len(cli_apikey) > 12
                else cli_apikey
            )
            typer.secho(f"[INFO] 检测到命令行传入 API Key: {masked}")
            typer.secho("[INFO] 正在验证命令行 API 密钥...")
        is_valid, message = validate_ncbi_api_key(cli_apikey)
        if is_valid:
            if not quiet:
                typer.secho(f"✓ {message}", fg=typer.colors.GREEN)
            return cli_apikey
        else:
            typer.secho(
                f"[WARNING] 命令行 API 密钥验证失败: {message}",
                fg=typer.colors.YELLOW,
                err=True,
            )

    # 尝试寻找环境变量中的API Key
    ncbi_api_key = os.environ.get("NCBI_API_KEY")
    if ncbi_api_key:
        if not quiet:
            masked = (
                ncbi_api_key[:6] + "..." + ncbi_api_key[-6:]
                if len(ncbi_api_key) > 12
                else ncbi_api_key
            )
            typer.secho(f"[INFO] 检测到环境变量 NCBI_API_KEY: {masked}")
            typer.secho("[INFO] 正在验证环境变量 NCBI API 密钥...")
        is_valid, message = validate_ncbi_api_key(ncbi_api_key)
        if is_valid:
            if not quiet:
                typer.secho(f"✓ {message}", fg=typer.colors.GREEN)
            return ncbi_api_key
        else:
            typer.secho(
                f"[WARNING] 环境变量 NCBI_API_KEY 验证失败: {message}",
                fg=typer.colors.YELLOW,
                err=True,
            )
    else:
        if not quiet:
            typer.secho("[INFO] 未检测到环境变量 NCBI_API_KEY。", err=True)

    # API获取失败，启用限速模式
    if not quiet:
        typer.secho(
            "[WARNING] 未找到有效的 NCBI API Key，将以限速模式运行。",
            fg=typer.colors.YELLOW,
            err=True,
        )
    return None


# =========================
# CLI 入口
# =========================
def cli(
    file: Path = typer.Option(
        ...,
        "--file",
        "-f",
        help="SRR 编号列表文件路径 [可对应 GEO 下载的 Accession List]",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="输出路径：默认输出在输入文件同级目录",
        file_okay=True,
        dir_okay=True,
        writable=True,
    ),
    threads: int = typer.Option(
        1,
        "--threads",
        "-t",
        help="并发线程数",
    ),
    detailed: bool = typer.Option(
        False,
        "--detailed",
        "-d",
        help="获取包括下载地址在内的详细元数据[部分 SRR编号 返回的信息存在格式不正确情况]",
    ),
    apikey: Optional[str] = typer.Option(
        None, "--api", "-a", help="NCBI API Key"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出详细调试信息"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="静默模式 [仅输出报错]"),
    replace: bool = typer.Option(False, "--replace", "-r", help="启用替换模式 [直接覆盖同名文件]"),
):
    """
    根据 SRR 编号列表调用 pysradb 获取信息并导出 CSV。
    """
    if verbose and quiet:
        typer.secho("错误: --verbose 和 --quiet 不能同时使用。", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # 初始化 & 验证 API Key（先 CLI 再环境变量，都失败返回 None）
    final_api_key = setup_ncbi_api_key(apikey, quiet=quiet)

    # 根据是否有 API Key 来限制线程数
    max_threads = 9 if final_api_key else 3
    if threads > max_threads:
        warning_msg = f"[WARNING] 线程数 {threads} 超过限制 {max_threads}，已自动调整为 {max_threads}"
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
    # 兼容-h 为 --help
    if "-h" in sys.argv and "--help" not in sys.argv:
        sys.argv[sys.argv.index("-h")] = "--help"

    typer.run(cli)
