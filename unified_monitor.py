#!/usr/bin/env python3
import subprocess
import csv
import io
import time
import wandb
import logging
import os
import requests
import threading
import json
from datetime import datetime
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich import box
from concurrent.futures import ThreadPoolExecutor
import math
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from dotenv import load_dotenv
import uuid
from datasets import load_dataset
import fire

# region: Global Configuration and Logging
# ==============================================================================
load_dotenv()

# Configure logging
console_log_level = os.getenv("CLL", "INFO").upper()
file_log_level = os.getenv("FLL", "").upper()

logger = logging.getLogger("UnifiedMonitor")
logger.setLevel(getattr(logging, console_log_level, logging.INFO))

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if console_log_level:
    ch = logging.StreamHandler()
    ch.setLevel(console_log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

if file_log_level:
    fh = logging.FileHandler("unified_monitor.log")
    fh.setLevel(file_log_level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

# Suppress SSL warnings
urllib3.disable_warnings(InsecureRequestWarning)

runtime_uuid = str(uuid.uuid4()).replace("-", "")
questions = []
count_id = 0

class Template(str): pass
class Conversation(str): pass
# ==============================================================================
# endregion

# region: System Performance Monitoring (from monitor.py)
# ==============================================================================
def parse_value(value):
    """Converts string values from hl-smi to appropriate numeric types."""
    value = value.strip()
    if value == 'N/A':
        return None
    if value.startswith('Gen'):
        try:
            return int(value[3:])
        except ValueError:
            pass
    if value.startswith('x'):
        try:
            return int(value[1:])
        except ValueError:
            pass
    if value.endswith('GT/s'):
        try:
            return float(value[:-4])
        except ValueError:
            pass
    try:
        return float(value)
    except ValueError:
        return value

def collect_hl_smi_data():
    """Executes hl-smi command and collects system metrics."""
    logger.info("Collecting hl-smi data...")
    fields = (
        "timestamp,name,bus_id,driver_version,nic_driver_version,temperature.aip,module_id,"
        "utilization.aip,utilization.memory,memory.total,memory.free,memory.used,index,serial,uuid,"
        "power.draw,ecc.errors.uncorrected.aggregate.total,ecc.errors.uncorrected.volatile.total,"
        "ecc.errors.corrected.aggregate.total,ecc.errors.corrected.volatile.total,"
        "ecc.errors.dram.aggregate.total,ecc.errors.dram-corrected.aggregate.total,"
        "ecc.errors.dram.volatile.total,ecc.errors.dram-corrected.volatile.total,ecc.mode.current,"
        "ecc.mode.pending,stats.violation.power,stats.violation.thermal,clocks.current.soc,"
        "clocks.max.soc,clocks.limit.soc,clocks.limit.tpc,pcie.link.gen.max,pcie.link.gen.current,"
        "pcie.link.width.max,pcie.link.width.current,pcie.link.speed.max,pcie.link.speed.current"
    )
    cmd = ['sudo', 'hl-smi', '--query-aip', fields, '--format', 'csv,nounits']
    
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
        logger.info("Successfully executed hl-smi command.")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error(f"Failed to execute hl-smi. Ensure it is installed and sudo is configured. Error: {e}")
        return {}

    reader = csv.reader(io.StringIO(output))
    try:
        header_raw = next(reader)
    except StopIteration:
        logger.warning("No output from hl-smi.")
        return {}
    
    header = [h.strip().split(' [')[0] for h in header_raw]
    metrics = {}
    
    for row_raw in reader:
        if row_raw:
            row = dict(zip(header, (v.strip() for v in row_raw)))
            name = row.get('name')
            uuid = row.get('serial')
            if uuid:
                parsed_dict = {k: parse_value(v) for k, v in row.items() if isinstance(parse_value(v), (int, float))}
                key = f"device-{name}-{uuid}"
                metrics[key] = parsed_dict
    
    logger.info(f"Collected data for {len(metrics)} devices.")
    return metrics
# ==============================================================================
# endregion

# region: API and Token Metrics (from metrics.py, now integrated)
# ==============================================================================
class FileHandler:
    def __init__(self, filename: str, mode: str, virtual: bool = False):
        self.filename = filename
        self.file = open(filename, mode) if not virtual else None

    def write(self, data):
        if self.file:
            self.file.write(data)

    def close(self):
        if self.file:
            self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class APIThroughputMonitor:
    def __init__(self, model: str, api_url: str, api_key: str, max_concurrent: int = 5, columns: int = 3, log_file: str = "api_monitor.jsonl", output_dir: str = None, wandb_project: str = "device-monitoring"):
        self.model = model
        self.api_url = api_url
        self.api_key = api_key
        self.max_concurrent = max_concurrent
        self.columns = columns
        self.log_file = log_file
        self.output_dir = output_dir
        self.wandb_project = wandb_project

        self.sessions = {}
        self.lock = threading.Lock()
        self.console = Console()
        self.active_sessions = 0
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time
        self.prev_total_chars = 0
        self.last_update_time = self.start_time
        self.update_interval = 0.25

        if self.log_file:
            with open(self.log_file, 'w') as f:
                f.write('')

    def get_session_status(self, session_id, info):
        status_style = {"Starting": "yellow", "Processing": "blue", "Completed": "green", "Failed": "red"}.get(info["status"], "white")
        return (f"{session_id:3d} | [{status_style}]{info['status']:10}[/{status_style}] | "
                f"Time: {info['response_time'] or '-':8} | Chars: {info['total_chars']:5} | Chunks: {info['chunks_received']:3}")

    def generate_status_table(self):
        table = Table(title="API & System Monitor", box=box.ROUNDED, title_style="bold magenta", header_style="bold cyan")
        for i in range(self.columns):
            table.add_column(f"Session Group {i+1}", justify="left")

        with self.lock:
            sorted_sessions = sorted(self.sessions.items(), key=lambda x: int(x[0]))
            num_rows = math.ceil(len(sorted_sessions) / self.columns)
            
            for row_idx in range(num_rows):
                row_data = [self.get_session_status(*sorted_sessions[row_idx * self.columns + col_idx]) if row_idx * self.columns + col_idx < len(sorted_sessions) else "" for col_idx in range(self.columns)]
                table.add_row(*row_data)

            elapsed_time = time.time() - self.start_time
            total_chars = sum(s["total_chars"] for s in self.sessions.values())
            total_chunks = sum(s["chunks_received"] for s in self.sessions.values())
            chars_per_sec = total_chars / elapsed_time if elapsed_time > 0 else 0
            
            table.add_section()
            stats_summary = (f"[bold cyan]API Stats:[/bold cyan]\n"
                             f"Time: {elapsed_time:.1f}s | Active: {self.active_sessions} | Total: {self.total_requests} | "
                             f"Success: {self.successful_requests} | Failed: {self.failed_requests}\n"
                             f"Chars/s: {chars_per_sec:.1f} | Total Chars: {total_chars} | Total Chunks: {total_chunks}")
            table.add_row(stats_summary)
        return table

    def log_status(self):
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        with self.lock:
            total_chars = sum(s["total_chars"] for s in self.sessions.values())
            chars_per_second = (total_chars - self.prev_total_chars) / (current_time - self.last_log_time) if current_time > self.last_log_time else 0
            
            # Aggregate token-level metrics
            all_tokens_latency = [latency for s in self.sessions.values() for latency in s.get("tokens_latency", [])]
            all_ftls = [s['first_token_latency'] for s in self.sessions.values() if 'first_token_latency' in s and s['first_token_latency'] != -1]

            api_metrics = {
                "timestamp": datetime.now().isoformat(), "elapsed_seconds": elapsed,
                "total_chars": total_chars, "chars_per_second": round(chars_per_second, 2),
                "active_sessions": self.active_sessions, "completed_sessions": self.successful_requests,
                "successful_requests": self.successful_requests, "failed_requests": self.failed_requests,
                "avg_token_latency": sum(all_tokens_latency) / len(all_tokens_latency) if all_tokens_latency else 0,
                "avg_first_token_latency": sum(all_ftls) / len(all_ftls) if all_ftls else 0
            }

            if self.log_file:
                with open(self.log_file, 'a') as f:
                    f.write(json.dumps(api_metrics) + '\n')
            
            # Log to wandb
            wandb.log({"api_metrics": api_metrics})
            
            self.prev_total_chars = total_chars
            self.last_log_time = current_time

    def process_stream_info(self, line):
        try:
            line = line.decode('utf-8') if isinstance(line, bytes) else line
            if line.startswith('data: '): line = line[6:]
            if line.strip() == '[DONE]': return None
            data = json.loads(line)
            return {"data": data, "timestamp": time.time()}
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Error processing stream line: {e}")
            return None

    def make_request(self, session_id):
        global count_id
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}
        messages = questions[session_id % len(questions)]
        payload = {"model": self.model, "stream": True, "messages": messages}
        
        with self.lock:
            count_id += 1
            self.sessions[session_id] = {
                "status": "Starting", "start_time": time.time(), "response_time": None, "error": None,
                "total_chars": 0, "chunks_received": 0, "tokens_latency": [], "first_token_latency": -1,
            }

        start_time = time.time()
        next_token_time = start_time
        
        try:
            response = requests.post(f"{self.api_url}/chat/completions", headers=headers, json=payload, stream=True, verify=False, timeout=180)
            
            with FileHandler(f"{self.output_dir}/in_{runtime_uuid}_{session_id}.json", "w", not self.output_dir) as payload_record, \
                 FileHandler(f"{self.output_dir}/out_{runtime_uuid}_{session_id}.json", "w", not self.output_dir) as output_record:
                
                payload_record.write(json.dumps(payload))
                
                for line in response.iter_lines():
                    if not line: continue
                    data_info = self.process_stream_info(line)
                    if not data_info: continue
                    
                    output_record.write(json.dumps(data_info) + "\n")
                    content = data_info["data"]["choices"][0]["delta"].get("content", "")
                    
                    with self.lock:
                        latency = time.time() - next_token_time
                        session = self.sessions[session_id]
                        session["status"] = "Processing"
                        session["chunks_received"] += 1
                        session["total_chars"] += len(content)
                        session["tokens_latency"].append(latency)
                        if session["first_token_latency"] == -1:
                            session["first_token_latency"] = time.time() - start_time
                        next_token_time = time.time()

            response_time = time.time() - start_time
            with self.lock:
                self.sessions[session_id].update({"status": "Completed", "response_time": f"{response_time:.2f}s"})
                self.successful_requests += 1
        except Exception as e:
            with self.lock:
                self.sessions[session_id].update({"status": "Failed", "error": str(e), "response_time": "N/A"})
                self.failed_requests += 1
            logger.error(f"Error in session {session_id}: {e}")
        finally:
            with self.lock:
                self.active_sessions -= 1
                self.total_requests += 1

    def should_update_display(self):
        current_time = time.time()
        if current_time - self.last_update_time >= self.update_interval:
            self.last_update_time = current_time
            return True
        return False

    def system_monitor_thread(self, interval, stop_event):
        """Thread function to collect and log system metrics."""
        while not stop_event.is_set():
            system_data = collect_hl_smi_data()
            if system_data:
                log_dict = {f"{key}/{metric}": value for key, data in system_data.items() for metric, value in data.items()}
                wandb.log({"system_metrics": log_dict})
                logger.info("Logged system data to wandb.")
            time.sleep(interval)

    def run(self, duration=60, system_monitoring_interval=5):
        try:
            wandb.login(key=os.environ.get("WANDB_API_KEY"))
            wandb.init(project=self.wandb_project, name=f"run-{runtime_uuid}")
            logger.info("Wandb initialized successfully.")

            stop_event = threading.Event()
            sys_monitor = threading.Thread(target=self.system_monitor_thread, args=(system_monitoring_interval, stop_event))
            sys_monitor.start()

            with Live(self.generate_status_table(), refresh_per_second=4, vertical_overflow="visible") as live:
                with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                    end_time = time.time() + duration
                    session_id = 0

                    while time.time() < end_time:
                        if time.time() - self.last_log_time >= 1.0:
                            self.log_status()
                        
                        if self.active_sessions < self.max_concurrent:
                            with self.lock:
                                self.active_sessions += 1
                            session_id += 1
                            executor.submit(self.make_request, session_id)
                        
                        if self.should_update_display():
                            live.update(self.generate_status_table())
                        
                        time.sleep(0.1)
        
        except Exception as e:
            logger.error(f"An error occurred during run: {e}")
        finally:
            stop_event.set()
            if 'sys_monitor' in locals() and sys_monitor.is_alive():
                sys_monitor.join()
            wandb.finish()
            logger.info("\n✨ Monitor stopped. Final statistics and logs are saved.")

# ==============================================================================
# endregion

def load_dataset_as_questions(dataset_name: str, key: Template | Conversation = None):
    dataset = load_dataset(dataset_name)['train']
    ret = []
    if isinstance(key, Template):
        for row in dataset:
            ret.append([{"role": "user", "content": key.format(**row)}])
    elif isinstance(key, Conversation):
        for row in dataset:
            try:
                messages = json.loads(row[key])
                if all('role' in turn and 'content' in turn for turn in messages):
                    ret.append(messages)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Cannot parse conversation from '{row.get(key, '')}'")
    return ret

def main(
    model: str = "gpt-3.5-turbo",
    api_url: str = None,
    max_concurrent: int = 5,
    columns: int = 3,
    log_file: str = "api_monitor.jsonl",
    output_dir: str = None,
    env: str = None,
    dataset: str = "tatsu-lab/alpaca",
    template: str = None,
    conversation: str = None,
    time_limit: int = 120,
    system_monitoring_interval: int = 5,
    wandb_project: str = "unified-monitoring"
):
    global questions
    if env: load_dotenv(env)
    
    if template:
        questions = load_dataset_as_questions(dataset, Template(template))
    elif conversation:
        questions = load_dataset_as_questions(dataset, Conversation(conversation))
    else:
        # Default to a simple instruction-input format if no template is provided
        questions = load_dataset_as_questions(dataset, Template("{instruction}\n{input}"))

    if not questions:
        logger.error("Failed to load any questions from the dataset. Please check your template/conversation key and dataset.")
        return

    if output_dir: os.makedirs(output_dir, exist_ok=True)
    
    api_url = api_url or os.environ.get('API_URL')
    api_key = os.environ.get('OPENAI_API_KEY')
    model = os.environ.get('MODEL', model)

    if not api_url or not api_key:
        logger.error("API_URL and OPENAI_API_KEY must be set in environment or provided as arguments.")
        return

    monitor = APIThroughputMonitor(
        model=model, api_url=api_url, api_key=api_key, max_concurrent=max_concurrent,
        columns=columns, log_file=log_file, output_dir=output_dir, wandb_project=wandb_project
    )
    
    logger.info("🚀 Starting Unified API and System Monitor...")
    try:
        monitor.run(duration=time_limit, system_monitoring_interval=system_monitoring_interval)
    except KeyboardInterrupt:
        logger.info("\n\n👋 Shutting down monitor...")
    finally:
        logger.info(f"Log file saved as: {monitor.log_file if monitor.log_file else 'None'}")

if __name__ == "__main__":
    fire.Fire(main)