# Unified API and System Monitor

This script monitors API throughput and hardware performance.
It sends concurrent API requests, tracks latency and token counts, and optionally logs system metrics using `hl-smi`.
Metrics are displayed in the terminal and can be logged to Weights & Biases (wandb) and JSON files.

## Requirements

* Python 3.9+
* `hl-smi` installed (optional, for system monitoring)
* API endpoint compatible with OpenAI `/chat/completions`
* HuggingFace Datasets
* Weights & Biases account (optional)

## Installation

```bash
pip install -r requirements.txt
```

Example `requirements.txt`:

```
fire
requests
rich
wandb
python-dotenv
datasets
```

## Environment Variables

* `API_URL` – Base API URL
* `OPENAI_API_KEY` – API key
* `WANDB_API_KEY` – Weights & Biases API key (optional)
* `MODEL` – Model name
* `CLL` – Console log level (default INFO)
* `FLL` – File log level (optional)

Example `.env`:

```
API_URL=https://api.openai.com/v1
OPENAI_API_KEY=sk-...
WANDB_API_KEY=...
MODEL=gpt-3.5-turbo
CLL=INFO
```

## Usage

```bash
python unified_monitor.py [OPTIONS]
```

### Options

| Name                           | Default            | Description                                |
| ------------------------------ | ------------------ | ------------------------------------------ |
| `--model`                      | gpt-3.5-turbo      | Model name                                 |
| `--api_url`                    | None               | API base URL                               |
| `--max_concurrent`             | 5                  | Max concurrent requests                    |
| `--columns`                    | 3                  | Dashboard columns                          |
| `--log_file`                   | api\_monitor.jsonl | API log file                               |
| `--output_dir`                 | None               | Output directory for request/response logs |
| `--env`                        | None               | Path to `.env` file                        |
| `--dataset`                    | tatsu-lab/alpaca   | Dataset name                               |
| `--template`                   | None               | Prompt template                            |
| `--conversation`               | None               | Dataset column with conversation JSON      |
| `--time_limit`                 | 120                | Duration in seconds                        |
| `--system_monitoring_interval` | 5                  | Seconds between system metric collection   |
| `--wandb_project`              | unified-monitoring | W\&B project name                          |

## Examples

Run with template:

```bash
python unified_monitor.py \
  --dataset tatsu-lab/alpaca \
  --template "{instruction}\n{input}" \
  --time_limit 180
```

Run with conversation column:

```bash
python unified_monitor.py \
  --dataset my_dataset \
  --conversation conversation_column
```
