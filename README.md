# pipe-sentinel

A lightweight CLI tool for monitoring and alerting on ETL pipeline failures with configurable retry logic.

---

## Installation

```bash
pip install pipe-sentinel
```

Or install from source:

```bash
git clone https://github.com/yourname/pipe-sentinel.git && cd pipe-sentinel && pip install .
```

---

## Usage

Wrap any ETL command with `pipe-sentinel` to enable monitoring and automatic retries:

```bash
pipe-sentinel run --retries 3 --alert-email ops@example.com "python etl/load_data.py"
```

Configure alerting thresholds in a YAML config file:

```yaml
# sentinel.yml
retries: 3
retry_delay: 30
alert:
  email: ops@example.com
  on_failure: true
  on_retry: false
```

Then run with:

```bash
pipe-sentinel run --config sentinel.yml "python etl/load_data.py"
```

### Key Options

| Flag | Description |
|------|-------------|
| `--retries` | Number of retry attempts on failure (default: 0) |
| `--retry-delay` | Seconds to wait between retries (default: 10) |
| `--alert-email` | Email address to notify on failure |
| `--config` | Path to a YAML configuration file |
| `--verbose` | Enable detailed logging output |

---

## Requirements

- Python 3.8+
- `click`, `pyyaml`, `smtplib` (standard library)

---

## License

This project is licensed under the [MIT License](LICENSE).