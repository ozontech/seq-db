# K6 Benchmarks

## Preparation

### Test Dataset Preparation
Before running the benchmarks, you must start the seq-db instance and load the test data into it. Instructions for setup and data loading are provided in the [document](/benchmarks/README.md).

### Installing k6
```bash
# For MacOS (via Homebrew)
brew install k6

# Alternative installation methods:
# https://grafana.com/docs/k6/latest/set-up/install-k6/
```

## Running Benchmarks

### Basic Execution
```bash
BASE_URL=http://localhost:9002 k6 run <script_name>.js
```

### Running Against Different Environments
Replace the `BASE_URL` value depending on your target environment:

```bash
BASE_URL=https://api.example.com k6 run script.js
```

---

*For more detailed configuration, please refer to the [official k6 documentation](https://k6.io/docs/)*