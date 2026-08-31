# Data Commons RCA Agent

Automated Root Cause Analysis (RCA) and triage across Data Commons Batch import pipelines.

## 🚀 Quick Start

### 1. Installation
```bash
# Setup virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install package
pip install -e .

# Configure environment
cp .env.example .env
```

### 2. Run Dashboard
```bash
# Start the local development server
uvicorn dc_rca_agent.main:app --host 0.0.0.0 --port 8080 --reload
```
Open [http://localhost:8080](http://localhost:8080) in your browser.

---

## 🧪 Testing

```bash
# Run unit and integration tests
pytest
```

---

## 🚢 Deployment (Cloud Run)

```bash
# Deploy to Cloud Run
./deploy_dev.sh
```

