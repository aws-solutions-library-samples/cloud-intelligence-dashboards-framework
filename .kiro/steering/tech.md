# Technology Stack

## Core Technologies
- **Python 3.9-3.12**: Primary programming language
- **AWS SDK (boto3)**: AWS service integration
- **Click**: Command-line interface framework
- **PyYAML**: Configuration file handling
- **setuptools**: Package building and distribution

## Key Dependencies
- `boto3>=1.35.86` - AWS SDK for Python
- `Click>=8.0` - CLI framework
- `PyYAML` - YAML parsing
- `requests` - HTTP library
- `InquirerPy` - Interactive CLI prompts
- `tqdm` - Progress bars
- `tzlocal>=4.0` - Timezone handling

## AWS Services
- **Amazon QuickSight**: Dashboard visualization
- **Amazon Athena**: Data querying
- **AWS Glue**: Data cataloging and ETL
- **Amazon S3**: Data storage
- **AWS Lambda**: Serverless compute
- **AWS Step Functions**: Workflow orchestration
- **Amazon EventBridge**: Event-driven architecture

## Build System
- Uses `setuptools` with `pyproject.toml` and `setup.cfg`
- Entry point: `cid-cmd` console script
- Plugin system: `cid.plugins` entry points

## Common Commands

### Installation
```bash
pip3 install --upgrade cid-cmd
```

### Development Setup
```bash
pip install -e .
```

### Dashboard Operations
```bash
# Deploy dashboards
cid-cmd deploy

# Update existing dashboards
cid-cmd update

# Force update with dependencies
cid-cmd update --force --recursive

# Check dashboard status
cid-cmd status

# Share QuickSight resources
cid-cmd share

# Initialize QuickSight
cid-cmd init-qs

# Initialize CUR
cid-cmd init-cur

# Delete dashboards
cid-cmd delete
```

## File Formats
- **YAML**: Dashboard definitions and configuration
- **JSON**: Data structures and metadata
- **SQL**: Athena views and queries
- **CloudFormation**: Infrastructure as code templates