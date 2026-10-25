#!/bin/bash
# Get Started with Snowflake-Managed Iceberg Tables — Setup Script
# This script creates all necessary Snowflake objects for the Quickstart.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Snowflake-Managed Iceberg Tables — Setup${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Check for config file
if [ ! -f "config.env" ]; then
    if [ -f "config.env.template" ]; then
        echo -e "${YELLOW}config.env not found. Creating from template...${NC}"
        cp config.env.template config.env
        echo ""
        echo -e "${YELLOW}IMPORTANT: Edit config.env with your Snowflake details before continuing.${NC}"
        echo "At minimum, update:"
        echo "  - SNOWFLAKE_CONNECTION (your Snowflake CLI connection name)"
        echo "  - SNOWFLAKE_ACCOUNT (your account identifier)"
        echo "  - SNOWFLAKE_USER (your username)"
        echo ""
        read -p "Open config.env in your editor now, then press Enter to continue... "
    else
        echo -e "${RED}Error: Neither config.env nor config.env.template found!${NC}"
        exit 1
    fi
fi

# Load configuration
source config.env

SNOWFLAKE_CONNECTION="${SNOWFLAKE_CONNECTION:-default}"
SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-FLEET_DB}"

# Check Snowflake CLI
if ! command -v snow &> /dev/null; then
    echo -e "${RED}Error: Snowflake CLI not found!${NC}"
    echo "Install it: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation"
    exit 1
fi

echo -e "${GREEN}Configuration loaded${NC}"
echo "  Connection: $SNOWFLAKE_CONNECTION"
echo "  Database:   $SNOWFLAKE_DATABASE"
echo ""

# Test connection
echo "Testing Snowflake CLI connection..."
if ! snow connection test --connection "$SNOWFLAKE_CONNECTION" &> /dev/null; then
    echo -e "${YELLOW}Warning: Could not verify connection '$SNOWFLAKE_CONNECTION'.${NC}"
    echo "Run: snow connection list"
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo -e "${GREEN}Connection verified!${NC}"
fi
echo ""

# Step 1: Python environment
echo -e "${YELLOW}Step 1: Setting up Python environment...${NC}"
if [ ! -d "fleet_venv" ]; then
    python3 -m venv fleet_venv
fi
source fleet_venv/bin/activate
pip install -q snowflake-connector-python python-dotenv
echo -e "${GREEN}Python environment ready${NC}"
echo ""

# Step 4: Spark environment (optional)
echo -e "${YELLOW}Step 4: Setting up Spark environment (optional)...${NC}"
if command -v conda &> /dev/null; then
    if ! conda env list | grep -q "fleet-spark"; then
        conda create -n fleet-spark python=3.10 -y
        conda run -n fleet-spark pip install pyspark==4.0.0 jupyter python-dotenv
        echo -e "${GREEN}Spark environment 'fleet-spark' created${NC}"
    else
        echo "Environment 'fleet-spark' already exists"
    fi
else
    echo "Conda not found. To use the Spark section:"
    echo "  conda create -n fleet-spark python=3.10 -y && conda activate fleet-spark"
    echo "  pip install pyspark==4.0.0 jupyter python-dotenv"
fi
echo ""

echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Next steps:"
echo ""
echo "1. Start the streaming simulation (in a separate terminal):"
echo "   source fleet_venv/bin/activate"
echo "   python stream_telemetry.py"
echo ""
echo "2. Follow the Quickstart guide!"
echo ""
echo "3. (Optional) For Spark interoperability:"
echo "   conda activate fleet-spark"
echo "   jupyter notebook spark_iceberg_interop.ipynb"
echo ""
