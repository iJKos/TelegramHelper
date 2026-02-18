#!/bin/bash
set -e

# ==========================================
# Telegram Helper - Deploy Script
# ==========================================
# Usage: ./deploy.sh <droplet-ip> [ssh-key-path]
#
# Example:
#   ./deploy.sh 123.45.67.89
#   ./deploy.sh 123.45.67.89 ~/.ssh/id_rsa_digitalocean
# ==========================================

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DROPLET_IP="${1}"
SSH_KEY="${2:-~/.ssh/id_rsa}"
SSH_USER="root"
REMOTE_DIR="/opt/telegram-helper"
APP_NAME="telegram-helper"

# Validate arguments
if [ -z "$DROPLET_IP" ]; then
    echo -e "${RED}Error: Droplet IP is required${NC}"
    echo "Usage: ./deploy.sh <droplet-ip> [ssh-key-path]"
    exit 1
fi

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo "Create .env file with required variables before deploying"
    exit 1
fi

# Check if session.session exists
if [ ! -f "session.session" ]; then
    echo -e "${YELLOW}Warning: session.session not found${NC}"
    echo "You may need to authenticate Telegram on the server"
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deploying Telegram Helper to $DROPLET_IP${NC}"
echo -e "${GREEN}========================================${NC}"

# SSH command helper
SSH_CMD="ssh -i $SSH_KEY -o StrictHostKeyChecking=no $SSH_USER@$DROPLET_IP"
SCP_CMD="scp -i $SSH_KEY -o StrictHostKeyChecking=no"

# Step 1: Install Docker on server if needed
echo -e "\n${YELLOW}[1/6] Checking Docker on server...${NC}"
$SSH_CMD "command -v docker > /dev/null 2>&1 || {
    echo 'Installing Docker...'
    apt-get update
    apt-get install -y apt-transport-https ca-certificates curl software-properties-common
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
    add-apt-repository -y 'deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable'
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    systemctl enable docker
    systemctl start docker
}"

# Step 2: Create remote directory
echo -e "\n${YELLOW}[2/6] Creating remote directory...${NC}"
$SSH_CMD "mkdir -p $REMOTE_DIR/data"

# Step 3: Copy files to server
echo -e "\n${YELLOW}[3/6] Copying files to server...${NC}"

# Create tar archive excluding unnecessary files
tar --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='data' \
    --exclude='data' \
    --exclude='.vscode' \
    --exclude='.DS_Store' \
    --exclude='*.session' \
    -czf /tmp/telegram-helper.tar.gz .

$SCP_CMD /tmp/telegram-helper.tar.gz $SSH_USER@$DROPLET_IP:/tmp/
rm /tmp/telegram-helper.tar.gz

# Extract on server
$SSH_CMD "cd $REMOTE_DIR && tar -xzf /tmp/telegram-helper.tar.gz && rm /tmp/telegram-helper.tar.gz"

# Step 4: Copy sensitive files separately
echo -e "\n${YELLOW}[4/6] Copying configuration files...${NC}"
$SCP_CMD .env $SSH_USER@$DROPLET_IP:$REMOTE_DIR/.env

# Copy session file if exists
if [ -f "session.session" ]; then
    $SCP_CMD session.session $SSH_USER@$DROPLET_IP:$REMOTE_DIR/session.session
fi

# Backup remote database before deploy
echo -e "\n${YELLOW}[4.5/6] Backing up remote database...${NC}"
BACKUP_NAME="backup_$(date +%Y%m%d_%H%M%S)"
$SSH_CMD "if [ -d $REMOTE_DIR/data ] && [ \"\$(ls -A $REMOTE_DIR/data 2>/dev/null)\" ]; then
    mkdir -p $REMOTE_DIR/backups
    cp -r $REMOTE_DIR/data $REMOTE_DIR/backups/$BACKUP_NAME
    echo 'Backup created: $REMOTE_DIR/backups/$BACKUP_NAME'
    # Keep only last 5 backups
    cd $REMOTE_DIR/backups && ls -t | tail -n +6 | xargs -r rm -rf
else
    echo 'No data to backup'
fi"

# Step 4.7: Reset scorer model (feature dimensions changed)
echo -e "\n${YELLOW}[4.7/6] Resetting scorer model (incompatible with new features)...${NC}"
$SSH_CMD "rm -f $REMOTE_DIR/data/scorer_model.joblib $REMOTE_DIR/data/scorer_meta.json && echo 'Scorer model reset' || echo 'No scorer files to remove'"

# Step 5: Build and run with Docker Compose
echo -e "\n${YELLOW}[5/6] Building and starting container...${NC}"
$SSH_CMD "cd $REMOTE_DIR && docker compose down 2>/dev/null || true"
$SSH_CMD "cd $REMOTE_DIR && docker compose build --no-cache"
$SSH_CMD "cd $REMOTE_DIR && docker compose up -d"

# Step 6: Check status
echo -e "\n${YELLOW}[6/6] Checking deployment status...${NC}"
sleep 3
$SSH_CMD "docker ps | grep $APP_NAME"

# Get logs
echo -e "\n${YELLOW}Recent logs:${NC}"
$SSH_CMD "docker logs --tail 20 $APP_NAME"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "App URL: http://$DROPLET_IP:8000"
echo -e "\nUseful commands:"
echo -e "  View logs:    ssh -i $SSH_KEY $SSH_USER@$DROPLET_IP 'docker logs -f $APP_NAME'"
echo -e "  Restart:      ssh -i $SSH_KEY $SSH_USER@$DROPLET_IP 'cd $REMOTE_DIR && docker compose restart'"
echo -e "  Stop:         ssh -i $SSH_KEY $SSH_USER@$DROPLET_IP 'cd $REMOTE_DIR && docker compose down'"
