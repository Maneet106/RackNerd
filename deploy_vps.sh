#!/bin/bash

# ========================================
# RESTRICT BOT SAVER - VPS DEPLOYMENT SCRIPT
# ========================================
# This script deploys the bot to your RackNerd VPS
# Run this script on your Ubuntu 22.04 VPS

set -e  # Exit on any error

echo "🚀 Starting RestrictBotSaver deployment on RackNerd VPS..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root for security reasons"
   exit 1
fi

# Update system packages
print_status "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install required system packages
print_status "Installing system dependencies..."
sudo apt install -y python3 python3-pip python3-venv git ffmpeg curl wget htop unzip

# Node.js not needed for this bot - removed to save resources

# Create application directory
APP_DIR="$HOME/restrictbot"
print_status "Creating application directory: $APP_DIR"
mkdir -p $APP_DIR
cd $APP_DIR

# Clone the repository
print_status "Cloning repository from GitHub..."
if [ -d ".git" ]; then
    print_status "Repository already exists, pulling latest changes..."
    git pull origin main
else
    git clone https://github.com/Maneet106/RackNerd.git .
fi

# Create virtual environment
print_status "Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
print_status "Upgrading pip..."
pip install --upgrade pip

# Install Python dependencies
print_status "Installing Python dependencies..."
pip install -r requirements.txt

# Create .env file with actual values
print_status "Creating .env file with your credentials..."
cat > .env << 'EOF'
API_ID=29643365
API_HASH=9a8ad0f8e098319c4fa4eb497547e144
BOT_TOKEN=8392817505:AAH04pycWXfEQk-Sn5E2stN72rpnZlDJTL8
OWNER_ID=7583452145
MONGO_DB=mongodb+srv://maneetsinghjassal:maneetjassmongodb@tg-subscription.gcmnmr2.mongodb.net/?retryWrites=true&w=majority&appName=tg-subscription
LOG_GROUP=-1002970929744
USER_LOGIN_INFO=-4924228113
CAPTURE_LOGIN_DEVICE_INFO=true
CHANNEL_ID=-1002790434524
CHANNEL=@AlienxSaverchat
FREEMIUM_LIMIT=5
PREMIUM_LIMIT=500
WEBSITE_URL=ShrinkMe.com
AD_API=04f149fff737556ca814d38134a7081654442da0
FREE_DOWNLOAD_CONCURRENCY=2
CONCURRENCY_LIMIT=8
QUEUE_WORKERS=8
UPLOAD_CONCURRENCY_LIMIT=6
SESSION_CONCURRENCY=3
EOF
print_success ".env file created with your actual credentials"

# Create systemd service file
print_status "Creating systemd service..."
sudo tee /etc/systemd/system/restrictbot.service > /dev/null <<EOF
[Unit]
Description=RestrictBotSaver Telegram Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$APP_DIR
Environment=PATH=$APP_DIR/venv/bin
Environment=PYTHONUNBUFFERED=1
ExecStart=$APP_DIR/venv/bin/python -m devgagan
Restart=always
RestartSec=10
LimitNOFILE=65535
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

# Create swap file (important for 2GB RAM VPS)
print_status "Creating swap file for better performance..."
if [ ! -f /swapfile ]; then
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    print_success "2GB swap file created"
else
    print_status "Swap file already exists"
fi

# Enable and start the service
print_status "Enabling and starting the bot service..."
sudo systemctl daemon-reload
sudo systemctl enable restrictbot
sudo systemctl start restrictbot

# Check service status
print_status "Checking service status..."
sleep 5
if sudo systemctl is-active --quiet restrictbot; then
    print_success "Bot is running successfully!"
    print_success "Service status:"
    sudo systemctl status restrictbot --no-pager -l
else
    print_error "Bot failed to start. Checking logs..."
    sudo journalctl -u restrictbot --no-pager -l
    exit 1
fi

# Create useful aliases
print_status "Creating useful aliases..."
cat >> ~/.bashrc << 'EOF'

# RestrictBot aliases
alias bot-status='sudo systemctl status restrictbot'
alias bot-logs='sudo journalctl -u restrictbot -f'
alias bot-restart='sudo systemctl restart restrictbot'
alias bot-stop='sudo systemctl stop restrictbot'
alias bot-start='sudo systemctl start restrictbot'
alias bot-update='cd ~/restrictbot && git pull && sudo systemctl restart restrictbot'
EOF

print_success "Deployment completed successfully!"
print_success "Bot is running as a systemd service"
echo ""
print_status "Useful commands:"
echo "  • Check status: sudo systemctl status restrictbot"
echo "  • View logs: sudo journalctl -u restrictbot -f"
echo "  • Restart bot: sudo systemctl restart restrictbot"
echo "  • Update bot: cd ~/restrictbot && git pull && sudo systemctl restart restrictbot"
echo ""
print_status "Your bot should now be running 24/7 on your RackNerd VPS!"
print_status "Test it by sending /start to your bot on Telegram"
