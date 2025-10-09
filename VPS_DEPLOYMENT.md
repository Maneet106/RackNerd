# 🚀 VPS Deployment Guide - RackNerd Ubuntu 22.04

This guide will help you deploy your RestrictBotSaver Telegram bot to your RackNerd VPS.

## 📋 VPS Specifications
- **RAM**: 2 GB
- **CPU**: 2 cores
- **OS**: Ubuntu 22.04 64-bit
- **Location**: Toronto, Canada
- **Test IP**: 167.160.186.8

## 🚀 Quick Deployment (Automated)

### Option 1: One-Command Deployment
```bash
curl -fsSL https://raw.githubusercontent.com/Maneet106/RackNerd/main/deploy_vps.sh | bash
```

### Option 2: Manual Step-by-Step Deployment

#### Step 1: Connect to Your VPS
```bash
ssh root@167.160.186.8
# Or ssh username@167.160.186.8
```

#### Step 2: Update System
```bash
apt update && apt upgrade -y
apt install -y python3 python3-pip python3-venv git ffmpeg curl wget htop unzip
```

#### Step 3: Create Application User (Security Best Practice)
```bash
adduser botuser
usermod -aG sudo botuser
su - botuser
```

#### Step 4: Clone Repository
```bash
cd ~
git clone https://github.com/Maneet106/RackNerd.git restrictbot
cd restrictbot
```

#### Step 5: Setup Python Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

#### Step 6: Configure Environment Variables
```bash
cat > .env << 'EOF'
your env configs
EOF
```

#### Step 7: Create Swap File (Important for 2GB RAM)
```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

#### Step 8: Create Systemd Service
```bash
sudo nano /etc/systemd/system/restrictbot.service
```

Add this content:
```ini
[Unit]
Description=RestrictBotSaver Telegram Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=botuser
WorkingDirectory=/home/botuser/restrictbot
Environment=PATH=/home/botuser/restrictbot/venv/bin
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/botuser/restrictbot/venv/bin/python -m devgagan
Restart=always
RestartSec=10
LimitNOFILE=65535
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

#### Step 9: Start the Service
```bash
sudo systemctl daemon-reload
sudo systemctl enable restrictbot
sudo systemctl start restrictbot
```

#### Step 10: Check Status
```bash
sudo systemctl status restrictbot
```

## 🔧 Management Commands

### Service Management
```bash
# Check status
sudo systemctl status restrictbot

# View live logs
sudo journalctl -u restrictbot -f

# Restart bot
sudo systemctl restart restrictbot

# Stop bot
sudo systemctl stop restrictbot

# Start bot
sudo systemctl start restrictbot
```

### Update Bot
```bash
cd ~/restrictbot
git pull origin main
sudo systemctl restart restrictbot
```

### Monitor Resources
```bash
# Check memory usage
free -h

# Check disk usage
df -h

# Check CPU usage
htop
```

## 🛡️ Security Recommendations

1. **Firewall Setup**
```bash
sudo ufw enable
sudo ufw allow ssh
sudo ufw allow 80
sudo ufw allow 443
```

2. **SSH Key Authentication**
- Disable password authentication
- Use SSH keys only

3. **Regular Updates**
```bash
sudo apt update && sudo apt upgrade -y
```

4. **Monitor Logs**
```bash
sudo journalctl -u restrictbot --since "1 hour ago"
```

## 🚨 Troubleshooting

### Bot Not Starting
1. Check logs: `sudo journalctl -u restrictbot -f`
2. Verify .env file has correct values
3. Check MongoDB connection
4. Ensure bot token is valid

### Memory Issues
1. Check swap is active: `swapon --show`
2. Monitor memory: `free -h`
3. Restart if needed: `sudo systemctl restart restrictbot`

### Permission Errors
1. Check file ownership: `ls -la`
2. Fix permissions: `sudo chown -R botuser:botuser /home/botuser/restrictbot`

## 📊 Performance Optimization

### For 2GB RAM VPS:
1. ✅ Swap file created (2GB)
2. ✅ Service restart on failure
3. ✅ Optimized Python dependencies
4. ✅ Efficient memory usage

### Monitoring:
```bash
# Create monitoring script
cat > ~/monitor.sh << 'EOF'
#!/bin/bash
echo "=== Bot Status ==="
sudo systemctl status restrictbot --no-pager
echo "=== Memory Usage ==="
free -h
echo "=== Disk Usage ==="
df -h /
echo "=== Recent Logs ==="
sudo journalctl -u restrictbot --since "10 minutes ago" --no-pager
EOF

chmod +x ~/monitor.sh
```

## 🎉 Success!

Your RestrictBotSaver bot is now running 24/7 on your RackNerd VPS!

- **Service**: `restrictbot`
- **Location**: `/home/botuser/restrictbot`
- **Logs**: `sudo journalctl -u restrictbot -f`
- **Status**: `sudo systemctl status restrictbot`

Test your bot by sending `/start` on Telegram!
