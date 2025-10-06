# 🚀 Deployment Guide

This guide will help you deploy your Telegram bot to GitHub and Railway App.

## 📋 Prerequisites

Before deploying, make sure you have:

1. **Telegram API Credentials**
   - Get `API_ID` and `API_HASH` from [my.telegram.org](https://my.telegram.org)
   - Create a bot with [@BotFather](https://t.me/BotFather) and get `BOT_TOKEN`

2. **MongoDB Database**
   - Create a free MongoDB Atlas account at [cloud.mongodb.com](https://cloud.mongodb.com)
   - Get your connection string

3. **Telegram Groups/Channels**
   - Create a log group and add your bot as admin
   - Create a channel and add your bot as admin
   - Get IDs using [@userinfobot](https://t.me/userinfobot)

4. **Your Telegram User ID**
   - Get from [@userinfobot](https://t.me/userinfobot)

## 🐙 GitHub Deployment

### Step 1: Prepare Your Repository

1. **Initialize Git Repository**
   ```bash
   git init
   git add .
   git commit -m "Initial commit: Telegram bot ready for deployment"
   ```

2. **Create GitHub Repository**
   - Go to [GitHub](https://github.com) and create a new repository
   - Name it something like `telegram-bot` or `restrict-bot-saver`
   - Don't initialize with README (we already have one)

3. **Connect and Push**
   ```bash
   git remote add origin https://github.com/yourusername/your-repo-name.git
   git branch -M main
   git push -u origin main
   ```

### Step 2: Configure Environment Variables

1. Copy `.env.example` to `.env`
2. Fill in all your actual values in `.env`
3. **Never commit the real `.env` file** (it's already in `.gitignore`)

## 🚂 Railway Deployment

### Step 1: Setup Railway Account

1. Go to [Railway.app](https://railway.app)
2. Sign up/login with your GitHub account
3. Connect your GitHub repository

### Step 2: Deploy from GitHub

1. **Create New Project**
   - Click "New Project"
   - Select "Deploy from GitHub repo"
   - Choose your bot repository

2. **Configure Environment Variables**
   In Railway dashboard, go to Variables tab and add:
   ```
   API_ID=your_actual_api_id
   API_HASH=your_actual_api_hash
   BOT_TOKEN=your_actual_bot_token
   OWNER_ID=your_actual_owner_id
   MONGO_DB=your_actual_mongodb_connection_string
   LOG_GROUP=your_actual_log_group_id
   CHANNEL_ID=your_actual_channel_id
   CHANNEL=@your_channel_username
   FREEMIUM_LIMIT=50
   PREMIUM_LIMIT=500
   WEBSITE_URL=upshrink.com
   AD_API=your_shortener_api_key_here
   STRING=your_premium_session_string_here
   DEFAULT_SESSION=your_default_session_string_here
   ```

3. **Deploy**
   - Railway will automatically detect the `Procfile` and `railway.json`
   - The deployment will start automatically
   - Monitor the build logs for any issues

### Step 3: Monitor Deployment

1. **Check Logs**
   - Go to your Railway project dashboard
   - Click on "Deployments" tab
   - Monitor the build and runtime logs

2. **Verify Bot is Running**
   - Check if your bot responds to `/start` command
   - Monitor the logs for any errors

## 🔧 Configuration Files Explained

### `Procfile`
```
web: python -m devgagan
```
Tells Railway how to start your bot.

### `railway.json`
Configures Railway deployment settings:
- Build command
- Start command
- Restart policy
- Environment variables

### `.gitignore`
Prevents sensitive files from being committed:
- `.env` file with real credentials
- Session files
- Cache directories
- Temporary files

### `.env.example`
Template for environment variables with placeholder values.

## 🚨 Security Best Practices

1. **Never commit sensitive data**
   - Real `.env` file is in `.gitignore`
   - Use Railway's environment variables for production

2. **Use strong passwords**
   - MongoDB connection string should use strong password
   - Keep your bot token secure

3. **Regular updates**
   - Keep dependencies updated
   - Monitor for security vulnerabilities

## 🐛 Troubleshooting

### Common Issues

1. **Bot not starting**
   - Check environment variables are set correctly
   - Verify MongoDB connection string
   - Check Railway logs for error messages

2. **Import errors**
   - Ensure all dependencies are in `requirements.txt`
   - Check Python version compatibility

3. **Permission errors**
   - Verify bot is admin in log group and channel
   - Check bot token is valid

### Railway Specific Issues

1. **Build failures**
   - Check `requirements.txt` for invalid packages
   - Monitor build logs in Railway dashboard

2. **Runtime errors**
   - Check application logs in Railway
   - Verify all environment variables are set

## 📞 Support

If you encounter issues:
1. Check the logs first
2. Verify all environment variables
3. Ensure bot has proper permissions
4. Check MongoDB connection

## 🎉 Success!

Once deployed successfully:
- Your bot will be running 24/7 on Railway
- Code is safely stored on GitHub
- Environment variables are secure
- Auto-deployment on code changes (if configured)

---

**Note**: This bot is now optimized for Railway deployment with proper configuration files and security practices.