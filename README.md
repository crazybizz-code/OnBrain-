# 🤖 OnBrain AI - Telegram Bot

**Production-ready Telegram bot with Google Sheets integration, Excel file support, and AI-powered chat using OpenAI GPT-4o**

## ✨ Features

### 📱 User Registration
- `/start` command triggers registration flow
- Collects user's full name and email
- Stores data securely in Supabase
- Prevents duplicate email registration

### 📊 Google Sheets Integration
- OAuth 2.0 authentication with Google
- Lists all user's Google Sheets
- Read-only access to spreadsheet data
- Automatic token refresh
- Secure credential management

### 📁 Excel File Support
- Upload `.xlsx` and `.xls` files
- Automatic file validation
- Secure file storage
- Data extraction and indexing

### 🧠 AI-Powered Chat
- Question answering based on connected data
- GPT-4o model for intelligent responses
- Answers in Uzbek language
- Conversation history tracking
- Context-aware responses

### 💾 Data Management
- Supabase cloud database integration
- User profile management
- Integration tracking
- Message history logging
- Automatic data cleanup

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Bot Framework** | aiogram | 3.3.0 |
| **Language** | Python | 3.11+ |
| **AI Model** | OpenAI GPT-4o | Latest |
| **Sheets API** | gspread | 5.12.0 |
| **Excel** | openpyxl | 3.11.0 |
| **Database** | Supabase (PostgreSQL) | 2.3.5 |
| **HTTP Client** | aiohttp | 3.9.1 |
| **Authentication** | google-auth-oauthlib | 1.2.0 |

## 📋 Prerequisites

Before starting, ensure you have:

- **Python 3.11+** installed
- **pip** package manager
- **Telegram account** with BotFather bot created
- **Google Cloud account** with project setup
- **OpenAI account** with API access
- **Supabase account** with database created

## 🚀 Installation

### Step 1: Clone Repository
```bash
cd "c:\Users\Lenovo\OneDrive\OnBrain AI"
git init
git config user.email "your@email.com"
git config user.name "Your Name"
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 3: Configure Environment
Edit `.env` file and add all required credentials:
```env
BOT_TOKEN=your_telegram_bot_token
OPENAI_API_KEY=your_openai_api_key
GOOGLE_CLIENT_ID=your_google_client_id
GOOGLE_CLIENT_SECRET=your_google_client_secret
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_supabase_anon_key
OAUTH_REDIRECT_URI=http://localhost:8080/
```

### Step 4: Setup Google OAuth
1. Download `credentials.json` from Google Cloud Console
2. Place it in project root directory
3. Verify redirect URI is set to `http://localhost:8080/`

### Step 5: Setup Supabase Database
Run SQL queries in Supabase to create tables:
```sql
-- Users table
create table users (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint unique not null,
  full_name text not null,
  email text not null,
  created_at timestamp default now()
);

-- Integrations table
create table integrations (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint not null references users(telegram_id),
  sheet_id text not null,
  sheet_name text not null,
  is_active boolean default true,
  created_at timestamp default now()
);

-- Messages table
create table messages (
  id uuid primary key default gen_random_uuid(),
  telegram_id bigint not null references users(telegram_id),
  question text not null,
  answer text not null,
  created_at timestamp default now()
);
```

### Step 6: Run the Bot
```bash
python bot.py
```

## 📱 Bot Usage

### For Users

1. **Start Bot**
   - Send `/start` to begin
   - Register with name and email

2. **Connect Google Sheets**
   - Click "📊 Google Sheets ulash"
   - Authorize with Google account
   - Select spreadsheet to connect

3. **Upload Excel File**
   - Click "📁 Excel fayl yuklash"
   - Send `.xlsx` or `.xls` file
   - Bot confirms upload

4. **Ask Questions**
   - Send any question about your data
   - Bot analyzes with GPT-4o
   - Receives answer in Uzbek

## 🏗️ Project Structure

```
OnBrain AI/
├── bot.py                    # Main bot application
├── oauth_handler.py          # Google OAuth handler
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (git ignored)
├── .env.example              # Example environment file
├── credentials.json          # Google OAuth credentials (git ignored)
├── token.json               # Google OAuth token (auto-generated)
├── SETUP_GUIDE.md           # Detailed setup instructions
├── README.md                # This file
└── excel_files/             # Uploaded Excel files (auto-created)
```

## 🔄 Bot Workflow

```
User sends /start
    ↓
Check if registered
    ├─ Yes → Show main menu
    └─ No → Ask for registration
         ↓
         Collect name & email
         ↓
         Save to Supabase
         ↓
         Show main menu
    ↓
User chooses action
    ├─ Google Sheets
    │   ├─ Generate OAuth link
    │   ├─ User authenticates
    │   ├─ Show sheets list
    │   └─ Save selection
    │
    └─ Excel Upload
        ├─ Ask for file
        ├─ Validate format
        ├─ Read & store data
        └─ Ready for chat
    ↓
User asks question
    ↓
Bot reads connected data
    ↓
Send to GPT-4o with context
    ↓
Receive AI response
    ↓
Save to database
    ↓
Send answer to user (Uzbek)
```

## 🔐 Security Features

✅ **Secure Credential Storage**
- Environment variables for all secrets
- Never commit `.env` or `credentials.json`
- OAuth tokens stored locally with encryption

✅ **Data Protection**
- Supabase RLS (Row Level Security) ready
- User-scoped data isolation
- Secure file upload validation

✅ **Error Handling**
- Graceful error messages
- No sensitive data in logs
- Input validation on all fields

✅ **Authentication**
- Telegram user ID verification
- Google OAuth 2.0 with refresh tokens
- Supabase authentication ready

## 📊 Database Schema

### users
```
id (uuid)              - Primary key
telegram_id (bigint)   - Unique telegram ID
full_name (text)       - User's full name
email (text)           - User's email
created_at (timestamp) - Registration date
```

### integrations
```
id (uuid)              - Primary key
telegram_id (bigint)   - References users
sheet_id (text)        - Google Sheet ID
sheet_name (text)      - Sheet name
is_active (boolean)    - Currently active
created_at (timestamp) - Creation date
```

### messages
```
id (uuid)              - Primary key
telegram_id (bigint)   - References users
question (text)        - User's question
answer (text)          - Bot's answer
created_at (timestamp) - Message date
```

## ⚙️ Configuration

### Bot Settings
- **Polling**: Long polling for message retrieval
- **Timeout**: 30 seconds per request
- **Retry**: Automatic reconnection
- **Language**: Uzbek (O'zbek)

### Keyboard Layout
- **Main Menu**: 2 inline buttons
- **Excel Upload**: 1 cancel button
- **Chat Mode**: Standard text input

### State Management
Uses FSM (Finite State Machine) for clean state handling:
- Registration states
- Data processing states
- Chat mode states

## 🐛 Troubleshooting

### Bot doesn't start
```bash
# Check Python version
python --version  # Should be 3.11+

# Verify dependencies
pip list | grep aiogram

# Check .env file exists
dir .env
```

### Can't connect to Google Sheets
- ✅ Verify `credentials.json` exists
- ✅ Check Google APIs are enabled
- ✅ Ensure OAuth redirect URI matches
- ✅ Check internet connection

### OpenAI API errors
- ✅ Verify API key is correct
- ✅ Check API quota and billing
- ✅ Ensure model `gpt-4o` is available

### Supabase connection fails
- ✅ Check URL and key are correct
- ✅ Verify tables exist in database
- ✅ Check project is active

### Excel upload issues
- ✅ File must be `.xlsx` or `.xls`
- ✅ File shouldn't be corrupted
- ✅ Check disk space available

## 📚 Documentation

- [Aiogram Docs](https://docs.aiogram.dev/)
- [Telegram Bot API](https://core.telegram.org/bots/api)
- [OpenAI API Docs](https://platform.openai.com/docs)
- [Google Sheets API](https://developers.google.com/sheets/api)
- [Supabase Docs](https://supabase.com/docs)
- [gspread Docs](https://docs.gspread.org/)

## 🚀 Deployment

### Local Development
```bash
python bot.py
```

### Heroku/Cloud Platforms
1. Create `Procfile`:
   ```
   worker: python bot.py
   ```
2. Set environment variables on platform
3. Deploy code

### Docker Deployment
Create `Dockerfile`:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "bot.py"]
```

## 🎯 Features Roadmap

- [ ] Webhook support for production
- [ ] Multi-language support expansion
- [ ] Advanced data visualization
- [ ] Scheduled reports
- [ ] Admin dashboard
- [ ] Rate limiting
- [ ] Analytics tracking
- [ ] Payment integration

## 📝 License

This project is provided as-is for production use.

## 🤝 Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## 📞 Support

For issues or questions:
1. Check SETUP_GUIDE.md
2. Review error logs
3. Check troubleshooting section
4. Open an issue on GitHub

## 👨‍💻 Author

**OnBrain AI Team**
- Created: March 2026
- Language: Uzbek (O'zbek)
- Status: Production Ready

## 📦 Version Info

```
Bot Version: 1.0.0
Python: 3.11+
aiogram: 3.3.0
Status: Stable
Last Updated: March 2026
```

---

**🌟 Star us if you find this helpful!**

Made with ❤️ for the Uzbek developer community.
