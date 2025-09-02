# Keansa AI Suite 2025 - Backend

A comprehensive data validation and management backend built with Flask, MySQL, and modern Python practices.

## 🚀 Railway Deployment

This backend is configured for Railway deployment with the following features:

### Features
- **Data Validation Engine**: Excel/CSV file validation with custom rules
- **User Authentication**: Secure login/registration system
- **SFTP Integration**: Remote file handling capabilities  
- **Template Management**: Reusable validation templates
- **Analytics Dashboard**: Data processing insights

### Tech Stack
- **Framework**: Flask 2.3.3
- **Database**: MySQL 8.0
- **Authentication**: bcrypt
- **File Processing**: pandas, openpyxl
- **Production Server**: Gunicorn

## 🔧 Environment Variables

Configure these in your Railway service:

```bash
# Flask Configuration
FLASK_ENV=production
HOST=0.0.0.0  
PORT=${PORT}
SECRET_KEY=${SECRET_KEY}

# Database (Railway MySQL Plugin)
MYSQL_HOST=${MYSQL_HOST}
MYSQL_USER=${MYSQL_USER}
MYSQL_PASSWORD=${MYSQL_PASSWORD}
MYSQL_DATABASE=${MYSQL_DATABASE}
MYSQL_PORT=${MYSQL_PORT}

# Railway Specific
RAILWAY_ENVIRONMENT=production
PYTHONPATH=/app

# Optional: Frontend URL for CORS
FRONTEND_URL=https://your-frontend-url.railway.app
```

## 📦 Deployment Files

- `Procfile`: Gunicorn production server configuration
- `requirements.txt`: Python dependencies
- `runtime.txt`: Python version specification
- `railway.json`: Railway deployment configuration
- `nixpacks.toml`: Advanced build configuration
- `app.py`: Railway entry point

## 🗄️ Database Schema

The application automatically creates the following tables:
- `login_details`: User accounts
- `excel_templates`: File templates
- `template_columns`: Column definitions
- `validation_rule_types`: Validation rules
- `column_validation_rules`: Applied rules
- `validation_history`: Processing history
- `validation_corrections`: Error corrections

## 🔗 API Endpoints

### Authentication
- `POST /authenticate` - User login
- `POST /register` - User registration  
- `POST /logout` - User logout
- `GET /check-auth` - Authentication status

### Templates
- `GET /templates` - List user templates
- `POST /upload` - Upload new file
- `GET /template/<id>/<sheet>` - Get template details

### Validation
- `POST /step/1` - Select headers
- `POST /step/2` - Configure rules
- `GET /rule-configurations` - List configurations

### Health Check
- `GET /health` - Service health status

## 🛠️ Local Development

1. Install dependencies: `pip install -r requirements.txt`
2. Configure `.env` file with local MySQL settings
3. Run: `python run.py`

## 🚀 Production Notes

- Uses `/tmp` directory for file storage in Railway
- Configures SSL settings for production
- Optimized for Railway's ephemeral filesystem
- Health checks configured for reliability