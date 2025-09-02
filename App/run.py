#!/usr/bin/env python3
"""
Keansa AI Suite 2025 - Complete Application
This script contains the complete Flask application with all routes and functionality
"""

import os
import sys
import logging
import io
import csv
import re
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, session, send_file, g
from flask_session import Session
from flask_cors import CORS
from openpyxl import Workbook
from openpyxl.utils.exceptions import IllegalCharacterError
from openpyxl.utils import get_column_letter
import mysql.connector
from mysql.connector import errorcode
import bcrypt
import paramiko
import json
from io import StringIO
import numexpr
import numpy as np
from typing import Dict, Tuple, List
import operator

# Add the current directory to Python path
current_dir = Path(__file__).parent.absolute()
sys.path.insert(0, str(current_dir))

# Load environment variables
load_dotenv()

def setup_logging():
    """Setup application logging with Windows Unicode support"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('app.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def create_directories():
    """Create necessary directories"""
    # Use /tmp for Railway production, local directories for development
    if os.getenv('RAILWAY_ENVIRONMENT') == 'production':
        directories = {
            'uploads': '/tmp/uploads',
            'sessions': '/tmp/sessions', 
            'logs': '/tmp/logs',
            'backups': '/tmp/backups'
        }
    else:
        directories = {
            'uploads': str(current_dir / 'uploads'),
            'sessions': str(current_dir / 'sessions'),
            'logs': str(current_dir / 'logs'),
            'backups': str(current_dir / 'backups')
        }
    
    for name, path in directories.items():
        os.makedirs(path, exist_ok=True)
        print(f"[✓] Directory ensured: {name} -> {path}")
    
    return directories

# Database configuration
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'Keansa@2024'),
    'database': os.getenv('MYSQL_DATABASE', 'data_validation_2'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'ssl_disabled': os.getenv('RAILWAY_ENVIRONMENT') == 'production'
}

def get_db_connection():
    if 'db' not in g:
        try:
            conn = mysql.connector.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password']
            )
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            cursor.close()
            conn.close()
            g.db = mysql.connector.connect(**DB_CONFIG)
            logging.info("Database connection established successfully")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error("Database connection failed: Access denied")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error("Database connection failed: Database does not exist")
            else:
                logging.error(f"Database connection failed: {err}")
            raise Exception(f"Failed to connect to database: {str(err)}")
    return g.db

def close_db(error):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        tables = [
            """
            CREATE TABLE IF NOT EXISTS login_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255) UNIQUE,
                mobile VARCHAR(10),
                password VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS excel_templates (
                template_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                template_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                user_id INT NOT NULL,
                sheet_name VARCHAR(255),
                headers JSON,
                status ENUM('ACTIVE', 'INACTIVE') DEFAULT 'ACTIVE',
                is_corrected BOOLEAN DEFAULT FALSE,
                remote_file_path VARCHAR(512),
                FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS template_columns (
                column_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                template_id BIGINT NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                column_position INT NOT NULL,
                is_validation_enabled BOOLEAN DEFAULT FALSE,
                is_selected BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                UNIQUE (template_id, column_name)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS validation_rule_types (
                rule_type_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                rule_name VARCHAR(255) NOT NULL,
                description TEXT,
                parameters TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                is_custom BOOLEAN DEFAULT FALSE,
                column_name VARCHAR(255),
                template_id BIGINT,
                data_type VARCHAR(50),
                source_format VARCHAR(50),
                target_format VARCHAR(50),
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS column_validation_rules (
                column_validation_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                column_id BIGINT NOT NULL,
                rule_type_id BIGINT NOT NULL,
                rule_config JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (column_id) REFERENCES template_columns(column_id) ON DELETE CASCADE,
                FOREIGN KEY (rule_type_id) REFERENCES validation_rule_types(rule_type_id) ON DELETE RESTRICT,
                UNIQUE (column_id, rule_type_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS validation_history (
                history_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                template_id BIGINT NOT NULL,
                template_name VARCHAR(255) NOT NULL,
                error_count INT NOT NULL,
                corrected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                corrected_file_path VARCHAR(512) NOT NULL,
                user_id INT NOT NULL,
                FOREIGN KEY (template_id) REFERENCES excel_templates(template_id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES login_details(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS validation_corrections (
                correction_id BIGINT PRIMARY KEY AUTO_INCREMENT,
                history_id BIGINT NOT NULL,
                row_index INT NOT NULL,
                column_name VARCHAR(255) NOT NULL,
                original_value TEXT,
                corrected_value TEXT,
                rule_failed VARCHAR(255) DEFAULT NULL,
                FOREIGN KEY (history_id) REFERENCES validation_history(history_id) ON DELETE CASCADE
            )
            """
        ]
        for table_sql in tables:
            cursor.execute(table_sql)
        
        # Add missing columns if they don't exist
        add_columns = [
            ("validation_rule_types", "source_format", "VARCHAR(50)"),
            ("validation_rule_types", "target_format", "VARCHAR(50)"),
            ("validation_rule_types", "data_type", "VARCHAR(50)"),
            ("excel_templates", "remote_file_path", "VARCHAR(512)"),
            ("template_columns", "is_selected", "BOOLEAN DEFAULT FALSE")
        ]
        
        for table, column, definition in add_columns:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                logging.info(f"Added {column} column to {table} table")
            except mysql.connector.Error:
                pass  # Column already exists
        
        conn.commit()
        cursor.close()
        logging.info("Database tables initialized")
    except Exception as e:
        logging.error(f"Failed to initialize database: {str(e)}")
        raise

def create_admin_user():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        admin_password = bcrypt.hashpw('admin'.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cursor.execute("""
            INSERT IGNORE INTO login_details (first_name, last_name, email, mobile, password)
            VALUES (%s, %s, %s, %s, %s)
        """, ('Admin', 'User', 'admin@example.com', '1234567890', admin_password))
        conn.commit()
        cursor.close()
        logging.info("Admin user created or already exists")
    except Exception as e:
        logging.error(f"Failed to create admin user: {str(e)}")
        raise

def create_default_validation_rules():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        default_rules = [
            ("Required", "Ensures the field is not null", '{"allow_null": false}', None, None, None),
            ("Int", "Validates integer format", '{"format": "integer"}', None, None, "Int"),
            ("Float", "Validates number format (integer or decimal)", '{"format": "float"}', None, None, "Float"),
            ("Text", "Allows text with quotes and parentheses", '{"allow_special": false}', None, None, "Text"),
            ("Email", "Validates email format", '{"regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$"}', None, None, "Email"),
            ("Date(DD-MM-YYYY)", "Validates date format DD-MM-YYYY", '{"format": "%d-%m-%Y"}', "DD-MM-YYYY", None, "Date"),
            ("Boolean", "Validates boolean format (true/false or 0/1)", '{"format": "boolean"}', None, None, "Boolean"),
            ("Alphanumeric", "Validates alphanumeric format", '{"format": "alphanumeric"}', None, None, "Alphanumeric")
        ]
        cursor.executemany("""
            INSERT IGNORE INTO validation_rule_types (rule_name, description, parameters, is_custom, source_format, target_format, data_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, [(name, desc, params, False, source, target, dtype) for name, desc, params, source, target, dtype in default_rules])
        conn.commit()
        cursor.close()
        logging.info("Default validation rules ensured successfully")
    except Exception as e:
        logging.error(f"Failed to ensure default validation rules: {str(e)}")
        raise

def detect_column_type(series):
    non_null = series.dropna().astype(str)
    if non_null.empty:
        return "Text"
    if non_null.str.match(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$").all():
        return "Email"
    try:
        pd.to_datetime(non_null, format="%d-%m-%Y")
        return "Date"
    except Exception:
        try:
            pd.to_datetime(non_null, format="%Y-%m-%d")
            return "Date"
        except Exception:
            pass
    if non_null.str.lower().isin(['true', 'false', '0', '1']).all():
        return "Boolean"
    if non_null.str.match(r"^-?\d+$").all():
        return "Int"
    if non_null.str.match(r"^-?\d+(\.\d+)?$").all():
        return "Float"
    if non_null.str.match(r"^[a-zA-Z0-9]+$").all():
        return "Alphanumeric"
    return "Text"

def assign_default_rules_to_columns(df, headers):
    assignments = {}
    for col in headers:
        col_type = detect_column_type(df[col])
        rules = ["Required"]
        if col_type != "Text" or not any(
            col.lower().startswith(prefix) for prefix in ["name", "address", "phone", "username", "status", "period"]
        ):
            rules.append(col_type)
        else:
            rules.append("Text")
        assignments[col] = rules
    return assignments

def read_file(file_path):
    try:
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            xl = pd.ExcelFile(file_path)
            sheets = {sheet_name: pd.read_excel(file_path, sheet_name=sheet_name, header=None) 
                     for sheet_name in xl.sheet_names}
            return sheets
        elif file_path.endswith(('.txt', '.csv', '.dat')):
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            if not content.strip():
                raise ValueError("File is empty.")
            try:
                dialect = csv.Sniffer().sniff(content[:1024])
                sep = dialect.delimiter
            except:
                sep = detect_delimiter(file_path)
            df = pd.read_csv(file_path, header=None, sep=sep, encoding='utf-8', quotechar='"', engine='python')
            df.columns = [str(col) for col in df.columns]
            return {'Sheet1': df}
        else:
            raise ValueError("Unsupported file type.")
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {str(e)}")
        raise ValueError(f"Error reading file: {str(e)}")

def detect_delimiter(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read(1024)
        if not content.strip():
            return ','
        delimiters = [',', ';', '|', '/', '\t', ':', '-']
        best_delimiter, max_columns, best_consistency = None, 0, 0
        for delim in delimiters:
            try:
                sample_df = pd.read_csv(io.StringIO(content), sep=delim, header=None, nrows=5, quotechar='"', engine='python')
                column_count = sample_df.shape[1]
                row_lengths = [len(row.dropna()) for _, row in sample_df.iterrows()]
                consistency = sum(1 for length in row_lengths if length == column_count) / len(row_lengths)
                if column_count > 1 and column_count > max_columns and consistency > best_consistency:
                    max_columns = column_count
                    best_consistency = consistency
                    best_delimiter = delim
            except Exception:
                continue
        return best_delimiter or ','
    except Exception as e:
        logging.error(f"Error detecting delimiter for {file_path}: {str(e)}")
        return ','

def find_header_row(df, max_rows=10):
    try:
        for i in range(min(len(df), max_rows)):
            row = df.iloc[i].dropna()
            if not row.empty and all(isinstance(x, str) for x in row if pd.notna(x)):
                return i
        return 0 if not df.empty and len(df.columns) > 0 else -1
    except Exception as e:
        logging.error(f"Error finding header row: {str(e)}")
        return -1

def create_app(directories=None):
    """Flask application factory"""
    if directories is None:
        directories = create_directories()
    
    # Determine if running in Railway production
    is_production = os.getenv('RAILWAY_ENVIRONMENT') == 'production'
    
    # Frontend static files path
    if is_production:
        # In Railway, serve API only (frontend deployed separately)
        app = Flask(__name__)
    else:
        # Local development - serve frontend from dist folder
        frontend_dist_path = current_dir.parent.parent / 'Keansa-Ai-Suite2025may-frontend11' / 'dist'
        frontend_dist_path = str(frontend_dist_path.resolve())
        app = Flask(__name__, static_folder=frontend_dist_path, static_url_path='')
    
    # CORS configuration
    allowed_origins = [
        "http://localhost:3000", 
        "http://localhost:8080", 
        "http://localhost:5000",
        os.getenv('FRONTEND_URL', '')
    ]
    if is_production:
        allowed_origins.append("*")  # Allow all origins in production for now
    
    CORS(app, supports_credentials=True, origins=allowed_origins)
    
    # App configuration
    app.secret_key = os.getenv('SECRET_KEY', os.urandom(24).hex())
    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['SESSION_FILE_DIR'] = directories['sessions']
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SECURE'] = is_production
    app.config['PERMANENT_SESSION_LIFETIME'] = 86400
    app.config['UPLOAD_FOLDER'] = directories['uploads']
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
    
    Session(app)
    
    # Register teardown handler
    app.teardown_appcontext(close_db)
    
    return app

def main():
    """Main startup function"""
    print("[🚀] Starting Keansa AI Suite 2025...")
    print(f"[📁] Working directory: {current_dir}")
    
    # Load environment variables from .env file
    env_path = current_dir / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print("[✓] Environment variables loaded from .env file")
    else:
        print("[!] No .env file found")
    
    # Setup logging
    setup_logging()
    
    # Create directories
    directories = create_directories()
    
    try:
        # Get configuration
        port = int(os.getenv('PORT', 5000))
        host = os.getenv('HOST', '0.0.0.0')
        debug = os.getenv('FLASK_ENV', 'production') == 'development'
        
        # Create Flask application
        app = create_app(directories)
        
        print("[✓] Flask application created successfully!")
        print(f"[📁] Upload folder: {app.config['UPLOAD_FOLDER']}")
        print(f"[📁] Session folder: {app.config['SESSION_FILE_DIR']}")

        # Initialize database
        print("[🗃️] Initializing database and default data...")
        with app.app_context():
            init_db()
            create_admin_user()
            create_default_validation_rules()
        
        # Define all routes directly
        @app.route('/', defaults={'path': ''})
        @app.route('/<path:path>')
        def serve(path):
            if path and os.path.exists(os.path.join(app.static_folder, path)):
                return app.send_static_file(path)
            return app.send_static_file('index.html')

        @app.route('/check-auth', methods=['GET'])
        def check_auth():
            try:
                if 'loggedin' in session and 'user_id' in session:
                    conn = get_db_connection()
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT email, first_name FROM login_details WHERE id = %s", (session['user_id'],))
                    user = cursor.fetchone()
                    cursor.close()
                    if user:
                        return jsonify({
                            'success': True,
                            'user': {
                                'email': user['email'],
                                'id': session['user_id'],
                                'first_name': user['first_name']
                            }
                        })
                    else:
                        session.clear()
                        return jsonify({'success': False, 'message': 'User not found'}), 401
                return jsonify({'success': False, 'message': 'Not logged in'}), 401
            except Exception as e:
                return jsonify({'success': False, 'message': f'Server error: {str(e)}'}), 500

        @app.route('/authenticate', methods=['POST'])
        def authenticate():
            try:
                email = request.form.get('username') or request.form.get('email')
                password = request.form.get('password')
                if not email or not password:
                    return jsonify({'success': False, 'message': 'Email and password are required'}), 400

                if email == "admin" and password == "admin":
                    session['loggedin'] = True
                    session['user_email'] = "admin@example.com"
                    session['user_id'] = 1
                    session.permanent = True
                    return jsonify({'success': True, 'message': 'Login successful', 'user': {'email': 'admin@example.com', 'id': 1}}), 200

                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM login_details WHERE LOWER(email) = LOWER(%s)", (email.lower(),))
                account = cursor.fetchone()
                cursor.close()
                
                if account and bcrypt.checkpw(password.encode('utf-8'), account['password'].encode('utf-8')):
                    session['loggedin'] = True
                    session['user_email'] = account['email']
                    session['user_id'] = account['id']
                    session.permanent = True
                    return jsonify({'success': True, 'message': 'Login successful', 'user': {'email': account['email'], 'id': account['id']}}), 200
                else:
                    return jsonify({'success': False, 'message': 'Invalid credentials'}), 401
            except Exception as e:
                return jsonify({'success': False, 'message': f'Login error: {str(e)}'}), 500

        @app.route('/register', methods=['POST'])
        def register():
            first_name = request.form.get('first_name')
            last_name = request.form.get('last_name')
            email = request.form.get('email')
            mobile = request.form.get('mobile')
            password = request.form.get('password')
            confirm_password = request.form.get('confirm_password')

            if not all([first_name, last_name, email, mobile, password, confirm_password]):
                return jsonify({'success': False, 'message': 'All fields are required'}), 400
            if password != confirm_password:
                return jsonify({'success': False, 'message': 'Passwords do not match'}), 400

            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            try:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO login_details (first_name, last_name, email, mobile, password)
                    VALUES (%s, %s, %s, %s, %s)
                """, (first_name, last_name, email, mobile, hashed_password))
                user_id = cursor.lastrowid
                conn.commit()
                cursor.close()
                session['loggedin'] = True
                session['user_email'] = email
                session['user_id'] = user_id
                return jsonify({'success': True, 'message': 'Registration successful', 'user': {'email': email, 'id': user_id}}), 200
            except mysql.connector.Error as e:
                return jsonify({'success': False, 'message': f'Registration error: {str(e)}'}), 500

        @app.route('/logout', methods=['POST'])
        def logout():
            session.clear()
            return jsonify({'success': True, 'message': 'Logged out successfully'})

        @app.route('/templates', methods=['GET'])
        def get_templates():
            if 'loggedin' not in session or 'user_id' not in session:
                return jsonify({'success': False, 'message': 'Not logged in'}), 401
            try:
                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("""
                    SELECT template_id, template_name, created_at, status
                    FROM excel_templates
                    WHERE user_id = %s AND status = 'ACTIVE'
                    ORDER BY created_at DESC
                    LIMIT 100
                """, (session['user_id'],))
                templates = cursor.fetchall()
                cursor.close()
                return jsonify({'success': True, 'templates': templates})
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500

        @app.route('/rule-configurations', methods=['GET'])
        def get_rule_configurations():
            if 'loggedin' not in session or 'user_id' not in session:
                return jsonify({'success': False, 'message': 'Not logged in'}), 401
            try:
                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("""
                    SELECT 
                        t.template_id, 
                        t.template_name, 
                        t.created_at, 
                        COUNT(cvr.column_validation_id) as rule_count
                    FROM excel_templates t
                    LEFT JOIN template_columns tc ON t.template_id = tc.template_id
                    LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                    WHERE t.user_id = %s AND t.status = 'ACTIVE' AND t.is_corrected = FALSE
                    GROUP BY t.template_id, t.template_name, t.created_at
                    HAVING rule_count > 0
                    ORDER BY t.created_at DESC
                    LIMIT 100
                """, (session['user_id'],))
                templates = cursor.fetchall()
                cursor.close()
                return jsonify({'success': True, 'templates': templates})
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500

        @app.route('/upload', methods=['POST'])
        def upload():
            if 'loggedin' not in session or 'user_id' not in session:
                return jsonify({'error': 'Not logged in'}), 401
            if 'file' not in request.files:
                return jsonify({'error': 'No file uploaded'}), 400
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            try:
                file.save(file_path)
            except Exception as e:
                return jsonify({'error': f'Failed to save file: {str(e)}'}), 500

            try:
                sheets = read_file(file_path)
            except Exception as e:
                return jsonify({'error': f'Failed to read file: {str(e)}'}), 400

            try:
                sheet_names = list(sheets.keys())
                if not sheet_names:
                    return jsonify({'error': 'No sheets found in the file'}), 400
                sheet_name = sheet_names[0]
                df = sheets[sheet_name]
                header_row = find_header_row(df)
                if header_row == -1:
                    return jsonify({'error': 'Could not detect header row'}), 400
                headers = df.iloc[header_row].tolist()
                if not headers or all(not h for h in headers):
                    return jsonify({'error': 'No valid headers found in the file'}), 400
            except Exception as e:
                return jsonify({'error': f'Error processing file: {str(e)}'}), 400

            # Clear session data
            for key in ['df', 'header_row', 'headers', 'sheet_name', 'current_step', 'selected_headers', 'validations', 'error_cell_locations', 'data_rows', 'corrected_file_path']:
                session.pop(key, None)

            try:
                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)

                # Check if template exists
                cursor.execute("""
                    SELECT template_id, headers, sheet_name
                    FROM excel_templates
                    WHERE template_name = %s AND user_id = %s AND status = 'ACTIVE'
                    ORDER BY created_at DESC
                """, (file.filename, session['user_id']))
                existing_templates = cursor.fetchall()

                template_id = None
                has_existing_rules = False
                validations = {}
                selected_headers = []

                # Check for matching template
                matching_template = None
                for template in existing_templates:
                    stored_headers = json.loads(template['headers']) if template['headers'] else []
                    stored_sheet_name = template['sheet_name']
                    if stored_headers == headers and stored_sheet_name == sheet_name:
                        matching_template = template
                        break

                if matching_template:
                    template_id = matching_template['template_id']
                    # Check for existing rules
                    cursor.execute("""
                        SELECT tc.column_name, vrt.rule_name
                        FROM template_columns tc
                        JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                        JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
                        WHERE tc.template_id = %s AND tc.is_selected = TRUE
                    """, (template_id,))
                    rules_data = cursor.fetchall()
                    for row in rules_data:
                        column_name = row['column_name']
                        rule_name = row['rule_name']
                        if column_name not in validations:
                            validations[column_name] = []
                        validations[column_name].append(rule_name)
                        if column_name not in selected_headers:
                            selected_headers.append(column_name)
                    has_existing_rules = len(validations) > 0
                else:
                    # New template
                    cursor.execute("""
                        INSERT INTO excel_templates (template_name, user_id, sheet_name, headers, is_corrected)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (file.filename, session['user_id'], sheet_name, json.dumps(headers), False))
                    template_id = cursor.lastrowid
                    column_data = [(template_id, header, i + 1, False) for i, header in enumerate(headers)]
                    cursor.executemany("""
                        INSERT INTO template_columns (template_id, column_name, column_position, is_selected)
                        VALUES (%s, %s, %s, %s)
                    """, column_data)

                conn.commit()
                cursor.close()

                # Store session data
                session['file_path'] = file_path
                session['template_id'] = template_id
                session['df'] = df.to_json()
                session['header_row'] = header_row
                session['headers'] = headers
                session['sheet_name'] = sheet_name
                session['current_step'] = 1 if not has_existing_rules else 3
                session['validations'] = validations
                session['selected_headers'] = selected_headers
                session['has_existing_rules'] = has_existing_rules

                return jsonify({
                    'success': True,
                    'sheets': {sheet_name: {'headers': headers}},
                    'file_name': file.filename,
                    'template_id': template_id,
                    'has_existing_rules': has_existing_rules,
                    'sheet_name': sheet_name,
                    'skip_to_step_3': has_existing_rules
                })
            except Exception as e:
                return jsonify({'error': f'Error saving template: {str(e)}'}), 500

        @app.route('/step/1', methods=['POST'])
        def submit_step_one():
            if 'loggedin' not in session:
                return jsonify({'success': False, 'message': 'Not logged in'}), 401
            try:
                headers = request.form.getlist('headers')
                if not headers:
                    return jsonify({'success': False, 'message': 'No headers provided'}), 400
                    
                template_id = session.get('template_id')
                if not template_id:
                    return jsonify({'success': False, 'message': 'Session data missing'}), 400

                file_path = session['file_path']
                sheets = read_file(file_path)
                sheet_name = session.get('sheet_name', list(sheets.keys())[0])
                df = sheets[sheet_name]
                header_row = find_header_row(df)
                df.columns = session['headers']
                df = df.iloc[header_row + 1:].reset_index(drop=True)

                validations = assign_default_rules_to_columns(df, headers)
                session['selected_headers'] = headers
                session['validations'] = validations
                session['current_step'] = 2

                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("UPDATE template_columns SET is_selected = FALSE WHERE template_id = %s", (template_id,))
                for header in headers:
                    cursor.execute("""
                        UPDATE template_columns SET is_selected = TRUE WHERE template_id = %s AND column_name = %s
                    """, (template_id, header))
                    cursor.execute("""
                        SELECT column_id FROM template_columns WHERE template_id = %s AND column_name = %s
                    """, (template_id, header))
                    column_id = cursor.fetchone()[0]
                    for rule_name in validations.get(header, []):
                        cursor.execute("""
                            SELECT rule_type_id FROM validation_rule_types WHERE rule_name = %s AND is_custom = FALSE
                        """, (rule_name,))
                        result = cursor.fetchone()
                        if result:
                            rule_type_id = result[0]
                            cursor.execute("""
                                INSERT IGNORE INTO column_validation_rules (column_id, rule_type_id, rule_config)
                                VALUES (%s, %s, %s)
                            """, (column_id, rule_type_id, '{}'))
                conn.commit()
                cursor.close()
                return jsonify({'success': True, 'headers': headers, 'validations': validations})
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500

        @app.route('/step/2', methods=['POST'])
        def submit_step_two():
            if 'loggedin' not in session:
                return jsonify({'success': False, 'message': 'Not logged in'}), 401
            try:
                action = request.form.get('action', 'save')
                validations = {}
                for key, values in request.form.lists():
                    if key.startswith('validations_'):
                        header = key.replace('validations_', '')
                        validations[header] = values

                template_id = session.get('template_id')
                if not template_id:
                    return jsonify({'success': False, 'message': 'Session data missing'}), 400

                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM column_validation_rules
                    WHERE column_id IN (SELECT column_id FROM template_columns WHERE template_id = %s)
                """, (template_id,))
                
                for header, rules in validations.items():
                    cursor.execute("""
                        SELECT column_id FROM template_columns WHERE template_id = %s AND column_name = %s
                    """, (template_id, header))
                    result = cursor.fetchone()
                    if not result:
                        continue
                    column_id = result[0]
                    for rule_name in rules:
                        cursor.execute("""
                            SELECT rule_type_id FROM validation_rule_types WHERE rule_name = %s
                        """, (rule_name,))
                        result = cursor.fetchone()
                        if result:
                            rule_type_id = result[0]
                            cursor.execute("""
                                INSERT IGNORE INTO column_validation_rules (column_id, rule_type_id, rule_config)
                                VALUES (%s, %s, %s)
                            """, (column_id, rule_type_id, '{}'))
                
                conn.commit()
                cursor.close()

                session['validations'] = validations
                session['current_step'] = 3 if action == 'review' else 2
                return jsonify({'success': True, 'message': 'Step 2 completed successfully'})
            except Exception as e:
                return jsonify({'success': False, 'message': str(e)}), 500

        @app.route('/template/<int:template_id>/<sheet_name>', methods=['GET'])
        def get_template(template_id, sheet_name):
            if 'loggedin' not in session:
                return jsonify({'error': 'Not logged in'}), 401
            try:
                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("""
                    SELECT template_name, sheet_name, headers
                    FROM excel_templates
                    WHERE template_id = %s AND user_id = %s AND status = 'ACTIVE'
                """, (template_id, session['user_id']))
                template_record = cursor.fetchone()
                if not template_record:
                    cursor.close()
                    return jsonify({'error': 'Template not found'}), 404

                headers = json.loads(template_record['headers']) if template_record['headers'] else []
                stored_sheet_name = template_record['sheet_name'] or sheet_name
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], template_record['template_name'])

                cursor.execute("""
                    SELECT COUNT(*) as rule_count
                    FROM template_columns tc
                    JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
                    WHERE tc.template_id = %s AND tc.is_selected = TRUE
                """, (template_id,))
                rule_count = cursor.fetchone()['rule_count']
                has_existing_rules = rule_count > 0

                cursor.close()
                return jsonify({
                    'success': True,
                    'sheets': {stored_sheet_name: {'headers': headers}},
                    'file_name': template_record['template_name'],
                    'file_path': file_path,
                    'sheet_name': stored_sheet_name,
                    'has_existing_rules': has_existing_rules
                })
            except Exception as e:
                return jsonify({'error': str(e)}), 500

        print("[🔧] Creating Flask application...")
        
        # Initialize database
        print("[🗃️] Initializing database and default data...")
        with app.app_context():
            init_db()
            create_admin_user()
            create_default_validation_rules()
        
        print("[✓] Application imported successfully!")
        print(f"[🌐] Server configuration:")
        print(f"   Host: {host}")
        print(f"   Port: {port}")
        print(f"   Debug: {debug}")
        print(f"   Environment: {os.getenv('FLASK_ENV', 'production')}")
        
        print("[✓] Application initialized successfully!")
        print(f"[🚀] Starting server on http://{host}:{port}")
        print("[🛑] Press Ctrl+C to stop the server")
        
        # Start the Flask development server
        app.run(
            host=host,
            port=port,
            debug=debug,
            threaded=True
        )
        
    except KeyboardInterrupt:
        print("\n[🛑] Server stopped by user")
    except Exception as e:
        logging.error(f"[X] Failed to start application: {str(e)}")
        print(f"[X] Error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
