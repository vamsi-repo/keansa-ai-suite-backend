import os
import io
import csv
import re
import pandas as pd
from datetime import datetime
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
import logging
from io import StringIO
import numexpr
import numpy as np
import logging
from typing import Dict,Tuple, List
import pandas as pd
import numexpr
import logging
import pandas as pd
import re
from datetime import datetime, timedelta
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
import logging
import json
import mysql.connector
from io import BytesIO
import operator
from typing import Tuple, List
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.FileHandler('app.log', encoding='utf-8'), logging.StreamHandler()]
)

app = Flask(__name__, static_folder='./dist', static_url_path='')
CORS(app, supports_credentials=True, origins=["http://localhost:3000", "http://localhost:8080", "*"])

app.secret_key = os.urandom(24).hex()
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sessions')
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = False
app.config['PERMANENT_SESSION_LIFETIME'] = 86400  # 24 hours
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
Session(app)

try:
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)
except OSError as e:
    logging.error(f"Failed to create directories: {e}")
    app.config['UPLOAD_FOLDER'] = '/tmp/uploads'
    app.config['SESSION_FILE_DIR'] = '/tmp/sessions'
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'Keansa@2024'),
    'database': os.getenv('MYSQL_DATABASE', 'data_validation_2'),
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
                logging.error("Database connection failed: Access denied for user - check username/password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error("Database connection failed: Database does not exist")
            else:
                logging.error(f"Database connection failed: {err}")
            raise Exception(f"Failed to connect to database: {str(err)}")
    return g.db

@app.teardown_appcontext
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
        cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'source_format'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN source_format VARCHAR(50)")
            logging.info("Added source_format column to validation_rule_types table")
        cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'target_format'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN target_format VARCHAR(50)")
            logging.info("Added target_format column to validation_rule_types table")
        cursor.execute("SHOW COLUMNS FROM validation_rule_types LIKE 'data_type'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE validation_rule_types ADD COLUMN data_type VARCHAR(50)")
            logging.info("Added data_type column to validation_rule_types table")
        cursor.execute("SHOW COLUMNS FROM excel_templates LIKE 'remote_file_path'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE excel_templates ADD COLUMN remote_file_path VARCHAR(512)")
            logging.info("Added remote_file_path column to excel_templates table")
        cursor.execute("SHOW COLUMNS FROM template_columns LIKE 'is_selected'")
        if not cursor.fetchone():
            cursor.execute("ALTER TABLE template_columns ADD COLUMN is_selected BOOLEAN DEFAULT FALSE")
            logging.info("Added is_selected column to template_columns table")
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

# Static file serving
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if path and os.path.exists(os.path.join(app.static_folder, path)):
        return app.send_static_file(path)
    return app.send_static_file('index.html')

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

if __name__ == '__main__':
    try:
        with app.app_context():
            init_db()
            create_admin_user()
            create_default_validation_rules()
        port = int(os.environ.get('PORT', 5000))
        app.run(debug=True, host='0.0.0.0', port=port)
    except Exception as e:
        logging.error(f"Failed to start application: {e}")
        raise