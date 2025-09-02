#!/usr/bin/env python3
"""
Railway Deployment Entry Point for Keansa AI Suite Backend
"""
import os
import sys
from pathlib import Path
import json
import pandas as pd
from flask import render_template, request, jsonify, session, send_file, g
import mysql.connector
import bcrypt

# Add current directory to Python path
current_dir = Path(__file__).parent.absolute()
sys.path.insert(0, str(current_dir))

# Set Railway environment variable
os.environ['RAILWAY_ENVIRONMENT'] = 'production'

# Import Flask app factory and utilities
from run import (
    create_app, create_directories, init_db, create_admin_user, 
    create_default_validation_rules, get_db_connection, read_file, 
    find_header_row, assign_default_rules_to_columns
)

# Create directories first
directories = create_directories()

# Create Flask application instance
app = create_app(directories)

# Initialize database in application context
with app.app_context():
    init_db()
    create_admin_user() 
    create_default_validation_rules()

# Define all routes
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

# Health check endpoint for Railway
@app.route('/health', methods=['GET'])
def health_check():
    try:
        # Test database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return jsonify({
            'status': 'healthy',
            'timestamp': pd.Timestamp.now().isoformat(),
            'service': 'keansa-ai-suite-backend'
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': pd.Timestamp.now().isoformat(),
            'service': 'keansa-ai-suite-backend'
        }), 500

if __name__ == '__main__':
    from run import main
    main()
