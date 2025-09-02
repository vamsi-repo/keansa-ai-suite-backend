from flask import Blueprint, request, jsonify, session, current_app
import os
import json
import pandas as pd
from io import StringIO
import logging
from models.validation import ValidationRule, DataValidator
from services.file_handler import FileHandler
from config.database import get_db_connection

step_bp = Blueprint('steps', __name__)

@step_bp.route('/<int:step>', methods=['GET', 'POST'])
def handle_step(step):
    """Handle different validation steps - from original app.py"""
    if 'loggedin' not in session:
        return jsonify({'error': 'Not logged in'}), 401
    if 'df' not in session or session['df'] is None:
        logging.error("Session data missing: 'df' not found or is None")
        return jsonify({'error': 'Please upload a file first'}), 400
    
    session['current_step'] = step
    try:
        df = pd.read_json(StringIO(session['df']))
    except Exception as e:
        logging.error(f"Error reading session['df']: {str(e)}")
        return jsonify({'error': 'Invalid session data: Unable to load DataFrame'}), 500
    
    headers = session['headers']
    
    if step == 1:
        if request.method == 'POST':
            selected_headers = request.form.getlist('headers')
            new_header_row = request.form.get('new_header_row')
            if new_header_row:
                try:
                    header_row = int(new_header_row)
                    headers = df.iloc[header_row].tolist()
                    session['header_row'] = header_row
                    session['headers'] = headers
                    return jsonify({'headers': headers})
                except ValueError:
                    return jsonify({'error': 'Invalid header row number'}), 400
            if not selected_headers:
                return jsonify({'error': 'Please select at least one column'}), 400
            session['selected_headers'] = selected_headers
            session['current_step'] = 2

            # Mark selected headers in the database
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE template_columns SET is_selected = FALSE WHERE template_id = %s", (session['template_id'],))
            for header in selected_headers:
                cursor.execute("""
                    UPDATE template_columns
                    SET is_selected = TRUE
                    WHERE template_id = %s AND column_name = %s
                """, (session['template_id'], header))
            conn.commit()
            cursor.close()

            return jsonify({'success': True})
        return jsonify({'headers': headers})
    
    elif step == 2:
        if 'selected_headers' not in session:
            session['current_step'] = 1
            return jsonify({'error': 'Select headers first'}), 400
        selected_headers = session['selected_headers']
        if request.method == 'POST':
            try:
                logging.debug(f"Received form data: {dict(request.form)}")
                validations = {header: request.form.getlist(f'validations_{header}') 
                              for header in selected_headers}
                logging.debug(f"Constructed validations: {validations}")
                session['validations'] = validations
                df.columns = session['headers']
                logging.debug(f"DataFrame after setting headers: {df.to_dict()}")
                df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)
                logging.debug(f"DataFrame after removing header row: {df.to_dict()}")

                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT column_id, column_name FROM template_columns WHERE template_id = %s", (session['template_id'],))
                column_map = {row['column_name']: row['column_id'] for row in cursor.fetchall()}
                cursor.execute("SELECT rule_type_id, rule_name FROM validation_rule_types WHERE is_active = TRUE")
                rule_map = {row['rule_name']: row['rule_type_id'] for row in cursor.fetchall()}
                cursor.execute("DELETE FROM column_validation_rules WHERE column_id IN (SELECT column_id FROM template_columns WHERE template_id = %s)", (session['template_id'],))

                validation_data = []
                for header, rule_names in validations.items():
                    column_id = column_map.get(header)
                    if not column_id:
                        continue
                    for rule_name in rule_names:
                        rule_type_id = rule_map.get(rule_name)
                        if rule_type_id:
                            validation_data.append((column_id, rule_type_id, json.dumps({})))
                        else:
                            logging.warning(f"No rule_type_id found for validation {rule_name}")
                if validation_data:
                    cursor.executemany("""
                        INSERT INTO column_validation_rules (column_id, rule_type_id, rule_config)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE rule_config = VALUES(rule_config)
                    """, validation_data)
                    logging.debug(f"Inserted validation rules: {validation_data}")
                conn.commit()
                cursor.close()
                session['current_step'] = 3
                return jsonify({'success': True})
            except Exception as e:
                logging.error(f"Error in step 2: {str(e)}")
                return jsonify({'error': str(e)}), 500
        return jsonify({'headers': selected_headers, 'validations': session.get('validations', {})})
    
    elif step == 3:
        if 'validations' not in session:
            session['current_step'] = 2
            return jsonify({'error': 'Configure validations first'}), 400
        
        # Perform validation
        validations = session['validations']
        df.columns = session['headers']
        df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)
        
        error_cell_locations = {}
        accepted_date_formats = ['%d-%m-%Y', '%m-%d-%Y', '%m/%d/%Y', '%d/%m/%Y', '%m-%Y', '%m-%y', '%m/%Y', '%m/%y']
        
        for header, rules in validations.items():
            for rule in rules:
                if rule.startswith('Date(') and ')' in rule:
                    format_part = rule[5:-1]
                    format_map = {
                        'MM-DD-YYYY': '%m-%d-%Y', 'DD-MM-YYYY': '%d-%m-%Y', 'MM/DD/YYYY': '%m/%d/%Y', 'DD/MM/YYYY': '%d/%m/%Y',
                        'MM-YYYY': '%m-%Y', 'MM-YY': '%m-%y', 'MM/YYYY': '%m/%Y', 'MM/YY': '%m/%y'
                    }
                    accepted_date_formats = [format_map.get(format_part, '%d-%m-%Y')]
                
                error_count, locations = DataValidator.check_special_characters_in_column(
                    df, header, rule, accepted_date_formats, check_null_cells=True
                )
                if error_count > 0:
                    if header not in error_cell_locations:
                        error_cell_locations[header] = []
                    error_cell_locations[header].extend([
                        {'row': loc[0], 'value': loc[1], 'rule_failed': loc[2], 'reason': loc[3]}
                        for loc in locations
                    ])

        data_rows = df.to_dict('records')
        for row in data_rows:
            for key, value in row.items():
                if pd.isna(value) or value == '':
                    row[key] = 'NULL'

        session['error_cell_locations'] = error_cell_locations
        session['data_rows'] = data_rows

        return jsonify({
            'success': True,
            'error_cell_locations': error_cell_locations,
            'data_rows': data_rows
        })
    
    return jsonify({'error': 'Invalid step'}), 400

@step_bp.route('/<int:step>/save-corrections', methods=['POST'])
def save_corrections(step):
    """Save corrections for a specific step"""
    if 'loggedin' not in session:
        return jsonify({'error': 'Not logged in'}), 401
    
    try:
        data = request.get_json()
        corrections = data.get('corrections', {})
        
        if step == 3:
            # Process step 3 corrections
            df_json = session.get('df')
            if not df_json:
                return jsonify({'error': 'No data available in session'}), 400
            
            df = pd.read_json(StringIO(df_json))
            headers = session['headers']
            df.columns = headers
            df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)
            
            # Apply corrections
            correction_count = 0
            for column, row_corrections in corrections.items():
                if column not in headers:
                    continue
                for row_str, value in row_corrections.items():
                    try:
                        row_index = int(row_str)
                        if 0 <= row_index < len(df):
                            df.at[row_index, column] = value
                            correction_count += 1
                    except (ValueError, IndexError):
                        continue
            
            # Save corrected file
            template_name = session.get('template_name', 'corrected_file')
            corrected_file_path = FileHandler.save_corrected_file(
                df, template_name, current_app.config['UPLOAD_FOLDER'], session.get('sheet_name')
            )
            
            # Save to validation history
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO validation_history (template_id, template_name, error_count, corrected_file_path, user_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (session['template_id'], os.path.basename(corrected_file_path), correction_count, corrected_file_path, session['user_id']))
            history_id = cursor.lastrowid
            
            # Save individual corrections
            correction_records = []
            for column, row_corrections in corrections.items():
                for row_str, corrected_value in row_corrections.items():
                    try:
                        row_index = int(row_str)
                        if 0 <= row_index < len(df):
                            correction_records.append((
                                history_id, row_index + 1, column, 'original_value', corrected_value, 'validation_rule'
                            ))
                    except (ValueError, IndexError):
                        continue
            
            if correction_records:
                cursor.executemany("""
                    INSERT INTO validation_corrections 
                    (history_id, row_index, column_name, original_value, corrected_value, rule_failed)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, correction_records)
            
            conn.commit()
            cursor.close()
            
            session['corrected_file_path'] = corrected_file_path
            
            return jsonify({
                'success': True,
                'corrected_file_path': corrected_file_path,
                'history_id': history_id,
                'correction_count': correction_count
            })
        else:
            return jsonify({'error': f'Corrections not supported for step {step}'}), 400
            
    except Exception as e:
        logging.error(f"Error saving corrections for step {step}: {str(e)}")
        return jsonify({'error': f'Failed to save corrections: {str(e)}'}), 500

@step_bp.route('/custom-rule', methods=['POST'])
def create_custom_rule():
    """Create custom validation rule"""
    if 'loggedin' not in session:
        return jsonify({'error': 'Not logged in'}), 401
    
    try:
        data = request.get_json()
        rule_name = data.get('rule_name')
        formula = data.get('formula')
        column_name = data.get('column_name')
        template_id = session.get('template_id')
        
        if not all([rule_name, formula, column_name, template_id]):
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Validate formula syntax
        from utils.validators import InputValidator
        is_valid, message = InputValidator.validate_formula_syntax(formula, column_name, session.get('headers', []))
        if not is_valid:
            return jsonify({'error': f'Invalid formula: {message}'}), 400
        
        # Create custom rule
        rule_type_id = ValidationRule.create_custom_rule(rule_name, formula, column_name, template_id)
        
        return jsonify({
            'success': True,
            'message': 'Custom rule created successfully',
            'rule_type_id': rule_type_id
        })
        
    except Exception as e:
        logging.error(f"Error creating custom rule: {str(e)}")
        return jsonify({'error': f'Failed to create custom rule: {str(e)}'}), 500

@step_bp.route('/validate-formula', methods=['POST'])
def validate_formula():
    """Validate formula syntax"""
    try:
        data = request.get_json()
        formula = data.get('formula')
        column_name = data.get('column_name')
        
        if not formula or not column_name:
            return jsonify({'error': 'Missing formula or column name'}), 400
        
        from utils.validators import InputValidator
        is_valid, message = InputValidator.validate_formula_syntax(formula, column_name, session.get('headers', []))
        
        return jsonify({
            'valid': is_valid,
            'message': message
        })
        
    except Exception as e:
        logging.error(f"Error validating formula: {str(e)}")
        return jsonify({'error': f'Formula validation failed: {str(e)}'}), 500
