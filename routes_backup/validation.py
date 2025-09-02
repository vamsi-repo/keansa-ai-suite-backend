from flask import Blueprint, request, jsonify, session, current_app
import os
import json
import pandas as pd
from io import StringIO
import logging
from models.validation import ValidationRule, DataValidator
from services.file_handler import FileHandler
from config.database import get_db_connection

validation_bp = Blueprint('validation', __name__)

@validation_bp.route('/rule-configurations', methods=['GET'])
def get_rule_configurations():
    """Get rule configurations - from original app.py"""
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning(f"Unauthorized access to /rule-configurations: session={dict(session)}")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        user_id = session['user_id']
        logging.debug(f"Fetching rule configurations for user_id: {user_id}")

        # Main query to fetch templates with configured rules
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
        """, (user_id,))
        templates = cursor.fetchall()
        logging.debug(f"Fetched rule-configured templates: {templates}")

        # Additional debug: Log all templates for the user
        cursor.execute("""
            SELECT 
                t.template_id, 
                t.template_name, 
                t.created_at, 
                t.user_id, 
                t.status, 
                t.is_corrected, 
                t.headers,
                COUNT(cvr.column_validation_id) as rule_count
            FROM excel_templates t
            LEFT JOIN template_columns tc ON t.template_id = tc.template_id
            LEFT JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            WHERE t.user_id = %s
            GROUP BY t.template_id, t.template_name, t.created_at
        """, (user_id,))
        all_templates = cursor.fetchall()
        logging.debug(f"All templates for user_id {user_id}: {all_templates}")

        # Log details for each template
        for template in all_templates:
            cursor.execute("""
                SELECT 
                    tc.column_name, 
                    tc.is_selected, 
                    tc.is_validation_enabled
                FROM template_columns tc
                WHERE tc.template_id = %s
            """, (template['template_id'],))
            columns = cursor.fetchall()
            logging.debug(f"Columns for template_id {template['template_id']}: {columns}")

            cursor.execute("""
                SELECT 
                    cvr.column_validation_id, 
                    vrt.rule_name
                FROM column_validation_rules cvr
                JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
                WHERE cvr.column_id IN (
                    SELECT column_id FROM template_columns WHERE template_id = %s
                )
            """, (template['template_id'],))
            rules = cursor.fetchall()
            logging.debug(f"Rules for template_id {template['template_id']}: {rules}")

        cursor.close()
        conn.close()

        if not templates:
            logging.info(f"No templates with rules found for user_id: {user_id}")
        else:
            logging.info(f"Found {len(templates)} templates with rules for user_id: {user_id}")

        return jsonify({'success': True, 'templates': templates})
    except Exception as e:
        logging.error(f"Database error fetching rule configurations: {str(e)}")
        return jsonify({'success': False, 'message': f"Database error: {str(e)}"}), 500

@validation_bp.route('/history', methods=['GET'])
def get_validation_history():
    """Get validation history - from original app.py"""
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /validation-history: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT 
                vh.history_id, 
                vh.template_id, 
                vh.template_name, 
                vh.error_count, 
                vh.corrected_at, 
                vh.corrected_file_path,
                et.created_at AS original_uploaded_at
            FROM validation_history vh
            JOIN excel_templates et ON vh.template_id = et.template_id
            WHERE vh.user_id = %s
            ORDER BY et.created_at DESC, vh.corrected_at DESC
        """, (session['user_id'],))
        history_entries = cursor.fetchall()

        grouped_history = {}
        for entry in history_entries:
            template_name = entry['template_name']
            if not template_name.endswith('_corrected.xlsx') and not template_name.endswith('_corrected.csv'):
                continue
            base_template_name = template_name.replace('_corrected.xlsx', '').replace('_corrected.csv', '')
            if base_template_name not in grouped_history:
                cursor.execute("""
                    SELECT created_at
                    FROM excel_templates
                    WHERE template_name = %s AND user_id = %s
                    ORDER BY created_at ASC
                    LIMIT 1
                """, (base_template_name, session['user_id']))
                original_entry = cursor.fetchone()
                original_uploaded_at = original_entry['created_at'] if original_entry else entry['original_uploaded_at']
                grouped_history[base_template_name] = {
                    'original_uploaded_at': original_uploaded_at.isoformat(),
                    'data_loads': []
                }
            grouped_history[base_template_name]['data_loads'].append({
                'history_id': entry['history_id'],
                'template_id': entry['template_id'],
                'template_name': entry['template_name'],
                'error_count': entry['error_count'],
                'corrected_at': entry['corrected_at'].isoformat(),
                'corrected_file_path': entry['corrected_file_path']
            })

        cursor.close()
        logging.info(f"Fetched validation history for user {session['user_id']}: {len(grouped_history)} templates")
        logging.debug(f"Validation history response: {json.dumps(grouped_history, default=str)}")
        return jsonify({'success': True, 'history': grouped_history})
    except Exception as e:
        logging.error(f'Error fetching validation history: {str(e)}')
        return jsonify({'success': False, 'message': f'Error fetching validation history: {str(e)}'}), 500

@validation_bp.route('/corrections/<int:history_id>', methods=['GET'])
def get_validation_corrections(history_id):
    """Get validation corrections - from original app.py"""
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /validation-corrections: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT vh.template_id, vh.template_name, vh.corrected_file_path, et.headers
            FROM validation_history vh
            JOIN excel_templates et ON vh.template_id = et.template_id
            WHERE vh.history_id = %s AND vh.user_id = %s
        """, (history_id, session['user_id']))
        history_entry = cursor.fetchone()
        if not history_entry:
            cursor.close()
            return jsonify({'error': 'Validation history not found'}), 404

        headers = json.loads(history_entry['headers']) if history_entry['headers'] else []

        cursor.execute("""
            SELECT row_index, column_name, original_value, corrected_value, rule_failed
            FROM validation_corrections
            WHERE history_id = %s
        """, (history_id,))
        corrections = cursor.fetchall()

        file_path = history_entry['corrected_file_path']
        if not os.path.exists(file_path):
            cursor.close()
            return jsonify({'error': 'Corrected file not found'}), 404

        sheets = FileHandler.read_file(file_path)
        sheet_name = list(sheets.keys())[0]
        df = sheets[sheet_name]
        header_row = FileHandler.find_header_row(df)
        if header_row == -1:
            cursor.close()
            return jsonify({'error': 'Could not detect header row'}), 400
        df.columns = headers
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        correction_details = []
        for correction in corrections:
            row_index = correction['row_index'] - 1
            if row_index < 0 or row_index >= len(df):
                continue
            row_data = df.iloc[row_index].to_dict()
            correction_details.append({
                'row_index': correction['row_index'],
                'column_name': correction['column_name'],
                'original_value': correction['original_value'],
                'corrected_value': correction['corrected_value'],
                'row_data': row_data,
                'rule_failed': correction['rule_failed']
            })

        cursor.close()
        return jsonify({
            'success': True,
            'headers': headers,
            'corrections': correction_details
        })
    except Exception as e:
        logging.error(f'Error fetching validation corrections: {str(e)}')
        return jsonify({'error': f'Error fetching validation corrections: {str(e)}'}), 500

@validation_bp.route('/delete-validation/<int:history_id>', methods=['DELETE'])
def delete_validation(history_id):
    """Delete validation history - from original app.py"""
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /delete-validation: session missing")
        return jsonify({'error': 'Not logged in'}), 401
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT corrected_file_path
            FROM validation_history
            WHERE history_id = %s AND user_id = %s
        """, (history_id, session['user_id']))
        history_entry = cursor.fetchone()
        if not history_entry:
            cursor.close()
            return jsonify({'error': 'Validation history not found'}), 404

        file_path = history_entry[0]
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Deleted file: {file_path}")

        cursor.execute("""
            DELETE FROM validation_history
            WHERE history_id = %s AND user_id = %s
        """, (history_id, session['user_id']))

        conn.commit()
        cursor.close()
        return jsonify({'success': True, 'message': 'Validation history deleted successfully'})
    except Exception as e:
        logging.error(f'Error deleting validation history: {str(e)}')
        return jsonify({'error': f'Error deleting validation history: {str(e)}'}), 500

@validation_bp.route('/validate-existing/<int:template_id>', methods=['GET'])
def validate_existing_template(template_id):
    """Validate existing template - from original app.py"""
    if 'loggedin' not in session or 'user_id' not in session:
        logging.warning("Unauthorized access to /validate-existing: session missing")
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    try:
        df_json = session.get('df')
        if not df_json:
            logging.error("No data available in session")
            return jsonify({'success': False, 'message': 'No data available'}), 400
        df = pd.read_json(StringIO(df_json))
        headers = session['headers']
        df.columns = headers
        df = df.iloc[session['header_row'] + 1:].reset_index(drop=True)

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT tc.column_name, vrt.rule_name, vrt.source_format
            FROM template_columns tc
            JOIN column_validation_rules cvr ON tc.column_id = cvr.column_id
            JOIN validation_rule_types vrt ON cvr.rule_type_id = vrt.rule_type_id
            WHERE tc.template_id = %s AND tc.is_selected = TRUE AND vrt.rule_name NOT LIKE 'Transform-Date(%'
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()

        error_cell_locations = {}
        accepted_date_formats = ['%d-%m-%Y', '%m-%d-%Y', '%m/%d/%Y', '%d/%m/%Y', '%m-%Y', '%m-%y', '%m/%Y', '%m/%y']
        for rule in rules:
            column_name = rule['column_name']
            rule_name = rule['rule_name']
            if rule_name.startswith('Date(') and rule['source_format']:
                format_map = {
                    'MM-DD-YYYY': '%m-%d-%Y', 'DD-MM-YYYY': '%d-%m-%Y', 'MM/DD/YYYY': '%m/%d/%Y', 'DD/MM/YYYY': '%d/%m/%Y',
                    'MM-YYYY': '%m-%Y', 'MM-YY': '%m-%y', 'MM/YYYY': '%m/%Y', 'MM/YY': '%m/%y'
                }
                accepted_date_formats = [format_map.get(rule['source_format'], '%d-%m-%Y')]
            error_count, locations = DataValidator.check_special_characters_in_column(
                df, column_name, rule_name, accepted_date_formats, check_null_cells=True
            )
            if error_count > 0:
                error_cell_locations[column_name] = [
                    {'row': loc[0], 'value': loc[1], 'rule_failed': loc[2], 'reason': loc[3]}
                    for loc in locations
                ]

        data_rows = df.to_dict('records')
        for row in data_rows:
            for key, value in row.items():
                if pd.isna(value) or value == '':
                    row[key] = 'NULL'

        logging.info(f"Validation completed for template {template_id}: {len(error_cell_locations)} columns with errors")
        return jsonify({
            'success': True,
            'error_cell_locations': error_cell_locations,
            'data_rows': data_rows
        })
    except Exception as e:
        logging.error(f"Error validating template {template_id}: {str(e)}")
        return jsonify({'success': False, 'message': str(e)}), 500

@validation_bp.route('/validate-existing/<int:template_id>', methods=['POST'])
def save_existing_template_corrections(template_id):
    """Save corrections for existing template - from original app.py"""
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    try:
        data = request.get_json()
        corrections = data.get('corrections', {})
        phase = data.get('phase', 'generic')
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT template_name, sheet_name, headers 
            FROM excel_templates 
            WHERE template_id = %s AND user_id = %s
        """, (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404
        
        df_json = session.get('df')
        if not df_json:
            cursor.close()
            return jsonify({'success': False, 'message': 'No data available in session'}), 400
        
        df = pd.read_json(StringIO(df_json))
        headers = json.loads(template['headers'])
        df.columns = headers
        df = df.iloc[session.get('header_row', 0) + 1:].reset_index(drop=True)
        
        # Apply corrections
        correction_count = 0
        for column, row_corrections in corrections.items():
            if column not in headers:
                continue
            for row_str, value in row_corrections.items():
                try:
                    row_index = int(row_str)
                    if 0 <= row_index < len(df):
                        original_value = df.at[row_index, column]
                        df.at[row_index, column] = value
                        correction_count += 1
                        logging.info(f"Applied correction: Row {row_index+1}, Column {column}, {original_value} → {value}")
                except (ValueError, IndexError) as e:
                    logging.warning(f"Invalid correction: {row_str}, {column}, {value} - {str(e)}")
                    continue
        
        # Save corrected file
        base_name, ext = os.path.splitext(template['template_name'])
        corrected_filename = f"{base_name}_corrected{ext}"
        corrected_file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], corrected_filename)
        
        try:
            corrected_file_path = FileHandler.save_corrected_file(df, template['template_name'], current_app.config['UPLOAD_FOLDER'], template['sheet_name'])
        except Exception as save_error:
            cursor.close()
            logging.error(f"Failed to save corrected file: {str(save_error)}")
            return jsonify({'success': False, 'message': f'Failed to save corrected file: {str(save_error)}'}), 500
        
        # Save to validation history
        cursor.execute("""
            INSERT INTO validation_history (template_id, template_name, error_count, corrected_file_path, user_id)
            VALUES (%s, %s, %s, %s, %s)
        """, (template_id, corrected_filename, correction_count, corrected_file_path, session['user_id']))
        history_id = cursor.lastrowid
        
        # Save individual corrections for tracking
        correction_records = []
        for column, row_corrections in corrections.items():
            if column not in headers:
                continue
            for row_str, corrected_value in row_corrections.items():
                try:
                    row_index = int(row_str)
                    if 0 <= row_index < len(df):
                        original_df = pd.read_json(StringIO(session['df']))
                        original_df.columns = headers
                        original_df = original_df.iloc[session.get('header_row', 0) + 1:].reset_index(drop=True)
                        
                        original_value = str(original_df.at[row_index, column]) if row_index < len(original_df) else 'NULL'
                        
                        correction_records.append((
                            history_id, 
                            row_index + 1, 
                            column, 
                            original_value, 
                            corrected_value, 
                            f'{phase}_rule'
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
        
        # Update session with corrected data for future steps
        session['corrected_df'] = df.to_json()
        session['corrected_file_path'] = corrected_file_path
        
        logging.info(f"Successfully saved {correction_count} corrections for template {template_id}")
        
        return jsonify({
            'success': True, 
            'corrected_file_path': corrected_file_path, 
            'history_id': history_id,
            'correction_count': correction_count,
            'message': f'{correction_count} corrections applied successfully'
        })
        
    except Exception as e:
        logging.error(f"Error saving corrections: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.rollback()
        return jsonify({'success': False, 'message': f'Failed to save corrections: {str(e)}'}), 500

@validation_bp.route('/validate-row/<int:template_id>', methods=['POST'])
def validate_row(template_id):
    """Validate single row - from original app.py"""
    if 'loggedin' not in session or 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
   
    try:
        data = request.get_json()
        row_index = data['row_index']
        updated_row = data['updated_row']
        use_corrected = data.get('use_corrected', True)
       
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
       
        # Get template
        cursor.execute("SELECT * FROM excel_templates WHERE template_id = %s AND user_id = %s",
                      (template_id, session['user_id']))
        template = cursor.fetchone()
        if not template:
            cursor.close()
            return jsonify({'success': False, 'message': 'Template not found'}), 404
       
        # Get headers and create single-row dataframe
        headers = json.loads(template['headers'])
        single_row_df = pd.DataFrame([updated_row], columns=headers)
       
        # Fetch only active custom rules for this template
        cursor.execute("""
            SELECT vrt.rule_name, vrt.parameters, vrt.column_name
            FROM validation_rule_types vrt
            WHERE vrt.template_id = %s AND vrt.is_custom = TRUE AND vrt.is_active = TRUE
        """, (template_id,))
        rules = cursor.fetchall()
        cursor.close()
 
        # Validate the single row against active custom rules only
        errors = []
       
        for rule in rules:
            try:
                column_name = rule['column_name']
                formula = rule['parameters']
                rule_name = rule['rule_name']
               
                # Use the updated evaluate_column_rule function
                data_type = 'Float'  # Default for custom rules
                is_valid, error_locations = DataValidator.evaluate_column_rule(single_row_df, column_name, formula, headers, data_type)
               
                for error in error_locations:
                    if len(error) > 3:  # Ensure we have all error details
                        errors.append({
                            'column': column_name,
                            'rule_failed': error[2],
                            'reason': error[3],
                            'value': error[1]
                        })
            except Exception as e:
                logging.error(f"Error validating rule {rule['rule_name']} for row: {str(e)}")
                errors.append({
                    'column': rule['column_name'],
                    'rule_failed': rule['rule_name'],
                    'reason': f'Validation error: {str(e)}',
                    'value': updated_row.get(rule['column_name'], 'NULL')
                })
 
        valid = len(errors) == 0
        updated_data_row = single_row_df.iloc[0].to_dict()
       
        return jsonify({
            'success': True,
            'valid': valid,
            'errors': errors,
            'updated_data_row': updated_data_row,
            'validation_details': {
                'rules_checked': len(rules),
                'errors_found': len(errors),
                'row_index': row_index
            }
        })
       
    except Exception as e:
        logging.error(f"Error validating row: {str(e)}")
        return jsonify({'success': False, 'message': f'Validation error: {str(e)}'}), 500
