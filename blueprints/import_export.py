import os
import shutil
import pandas as pd
import io
import re
from datetime import datetime, date
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, Response, make_response, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import func, and_
from models import db, Material, Entry, Client, PendingBill, Booking, BookingItem, Payment, DirectSale, DirectSaleItem, GRN, GRNItem, Delivery, DeliveryItem, Invoice, Settings, BillCounter

# Module configuration
MODULE_CONFIG = {
    'name': 'Import/Export Module',
    'description': 'Data import and export functionality',
    'url_prefix': '/import_export',
    'enabled': True
}

import_export_bp = Blueprint('import_export', __name__)

# --- Constants & Schemas ---
CLIENT_SCHEMA = ['code', 'name', 'phone', 'address', 'category', 'status']
DISPATCH_SCHEMA = ['CLIENT_CODE', 'CLIENT_NAME', 'CLIENT_CATEGORY', 'TRANSACTION_CATEGORY', 'BILL_NO', 'BILL_DATE', 'CEMENT_BRAND', 'QTY', 'NIMBUS', 'NOTES', 'SOURCE', 'MATCH_STATUS']
PENDING_BILL_SCHEMA = ['client_code', 'bill_no', 'name', 'amount', 'reason', 'nimbus']

# --- Helper Functions ---

def generate_client_code():
    """Generate next client code in format tmpc-000001"""
    last_client = Client.query.filter(Client.code.like('tmpc-%')).order_by(
        Client.code.desc()).first()
    if last_client and last_client.code:
        try:
            num = int(last_client.code.split('-')[1]) + 1
        except (ValueError, IndexError):
            num = Client.query.count() + 1
    else:
        num = 1
    return f"tmpc-{num:06d}"

def backup_database():
    """Creates a timestamped backup of the database before import."""
    try:
        db_path = current_app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
        if os.path.exists(db_path):
            backup_dir = os.path.join(os.path.dirname(db_path), 'backups')
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(backup_dir, f"ahmed_cement_backup_{timestamp}.db")
            shutil.copy2(db_path, backup_path)
            return True, f"Backup created: {os.path.basename(backup_path)}"
    except Exception as e:
        return False, str(e)
    return False, "Database file not found"

def validate_client_row(row):
    errors = []
    # Code pattern FBM-XXXXX or similar
    if not re.match(r'^FBM-\d+$', str(row.get('code', '')), re.IGNORECASE) and not str(row.get('code', '')).startswith('tmpc-'):
        # Relaxed validation to allow existing tmpc codes, but warn
        pass 
    
    if str(row.get('status', '')).upper() not in ['ACTIVE', 'INACTIVE', 'TRUE', 'FALSE', '1', '0', '']:
        errors.append("Invalid Status")
        
    return errors

def validate_dispatch_row(row):
    errors = []
    try:
        float(row.get('qty', 0))
    except:
        errors.append("Qty must be numeric")
        
    bill = str(row.get('bill_no', '')).upper()
    if bill != 'NOT BILLED' and bill != '' and not bill.replace('-','').isalnum():
         pass # Allow alphanumeric bills
         
    return errors

def validate_pending_bill_row(row):
    errors = []
    if not row.get('client_code'):
        errors.append("Missing Client Code")
    return errors

# --- Routes & Views ---

@import_export_bp.route('/')
@login_required
def import_export_page():
    return render_template('import_export_new.html')

@import_export_bp.route('/template/<dataset>')
@login_required
def get_template(dataset):
    """Generate empty templates for manual entry."""
    if dataset == 'clients':
        df = pd.DataFrame(columns=CLIENT_SCHEMA)
    elif dataset == 'dispatch':
        df = pd.DataFrame(columns=DISPATCH_SCHEMA)
    elif dataset == 'pending_bills':
        df = pd.DataFrame(columns=PENDING_BILL_SCHEMA)
    else:
        return "Invalid dataset", 400
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    
    return send_file(output, as_attachment=True, download_name=f"template_{dataset}.xlsx")

@import_export_bp.route('/preview', methods=['POST'])
@login_required
def preview_import():
    """Analyze file and return preview data."""
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400
    
    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
            
        # Normalize columns
        df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
        
        # Detect Dataset Type
        dataset_type = 'unknown'
        if 'qty' in df.columns and 'item' in df.columns:
            dataset_type = 'dispatch'
        elif 'amount' in df.columns and 'reason' in df.columns:
            dataset_type = 'pending_bills'
        elif 'phone' in df.columns and 'address' in df.columns:
            dataset_type = 'clients'
            
        preview_data = df.head(10).fillna('').to_dict(orient='records')
        
        return jsonify({
            'success': True,
            'dataset_type': dataset_type,
            'columns': list(df.columns),
            'row_count': len(df),
            'preview': preview_data
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@import_export_bp.route('/execute_import', methods=['POST'])
@login_required
def execute_import():
    """Process the import with selected options."""
    file = request.files.get('file')
    dataset_type = request.form.get('dataset_type')
    conflict_strategy = request.form.get('conflict_strategy', 'skip') # skip, update
    missing_client_strategy = request.form.get('missing_client_strategy', 'skip') # create, skip, stop
    
    # 1. Safety Backup
    success, msg = backup_database()
    if not success:
        return jsonify({'error': f"Backup failed: {msg}"}), 500
        
    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
        
        df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
        df = df.fillna('')
        
        report = {'imported': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'error_details': []}
        
        # 2. Process based on type
        if dataset_type == 'clients':
            _process_clients(df, conflict_strategy, report)
        elif dataset_type == 'pending_bills':
            _process_pending_bills(df, conflict_strategy, missing_client_strategy, report)
        elif dataset_type == 'dispatch':
            # Add renaming for user's format
            df.rename(columns={
                'cement_brand': 'item',
                'client_name': 'customer',
                'bill_date': 'date',
                'nimbus': 'nimbus_no'
            }, inplace=True)
            _process_dispatch(df, conflict_strategy, missing_client_strategy, report)
        else:
            return jsonify({'error': 'Unknown dataset type'}), 400
            
        db.session.commit()
        return jsonify({'success': True, 'report': report})
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

def _process_clients(df, strategy, report):
    for _, row in df.iterrows():
        code = str(row.get('code', '')).strip()
        if not code:
            report['errors'] += 1
            continue
            
        existing = Client.query.filter_by(code=code).first()
        
        if existing:
            if strategy == 'update':
                existing.name = row.get('name', existing.name)
                existing.phone = str(row.get('phone', existing.phone))
                existing.address = str(row.get('address', existing.address))
                existing.category = row.get('category', existing.category)
                status_val = str(row.get('status', '')).upper()
                if status_val:
                    existing.is_active = (status_val == 'ACTIVE' or status_val == 'TRUE')
                report['updated'] += 1
            else:
                report['skipped'] += 1
        else:
            new_client = Client(
                code=code,
                name=row.get('name', 'Unknown'),
                phone=str(row.get('phone', '')),
                address=str(row.get('address', '')),
                category=row.get('category', 'General'),
                is_active=True
            )
            db.session.add(new_client)
            report['imported'] += 1

def _process_pending_bills(df, strategy, missing_client_strategy, report):
    for _, row in df.iterrows():
        bill_no = str(row.get('bill_no', '')).strip()
        client_code = str(row.get('client_code', '')).strip()
        
        if not bill_no or not client_code:
            report['errors'] += 1
            continue
            
        # Check Client Dependency
        client = Client.query.filter_by(code=client_code).first()
        if not client:
            if missing_client_strategy == 'stop':
                raise Exception(f"Missing client {client_code} for bill {bill_no}")
            elif missing_client_strategy == 'skip':
                report['skipped'] += 1
                continue
            elif missing_client_strategy == 'create':
                client = Client(code=client_code, name=row.get('name', 'Imported Client'), is_active=True)
                db.session.add(client)
                db.session.flush() # Get ID
        
        existing = PendingBill.query.filter_by(bill_no=bill_no).first()
        
        if existing:
            if strategy == 'update':
                existing.amount = float(row.get('amount', 0))
                existing.reason = row.get('reason', existing.reason)
                existing.nimbus_no = row.get('nimbus', existing.nimbus_no)
                report['updated'] += 1
            else:
                report['skipped'] += 1
        else:
            new_bill = PendingBill(
                client_code=client_code,
                client_name=client.name,
                bill_no=bill_no,
                amount=float(row.get('amount', 0)),
                reason=row.get('reason', 'Imported'),
                nimbus_no=row.get('nimbus', ''),
                created_at=datetime.now().strftime('%Y-%m-%d %H:%M'),
                created_by=current_user.username
            )
            db.session.add(new_bill)
            report['imported'] += 1

def _process_dispatch(df, strategy, missing_client_strategy, report):
    for _, row in df.iterrows():
        # Helper to get stripped string value, returns empty string for NaN/None
        def get_val(key):
            val = row.get(key)
            return str(val).strip() if pd.notna(val) and val is not None else ''

        item = get_val('item')
        qty_str = get_val('qty')

        # Skip row if both material and qty are missing or qty is 0.
        if not item and (not qty_str or float(qty_str or 0) == 0):
            continue

        try:
            qty = float(qty_str) if qty_str else 0.0
        except ValueError:
            report['errors'] += 1
            report['error_details'].append(f"Invalid Qty '{qty_str}' for item '{item}'")
            continue

        client_code = get_val('client_code')
        client_name = get_val('customer')
        client_category = get_val('client_category')
        bill_no = get_val('bill_no')
        entry_date_str = get_val('date')
        nimbus_no = get_val('nimbus_no')

        # Handle date format
        entry_date = date.today().strftime('%Y-%m-%d')
        if entry_date_str:
            try:
                entry_date = pd.to_datetime(entry_date_str).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass # Keep today's date as fallback

        # Check Client Dependency
        client = Client.query.filter_by(code=client_code).first()
        
        if not client:
            if client_name:
                client = Client.query.filter_by(name=client_name).first()

            if not client and (client_code or client_name):
                if missing_client_strategy == 'create':
                    new_code = client_code if client_code else generate_client_code()
                    if not Client.query.filter_by(code=new_code).first():
                        client = Client(code=new_code, name=client_name or 'Imported Client', category=client_category or 'General', is_active=True)
                        db.session.add(client)
                        db.session.flush()
                elif missing_client_strategy == 'stop':
                    raise Exception(f"Missing client '{client_code or client_name}' for dispatch")
                else: # 'skip' is the default
                    report['skipped'] += 1
                    report['error_details'].append(f"Skipped: Client '{client_code or client_name}' not found.")
                    continue
        
        final_client_code = client.code if client else client_code
        final_client_name = client.name if client else client_name

        # Ensure Material Exists
        mat = None
        if item:
            mat = Material.query.filter(func.lower(Material.name) == item.lower()).first()
            if not mat:
                mat = Material(name=item, code=f"MAT-{datetime.now().strftime('%f')}")
                db.session.add(mat)
                db.session.flush()
        
        # --- Create Entry ---
        entry = Entry(
            date=entry_date,
            time=datetime.now().strftime('%H:%M:%S'),
            type='OUT',
            material=mat.name if mat else None,
            client=final_client_name,
            client_code=final_client_code,
            client_category=client_category,
            qty=qty,
            bill_no=bill_no,
            nimbus_no=nimbus_no,
            created_by=current_user.username
        )
        db.session.add(entry)
        
        if mat and qty > 0:
            mat.total = (mat.total or 0) - qty
        
        # --- Sync Pending Bill ---
        # If data matches with client names and codes and bill no in pending bills it must sync
        if bill_no and str(bill_no).upper() not in ['CASH', 'NOT BILLED', '']:
            pb = PendingBill.query.filter_by(bill_no=bill_no).first()
            if pb:
                # Sync client details if there is a mismatch
                if pb.client_code != final_client_code:
                    pb.client_code = final_client_code
                    pb.client_name = final_client_name
            else:
                # Create new Pending Bill if it doesn't exist
                unit_price = mat.unit_price if mat else 0
                amount = qty * unit_price
                
                new_pb = PendingBill(
                    client_code=final_client_code,
                    client_name=final_client_name,
                    bill_no=bill_no,
                    nimbus_no=nimbus_no,
                    amount=amount,
                    reason=f"Imported Dispatch: {qty} {item}",
                    is_paid=False,
                    created_at=datetime.now().strftime('%Y-%m-%d %H:%M'),
                    created_by=current_user.username,
                    is_manual=True
                )
                db.session.add(new_pb)
        
        report['imported'] += 1

@import_export_bp.route('/export', methods=['GET'])
@login_required
def export_data():
    dataset = request.args.get('dataset')
    fmt = request.args.get('format', 'excel')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    if dataset == 'clients':
        query = Client.query.all()
        data = [{k: getattr(x, k) for k in CLIENT_SCHEMA if hasattr(x, k)} for x in query]
        # Map status
        for d, x in zip(data, query):
            d['status'] = 'ACTIVE' if x.is_active else 'INACTIVE'
            
    elif dataset == 'dispatch':
        query = Entry.query.filter_by(type='OUT')
        if start_date:
            query = query.filter(Entry.date >= start_date)
        if end_date:
            query = query.filter(Entry.date <= end_date)
        
        entries = query.all()
        data = []
        for x in entries:
            match_status = "UNMATCHED"
            if x.bill_no:
                if PendingBill.query.filter_by(bill_no=x.bill_no, client_code=x.client_code).first():
                    match_status = "MATCHED"
            
            data.append({
                'CLIENT_CODE': x.client_code,
                'CLIENT_NAME': x.client,
                'CLIENT_CATEGORY': x.client_category,
                'TRANSACTION_CATEGORY': 'CEMENT+BILL' if x.bill_no else 'CEMENT',
                'BILL_NO': x.bill_no,
                'BILL_DATE': x.date,
                'CEMENT_BRAND': x.material,
                'QTY': x.qty,
                'NIMBUS': x.nimbus_no,
                'NOTES': '',
                'SOURCE': 'CEMENT',
                'MATCH_STATUS': match_status
            })
        
    elif dataset == 'pending_bills':
        query = PendingBill.query
        if start_date:
            query = query.filter(PendingBill.created_at >= start_date)
        if end_date:
            query = query.filter(PendingBill.created_at <= f"{end_date} 23:59:59")
            
        bills = query.all()
        data = [{
            'client_code': x.client_code, 'bill_no': x.bill_no,
            'name': x.client_name, 'amount': x.amount,
            'reason': x.reason, 'nimbus': x.nimbus_no
        } for x in bills]
    else:
        return "Invalid dataset", 400
        
    df = pd.DataFrame(data)
    filename = f"{dataset}_export_{date.today()}"
    
    if fmt == 'csv':
        return Response(
            df.to_csv(index=False),
            mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename={filename}.csv"}
        )
    elif fmt == 'pdf':
        # Basic HTML to PDF using weasyprint if available
        html = f"""
        <html><head><style>
            body {{ font-family: sans-serif; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 10px; }}
            th, td {{ border: 1px solid #ddd; padding: 4px; }}
            th {{ background: #f2f2f2; }}
        </style></head><body>
        <h2>{dataset.upper()} EXPORT</h2>
        <p>Generated: {datetime.now()}</p>
        {df.to_html(index=False)}
        </body></html>
        """
        try:
            from flask_weasyprint import HTML, render_pdf
            return render_pdf(HTML(string=html), download_name=f"{filename}.pdf")
        except:
            return "PDF generation not available", 500
    else:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{filename}.xlsx")

@import_export_bp.route('/master/export')
@login_required
def export_master():
    """Export all datasets into a single Excel file with multiple sheets."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Clients
        clients = Client.query.all()
        client_data = [{k: getattr(x, k) for k in CLIENT_SCHEMA if hasattr(x, k)} for x in clients]
        for d, c in zip(client_data, clients):
            d['status'] = 'ACTIVE' if c.is_active else 'INACTIVE'
        if client_data:
            pd.DataFrame(client_data).to_excel(writer, sheet_name='Clients', index=False)
        else:
            pd.DataFrame(columns=CLIENT_SCHEMA).to_excel(writer, sheet_name='Clients', index=False)
        
        # Pending Bills
        bills = PendingBill.query.all()
        bill_data = [{
            'client_code': x.client_code, 'bill_no': x.bill_no,
            'name': x.client_name, 'amount': x.amount,
            'reason': x.reason, 'nimbus': x.nimbus_no
        } for x in bills]
        if bill_data:
            pd.DataFrame(bill_data).to_excel(writer, sheet_name='PendingBills', index=False)
        else:
            pd.DataFrame(columns=PENDING_BILL_SCHEMA).to_excel(writer, sheet_name='PendingBills', index=False)

        # Dispatch
        entries = Entry.query.filter_by(type='OUT').all()
        dispatch_data = []
        for x in entries:
            match_status = "UNMATCHED"
            if x.bill_no:
                if PendingBill.query.filter_by(bill_no=x.bill_no, client_code=x.client_code).first():
                    match_status = "MATCHED"
            
            dispatch_data.append({
                'CLIENT_CODE': x.client_code,
                'CLIENT_NAME': x.client,
                'CLIENT_CATEGORY': x.client_category,
                'TRANSACTION_CATEGORY': 'CEMENT+BILL' if x.bill_no else 'CEMENT',
                'BILL_NO': x.bill_no,
                'BILL_DATE': x.date,
                'CEMENT_BRAND': x.material,
                'QTY': x.qty,
                'NIMBUS': x.nimbus_no,
                'NOTES': '',
                'SOURCE': 'CEMENT',
                'MATCH_STATUS': match_status
            })
            
        if dispatch_data:
            pd.DataFrame(dispatch_data).to_excel(writer, sheet_name='Dispatch', index=False)
        else:
            pd.DataFrame(columns=DISPATCH_SCHEMA).to_excel(writer, sheet_name='Dispatch', index=False)

    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"master_backup_{date.today()}.xlsx")

@import_export_bp.route('/master/import', methods=['POST'])
@login_required
def import_master():
    """Import multiple datasets from a single Excel file."""
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400
    
    # Safety Backup
    success, msg = backup_database()
    if not success:
        return jsonify({'error': f"Backup failed: {msg}"}), 500

    try:
        xls = pd.ExcelFile(file)
        report = {'imported': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        # 1. Clients
        if 'Clients' in xls.sheet_names:
            df = pd.read_excel(xls, 'Clients')
            df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
            df = df.fillna('')
            _process_clients(df, 'update', report)
            
        # 2. Pending Bills
        if 'PendingBills' in xls.sheet_names:
            df = pd.read_excel(xls, 'PendingBills')
            df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
            df = df.fillna('')
            _process_pending_bills(df, 'update', 'create', report)
            
        # 3. Dispatch
        if 'Dispatch' in xls.sheet_names:
            df = pd.read_excel(xls, 'Dispatch')
            df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
            df = df.fillna('')
            _process_dispatch(df, 'skip', 'create', report)
            
        db.session.commit()
        return jsonify({'success': True, 'report': report})
        
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
