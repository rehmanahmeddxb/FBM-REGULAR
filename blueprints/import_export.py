import os
import shutil
import pandas as pd
import io
import re
import zipfile
import csv
import json
import smtplib
import hashlib
import threading
import uuid
import logging
import sqlite3
import tempfile
from datetime import datetime, date
from zoneinfo import ZoneInfo
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, Response, make_response, jsonify, current_app, session, g
from flask_login import login_required, current_user
from sqlalchemy import func, and_, or_, Date, DateTime, select, text
from sqlalchemy.engine.url import make_url
from email.message import EmailMessage
from models import db, Tenant, User, Material, MaterialCategory, Entry, Client, PendingBill, Booking, BookingItem, Payment, DirectSale, DirectSaleItem, GRN, GRNItem, Delivery, DeliveryItem, DeliveryPerson, DeliveryRent, Invoice, Settings, BillCounter, StaffEmail, get_or_create_material_category

# Module configuration
MODULE_CONFIG = {
    'name': 'Import/Export Module',
    'description': 'Data import and export functionality',
    'url_prefix': '/import_export',
    'enabled': True
}

import_export_bp = Blueprint('import_export', __name__)
_DEPLOY_PROGRESS = {}
_DEPLOY_PROGRESS_LOCK = threading.Lock()
_MASTER_IMPORT_PROGRESS = {}
_MASTER_IMPORT_PROGRESS_LOCK = threading.Lock()
_IMPORT_ACTOR_CTX = threading.local()
APP_UPGRADE_ENABLED = False
PK_TZ = ZoneInfo('Asia/Karachi')
FULL_RAW_EXCLUDE_TABLES = {
    # Root forensic log table; keep out of tenant replace/restore data path.
    'tenant_wipe_backup_history',
}


@import_export_bp.before_request
def _import_export_access_guard():
    # Tenant admin can import/export tenant-scoped data.
    # Only root can use app-upgrade endpoints.
    if not current_user.is_authenticated:
        return None
    role = getattr(current_user, 'role', None)
    endpoint = (request.endpoint or '')

    root_only = {
        'import_export.app_upgrade',
        'import_export.app_upgrade_start',
        'import_export.app_upgrade_status',
        'import_export.app_upgrade_rollback',
        'import_export.app_upgrade_migrate',
    }
    if endpoint in root_only and role != 'root':
        flash('Only root account can access App Upgrade operations.', 'danger')
        return redirect(url_for('index'))

    if role not in ['admin', 'root']:
        flash('Only tenant admin or root can access Import/Export operations.', 'danger')
        return redirect(url_for('index'))
    return None


def pk_now():
    return datetime.now(PK_TZ).replace(tzinfo=None)

def pk_today():
    return pk_now().date()


def _safe_name(value, fallback='unknown'):
    raw = str(value or '').strip()
    if not raw:
        raw = fallback
    return re.sub(r'[^A-Za-z0-9_.-]+', '_', raw)


def _tenant_release_dir(kind='artifacts'):
    """
    Store tenant import/export snapshots in release folders under DEPLOY_BASE_DIR.
    Layout:
      <DEPLOY_BASE_DIR>/tenant_data/<tenant-key>/<kind>/
    """
    base_dir = current_app.config.get('DEPLOY_BASE_DIR') or os.path.join(os.path.expanduser('~'), 'releases')
    role = _actor_role()
    tenant_id = _actor_tenant_id()
    username = _actor_username()
    if role == 'root':
        tenant_key = 'root'
    else:
        tenant_key = f"tenant_{_safe_name(tenant_id, 'unknown')}_{_safe_name(username, 'admin')}"
    path = os.path.join(base_dir, 'tenant_data', tenant_key, _safe_name(kind, 'artifacts'))
    try:
        os.makedirs(path, exist_ok=True)
        return path
    except Exception:
        fallback = os.path.join(current_app.instance_path, 'tenant_releases', tenant_key, _safe_name(kind, 'artifacts'))
        os.makedirs(fallback, exist_ok=True)
        return fallback


def _archive_artifact_bytes(content, filename, kind='artifacts'):
    try:
        folder = _tenant_release_dir(kind=kind)
        stamp = pk_now().strftime('%Y%m%d_%H%M%S')
        safe_filename = _safe_name(filename, 'artifact.bin')
        out_path = os.path.join(folder, f"{stamp}_{safe_filename}")
        data = content.encode('utf-8') if isinstance(content, str) else content
        with open(out_path, 'wb') as f:
            f.write(data or b'')
        return out_path
    except Exception:
        logging.exception('Archive artifact failed for %s', filename)
        return None


# --- Constants & Schemas ---
CLIENT_SCHEMA = [
    'code', 'name', 'phone', 'address', 'category',
    'financial_book_no', 'financial_page',
    'cement_book_no', 'cement_page',
    'steel_book_no', 'steel_page',
    'book_no', 'location_url', 'page_notes', 'status'
]
DISPATCH_SCHEMA = ['CLIENT_CODE', 'CLIENT_NAME', 'CLIENT_CATEGORY', 'TRANSACTION_CATEGORY', 'BILL_NO', 'BILL_DATE', 'CEMENT_BRAND', 'QTY', 'NIMBUS', 'NOTES', 'SOURCE', 'MATCH_STATUS']
PENDING_BILL_SCHEMA = ['client_code', 'bill_no', 'name', 'amount', 'reason', 'nimbus']
BOOKING_SCHEMA = ['client_name', 'manual_bill_no', 'amount', 'paid_amount', 'date_posted', 'note']
BOOKING_ITEM_SCHEMA = ['booking_bill_no', 'booking_client_name', 'material_name', 'qty', 'price_at_time']
PAYMENT_SCHEMA = ['client_name', 'manual_bill_no', 'amount', 'method', 'date_posted', 'note']
SALE_SCHEMA = [
    'client_name', 'manual_bill_no', 'auto_bill_no', 'category',
    'amount', 'paid_amount',
    'rent_item_revenue', 'delivery_rent_cost', 'rent_variance_loss',
    'date_posted', 'note'
]
SALE_ITEM_SCHEMA = ['sale_bill_no', 'sale_client_name', 'product_name', 'qty', 'price_at_time']
MASTER_SHEET_SECTIONS = {
    'clients': ['Clients'],
    'materials': ['MaterialCategories', 'Materials'],
    'dispatch': ['Dispatch'],
    'bookings': ['Bookings', 'BookingItems'],
    'payments': ['Payments'],
    'sales': ['Sales', 'SaleItems'],
    'supplier': ['GRN', 'GRNItems'],
    'delivery': ['DeliveryPersons', 'DeliveryRents'],
    'pending': ['PendingBills'],
}
MASTER_ALL_SHEETS = [
    'Clients', 'MaterialCategories', 'Materials', 'PendingBills',
    'Dispatch', 'Bookings', 'BookingItems', 'Payments', 'Sales',
    'SaleItems', 'GRN', 'GRNItems', 'DeliveryPersons', 'DeliveryRents',
]

# --- Helper Functions ---

def generate_client_code():
    """Generate next client code in format FBMCL-00001."""
    prefix = 'FBMCL-'
    max_num = 0
    rx = re.compile(r'^FBMCL-(\d+)$', re.IGNORECASE)
    for (raw_code,) in Client.query.with_entities(Client.code).all():
        code = (raw_code or '').strip()
        m = rx.match(code)
        if not m:
            continue
        try:
            max_num = max(max_num, int(m.group(1)))
        except Exception:
            continue
    return f"{prefix}{(max_num + 1):05d}"

def backup_database():
    """Creates a timestamped backup of the database before import."""
    try:
        role = _actor_role()
        timestamp = pk_now().strftime('%Y%m%d_%H%M%S')
        if role == 'root':
            db_path = current_app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
            if os.path.exists(db_path):
                backup_dir = _tenant_release_dir(kind='backups')
                backup_path = os.path.join(backup_dir, f"ahmed_cement_backup_{timestamp}.db")
                shutil.copy2(db_path, backup_path)
                return True, f"Backup created: {os.path.basename(backup_path)}"
        else:
            # Tenant-safe backup: only tenant-scoped export snapshot, not full DB copy.
            content = _build_master_export_bytes()
            backup_dir = _tenant_release_dir(kind='backups')
            backup_path = os.path.join(backup_dir, f"tenant_scope_backup_{timestamp}.xlsx")
            with open(backup_path, 'wb') as f:
                f.write(content)
            return True, f"Tenant backup created: {os.path.basename(backup_path)}"
    except Exception as e:
        return False, str(e)
    return False, "Database file not found"

def _record_discrepancy(report, msg):
    if 'discrepancies' not in report:
        report['discrepancies'] = []
    report['discrepancies'].append(msg)

def _clean_category(value, fallback='General'):
    cat = str(value or '').strip()
    return cat or fallback

def _set_import_actor_context(username=None, tenant_id=None, role=None):
    _IMPORT_ACTOR_CTX.username = username
    _IMPORT_ACTOR_CTX.tenant_id = tenant_id
    _IMPORT_ACTOR_CTX.role = role


def _clear_import_actor_context():
    _IMPORT_ACTOR_CTX.username = None
    _IMPORT_ACTOR_CTX.tenant_id = None
    _IMPORT_ACTOR_CTX.role = None


def _actor_username():
    try:
        if getattr(current_user, 'is_authenticated', False):
            return current_user.username
    except Exception:
        pass
    return getattr(_IMPORT_ACTOR_CTX, 'username', None) or 'system'


def _actor_tenant_id():
    try:
        if getattr(current_user, 'is_authenticated', False):
            return current_user.tenant_id
    except Exception:
        pass
    return getattr(_IMPORT_ACTOR_CTX, 'tenant_id', None)


def _actor_role():
    try:
        if getattr(current_user, 'is_authenticated', False):
            return getattr(current_user, 'role', None)
    except Exception:
        pass
    try:
        if bool(getattr(g, 'is_root', False)):
            return 'root'
    except Exception:
        pass
    return getattr(_IMPORT_ACTOR_CTX, 'role', None)

def _resolve_scope_context(scope_raw=None, tenant_id_raw=None):
    """
    Determine import/export scope.
    - admin: forced to own tenant
    - root: supports all_tenants or a single tenant scope
    """
    role = _actor_role()
    actor_tenant_id = _actor_tenant_id()
    if role != 'root':
        if not actor_tenant_id:
            raise ValueError('Tenant admin account is not linked to a tenant.')
        tenant = Tenant.query.filter_by(id=actor_tenant_id).first()
        return {
            'scope': 'tenant',
            'target_tenant_id': actor_tenant_id,
            'target_tenant_name': tenant.name if tenant else None,
            'role': role,
        }

    scope = str(scope_raw or 'all_tenants').strip().lower()
    if scope != 'tenant':
        return {
            'scope': 'all_tenants',
            'target_tenant_id': None,
            'target_tenant_name': 'All Tenants',
            'role': role,
        }

    target_tenant_id = str(tenant_id_raw or '').strip()
    if not target_tenant_id:
        raise ValueError('Select a tenant for tenant-scoped import/export.')
    tenant = Tenant.query.filter_by(id=target_tenant_id).first()
    if not tenant:
        raise ValueError('Selected tenant was not found.')
    return {
        'scope': 'tenant',
        'target_tenant_id': target_tenant_id,
        'target_tenant_name': tenant.name,
        'role': role,
    }

def _default_scope_context():
    scope_raw = None
    tenant_id_raw = None
    try:
        scope_raw = request.args.get('scope')
        tenant_id_raw = request.args.get('tenant_id')
    except Exception:
        scope_raw = None
        tenant_id_raw = None
    return _resolve_scope_context(scope_raw=scope_raw, tenant_id_raw=tenant_id_raw)

def _full_raw_tables_for_scope(scope_ctx):
    if scope_ctx.get('scope') == 'all_tenants':
        return [t for t in db.metadata.sorted_tables if t.name not in FULL_RAW_EXCLUDE_TABLES]
    return [t for t in db.metadata.sorted_tables if 'tenant_id' in t.c and t.name not in FULL_RAW_EXCLUDE_TABLES]

def _scope_table_select(table, scope_ctx):
    if scope_ctx.get('scope') == 'all_tenants':
        return table.select()
    if 'tenant_id' not in table.c:
        return None
    return table.select().where(table.c.tenant_id == scope_ctx.get('target_tenant_id'))

def _scoped_model_query(model, scope_ctx):
    q = model.query
    if scope_ctx.get('scope') == 'tenant' and hasattr(model, 'tenant_id'):
        q = q.filter(model.tenant_id == scope_ctx.get('target_tenant_id'))
    return q

def _sqlite_db_file_path():
    uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
    try:
        parsed = make_url(uri)
    except Exception:
        return None
    if (parsed.drivername or '').startswith('sqlite'):
        return parsed.database
    return None

def _normalize_sqlite_value_for_column(value, col):
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        if isinstance(col.type, DateTime):
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return value
        if isinstance(col.type, Date):
            try:
                if 'T' in s:
                    s = s.split('T', 1)[0]
                elif ' ' in s:
                    s = s.split(' ', 1)[0]
                return date.fromisoformat(s)
            except Exception:
                return value
    return value

def _default_material_category_id():
    try:
        cat = get_or_create_material_category(_actor_tenant_id(), 'General')
        return cat.id if cat else None
    except Exception:
        return None

def _build_triage_maps(dfs):
    name_to_code = {}
    code_to_name = {}
    name_cols = ['client_name', 'name', 'customer']
    code_cols = ['client_code', 'code']
    for df in dfs:
        if df is None:
            continue
        cols = set(df.columns)
        for _, row in df.iterrows():
            code = ''
            for c in code_cols:
                if c in cols:
                    code = str(row.get(c, '')).strip()
                    if code:
                        break
            name = ''
            for n in name_cols:
                if n in cols:
                    name = str(row.get(n, '')).strip()
                    if name:
                        break
            if name and code:
                name_to_code[name] = code
                code_to_name[code] = name
    return name_to_code, code_to_name

def _apply_triage(df, name_to_code, code_to_name):
    if df is None:
        return df
    cols = set(df.columns)
    name_cols = [c for c in ['client_name', 'name', 'customer'] if c in cols]
    has_code = 'client_code' in cols
    for idx, row in df.iterrows():
        code = str(row.get('client_code', '')).strip() if has_code else ''
        name = ''
        for n in name_cols:
            name = str(row.get(n, '')).strip()
            if name:
                break
        if has_code and not code and name and name in name_to_code:
            df.at[idx, 'client_code'] = name_to_code[name]
            code = name_to_code[name]
        if name_cols and not name and code and code in code_to_name:
            df.at[idx, name_cols[0]] = code_to_name[code]
    return df

def validate_client_row(row):
    errors = []
    # Relaxed code validation for legacy + current client code formats.
    code_raw = str(row.get('code', '')).strip()
    if not re.match(r'^(FBMCL-\d+|FBM-\d+|tmpc-\d+)$', code_raw, re.IGNORECASE):
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
    full_raw_import_enabled = str(
        os.environ.get('FULL_RAW_IMPORT_ENABLED', current_app.config.get('FULL_RAW_IMPORT_ENABLED', '0'))
    ).strip().lower() in ['1', 'true', 'on', 'yes']
    report_name = request.args.get('full_raw_import_report') or session.get('full_raw_import_report')
    report_meta = session.get('full_raw_import_report_meta')
    if report_name and report_meta and report_meta.get('name') == report_name:
        full_raw_import_report = report_meta
    elif report_name:
        full_raw_import_report = {'name': report_name, 'created_at': None}
    else:
        full_raw_import_report = None
    tenants = []
    if getattr(current_user, 'role', None) == 'root':
        tenants = Tenant.query.order_by(Tenant.name.asc()).all()
    return render_template(
        'import_export_new.html',
        full_raw_import_enabled=full_raw_import_enabled,
        full_raw_import_report=full_raw_import_report,
        tenants=tenants,
    )

@import_export_bp.route('/template/<dataset>')
@login_required
def get_template(dataset):
    """Generate empty templates for manual entry."""
    fmt = (request.args.get('format') or 'excel').lower()

    if dataset == 'clients':
        df = pd.DataFrame(columns=CLIENT_SCHEMA)
        if fmt == 'csv':
            return Response(
                df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename=template_{dataset}.csv"}
            )
    elif dataset == 'dispatch':
        df = pd.DataFrame(columns=DISPATCH_SCHEMA)
        if fmt == 'csv':
            return Response(
                df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename=template_{dataset}.csv"}
            )
    elif dataset == 'pending_bills':
        df = pd.DataFrame(columns=PENDING_BILL_SCHEMA)
        if fmt == 'csv':
            return Response(
                df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-disposition": f"attachment; filename=template_{dataset}.csv"}
            )
    elif dataset == 'client_full':
        if fmt == 'csv':
            output = io.BytesIO()
            with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.writestr('clients.csv', pd.DataFrame(columns=CLIENT_SCHEMA).to_csv(index=False))
                zf.writestr('bookings.csv', pd.DataFrame(columns=BOOKING_SCHEMA).to_csv(index=False))
                zf.writestr('booking_items.csv', pd.DataFrame(columns=BOOKING_ITEM_SCHEMA).to_csv(index=False))
                zf.writestr('dispatch.csv', pd.DataFrame(columns=DISPATCH_SCHEMA).to_csv(index=False))
                zf.writestr('payments.csv', pd.DataFrame(columns=PAYMENT_SCHEMA).to_csv(index=False))
                zf.writestr('sales.csv', pd.DataFrame(columns=SALE_SCHEMA).to_csv(index=False))
                zf.writestr('sale_items.csv', pd.DataFrame(columns=SALE_ITEM_SCHEMA).to_csv(index=False))
                zf.writestr('pending_bills.csv', pd.DataFrame(columns=PENDING_BILL_SCHEMA).to_csv(index=False))
            output.seek(0)
            return send_file(output, as_attachment=True, download_name="template_client_full_csv.zip", mimetype='application/zip')
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            pd.DataFrame(columns=CLIENT_SCHEMA).to_excel(writer, sheet_name='Clients', index=False)
            pd.DataFrame(columns=BOOKING_SCHEMA).to_excel(writer, sheet_name='Bookings', index=False)
            pd.DataFrame(columns=BOOKING_ITEM_SCHEMA).to_excel(writer, sheet_name='BookingItems', index=False)
            pd.DataFrame(columns=DISPATCH_SCHEMA).to_excel(writer, sheet_name='Dispatch', index=False)
            pd.DataFrame(columns=PAYMENT_SCHEMA).to_excel(writer, sheet_name='Payments', index=False)
            pd.DataFrame(columns=SALE_SCHEMA).to_excel(writer, sheet_name='Sales', index=False)
            pd.DataFrame(columns=SALE_ITEM_SCHEMA).to_excel(writer, sheet_name='SaleItems', index=False)
            pd.DataFrame(columns=PENDING_BILL_SCHEMA).to_excel(writer, sheet_name='PendingBills', index=False)
        output.seek(0)
        return send_file(output, as_attachment=True, download_name="template_client_full.xlsx")
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
    if file:
        try:
            raw = file.read()
            if hasattr(file, 'stream'):
                file.stream.seek(0)
            _archive_artifact_bytes(raw, f"execute_import_{dataset_type}_{file.filename}", kind='imports')
        except Exception:
            logging.exception('Failed to archive execute_import upload')
    
    # 1. Safety Backup
    success, msg = backup_database()
    if not success:
        return jsonify({'error': f"Backup failed: {msg}"}), 500
        
    try:
        report = {'imported': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'error_details': [], 'discrepancies': []}

        # 2. Process based on type
        if dataset_type == 'client_full':
            if not file.filename.lower().endswith(('.xlsx', '.xls')):
                return jsonify({'error': 'Client Full import requires Excel template (.xlsx/.xls).'}), 400
            xls = pd.ExcelFile(file)
            sheets = {}
            if 'Clients' in xls.sheet_names:
                d = pd.read_excel(xls, 'Clients').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['clients'] = d
            if 'Bookings' in xls.sheet_names:
                d = pd.read_excel(xls, 'Bookings').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['bookings'] = d
            if 'BookingItems' in xls.sheet_names:
                d = pd.read_excel(xls, 'BookingItems').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['booking_items'] = d
            if 'Dispatch' in xls.sheet_names:
                d = pd.read_excel(xls, 'Dispatch').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['dispatch'] = d
            if 'Payments' in xls.sheet_names:
                d = pd.read_excel(xls, 'Payments').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['payments'] = d
            if 'Sales' in xls.sheet_names:
                d = pd.read_excel(xls, 'Sales').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['sales'] = d
            if 'SaleItems' in xls.sheet_names:
                d = pd.read_excel(xls, 'SaleItems').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['sale_items'] = d
            if 'PendingBills' in xls.sheet_names:
                d = pd.read_excel(xls, 'PendingBills').fillna('')
                d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
                sheets['pending_bills'] = d

            name_to_code, code_to_name = _build_triage_maps(sheets.values())

            if 'clients' in sheets:
                _process_clients(_apply_triage(sheets['clients'], name_to_code, code_to_name), 'update', report)
            if 'bookings' in sheets:
                _process_bookings(_apply_triage(sheets['bookings'], name_to_code, code_to_name), conflict_strategy, report, allow_missing=True)
            if 'booking_items' in sheets:
                _process_booking_items(sheets['booking_items'], conflict_strategy, report)
            if 'dispatch' in sheets:
                d = sheets['dispatch']
                d.rename(columns={'cement_brand': 'item', 'client_name': 'customer', 'bill_date': 'date', 'nimbus': 'nimbus_no'}, inplace=True)
                _process_dispatch(_apply_triage(d, name_to_code, code_to_name), conflict_strategy, missing_client_strategy, report, allow_missing=True)
            if 'payments' in sheets:
                _process_payments(_apply_triage(sheets['payments'], name_to_code, code_to_name), conflict_strategy, report, allow_missing=True)
            if 'sales' in sheets:
                _process_sales(_apply_triage(sheets['sales'], name_to_code, code_to_name), conflict_strategy, report, allow_missing=True)
            if 'sale_items' in sheets:
                _process_sale_items(sheets['sale_items'], conflict_strategy, report)
            if 'pending_bills' in sheets:
                _process_pending_bills(_apply_triage(sheets['pending_bills'], name_to_code, code_to_name), conflict_strategy, missing_client_strategy, report, allow_missing=True)
        else:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
            
            df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
            df = df.fillna('')
        
        if dataset_type == 'client_full':
            pass
        elif dataset_type == 'clients':
            _process_clients(df, conflict_strategy, report)
        elif dataset_type == 'pending_bills':
            _process_pending_bills(df, conflict_strategy, missing_client_strategy, report, allow_missing=False)
        elif dataset_type == 'dispatch':
            # Add renaming for user's format
            df.rename(columns={
                'cement_brand': 'item',
                'client_name': 'customer',
                'bill_date': 'date',
                'nimbus': 'nimbus_no'
            }, inplace=True)
            _process_dispatch(df, conflict_strategy, missing_client_strategy, report, allow_missing=False)
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
        category_val = str(row.get('category', '')).strip()
            
        existing = Client.query.filter_by(code=code).first()
        
        if existing:
            if strategy == 'update':
                existing.name = row.get('name', existing.name)
                existing.phone = str(row.get('phone', existing.phone))
                existing.address = str(row.get('address', existing.address))
                if category_val:
                    existing.category = category_val
                existing.financial_book_no = str(row.get('financial_book_no', existing.financial_book_no or '') or '')
                existing.book_no = str(row.get('book_no', existing.book_no or '') or '')
                existing.location_url = str(row.get('location_url', existing.location_url or '') or '')
                existing.financial_page = str(row.get('financial_page', existing.financial_page or '') or '')
                existing.cement_book_no = str(row.get('cement_book_no', existing.cement_book_no or '') or '')
                existing.cement_page = str(row.get('cement_page', existing.cement_page or '') or '')
                existing.steel_book_no = str(row.get('steel_book_no', existing.steel_book_no or '') or '')
                existing.steel_page = str(row.get('steel_page', existing.steel_page or '') or '')
                existing.page_notes = str(row.get('page_notes', existing.page_notes or '') or '')
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
                category=_clean_category(category_val),
                financial_book_no=str(row.get('financial_book_no', '') or ''),
                book_no=str(row.get('book_no', '') or ''),
                location_url=str(row.get('location_url', '') or ''),
                financial_page=str(row.get('financial_page', '') or ''),
                cement_book_no=str(row.get('cement_book_no', '') or ''),
                cement_page=str(row.get('cement_page', '') or ''),
                steel_book_no=str(row.get('steel_book_no', '') or ''),
                steel_page=str(row.get('steel_page', '') or ''),
                page_notes=str(row.get('page_notes', '') or ''),
                is_active=True
            )
            db.session.add(new_client)
            report['imported'] += 1

def _process_pending_bills(df, strategy, missing_client_strategy, report, allow_missing=False):
    for _, row in df.iterrows():
        bill_no = str(row.get('bill_no', '')).strip()
        client_code = str(row.get('client_code', '')).strip()
        
        if not bill_no or not client_code:
            report['errors'] += 1
            _record_discrepancy(report, f"PendingBills: Missing bill_no/client_code (bill_no='{bill_no}', client_code='{client_code}')")
            if not allow_missing:
                continue
            
        # Check Client Dependency
        client = Client.query.filter_by(code=client_code).first()
        if not client:
            if missing_client_strategy == 'stop':
                raise Exception(f"Missing client {client_code} for bill {bill_no}")
            elif missing_client_strategy == 'skip':
                if not allow_missing:
                    report['skipped'] += 1
                    continue
                _record_discrepancy(report, f"PendingBills: Missing client {client_code} for bill {bill_no} (imported as-is)")
            elif missing_client_strategy == 'create':
                client = Client(code=client_code, name=row.get('name', 'Imported Client'), is_active=True)
                db.session.add(client)
                db.session.flush() # Get ID
        
        existing = PendingBill.query.filter_by(bill_no=bill_no).first() if bill_no else None
        
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
                client_name=client.name if client else str(row.get('name', '')).strip(),
                bill_no=bill_no,
                amount=float(row.get('amount', 0)),
                reason=row.get('reason', 'Imported'),
                nimbus_no=row.get('nimbus', ''),
                created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                created_by=_actor_username()
            )
            db.session.add(new_bill)
            report['imported'] += 1

def _process_dispatch(df, strategy, missing_client_strategy, report, allow_missing=False):
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
        transaction_category = get_val('transaction_category').upper()
        bill_no = get_val('bill_no')
        entry_date_str = get_val('date')
        nimbus_no = get_val('nimbus_no')

        # Normalize key categories so a single template can represent all types.
        upper_client_cat = client_category.upper()
        is_open_khata_row = (
            upper_client_cat in ['OPEN KHATA', 'OPEN_KHATA'] or
            transaction_category in ['OPEN KHATA', 'OPEN_KHATA']
        )
        is_cash_unbilled_row = (
            transaction_category in ['UNBILLED', 'CASH'] or
            upper_client_cat in ['CASH', 'UNBILLED']
        )

        if is_open_khata_row:
            client_code = client_code or 'OPEN-KHATA'
            client_name = client_name or 'OPEN KHATA'
            if not client_category:
                client_category = 'Open Khata'

        if is_cash_unbilled_row and str(bill_no).upper() in ['NOT BILLED', 'CASH', '']:
            # Keep unbilled cash entries without pending-bill linkage.
            bill_no = ''

        # Handle date format
        entry_date = None
        if entry_date_str:
            try:
                entry_date = pd.to_datetime(entry_date_str).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass 

        if not entry_date:
            report['skipped'] += 1
            report['error_details'].append(f"Skipped: Missing or invalid date for item '{item}' (Bill: {bill_no})")
            continue

        # Check Client Dependency
        client = Client.query.filter_by(code=client_code).first()
        
        if not client:
            if client_name:
                client = Client.query.filter_by(name=client_name).first()

            if not client and (client_code or client_name):
                if missing_client_strategy == 'create':
                    new_code = client_code if client_code else generate_client_code()
                    if not Client.query.filter_by(code=new_code).first():
                        client = Client(
                            code=new_code,
                            name=client_name or 'Imported Client',
                            category=_clean_category(client_category),
                            is_active=True
                        )
                        db.session.add(client)
                        db.session.flush()
                elif missing_client_strategy == 'stop':
                    raise Exception(f"Missing client '{client_code or client_name}' for dispatch")
                else: # 'skip' is the default
                    if not allow_missing:
                        report['skipped'] += 1
                        report['error_details'].append(f"Skipped: Client '{client_code or client_name}' not found.")
                        continue
                    _record_discrepancy(report, f"Dispatch: Missing client '{client_code or client_name}' (imported as-is)")
        elif client_category:
            # Keep client master category aligned when import contains a category.
            client.category = client_category
        
        final_client_code = client.code if client else client_code
        final_client_name = client.name if client else client_name
        final_client_category = client_category or (client.category if client and client.category else '')

        # Ensure Material Exists
        mat = None
        if item:
            mat = Material.query.filter(func.lower(Material.name) == item.lower()).first()
            if not mat:
                mat = Material(name=item, code=f"MAT-{pk_now().strftime('%f')}", category_id=_default_material_category_id())
                db.session.add(mat)
                db.session.flush()
        
        # --- Create Entry ---
        entry = Entry(
            date=entry_date,
            time=pk_now().strftime('%H:%M:%S'),
            type='OUT',
            material=mat.name if mat else None,
            client=final_client_name,
            client_code=final_client_code,
            client_category=final_client_category,
            transaction_category=transaction_category or None,
            qty=qty,
            bill_no=bill_no,
            nimbus_no=nimbus_no,
            created_by=_actor_username()
        )
        db.session.add(entry)
        
        if mat and qty > 0:
            mat.total = (mat.total or 0) - qty
        
        # --- Sync Pending Bill ---
        # If data matches with client names and codes and bill no in pending bills it must sync
        if bill_no and str(bill_no).upper() not in ['CASH', 'NOT BILLED', ''] and transaction_category not in ['UNBILLED', 'CASH']:
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
                    created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                    created_by=_actor_username(),
                    is_manual=True
                )
                db.session.add(new_pb)
        
        report['imported'] += 1

def _parse_dt(value):
    if value is None:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    try:
        return pd.to_datetime(txt).to_pydatetime()
    except Exception:
        return None

def _upsert_pending_bill_from_booking(client_name, manual_bill_no, amount, paid_amount, note=''):
    bill_no = str(manual_bill_no or '').strip()
    if not bill_no:
        return

    booking_client_name = str(client_name or '').strip()
    resolved_client = None
    if booking_client_name:
        resolved_client = Client.query.filter_by(name=booking_client_name).first()

    final_client_code = resolved_client.code if resolved_client else ''
    final_client_name = resolved_client.name if resolved_client else booking_client_name

    bill_amount = float(amount or 0)
    paid = float(paid_amount or 0)
    is_paid = paid >= bill_amount and bill_amount > 0

    pb = PendingBill.query.filter_by(bill_no=bill_no).first()
    if pb:
        if final_client_code:
            pb.client_code = final_client_code
        if final_client_name:
            pb.client_name = final_client_name
        pb.amount = bill_amount
        pb.is_paid = is_paid
        pb.is_manual = True
        if note:
            pb.note = note
        if not pb.reason:
            pb.reason = 'Imported Booking'
        return

    new_pb = PendingBill(
        client_code=final_client_code,
        client_name=final_client_name,
        bill_no=bill_no,
        amount=bill_amount,
        reason='Imported Booking',
        is_paid=is_paid,
        created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
        created_by=_actor_username(),
        is_manual=True,
        note=str(note or '').strip()
    )
    db.session.add(new_pb)

def _process_bookings(df, strategy, report, allow_missing=False):
    for _, row in df.iterrows():
        client_name = str(row.get('client_name', '')).strip()
        if not client_name:
            report['errors'] += 1
            _record_discrepancy(report, "Bookings: Missing client_name (imported as-is)")
            if not allow_missing:
                continue

        manual_bill_no = str(row.get('manual_bill_no', '')).strip()
        amount = float(row.get('amount', 0) or 0)
        paid_amount = float(row.get('paid_amount', 0) or 0)
        rent_item_revenue = float(row.get('rent_item_revenue', 0) or 0)
        delivery_rent_cost = float(row.get('delivery_rent_cost', 0) or 0)
        rent_variance_loss = float(row.get('rent_variance_loss', 0) or 0)
        note = str(row.get('note', '')).strip()
        date_posted = _parse_dt(row.get('date_posted')) or pk_now()

        existing = Booking.query.filter_by(client_name=client_name, manual_bill_no=manual_bill_no).first() if manual_bill_no else None
        if existing:
            # Keep pending bills in sync even when duplicate booking rows are skipped.
            _upsert_pending_bill_from_booking(client_name, manual_bill_no, amount, paid_amount, note)
            if strategy == 'update':
                existing.amount = amount
                existing.paid_amount = paid_amount
                existing.rent_item_revenue = rent_item_revenue
                existing.delivery_rent_cost = delivery_rent_cost
                existing.rent_variance_loss = rent_variance_loss
                existing.note = note
                existing.date_posted = date_posted
                report['updated'] += 1
            else:
                report['skipped'] += 1
            continue

        b = Booking(
            client_name=client_name,
            manual_bill_no=manual_bill_no or None,
            amount=amount,
            paid_amount=paid_amount,
            date_posted=date_posted,
            note=note
        )
        db.session.add(b)
        _upsert_pending_bill_from_booking(client_name, manual_bill_no, amount, paid_amount, note)
        report['imported'] += 1

def _process_booking_items(df, strategy, report):
    for _, row in df.iterrows():
        bill_no = str(row.get('booking_bill_no', '')).strip()
        client_name = str(row.get('booking_client_name', '')).strip()
        material_name = str(row.get('material_name', '')).strip()
        if not material_name:
            report['errors'] += 1
            continue

        booking = None
        if bill_no:
            booking = Booking.query.filter_by(manual_bill_no=bill_no, client_name=client_name).first()
        if not booking and client_name:
            booking = Booking.query.filter_by(client_name=client_name).order_by(Booking.id.desc()).first()
        if not booking:
            report['skipped'] += 1
            continue

        qty = float(row.get('qty', 0) or 0)
        price = float(row.get('price_at_time', 0) or 0)

        # Ensure material exists in master list so future forms can select it.
        mat = Material.query.filter(func.lower(Material.name) == material_name.lower()).first()
        if not mat:
            mat = Material(name=material_name, code=f"MAT-{pk_now().strftime('%f')}", unit_price=price or 0, category_id=_default_material_category_id())
            db.session.add(mat)
            db.session.flush()

        exists = BookingItem.query.filter_by(
            booking_id=booking.id,
            material_name=material_name,
            qty=qty,
            price_at_time=price
        ).first()
        if exists:
            report['skipped'] += 1
            continue

        db.session.add(BookingItem(
            booking_id=booking.id,
            material_name=material_name,
            qty=qty,
            price_at_time=price
        ))
        report['imported'] += 1

def _process_payments(df, strategy, report, allow_missing=False):
    for _, row in df.iterrows():
        client_name = str(row.get('client_name', '')).strip()
        amount = float(row.get('amount', 0) or 0)
        if not client_name:
            report['errors'] += 1
            _record_discrepancy(report, f"Payments: Missing client_name (amount={amount})")
            if not allow_missing and amount <= 0:
                continue
        manual_bill_no = str(row.get('manual_bill_no', '')).strip()
        method = str(row.get('method', 'Cash')).strip() or 'Cash'
        note = str(row.get('note', '')).strip()
        date_posted = _parse_dt(row.get('date_posted')) or pk_now()

        existing = Payment.query.filter_by(client_name=client_name, manual_bill_no=manual_bill_no, amount=amount).first() if manual_bill_no else None
        if existing:
            if strategy == 'update':
                existing.method = method
                existing.note = note
                existing.date_posted = date_posted
                report['updated'] += 1
            else:
                report['skipped'] += 1
            continue

        db.session.add(Payment(
            client_name=client_name,
            amount=amount,
            method=method,
            manual_bill_no=manual_bill_no or None,
            date_posted=date_posted,
            note=note
        ))
        report['imported'] += 1

def _process_sales(df, strategy, report, allow_missing=False):
    for _, row in df.iterrows():
        client_name = str(row.get('client_name', '')).strip()
        if not client_name:
            report['errors'] += 1
            _record_discrepancy(report, "Sales: Missing client_name (imported as-is)")
            if not allow_missing:
                continue
        manual_bill_no = str(row.get('manual_bill_no', '')).strip()
        auto_bill_no = str(row.get('auto_bill_no', '')).strip()
        category = str(row.get('category', 'Credit Customer')).strip() or 'Credit Customer'
        amount = float(row.get('amount', 0) or 0)
        paid_amount = float(row.get('paid_amount', 0) or 0)
        note = str(row.get('note', '')).strip()
        date_posted = _parse_dt(row.get('date_posted')) or pk_now()

        existing = None
        if manual_bill_no:
            existing = DirectSale.query.filter_by(client_name=client_name, manual_bill_no=manual_bill_no).first()
        elif auto_bill_no:
            existing = DirectSale.query.filter_by(client_name=client_name, auto_bill_no=auto_bill_no).first()

        if existing:
            if strategy == 'update':
                existing.category = category
                existing.amount = amount
                existing.paid_amount = paid_amount
                existing.note = note
                existing.date_posted = date_posted
                report['updated'] += 1
            else:
                report['skipped'] += 1
            continue

        db.session.add(DirectSale(
            client_name=client_name,
            manual_bill_no=manual_bill_no or None,
            auto_bill_no=auto_bill_no or None,
            category=category,
            amount=amount,
            paid_amount=paid_amount,
            rent_item_revenue=rent_item_revenue,
            delivery_rent_cost=delivery_rent_cost,
            rent_variance_loss=rent_variance_loss,
            date_posted=date_posted,
            note=note
        ))
        report['imported'] += 1

def _process_sale_items(df, strategy, report):
    for _, row in df.iterrows():
        bill_no = str(row.get('sale_bill_no', '')).strip()
        client_name = str(row.get('sale_client_name', '')).strip()
        product_name = str(row.get('product_name', '')).strip()
        if not product_name:
            report['errors'] += 1
            continue
        sale = None
        if bill_no:
            sale = DirectSale.query.filter(
                DirectSale.client_name == client_name,
                or_(DirectSale.manual_bill_no == bill_no, DirectSale.auto_bill_no == bill_no)
            ).first()
        if not sale and client_name:
            sale = DirectSale.query.filter_by(client_name=client_name).order_by(DirectSale.id.desc()).first()
        if not sale:
            report['skipped'] += 1
            continue

        qty = float(row.get('qty', 0) or 0)
        price = float(row.get('price_at_time', 0) or 0)

        # Ensure product exists in material master for consistent downstream behavior.
        mat = Material.query.filter(func.lower(Material.name) == product_name.lower()).first()
        if not mat:
            mat = Material(name=product_name, code=f"MAT-{pk_now().strftime('%f')}", unit_price=price or 0, category_id=_default_material_category_id())
            db.session.add(mat)
            db.session.flush()

        exists = DirectSaleItem.query.filter_by(
            sale_id=sale.id,
            product_name=product_name,
            qty=qty,
            price_at_time=price
        ).first()
        if exists:
            report['skipped'] += 1
            continue

        db.session.add(DirectSaleItem(
            sale_id=sale.id,
            product_name=product_name,
            qty=qty,
            price_at_time=price
        ))
        report['imported'] += 1

def _process_material_categories(df, report):
    for _, row in df.iterrows():
        name = str(row.get('name', '')).strip()
        if not name:
            continue
        
        # Check existence (case-insensitive)
        cat = MaterialCategory.query.filter(
            func.lower(MaterialCategory.name) == name.lower()
        ).first()
        
        if not cat:
            cat = MaterialCategory(name=name, is_active=True)
            db.session.add(cat)
            report['imported'] += 1
        else:
            # We don't overwrite existing categories to avoid breaking IDs, just ensure it exists
            pass

def _process_materials(df, report):
    for _, row in df.iterrows():
        code = str(row.get('code', '')).strip()
        name = str(row.get('name', '')).strip()
        if not name:
            continue
            
        cat_name = str(row.get('category_name', '')).strip()
        cat = None
        if cat_name:
            cat = MaterialCategory.query.filter(func.lower(MaterialCategory.name) == cat_name.lower()).first()
            if not cat:
                cat = MaterialCategory(name=cat_name, is_active=True)
                db.session.add(cat)
                db.session.flush()
        
        # Try finding by code first, then name
        mat = None
        if code:
            mat = Material.query.filter_by(code=code).first()
        if not mat:
            mat = Material.query.filter(func.lower(Material.name) == name.lower()).first()
            
        if mat:
            mat.name = name
            if code: mat.code = code
            if cat: mat.category_id = cat.id
            # During master import, always reactivate matched materials so dispatch/import doesn't block.
            mat.is_active = True
            try: mat.unit_price = float(row.get('unit_price', 0))
            except: pass
            try: mat.total = float(row.get('total', 0))
            except: pass
            if 'unit' in row: mat.unit = str(row.get('unit', '')).strip() or 'Bags'
            report['updated'] += 1
        else:
            new_mat = Material(
                code=code or f"MAT-{pk_now().strftime('%f')}",
                name=name,
                category_id=cat.id if cat else None,
                unit_price=float(row.get('unit_price', 0) or 0),
                total=float(row.get('total', 0) or 0),
                is_active=True,
                unit=str(row.get('unit', 'Bags')).strip() or 'Bags'
            )
            db.session.add(new_mat)
            report['imported'] += 1

def _process_grn(df, strategy, report):
    for _, row in df.iterrows():
        supplier = str(row.get('supplier', '')).strip()
        manual_bill_no = str(row.get('manual_bill_no', '')).strip()
        auto_bill_no = str(row.get('auto_bill_no', '')).strip()
        note = str(row.get('note', '')).strip()
        date_posted = _parse_dt(row.get('date_posted')) or pk_now()

        existing = None
        if manual_bill_no:
            existing = GRN.query.filter_by(manual_bill_no=manual_bill_no).first()
        elif auto_bill_no:
            existing = GRN.query.filter_by(auto_bill_no=auto_bill_no).first()
        
        if existing:
            if strategy == 'update':
                existing.supplier = supplier
                existing.note = note
                existing.date_posted = date_posted
                report['updated'] += 1
            else:
                report['skipped'] += 1
            continue

        db.session.add(GRN(
            supplier=supplier,
            manual_bill_no=manual_bill_no or None,
            auto_bill_no=auto_bill_no or None,
            date_posted=date_posted,
            note=note
        ))
        report['imported'] += 1

def _process_grn_items(df, strategy, report):
    for _, row in df.iterrows():
        manual_bill = str(row.get('grn_manual_bill_no', '') or row.get('grn_manual_bill', '')).strip()
        auto_bill = str(row.get('grn_auto_bill_no', '') or row.get('grn_auto_bill', '')).strip()
        mat_name = str(row.get('material_name', '')).strip()
        if not mat_name:
            continue
            
        grn = None
        if manual_bill:
            grn = GRN.query.filter_by(manual_bill_no=manual_bill).first()
        elif auto_bill:
            grn = GRN.query.filter_by(auto_bill_no=auto_bill).first()
            
        if not grn:
            report['skipped'] += 1
            continue

        qty = float(row.get('qty', 0) or row.get('quantity', 0) or 0)
        price = float(row.get('price', 0) or row.get('rate', 0) or 0)

        # Ensure material exists
        mat = Material.query.filter(func.lower(Material.name) == mat_name.lower()).first()
        if not mat:
            mat = Material(name=mat_name, code=f"MAT-{pk_now().strftime('%f')}", category_id=_default_material_category_id())
            db.session.add(mat)
            db.session.flush()

        exists = GRNItem.query.filter_by(
            grn_id=grn.id,
            mat_name=mat_name,
            qty=qty,
            price_at_time=price
        ).first()
        
        if exists:
            continue

        db.session.add(GRNItem(
            grn_id=grn.id,
            mat_name=mat_name,
            qty=qty,
            price_at_time=price
        ))
        report['imported'] += 1

def _smtp_send_attachments(subject, body, attachments):
    settings_obj = Settings.query.first()
    recipients = [x.email for x in StaffEmail.query.filter_by(is_active=True).all() if x.email]
    if not recipients:
        return False, 'No active staff emails configured in Notifications.'

    smtp_host = (settings_obj.smtp_host if settings_obj and settings_obj.smtp_host else os.environ.get('SMTP_HOST', '')).strip()
    smtp_user = (settings_obj.smtp_user if settings_obj and settings_obj.smtp_user else os.environ.get('SMTP_USER', '')).strip()
    smtp_pass = (settings_obj.smtp_pass if settings_obj and settings_obj.smtp_pass else os.environ.get('SMTP_PASS', '')).strip().replace(' ', '')
    smtp_port = int((settings_obj.smtp_port if settings_obj and settings_obj.smtp_port else os.environ.get('SMTP_PORT', '587')) or 587)
    if settings_obj and settings_obj.smtp_use_tls is not None:
        use_tls = bool(settings_obj.smtp_use_tls)
    else:
        use_tls = os.environ.get('SMTP_USE_TLS', '1').strip() != '0'
    from_email = (
        (settings_obj.smtp_from if settings_obj and settings_obj.smtp_from else '') or
        os.environ.get('SMTP_FROM', '') or
        smtp_user
    ).strip()

    if not smtp_host or not from_email:
        return False, 'SMTP settings missing. Configure in Settings first.'
    if smtp_user and not smtp_pass:
        return False, 'SMTP password missing. Enter SMTP App Password in Settings.'

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(recipients)
    msg.set_content(body)
    for fname, mime, content in attachments:
        maintype, subtype = mime.split('/', 1)
        msg.add_attachment(content, maintype=maintype, subtype=subtype, filename=fname)

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
                if smtp_user:
                    server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                if use_tls:
                    server.starttls()
                if smtp_user:
                    server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        return True, f'Sent to {len(recipients)} staff email(s).'
    except smtplib.SMTPAuthenticationError as e:
        detail = ''
        try:
            detail = (e.smtp_error or b'').decode(errors='ignore').strip()
        except Exception:
            detail = str(e)
        return False, f'SMTP login failed. {detail}'
    except Exception as e:
        return False, f'SMTP send failed: {e}'

def _build_template_attachment(dataset, fmt):
    fmt = (fmt or 'excel').lower()
    df = None

    if dataset == 'clients':
        df = pd.DataFrame(columns=CLIENT_SCHEMA)
    elif dataset == 'dispatch':
        df = pd.DataFrame(columns=DISPATCH_SCHEMA)
    elif dataset == 'pending_bills':
        df = pd.DataFrame(columns=PENDING_BILL_SCHEMA)
    elif dataset == 'client_full':
        if fmt == 'csv':
            output = io.BytesIO()
            with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.writestr('clients.csv', pd.DataFrame(columns=CLIENT_SCHEMA).to_csv(index=False))
                zf.writestr('bookings.csv', pd.DataFrame(columns=BOOKING_SCHEMA).to_csv(index=False))
                zf.writestr('booking_items.csv', pd.DataFrame(columns=BOOKING_ITEM_SCHEMA).to_csv(index=False))
                zf.writestr('dispatch.csv', pd.DataFrame(columns=DISPATCH_SCHEMA).to_csv(index=False))
                zf.writestr('payments.csv', pd.DataFrame(columns=PAYMENT_SCHEMA).to_csv(index=False))
                zf.writestr('sales.csv', pd.DataFrame(columns=SALE_SCHEMA).to_csv(index=False))
                zf.writestr('sale_items.csv', pd.DataFrame(columns=SALE_ITEM_SCHEMA).to_csv(index=False))
                zf.writestr('pending_bills.csv', pd.DataFrame(columns=PENDING_BILL_SCHEMA).to_csv(index=False))
            return "template_client_full_csv.zip", 'application/zip', output.getvalue()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            pd.DataFrame(columns=CLIENT_SCHEMA).to_excel(writer, sheet_name='Clients', index=False)
            pd.DataFrame(columns=BOOKING_SCHEMA).to_excel(writer, sheet_name='Bookings', index=False)
            pd.DataFrame(columns=BOOKING_ITEM_SCHEMA).to_excel(writer, sheet_name='BookingItems', index=False)
            pd.DataFrame(columns=DISPATCH_SCHEMA).to_excel(writer, sheet_name='Dispatch', index=False)
            pd.DataFrame(columns=PAYMENT_SCHEMA).to_excel(writer, sheet_name='Payments', index=False)
            pd.DataFrame(columns=SALE_SCHEMA).to_excel(writer, sheet_name='Sales', index=False)
            pd.DataFrame(columns=SALE_ITEM_SCHEMA).to_excel(writer, sheet_name='SaleItems', index=False)
            pd.DataFrame(columns=PENDING_BILL_SCHEMA).to_excel(writer, sheet_name='PendingBills', index=False)
        return "template_client_full.xlsx", 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', output.getvalue()
    else:
        return None, None, None

    if fmt == 'csv':
        return f"template_{dataset}.csv", 'text/csv', df.to_csv(index=False).encode('utf-8')

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return f"template_{dataset}.xlsx", 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', output.getvalue()

def _build_master_export_bytes(scope_ctx=None):
    if scope_ctx is None:
        scope_ctx = _default_scope_context()
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        clients = _scoped_model_query(Client, scope_ctx).order_by(Client.code.asc()).all()
        client_data = [{k: getattr(x, k) for k in CLIENT_SCHEMA if hasattr(x, k)} for x in clients]
        for d, c in zip(client_data, clients):
            d['status'] = 'ACTIVE' if c.is_active else 'INACTIVE'
        pd.DataFrame(client_data or [], columns=CLIENT_SCHEMA).to_excel(writer, sheet_name='Clients', index=False)

        bills = _scoped_model_query(PendingBill, scope_ctx).all()
        bill_data = [{
            'client_code': x.client_code, 'bill_no': x.bill_no, 'name': x.client_name,
            'amount': x.amount, 'reason': x.reason, 'nimbus': x.nimbus_no
        } for x in bills]
        pd.DataFrame(bill_data or [], columns=PENDING_BILL_SCHEMA).to_excel(writer, sheet_name='PendingBills', index=False)

        # Materials
        materials = _scoped_model_query(Material, scope_ctx).outerjoin(MaterialCategory).order_by(Material.code.asc()).all()
        material_data = [{
            'code': m.code,
            'name': m.name,
            'category_name': m.category.name if m.category else '',
            'unit_price': m.unit_price,
            'total': m.total,
            'unit': m.unit
        } for m in materials]
        pd.DataFrame(material_data or [], columns=['code', 'name', 'category_name', 'unit_price', 'total', 'unit']).to_excel(writer, sheet_name='Materials', index=False)

        # Material Categories
        categories = _scoped_model_query(MaterialCategory, scope_ctx).all()
        category_data = [{
            'id': c.id,
            'name': c.name,
            'is_active': c.is_active
        } for c in categories]
        pd.DataFrame(category_data or [], columns=['id', 'name', 'is_active']).to_excel(writer, sheet_name='MaterialCategories', index=False)

        entries = _scoped_model_query(Entry, scope_ctx).filter_by(type='OUT').all()
        dispatch_data = []
        for x in entries:
            match_status = "UNMATCHED"
            pending_match = _scoped_model_query(PendingBill, scope_ctx).filter_by(
                bill_no=x.bill_no,
                client_code=x.client_code
            ).first()
            if x.bill_no and pending_match:
                match_status = "MATCHED"
            dispatch_data.append({
                'CLIENT_CODE': x.client_code, 'CLIENT_NAME': x.client, 'CLIENT_CATEGORY': x.client_category,
                'TRANSACTION_CATEGORY': 'CEMENT+BILL' if x.bill_no else 'CEMENT', 'BILL_NO': x.bill_no,
                'BILL_DATE': x.date, 'CEMENT_BRAND': x.material, 'QTY': x.qty, 'NIMBUS': x.nimbus_no,
                'NOTES': '', 'SOURCE': 'CEMENT', 'MATCH_STATUS': match_status
            })
        pd.DataFrame(dispatch_data or [], columns=DISPATCH_SCHEMA).to_excel(writer, sheet_name='Dispatch', index=False)

        bookings = _scoped_model_query(Booking, scope_ctx).filter(Booking.is_void == False).all()
        booking_data = [{
            'client_name': b.client_name, 'manual_bill_no': b.manual_bill_no, 'amount': b.amount,
            'paid_amount': b.paid_amount, 'date_posted': b.date_posted, 'note': b.note
        } for b in bookings]
        pd.DataFrame(booking_data or [], columns=BOOKING_SCHEMA).to_excel(writer, sheet_name='Bookings', index=False)

        booking_items = []
        for b in bookings:
            for i in b.items:
                booking_items.append({
                    'booking_bill_no': b.manual_bill_no, 'booking_client_name': b.client_name,
                    'material_name': i.material_name, 'qty': i.qty, 'price_at_time': i.price_at_time
                })
        pd.DataFrame(booking_items or [], columns=BOOKING_ITEM_SCHEMA).to_excel(writer, sheet_name='BookingItems', index=False)

        payments = _scoped_model_query(Payment, scope_ctx).filter(Payment.is_void == False).all()
        payment_data = [{
            'client_name': p.client_name, 'manual_bill_no': p.manual_bill_no, 'amount': p.amount,
            'method': p.method, 'date_posted': p.date_posted, 'note': p.note
        } for p in payments]
        pd.DataFrame(payment_data or [], columns=PAYMENT_SCHEMA).to_excel(writer, sheet_name='Payments', index=False)

        sales = _scoped_model_query(DirectSale, scope_ctx).filter(DirectSale.is_void == False).all()
        sale_data = [{
            'client_name': s.client_name, 'manual_bill_no': s.manual_bill_no, 'auto_bill_no': s.auto_bill_no,
            'category': s.category, 'amount': s.amount, 'paid_amount': s.paid_amount,
            'date_posted': s.date_posted, 'note': s.note
        } for s in sales]
        pd.DataFrame(sale_data or [], columns=SALE_SCHEMA).to_excel(writer, sheet_name='Sales', index=False)

        sale_items = []
        for s in sales:
            bill_ref = s.manual_bill_no or s.auto_bill_no
            for i in s.items:
                sale_items.append({
                    'sale_bill_no': bill_ref, 'sale_client_name': s.client_name,
                    'product_name': i.product_name, 'qty': i.qty, 'price_at_time': i.price_at_time
                })
        pd.DataFrame(sale_items or [], columns=SALE_ITEM_SCHEMA).to_excel(writer, sheet_name='SaleItems', index=False)

        grns = _scoped_model_query(GRN, scope_ctx).filter(GRN.is_void == False).all()
        grn_data = [{
            'supplier': g.supplier,
            'manual_bill_no': g.manual_bill_no,
            'auto_bill_no': g.auto_bill_no,
            'date_posted': g.date_posted,
            'note': g.note
        } for g in grns]
        pd.DataFrame(grn_data or [], columns=['supplier', 'manual_bill_no', 'auto_bill_no', 'date_posted', 'note']).to_excel(writer, sheet_name='GRN', index=False)

        grn_items = []
        for g in grns:
            for i in g.items:
                grn_items.append({
                    'GRN Manual Bill': g.manual_bill_no,
                    'GRN Auto Bill': g.auto_bill_no,
                    'Material Name': i.mat_name,
                    'Quantity': i.qty,
                    'Rate': i.price_at_time
                })
        pd.DataFrame(grn_items or [], columns=['GRN Manual Bill', 'GRN Auto Bill', 'Material Name', 'Quantity', 'Rate']).to_excel(writer, sheet_name='GRNItems', index=False)

        delivery_persons = _scoped_model_query(DeliveryPerson, scope_ctx).order_by(DeliveryPerson.name.asc()).all()
        delivery_person_data = [{
            'name': d.name,
            'phone': d.phone,
            'is_active': d.is_active,
            'created_at': d.created_at
        } for d in delivery_persons]
        pd.DataFrame(
            delivery_person_data or [],
            columns=['name', 'phone', 'is_active', 'created_at']
        ).to_excel(writer, sheet_name='DeliveryPersons', index=False)

        delivery_rents = _scoped_model_query(DeliveryRent, scope_ctx).all()
        rent_data = [{
            'sale_id': r.sale_id,
            'delivery_person_name': r.delivery_person_name,
            'bill_no': r.bill_no,
            'amount': r.amount,
            'note': r.note,
            'date_posted': r.date_posted,
            'created_by': r.created_by,
            'is_void': r.is_void
        } for r in delivery_rents]
        pd.DataFrame(
            rent_data or [],
            columns=['sale_id', 'delivery_person_name', 'bill_no', 'amount', 'note', 'date_posted', 'created_by', 'is_void']
        ).to_excel(writer, sheet_name='DeliveryRents', index=False)

    return output.getvalue()


def _selected_master_sheets(section_keys):
    selected = []
    seen = set()
    for key in section_keys or []:
        for sheet in MASTER_SHEET_SECTIONS.get(key, []):
            if sheet not in seen:
                seen.add(sheet)
                selected.append(sheet)
    return selected


def _filter_excel_bytes_to_sheets(file_bytes, allowed_sheets):
    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    available = [s for s in xls.sheet_names if s in set(allowed_sheets or [])]
    if not available:
        raise ValueError('No selected section sheets were found in the uploaded file.')
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        for sheet in available:
            pd.read_excel(xls, sheet).to_excel(writer, sheet_name=sheet[:31], index=False)
    return out.getvalue()


def _full_raw_import_enabled():
    return str(
        os.environ.get('FULL_RAW_IMPORT_ENABLED', current_app.config.get('FULL_RAW_IMPORT_ENABLED', '0'))
    ).strip().lower() in ['1', 'true', 'on', 'yes']


def _normalize_excel_cell(value, col=None):
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return value
    if col is not None and isinstance(value, str):
        s = value.strip()
        if not s:
            return value
        # Normalize UTC "Z" suffix for fromisoformat.
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        if isinstance(col.type, DateTime):
            try:
                return datetime.fromisoformat(s)
            except Exception:
                return value
        if isinstance(col.type, Date):
            try:
                if 'T' in s:
                    s = s.split('T', 1)[0]
                elif ' ' in s:
                    s = s.split(' ', 1)[0]
                return date.fromisoformat(s)
            except Exception:
                return value
    return value


def _serialize_payload(payload):
    out = {}
    for k, v in payload.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _build_report_label(payload, pk_names):
    preferred_keys = [
        'name', 'code', 'username', 'action', 'details', 'client_name', 'client',
        'bill_no', 'manual_bill_no', 'auto_bill_no', 'nimbus_no', 'material',
        'mat_name', 'supplier', 'product_name', 'category', 'phone'
    ]
    parts = []
    for k in preferred_keys:
        if k in payload and k not in pk_names:
            v = payload.get(k)
            if v not in [None, '']:
                parts.append(f"{k}={v}")
        if len(parts) >= 3:
            break
    if not parts:
        for k in sorted(payload.keys()):
            if k in pk_names:
                continue
            v = payload.get(k)
            if v not in [None, '']:
                parts.append(f"{k}={v}")
            if len(parts) >= 3:
                break
    return "; ".join(parts)


def _deploy_paths():
    home = os.path.expanduser('~')
    base_dir = current_app.config.get('DEPLOY_BASE_DIR', os.path.join(home, 'releases'))
    current_path = current_app.config.get('DEPLOY_CURRENT_PATH', os.path.join(home, 'app_current'))
    wsgi_reload_path = current_app.config.get('WSGI_RELOAD_PATH')
    history_path = current_app.config.get(
        'DEPLOY_HISTORY_PATH',
        os.path.join(current_app.instance_path, 'deploy_history.json')
    )
    return base_dir, current_path, wsgi_reload_path, history_path


def _get_data_upgrade_queue_dir():
    queue_dir = current_app.config.get(
        'DATA_UPGRADE_QUEUE_DIR',
        os.path.join(current_app.instance_path, 'data_upgrade_queue')
    )
    os.makedirs(queue_dir, exist_ok=True)
    processed_dir = os.path.join(queue_dir, 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    return queue_dir, processed_dir


def _list_data_upgrade_excels(folder):
    files = []
    if not folder or not os.path.isdir(folder):
        return files
    for root, _, names in os.walk(folder):
        for name in names:
            low = name.lower()
            if low.endswith('.xlsx') or low.endswith('.xls'):
                files.append(os.path.join(root, name))
    files.sort()
    return files


def _safe_extract_zip(zip_path, extract_dir, progress_cb=None, percent_start=35, percent_end=52):
    with zipfile.ZipFile(zip_path, 'r') as zf:
        infos = zf.infolist()
        for info in infos:
            name = info.filename
            if name.startswith('/') or name.startswith('\\') or '..' in name.replace('\\', '/').split('/'):
                raise ValueError("Unsafe path in zip file.")

        file_infos = [i for i in infos if not i.is_dir()]
        total = len(file_infos) or 1
        span = max(1, int(percent_end) - int(percent_start))

        for i, info in enumerate(file_infos, start=1):
            zf.extract(info, extract_dir)
            if progress_cb:
                pct = int(percent_start) + int((i / total) * span)
                display_name = os.path.basename(info.filename.rstrip('/\\')) or info.filename
                progress_cb(pct, f"Copying file {i}/{len(file_infos)}: {display_name}")

        if not file_infos:
            zf.extractall(extract_dir)
            if progress_cb:
                progress_cb(percent_end, "ZIP contains no regular files.")


def _flatten_single_root_folder(release_dir):
    entries = [e for e in os.listdir(release_dir) if e not in ['__MACOSX']]
    if len(entries) != 1:
        return
    inner = os.path.join(release_dir, entries[0])
    if not os.path.isdir(inner):
        return
    for name in os.listdir(inner):
        shutil.move(os.path.join(inner, name), os.path.join(release_dir, name))
    shutil.rmtree(inner, ignore_errors=True)


def _hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _hash_bytes(blob):
    h = hashlib.sha256()
    h.update(blob or b'')
    return h.hexdigest()


def _scan_release_zip(zip_path):
    forbidden_prefixes = [
        '.git/', 'instance/', '__pycache__/', '.local/', '.virtualenvs/', '.venv/', 'venv/',
    ]
    forbidden_suffixes = [
        '.db', '.sqlite', '.sqlite3', '.log', '.pyc', '.pyo',
    ]
    forbidden_exact = {
        'errorlog.txt',
    }
    allowed_data_suffixes = ('.xlsx', '.xls')
    blocked = []
    payload_files = []
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for info in zf.infolist():
            name = (info.filename or '').replace('\\', '/').lstrip('./')
            if not name or info.is_dir():
                continue
            low = name.lower()
            if low in forbidden_exact:
                blocked.append(name)
                continue
            if any(low.startswith(p) for p in forbidden_prefixes):
                blocked.append(name)
                continue
            if any(low.endswith(s) for s in forbidden_suffixes):
                blocked.append(name)
                continue
            if low.startswith('data_upgrade/') and low.endswith(allowed_data_suffixes):
                payload_files.append(name)
    return {
        'blocked': sorted(set(blocked)),
        'payload_files': sorted(set(payload_files)),
    }


def _validate_release_zip(zip_path, require_data_upgrade=False):
    scan = _scan_release_zip(zip_path)
    blocked = scan.get('blocked') or []
    if blocked:
        preview = ", ".join(blocked[:8])
        more = f" (+{len(blocked) - 8} more)" if len(blocked) > 8 else ""
        raise ValueError(
            "Release ZIP contains forbidden files/folders "
            f"(e.g., {preview}{more}). Remove DB/log/git/instance/cache artifacts and retry."
        )
    if require_data_upgrade and not (scan.get('payload_files') or []):
        raise ValueError(
            "Release ZIP has no /data_upgrade/*.xlsx payload. "
            "Either include payload files or disable 'require data payload'."
        )
    return scan


def _load_deploy_history(history_path):
    if not os.path.exists(history_path):
        return []
    try:
        with open(history_path, 'r', encoding='utf-8') as f:
            return json.load(f) or []
    except Exception:
        return []


def _save_deploy_history(history_path, items):
    os.makedirs(os.path.dirname(history_path), exist_ok=True)
    with open(history_path, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=True, indent=2)


def _append_deploy_history(history_path, entry):
    items = _load_deploy_history(history_path)
    items.insert(0, entry)
    _save_deploy_history(history_path, items)


def _set_deploy_progress(job_id, **fields):
    with _DEPLOY_PROGRESS_LOCK:
        state = _DEPLOY_PROGRESS.get(job_id, {})
        state.update(fields)
        _DEPLOY_PROGRESS[job_id] = state


def _get_deploy_progress(job_id):
    with _DEPLOY_PROGRESS_LOCK:
        state = _DEPLOY_PROGRESS.get(job_id)
        return dict(state) if state else None


def _set_master_import_progress(job_id, **fields):
    with _MASTER_IMPORT_PROGRESS_LOCK:
        state = _MASTER_IMPORT_PROGRESS.get(job_id, {})
        state.update(fields)
        _MASTER_IMPORT_PROGRESS[job_id] = state


def _get_master_import_progress(job_id):
    with _MASTER_IMPORT_PROGRESS_LOCK:
        state = _MASTER_IMPORT_PROGRESS.get(job_id)
        return dict(state) if state else None


def _run_app_migrate_steps():
    from main import _bootstrap_database
    _bootstrap_database()
    return _run_sql_migrations()


def _get_sqlite_db_path():
    uri = str(current_app.config.get('SQLALCHEMY_DATABASE_URI', '') or '')
    prefix = 'sqlite:///'
    if uri.startswith(prefix):
        return uri[len(prefix):]
    return None


def _snapshot_sqlite_db(stamp, backup_dir=None):
    db_path = _get_sqlite_db_path()
    if not db_path or not os.path.exists(db_path):
        return None
    backup_dir = backup_dir or os.path.join(current_app.instance_path, 'deploy_db_backups')
    os.makedirs(backup_dir, exist_ok=True)
    snap_db = os.path.join(backup_dir, f"db_before_migrate_{stamp}.db")
    shutil.copy2(db_path, snap_db)

    snap = {'src_db': db_path, 'snap_db': snap_db}
    for suffix in ['-wal', '-shm']:
        src_sidecar = db_path + suffix
        if os.path.exists(src_sidecar):
            snap_sidecar = snap_db + suffix
            shutil.copy2(src_sidecar, snap_sidecar)
            snap[f'snap_{suffix[1:]}'] = snap_sidecar
            snap[f'src_{suffix[1:]}'] = src_sidecar
    return snap


def _restore_sqlite_snapshot(snap):
    if not snap:
        return
    src_db = snap.get('src_db')
    snap_db = snap.get('snap_db')
    if src_db and snap_db and os.path.exists(snap_db):
        shutil.copy2(snap_db, src_db)
    for suffix in ['wal', 'shm']:
        src_sidecar = snap.get(f'src_{suffix}')
        snap_sidecar = snap.get(f'snap_{suffix}')
        if src_sidecar and snap_sidecar and os.path.exists(snap_sidecar):
            shutil.copy2(snap_sidecar, src_sidecar)


def _capture_guard_counts():
    return {
        'tenant': int(Tenant.query.count()),
        'user': int(User.query.count()),
        'client': int(Client.query.count()),
        'material': int(Material.query.count()),
        'entry': int(Entry.query.count()),
        'booking': int(Booking.query.count()),
        'payment': int(Payment.query.count()),
        'direct_sale': int(DirectSale.query.count()),
    }


def _run_post_upgrade_integrity_checks(before_counts):
    row = db.session.execute(text("PRAGMA integrity_check")).fetchone()
    if not row:
        raise ValueError("Integrity check failed: PRAGMA integrity_check returned no result.")
    integrity_text = str(row[0]).strip().lower()
    if integrity_text != 'ok':
        raise ValueError(f"Integrity check failed: {row[0]}")

    fk_rows = db.session.execute(text("PRAGMA foreign_key_check")).fetchall()
    if fk_rows:
        raise ValueError(f"Foreign key check failed with {len(fk_rows)} violation(s).")

    after = _capture_guard_counts()
    must_not_be_zero = ['tenant', 'user']
    for key in must_not_be_zero:
        if after.get(key, 0) <= 0:
            raise ValueError(f"Integrity guard failed: '{key}' table is empty after upgrade.")

    for key, before_val in (before_counts or {}).items():
        if before_val > 0 and after.get(key, 0) == 0:
            raise ValueError(
                f"Integrity guard failed: '{key}' count dropped from {before_val} to 0."
            )

    return {
        'before': before_counts or {},
        'after': after,
        'integrity_check': row[0],
        'foreign_key_violations': len(fk_rows),
    }


def _ensure_data_upgrade_ledger():
    db.session.execute(text(
        "CREATE TABLE IF NOT EXISTS data_upgrade_applied ("
        "file_sha256 TEXT PRIMARY KEY, "
        "file_name TEXT, "
        "source TEXT, "
        "applied_at TEXT, "
        "report_json TEXT"
        ")"
    ))
    db.session.commit()


def _is_data_upgrade_applied(file_sha256):
    q = text("SELECT 1 FROM data_upgrade_applied WHERE file_sha256 = :h LIMIT 1")
    row = db.session.execute(q, {'h': file_sha256}).fetchone()
    return row is not None


def _mark_data_upgrade_applied(file_sha256, file_name, source, report):
    db.session.execute(
        text(
            "INSERT OR IGNORE INTO data_upgrade_applied "
            "(file_sha256, file_name, source, applied_at, report_json) "
            "VALUES (:h, :n, :s, :t, :r)"
        ),
        {
            'h': file_sha256,
            'n': file_name,
            's': source,
            't': pk_now().strftime('%Y-%m-%d %H:%M:%S'),
            'r': json.dumps(report or {}, ensure_ascii=True),
        },
    )
    db.session.commit()


def _deploy_release_bytes(
    file_bytes,
    run_migrate=False,
    require_data_upgrade=False,
    progress_cb=None,
    actor_username=None,
    job_id=None,
):
    base_dir, current_path, wsgi_reload_path, history_path = _deploy_paths()
    os.makedirs(base_dir, exist_ok=True)
    stamp = pk_now().strftime('%Y%m%d_%H%M%S')
    job_token = (job_id or uuid.uuid4().hex)[:8]
    job_root = os.path.join(current_app.instance_path, 'upgrade_jobs', f"{stamp}_{job_token}")
    incoming_dir = os.path.join(job_root, 'incoming')
    backup_dir = os.path.join(job_root, 'backup')
    report_dir = os.path.join(job_root, 'report')
    for folder in [incoming_dir, backup_dir, report_dir]:
        os.makedirs(folder, exist_ok=True)

    zip_name = f"app_release_{stamp}.zip"
    zip_path = os.path.join(incoming_dir, zip_name)

    release_dir = os.path.join(base_dir, stamp)
    os.makedirs(release_dir, exist_ok=True)

    switched = False
    previous_target = None
    db_snapshot = None
    zip_scan = {}
    integrity_report = None
    try:
        if progress_cb:
            progress_cb(6, 'Capturing baseline integrity state...')
        pre_counts = _capture_guard_counts()

        if progress_cb:
            progress_cb(12, 'Saving uploaded release...')
        with open(zip_path, 'wb') as f:
            f.write(file_bytes)

        if progress_cb:
            progress_cb(18, 'Validating release ZIP...')
        zip_scan = _validate_release_zip(zip_path, require_data_upgrade=require_data_upgrade)

        if progress_cb:
            progress_cb(24, 'Creating automatic backups...')
        db_snapshot = _snapshot_sqlite_db(stamp, backup_dir=backup_dir)
        if not db_snapshot:
            raise ValueError("Failed to create pre-upgrade DB snapshot.")
        backup_name = _create_full_raw_backup(backup_dir=backup_dir)

        if progress_cb:
            progress_cb(34, 'Extracting release files...')
        _safe_extract_zip(zip_path, release_dir, progress_cb=progress_cb, percent_start=34, percent_end=50)

        if progress_cb:
            progress_cb(50, 'Normalizing release structure...')
        _flatten_single_root_folder(release_dir)
        if not os.path.exists(os.path.join(release_dir, 'main.py')):
            raise ValueError("Release missing main.py at root.")

        sql_report = None
        if run_migrate:
            if progress_cb:
                progress_cb(62, 'Running migration...')
            sql_report = _run_app_migrate_steps()

        # Data-upgrade payload sources:
        # 1) release ZIP /data_upgrade/*.xlsx
        # 2) persistent queue folder in instance path
        data_upgrade_reports = []
        queue_dir, processed_dir = _get_data_upgrade_queue_dir()
        release_data_upgrade_dir = os.path.join(release_dir, 'data_upgrade')
        release_candidates = _list_data_upgrade_excels(release_data_upgrade_dir)
        queue_candidates = _list_data_upgrade_excels(queue_dir)
        # Never recurse processed history as pending queue.
        queue_candidates = [p for p in queue_candidates if not p.startswith(processed_dir + os.sep)]
        candidates = [('release', p) for p in release_candidates] + [('queue', p) for p in queue_candidates]
        if require_data_upgrade and not candidates:
            raise ValueError(
                "No data upgrade payload found in release ZIP or queue folder while 'require data payload' is enabled."
            )

        _ensure_data_upgrade_ledger()
        queued_processed = []

        if candidates:
            total_data = len(candidates)
            for i, (source, data_file) in enumerate(candidates, start=1):
                if progress_cb:
                    pct = 68 + int((i / total_data) * 12)
                    progress_cb(
                        pct,
                        f"Applying data upgrade {i}/{total_data} ({source}): {os.path.basename(data_file)}"
                    )
                with open(data_file, 'rb') as f:
                    data_blob = f.read()
                data_hash = _hash_bytes(data_blob)
                if _is_data_upgrade_applied(data_hash):
                    data_upgrade_reports.append({
                        'file': os.path.basename(data_file),
                        'source': source,
                        'sha256': data_hash,
                        'report': {'skipped': 1, 'reason': 'already_applied'},
                    })
                    if source == 'queue':
                        queued_processed.append(data_file)
                    continue
                report = _run_master_import_bytes(
                    file_bytes=data_blob,
                    actor_username=actor_username or 'system',
                    progress_cb=None,
                )
                _mark_data_upgrade_applied(
                    file_sha256=data_hash,
                    file_name=os.path.basename(data_file),
                    source=source,
                    report=report,
                )
                data_upgrade_reports.append({
                    'file': os.path.basename(data_file),
                    'source': source,
                    'sha256': data_hash,
                    'report': report,
                })
                if source == 'queue':
                    queued_processed.append(data_file)

        if progress_cb:
            progress_cb(82, 'Running integrity verification...')
        integrity_report = _run_post_upgrade_integrity_checks(pre_counts)

        if progress_cb:
            progress_cb(88, 'Switching current release...')
        if os.path.islink(current_path):
            previous_target = os.readlink(current_path)
        if os.path.islink(current_path) or not os.path.exists(current_path):
            if os.path.islink(current_path):
                os.unlink(current_path)
            os.symlink(release_dir, current_path)
            switched = True
        else:
            raise ValueError("Current path is not a symlink. Configure WSGI to use DEPLOY_CURRENT_PATH.")

        if progress_cb:
            progress_cb(95, 'Reloading app...')
        if wsgi_reload_path and os.path.exists(wsgi_reload_path):
            os.utime(wsgi_reload_path, None)
            reloaded = True
        else:
            reloaded = False

        # Archive queue files only after a fully successful deploy.
        if queued_processed:
            archived_at = pk_now().strftime('%Y%m%d_%H%M%S')
            for src in queued_processed:
                try:
                    target = os.path.join(processed_dir, f"{archived_at}_{os.path.basename(src)}")
                    shutil.move(src, target)
                except Exception:
                    pass

        entry = {
            'timestamp': stamp,
            'action': 'deploy',
            'zip_name': zip_name,
            'zip_sha256': _hash_file(zip_path),
            'release_dir': release_dir,
            'current_path': current_path,
            'reloaded': reloaded,
            'user': actor_username,
            'backup_name': backup_name,
            'run_migrate': bool(run_migrate),
            'sql_migrations': sql_report,
            'data_upgrade_reports': data_upgrade_reports,
            'require_data_upgrade': bool(require_data_upgrade),
            'zip_scan': {
                'payload_files': zip_scan.get('payload_files', []),
                'blocked_count': len(zip_scan.get('blocked', [])),
            },
            'integrity_report': integrity_report,
            'job_root': job_root,
        }
        _append_deploy_history(history_path, entry)
        return {
            'ok': True,
            'reloaded': reloaded,
            'sql_report': sql_report,
            'data_upgrade_reports': data_upgrade_reports,
            'integrity_report': integrity_report,
        }
    except Exception as e:
        if switched:
            try:
                if os.path.islink(current_path):
                    os.unlink(current_path)
                if previous_target:
                    os.symlink(previous_target, current_path)
            except Exception:
                pass
        restore_ok = False
        if db_snapshot:
            try:
                _restore_sqlite_snapshot(db_snapshot)
                restore_ok = True
            except Exception:
                restore_ok = False
        entry = {
            'timestamp': stamp,
            'action': 'deploy_failed',
            'zip_name': zip_name,
            'release_dir': release_dir,
            'error': str(e),
            'user': actor_username,
            'restored_snapshot': restore_ok,
            'require_data_upgrade': bool(require_data_upgrade),
            'job_root': job_root,
        }
        _append_deploy_history(history_path, entry)
        return {'ok': False, 'error': str(e)}


def _deploy_release_worker(
    flask_app,
    job_id,
    file_bytes,
    run_migrate,
    require_data_upgrade,
    username,
    tenant_id=None,
    role=None,
):
    with flask_app.app_context():
        with flask_app.test_request_context('/import_export/app_upgrade'):
            g.user = None
            g.is_root = (role == 'root')
            g.tenant_id = tenant_id
            g.enforce_tenant = (not g.is_root) and (tenant_id is not None)
            _set_import_actor_context(username=username, tenant_id=tenant_id, role=role)
            try:
                _set_deploy_progress(job_id, percent=2, message='Started upgrade...', done=False, success=False)
                result = _deploy_release_bytes(
                    file_bytes,
                    run_migrate=run_migrate,
                    require_data_upgrade=require_data_upgrade,
                    actor_username=username,
                    job_id=job_id,
                    progress_cb=lambda p, m: _set_deploy_progress(job_id, percent=p, message=m, done=False),
                )
                if result.get('ok'):
                    sql_report = result.get('sql_report')
                    data_reports = result.get('data_upgrade_reports') or []
                    if sql_report:
                        done_msg = f"Done. SQL applied: {sql_report.get('applied', 0)} / {sql_report.get('files', 0)}."
                    else:
                        done_msg = 'Done. Upgrade completed.'
                    if data_reports:
                        imported = sum((x.get('report') or {}).get('imported', 0) for x in data_reports)
                        updated = sum((x.get('report') or {}).get('updated', 0) for x in data_reports)
                        done_msg += f" Data upgrade files: {len(data_reports)} (Imported {imported}, Updated {updated})."
                    if result.get('reloaded'):
                        done_msg += " Reloaded automatically."
                    else:
                        done_msg += " Reload file not found; using in-browser refresh."
                    _set_deploy_progress(
                        job_id,
                        percent=100,
                        message=done_msg,
                        done=True,
                        success=True,
                        reloaded=result.get('reloaded'),
                        user=username,
                    )
                else:
                    _set_deploy_progress(
                        job_id,
                        percent=100,
                        message='Upgrade failed.',
                        done=True,
                        success=False,
                        error=result.get('error'),
                        user=username,
                    )
            finally:
                _clear_import_actor_context()


def _create_full_raw_backup(backup_dir=None):
    backup_dir = backup_dir or os.path.join(current_app.instance_path, 'full_raw_backups')
    os.makedirs(backup_dir, exist_ok=True)
    stamp = pk_now().strftime('%Y%m%d_%H%M%S')
    backup_name = f"full_raw_export_{stamp}.xlsx"
    backup_path = os.path.join(backup_dir, backup_name)
    data = _build_full_raw_export_bytes()
    with open(backup_path, 'wb') as f:
        f.write(data)
    return backup_name


def _get_migrations_dir():
    return current_app.config.get('MIGRATIONS_DIR', os.path.join(current_app.root_path, 'migrations'))


def _list_sql_migrations(migrations_dir):
    if not os.path.isdir(migrations_dir):
        return []
    files = [f for f in os.listdir(migrations_dir) if f.lower().endswith('.sql')]
    files.sort()
    return files


def _get_applied_migrations(conn):
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS migration_history (filename TEXT PRIMARY KEY, applied_at TEXT)"
    )
    cur.execute("SELECT filename FROM migration_history")
    rows = cur.fetchall()
    return {r[0] for r in rows}


def _run_sql_migrations():
    migrations_dir = _get_migrations_dir()
    files = _list_sql_migrations(migrations_dir)
    if not files:
        return {'applied': 0, 'skipped': 0, 'files': 0, 'dir': migrations_dir}

    conn = db.engine.raw_connection()
    try:
        allow_destructive = str(
            os.environ.get(
                'MIGRATIONS_ALLOW_DESTRUCTIVE',
                current_app.config.get('MIGRATIONS_ALLOW_DESTRUCTIVE', '0')
            )
        ).strip().lower() in ['1', 'true', 'on', 'yes']
        applied = _get_applied_migrations(conn)
        to_apply = [f for f in files if f not in applied]
        cur = conn.cursor()
        applied_count = 0
        for name in to_apply:
            path = os.path.join(migrations_dir, name)
            with open(path, 'r', encoding='utf-8') as f:
                sql = f.read()
            if not sql.strip():
                continue
            if not allow_destructive and re.search(
                r'\b(drop\s+table|truncate\s+table|delete\s+from)\b',
                sql,
                flags=re.IGNORECASE,
            ):
                raise ValueError(
                    f"Destructive SQL blocked in migration '{name}'. "
                    f"Set MIGRATIONS_ALLOW_DESTRUCTIVE=1 to allow it explicitly."
                )
            cur.executescript(sql)
            cur.execute(
                "INSERT OR IGNORE INTO migration_history (filename, applied_at) VALUES (?, ?)",
                (name, pk_now().strftime('%Y-%m-%d %H:%M:%S'))
            )
            applied_count += 1
        conn.commit()
        return {
            'applied': applied_count,
            'skipped': len(files) - applied_count,
            'files': len(files),
            'dir': migrations_dir,
        }
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _build_full_raw_export_bytes(scope_ctx=None):
    """Export physical tables with strict role/scope filtering."""
    if scope_ctx is None:
        scope_ctx = _default_scope_context()
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for table in _full_raw_tables_for_scope(scope_ctx):
            q = _scope_table_select(table, scope_ctx)
            if q is None:
                continue
            rows = db.session.execute(q).mappings().all()
            cols = [c.name for c in table.columns]
            data = []
            for r in rows:
                row_data = {}
                for col_name in cols:
                    cell = r.get(col_name)
                    if isinstance(cell, (datetime, date)):
                        row_data[col_name] = cell.isoformat()
                    else:
                        row_data[col_name] = cell
                data.append(row_data)
            # Excel sheet names max length = 31
            sheet_name = table.name[:31]
            pd.DataFrame(data or [], columns=cols).to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()


@import_export_bp.route('/full_raw_export')
@login_required
def full_raw_export():
    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.args.get('scope'),
            tenant_id_raw=request.args.get('tenant_id'),
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('import_export.import_export_page'))
    content = _build_full_raw_export_bytes(scope_ctx=scope_ctx)
    _archive_artifact_bytes(content, f"full_raw_export_{pk_now().strftime('%Y%m%d_%H%M%S')}.xlsx", kind='exports')
    output = io.BytesIO(content)
    output.seek(0)
    stamp = pk_now().strftime('%Y%m%d_%H%M%S')
    return send_file(
        output,
        as_attachment=True,
        download_name=f"full_raw_export_{stamp}.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@import_export_bp.route('/tenant_db_export')
@login_required
def tenant_db_export():
    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.args.get('scope'),
            tenant_id_raw=request.args.get('tenant_id'),
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('import_export.import_export_page'))

    if scope_ctx.get('scope') != 'tenant':
        flash('Tenant DB export requires tenant scope.', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    src_db_path = _sqlite_db_file_path()
    if not src_db_path or not os.path.exists(src_db_path):
        flash('DB file export is available only on SQLite file-based deployments.', 'danger')
        return redirect(url_for('settings'))

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    tmp_file.close()
    tmp_path = tmp_file.name
    try:
        shutil.copy2(src_db_path, tmp_path)
        tenant_id = scope_ctx.get('target_tenant_id')
        with sqlite3.connect(tmp_path) as conn:
            conn.execute('PRAGMA foreign_keys=OFF')
            for table in reversed(list(db.metadata.sorted_tables)):
                tname = table.name
                if tname == 'tenant':
                    conn.execute("DELETE FROM tenant WHERE id <> ?", (tenant_id,))
                    continue
                if 'tenant_id' in table.c:
                    conn.execute(
                        f"DELETE FROM {tname} WHERE tenant_id <> ? OR tenant_id IS NULL",
                        (tenant_id,)
                    )
                else:
                    conn.execute(f"DELETE FROM {tname}")
            conn.commit()
        with open(tmp_path, 'rb') as f:
            content = f.read()
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    stamp = pk_now().strftime('%Y%m%d_%H%M%S')
    fname = f"tenant_backup_{_safe_name(scope_ctx.get('target_tenant_name') or tenant_id, 'tenant')}_{stamp}.db"
    _archive_artifact_bytes(content, fname, kind='exports')
    output = io.BytesIO(content)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=fname, mimetype='application/octet-stream')


@import_export_bp.route('/tenant_db_restore', methods=['POST'])
@login_required
def tenant_db_restore():
    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.form.get('scope'),
            tenant_id_raw=request.form.get('tenant_id'),
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('settings'))

    if scope_ctx.get('scope') != 'tenant':
        flash('Tenant DB restore requires tenant scope.', 'danger')
        return redirect(url_for('settings'))

    file = request.files.get('file')
    if not file:
        flash('No DB backup file uploaded.', 'danger')
        return redirect(url_for('settings'))
    if not str(file.filename or '').lower().endswith('.db'):
        flash('Please upload a .db backup file.', 'danger')
        return redirect(url_for('settings'))

    src_db_path = _sqlite_db_file_path()
    if not src_db_path or not os.path.exists(src_db_path):
        flash('DB file restore is available only on SQLite file-based deployments.', 'danger')
        return redirect(url_for('settings'))

    tenant_id = scope_ctx.get('target_tenant_id')
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    tmp_file.close()
    tmp_path = tmp_file.name
    try:
        file_bytes = file.read()
        with open(tmp_path, 'wb') as f:
            f.write(file_bytes)
        _archive_artifact_bytes(file_bytes, f"tenant_db_restore_{file.filename}", kind='imports')

        with sqlite3.connect(tmp_path) as src_conn:
            src_conn.row_factory = sqlite3.Row
            src_tables = {
                r[0] for r in src_conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }

            tenant_tables = [t for t in db.metadata.sorted_tables if 'tenant_id' in t.c]
            restore_stats = {
                'inserted': 0,
                'updated': 0,
                'skipped_tenant_mismatch': 0,
                'skipped_null_tenant': 0,
                'skipped_root_user': 0,
            }
            # Replace target tenant dataset while preserving current admin login row.
            for t in reversed(tenant_tables):
                if (
                    getattr(current_user, 'role', None) != 'root'
                    and t.name == 'user'
                    and getattr(current_user, 'username', None)
                ):
                    db.session.execute(
                        t.delete().where(
                            and_(t.c.tenant_id == tenant_id, t.c.username != current_user.username)
                        )
                    )
                else:
                    db.session.execute(t.delete().where(t.c.tenant_id == tenant_id))

            for t in tenant_tables:
                if t.name not in src_tables:
                    continue
                src_rows = src_conn.execute(f"SELECT * FROM {t.name}").fetchall()
                for src_row in src_rows:
                    if 'tenant_id' in src_row.keys():
                        src_tid = src_row['tenant_id']
                        if src_tid is None:
                            restore_stats['skipped_null_tenant'] += 1
                            continue
                        if str(src_tid) != str(tenant_id):
                            restore_stats['skipped_tenant_mismatch'] += 1
                            continue
                    payload = {}
                    for col in t.columns:
                        cname = col.name
                        if cname not in src_row.keys():
                            continue
                        val = src_row[cname]
                        if cname == 'tenant_id':
                            val = tenant_id
                        val = _normalize_sqlite_value_for_column(val, col)
                        payload[cname] = val
                    if not payload:
                        continue
                    if t.name == 'user':
                        uname = str(payload.get('username') or '').strip()
                        if uname.lower() == 'root':
                            restore_stats['skipped_root_user'] += 1
                            continue
                        payload.pop('id', None)
                        if (
                            getattr(current_user, 'role', None) != 'root'
                            and uname == getattr(current_user, 'username', None)
                        ):
                            payload.pop('password_hash', None)
                            payload.pop('role', None)
                            payload.pop('status', None)
                        existing_user = db.session.execute(
                            select(t).where(and_(t.c.tenant_id == tenant_id, t.c.username == uname)).limit(1)
                        ).first()
                        if existing_user:
                            db.session.execute(
                                t.update().where(and_(t.c.tenant_id == tenant_id, t.c.username == uname)).values(**payload)
                            )
                            restore_stats['updated'] += 1
                            continue
                    db.session.execute(t.insert().values(**payload))
                    restore_stats['inserted'] += 1

        db.session.commit()
        flash(
            'Tenant DB restore completed successfully. '
            f"Inserted: {restore_stats['inserted']}, Updated: {restore_stats['updated']}, "
            f"Skipped(other-tenant): {restore_stats['skipped_tenant_mismatch']}, "
            f"Skipped(null-tenant): {restore_stats['skipped_null_tenant']}, "
            f"Skipped(root-user): {restore_stats['skipped_root_user']}.",
            'success'
        )
    except Exception as e:
        db.session.rollback()
        flash(f'Tenant DB restore failed: {e}', 'danger')
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
    return redirect(url_for('settings'))


def _run_full_raw_import_bytes(file_bytes, scope_ctx, mode, source_file_name):
    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f'Invalid Excel file: {e}')

    scoped_tables = _full_raw_tables_for_scope(scope_ctx)
    selected_tables = []
    for t in scoped_tables:
        if t.name[:31] in xls.sheet_names:
            selected_tables.append(t)

    if not selected_tables:
        raise ValueError('No importable sheets found for current scope.')

    report = {'inserted': 0, 'skipped': 0, 'tables': len(selected_tables)}
    report_name = None
    skipped_rows = []
    target_tenant_id = scope_ctx.get('target_tenant_id')

    if mode == 'replace_tenant_data':
        for t in reversed(selected_tables):
            if scope_ctx.get('scope') != 'tenant' or 'tenant_id' not in t.c:
                continue
            if (
                getattr(current_user, 'role', None) != 'root'
                and t.name == 'user'
                and getattr(current_user, 'username', None)
            ):
                db.session.execute(
                    t.delete().where(
                        and_(
                            t.c.tenant_id == target_tenant_id,
                            t.c.username != current_user.username
                        )
                    )
                )
            else:
                db.session.execute(t.delete().where(t.c.tenant_id == target_tenant_id))

    for t in selected_tables:
        df = pd.read_excel(xls, t.name[:31]).fillna('')
        pk_cols = [c for c in t.primary_key.columns]
        pk_names = {c.name for c in pk_cols}
        for _, src in df.iterrows():
            payload = {}
            for col in t.columns:
                name = col.name
                if name not in df.columns:
                    continue
                val = _normalize_excel_cell(src.get(name), col)
                if val == '':
                    val = None
                if name == 'tenant_id' and scope_ctx.get('scope') == 'tenant':
                    val = target_tenant_id
                if col.primary_key and val in [None, '']:
                    continue
                payload[name] = val
            if not payload:
                report['skipped'] += 1
                skipped_rows.append({
                    'table': t.name,
                    'reason': 'empty_payload',
                    'pk': '',
                    'label': '',
                    'row_json': '',
                })
                continue
            if t.name == 'user' and scope_ctx.get('scope') == 'tenant':
                username_value = str(payload.get('username') or '').strip()
                if not username_value:
                    report['skipped'] += 1
                    skipped_rows.append({
                        'table': t.name,
                        'reason': 'missing_username',
                        'pk': '',
                        'label': '',
                        'row_json': json.dumps(_serialize_payload(payload), ensure_ascii=True),
                    })
                    continue
                if (
                    getattr(current_user, 'role', None) != 'root'
                    and username_value == getattr(current_user, 'username', None)
                ):
                    payload.pop('password_hash', None)
                    payload.pop('role', None)
                    payload.pop('status', None)
                payload.pop('id', None)
                existing_user = db.session.execute(
                    select(t).where(
                        and_(t.c.tenant_id == target_tenant_id, t.c.username == username_value)
                    ).limit(1)
                ).first()
                if existing_user:
                    db.session.execute(
                        t.update().where(
                            and_(t.c.tenant_id == target_tenant_id, t.c.username == username_value)
                        ).values(**payload)
                    )
                    report['inserted'] += 1
                    continue
            if pk_cols:
                pk_values = []
                missing_pk = False
                for c in pk_cols:
                    if c.name not in payload or payload[c.name] in [None, '']:
                        missing_pk = True
                        break
                    pk_values.append(payload[c.name])
                if not missing_pk:
                    pk_cond = and_(*[c == v for c, v in zip(pk_cols, pk_values)])
                    existing = db.session.execute(select(t).where(pk_cond).limit(1)).first()
                    if existing:
                        report['skipped'] += 1
                        label = _build_report_label(payload, pk_names)
                        row_json = json.dumps(_serialize_payload(payload), ensure_ascii=True)
                        skipped_rows.append({
                            'table': t.name,
                            'reason': 'duplicate_pk',
                            'pk': ','.join([str(v) for v in pk_values]),
                            'label': label,
                            'row_json': row_json,
                        })
                        continue
            db.session.execute(t.insert().values(**payload))
            report['inserted'] += 1

    db.session.commit()
    if skipped_rows:
        try:
            report_dir = os.path.join(current_app.instance_path, 'import_reports')
            os.makedirs(report_dir, exist_ok=True)
            stamp = pk_now().strftime('%Y%m%d_%H%M%S')
            report_name = f"full_raw_import_report_{stamp}.csv"
            report_path = os.path.join(report_dir, report_name)
            with open(report_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=['table', 'reason', 'pk', 'label', 'row_json']
                )
                writer.writeheader()
                writer.writerows(skipped_rows)
            report_meta = {
                'name': report_name,
                'created_at': pk_now().strftime('%Y-%m-%d %H:%M:%S'),
                'mode': mode,
                'scope': scope_ctx.get('scope'),
                'tenant_id': target_tenant_id if scope_ctx.get('scope') == 'tenant' else None,
                'tenant_name': (
                    scope_ctx.get('target_tenant_name')
                    if scope_ctx.get('scope') == 'tenant'
                    else 'All Tenants'
                ),
                'inserted': report['inserted'],
                'skipped': report['skipped'],
                'tables': report['tables'],
                'source_file': source_file_name,
            }
            meta_path = report_path.replace('.csv', '.meta.json')
            try:
                with open(meta_path, 'w', encoding='utf-8') as f:
                    json.dump(report_meta, f, ensure_ascii=True)
            except Exception:
                pass
            session['full_raw_import_report'] = report_name
            session['full_raw_import_report_meta'] = report_meta
        except Exception:
            logging.exception('Full raw import committed but report generation failed')
    return report, report_name


@import_export_bp.route('/full_raw_import', methods=['POST'])
@login_required
def full_raw_import():
    if not _full_raw_import_enabled():
        flash('Full raw import is disabled by safety toggle.', 'warning')
        return redirect(url_for('import_export.import_export_page'))

    if current_user.role not in ['admin', 'root']:
        flash('Only admin or root can run full raw import.', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    file = request.files.get('file')
    if not file:
        flash('No file uploaded for full raw import.', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    mode = (request.form.get('mode') or 'append').strip().lower()
    if mode not in ['append', 'replace_tenant_data']:
        mode = 'append'
    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.form.get('scope'),
            tenant_id_raw=request.form.get('tenant_id'),
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('import_export.import_export_page'))
    if scope_ctx.get('scope') == 'all_tenants' and mode == 'replace_tenant_data':
        flash('Replace mode is blocked for all-tenants scope. Use append mode.', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    try:
        file_bytes = file.read()
        if hasattr(file, 'stream'):
            file.stream.seek(0)
        _archive_artifact_bytes(file_bytes, f"full_raw_import_{file.filename}", kind='imports')
    except Exception as e:
        flash(f'Invalid Excel file: {e}', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    report_name = None
    try:
        report, report_name = _run_full_raw_import_bytes(
            file_bytes=file_bytes,
            scope_ctx=scope_ctx,
            mode=mode,
            source_file_name=file.filename,
        )
        flash(
            f"Full raw import complete ({mode}). Inserted: {report['inserted']}, "
            f"Skipped: {report['skipped']}, Tables: {report['tables']}",
            'success'
        )
    except Exception as e:
        db.session.rollback()
        flash(f'Full raw import failed: {e}', 'danger')

    return redirect(url_for('import_export.import_export_page', full_raw_import_report=report_name))


@import_export_bp.route('/full_raw_import_report/<report_name>')
@login_required
def full_raw_import_report(report_name):
    if current_user.role not in ['admin', 'root']:
        return "Forbidden", 403
    safe_name = os.path.basename(report_name or '')
    if not safe_name.endswith('.csv') or safe_name != report_name:
        return "Invalid report", 400
    report_dir = os.path.join(current_app.instance_path, 'import_reports')
    report_path = os.path.join(report_dir, safe_name)
    if not os.path.exists(report_path):
        return "Report not found", 404
    return send_file(report_path, as_attachment=True, download_name=safe_name, mimetype='text/csv')


def _get_full_raw_report_dir():
    return os.path.join(current_app.instance_path, 'import_reports')


def _list_full_raw_reports():
    report_dir = _get_full_raw_report_dir()
    if not os.path.exists(report_dir):
        return []
    reports = []
    for name in os.listdir(report_dir):
        if not name.startswith('full_raw_import_report_') or not name.endswith('.csv'):
            continue
        path = os.path.join(report_dir, name)
        meta_path = os.path.join(report_dir, name.replace('.csv', '.meta.json'))
        try:
            stat = os.stat(path)
            created_at = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            with open(path, 'r', encoding='utf-8') as f:
                row_count = max(0, sum(1 for _ in f) - 1)
        except Exception:
            created_at = ''
            row_count = ''
        meta = None
        if os.path.exists(meta_path):
            try:
                with open(meta_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
            except Exception:
                meta = None
        reports.append({
            'name': name,
            'created_at': created_at,
            'row_count': row_count,
            'scope': (meta or {}).get('scope'),
            'mode': (meta or {}).get('mode'),
            'tenant_name': (meta or {}).get('tenant_name'),
            'inserted': (meta or {}).get('inserted'),
            'skipped': (meta or {}).get('skipped'),
            'tables': (meta or {}).get('tables'),
            'source_file': (meta or {}).get('source_file'),
        })
    reports.sort(key=lambda r: r['name'], reverse=True)
    return reports


@import_export_bp.route('/full_raw_import_history', methods=['GET', 'POST'])
@login_required
def full_raw_import_history():
    if current_user.role not in ['admin', 'root']:
        return "Forbidden", 403
    if request.method == 'POST':
        action = (request.form.get('action') or '').strip()
        selected = request.form.getlist('report')
        report_dir = _get_full_raw_report_dir()
        if action == 'delete_selected':
            removed = 0
            for name in selected:
                safe_name = os.path.basename(name)
                if safe_name != name or not safe_name.endswith('.csv'):
                    continue
                path = os.path.join(report_dir, safe_name)
                if os.path.exists(path):
                    try:
                        os.remove(path)
                        removed += 1
                    except Exception:
                        pass
            flash(f"Removed {removed} report(s).", 'info')
        elif action == 'delete_all':
            removed = 0
            if os.path.exists(report_dir):
                for name in os.listdir(report_dir):
                    if not name.endswith('.csv'):
                        continue
                    path = os.path.join(report_dir, name)
                    try:
                        os.remove(path)
                        removed += 1
                    except Exception:
                        pass
            flash(f"Removed {removed} report(s).", 'info')
        return redirect(url_for('import_export.full_raw_import_history'))
    reports = _list_full_raw_reports()
    return render_template('full_raw_import_history.html', reports=reports)


@import_export_bp.route('/full_raw_import_history_export')
@login_required
def full_raw_import_history_export():
    if current_user.role not in ['admin', 'root']:
        return "Forbidden", 403
    reports = _list_full_raw_reports()
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            'name', 'created_at', 'row_count', 'scope', 'mode', 'tenant_name',
            'inserted', 'skipped', 'tables', 'source_file'
        ]
    )
    writer.writeheader()
    writer.writerows(reports)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=full_raw_import_report_history.csv"}
    )


@import_export_bp.route('/app_upgrade', methods=['GET', 'POST'])
@login_required
def app_upgrade():
    if not APP_UPGRADE_ENABLED:
        return "Not Found", 404
    if current_user.role not in ['admin', 'root']:
        return "Forbidden", 403
    base_dir, current_path, wsgi_reload_path, history_path = _deploy_paths()
    os.makedirs(base_dir, exist_ok=True)

    if request.method == 'POST':
        file = request.files.get('file')
        if not file or not file.filename.lower().endswith('.zip'):
            flash('Please upload a .zip file.', 'danger')
            return redirect(url_for('import_export.app_upgrade'))
        run_migrate = str(request.form.get('run_migrate', '')).lower() in ['1', 'true', 'on', 'yes']
        require_data_upgrade = str(request.form.get('require_data_upgrade', '')).lower() in ['1', 'true', 'on', 'yes']
        result = _deploy_release_bytes(
            file.read(),
            run_migrate=run_migrate,
            require_data_upgrade=require_data_upgrade,
            actor_username=getattr(current_user, 'username', None),
        )
        if result.get('ok'):
            sql_report = result.get('sql_report')
            if sql_report:
                flash(
                    f"Upgrade complete. SQL applied: {sql_report.get('applied', 0)} / {sql_report.get('files', 0)}.",
                    'success'
                )
            else:
                flash('Upgrade complete. Reloaded app.' if result.get('reloaded') else 'Upgrade complete. Please reload the app.', 'success')
        else:
            flash(f"Upgrade failed: {result.get('error')}", 'danger')
        return redirect(url_for('import_export.app_upgrade'))

    history = _load_deploy_history(history_path)
    releases = []
    try:
        for name in sorted(os.listdir(base_dir), reverse=True):
            path = os.path.join(base_dir, name)
            if os.path.isdir(path):
                releases.append({'name': name, 'path': path})
    except Exception:
        releases = []

    current_target = None
    if os.path.islink(current_path):
        try:
            current_target = os.readlink(current_path)
        except Exception:
            current_target = None

    migrations_dir = _get_migrations_dir()
    migration_files = _list_sql_migrations(migrations_dir)
    data_upgrade_queue_dir, _ = _get_data_upgrade_queue_dir()
    queue_files = _list_data_upgrade_excels(data_upgrade_queue_dir)
    queue_files = [p for p in queue_files if '\\processed\\' not in p.replace('/', '\\')]
    return render_template(
        'app_upgrade.html',
        base_dir=base_dir,
        current_path=current_path,
        current_target=current_target,
        wsgi_reload_path=wsgi_reload_path,
        history=history,
        releases=releases,
        migrations_dir=migrations_dir,
        migration_files=migration_files,
        data_upgrade_queue_dir=data_upgrade_queue_dir,
        data_upgrade_queue_count=len(queue_files),
    )


@import_export_bp.route('/app_upgrade/start', methods=['POST'])
@login_required
def app_upgrade_start():
    if not APP_UPGRADE_ENABLED:
        return jsonify({'error': 'Not Found'}), 404
    if current_user.role not in ['admin', 'root']:
        return jsonify({'error': 'Forbidden'}), 403
    file = request.files.get('file')
    if not file or not file.filename.lower().endswith('.zip'):
        return jsonify({'error': 'Please upload a .zip file.'}), 400

    run_migrate = str(request.form.get('run_migrate', '')).lower() in ['1', 'true', 'on', 'yes']
    require_data_upgrade = str(request.form.get('require_data_upgrade', '')).lower() in ['1', 'true', 'on', 'yes']
    job_id = uuid.uuid4().hex
    username = getattr(current_user, 'username', None)
    tenant_id = getattr(current_user, 'tenant_id', None)
    role = getattr(current_user, 'role', None)
    _set_deploy_progress(job_id, percent=0, message='Queued...', done=False, success=False, user=username)
    flask_app = current_app._get_current_object()
    t = threading.Thread(
        target=_deploy_release_worker,
        args=(flask_app, job_id, file.read(), run_migrate, require_data_upgrade, username, tenant_id, role),
        daemon=True,
        name=f"deploy-worker-{job_id[:8]}",
    )
    t.start()
    return jsonify({'job_id': job_id})


@import_export_bp.route('/app_upgrade/status/<job_id>', methods=['GET'])
@login_required
def app_upgrade_status(job_id):
    if not APP_UPGRADE_ENABLED:
        return jsonify({'error': 'Not Found'}), 404
    if current_user.role not in ['admin', 'root']:
        return jsonify({'error': 'Forbidden'}), 403
    state = _get_deploy_progress(job_id)
    if not state:
        return jsonify({'error': 'Unknown job id'}), 404
    return jsonify(state)


@import_export_bp.route('/app_upgrade/rollback', methods=['POST'])
@login_required
def app_upgrade_rollback():
    if not APP_UPGRADE_ENABLED:
        return "Not Found", 404
    if current_user.role not in ['admin', 'root']:
        return "Forbidden", 403
    base_dir, current_path, wsgi_reload_path, history_path = _deploy_paths()
    target = request.form.get('release')
    if not target:
        flash('Select a release to rollback.', 'warning')
        return redirect(url_for('import_export.app_upgrade'))

    release_dir = os.path.join(base_dir, target)
    if not os.path.isdir(release_dir):
        flash('Invalid release selected.', 'danger')
        return redirect(url_for('import_export.app_upgrade'))

    if not (os.path.islink(current_path) or not os.path.exists(current_path)):
        flash('Current path is not a symlink. Configure WSGI to use DEPLOY_CURRENT_PATH.', 'danger')
        return redirect(url_for('import_export.app_upgrade'))

    backup_name = _create_full_raw_backup()
    if os.path.islink(current_path):
        os.unlink(current_path)
    os.symlink(release_dir, current_path)

    if wsgi_reload_path and os.path.exists(wsgi_reload_path):
        os.utime(wsgi_reload_path, None)
        reloaded = True
    else:
        reloaded = False

    entry = {
        'timestamp': pk_now().strftime('%Y%m%d_%H%M%S'),
        'action': 'rollback',
        'release_dir': release_dir,
        'current_path': current_path,
        'reloaded': reloaded,
        'user': getattr(current_user, 'username', None),
        'backup_name': backup_name,
    }
    _append_deploy_history(history_path, entry)

    flash('Rollback complete. Reloaded app.' if reloaded else 'Rollback complete. Please reload the app.', 'success')
    return redirect(url_for('import_export.app_upgrade'))


@import_export_bp.route('/app_upgrade/migrate', methods=['POST'])
@login_required
def app_upgrade_migrate():
    if not APP_UPGRADE_ENABLED:
        return "Not Found", 404
    if current_user.role not in ['admin', 'root']:
        return "Forbidden", 403
    _, _, wsgi_reload_path, history_path = _deploy_paths()
    stamp = pk_now().strftime('%Y%m%d_%H%M%S')
    try:
        backup_name = _create_full_raw_backup()
        sql_report = _run_app_migrate_steps()
        if wsgi_reload_path and os.path.exists(wsgi_reload_path):
            os.utime(wsgi_reload_path, None)
            reloaded = True
        else:
            reloaded = False

        entry = {
            'timestamp': stamp,
            'action': 'migrate',
            'reloaded': reloaded,
            'user': getattr(current_user, 'username', None),
            'bootstrap_ok': True,
            'sql_migrations': sql_report,
            'backup_name': backup_name,
        }
        _append_deploy_history(history_path, entry)

        flash(
            f"Migrate complete. SQL applied: {sql_report['applied']} / {sql_report['files']}.",
            'success'
        )
    except Exception as e:
        entry = {
            'timestamp': stamp,
            'action': 'migrate_failed',
            'error': str(e),
            'user': getattr(current_user, 'username', None),
        }
        _append_deploy_history(history_path, entry)
        flash(f"Migrate failed: {e}", 'danger')
    return redirect(url_for('import_export.app_upgrade'))

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
        query = PendingBill.query.filter(PendingBill.is_void == False)

        # Support filters from pending_bills page
        start_date = start_date or request.args.get('bill_from')
        end_date = end_date or request.args.get('bill_to')
        client_code = request.args.get('client_code')
        bill_no = request.args.get('bill_no')
        category = request.args.get('category')
        bill_kind = (request.args.get('bill_kind') or '').strip().upper()
        is_cash = request.args.get('is_cash')
        is_manual = request.args.get('is_manual')

        if start_date:
            query = query.filter(PendingBill.created_at >= start_date)
        if end_date:
            query = query.filter(PendingBill.created_at <= f"{end_date} 23:59:59")
        if client_code:
            query = query.filter(PendingBill.client_code == client_code)
        if bill_no:
            query = query.filter(PendingBill.bill_no.ilike(f"%{bill_no}%"))
        if bill_kind in ['SB', 'MB']:
            query = query.filter(PendingBill.bill_kind == bill_kind)
        if is_cash is not None and is_cash != '':
            query = query.filter(PendingBill.is_cash == (is_cash == '1'))
        if is_manual is not None and is_manual != '':
            query = query.filter(PendingBill.is_manual == (is_manual == '1'))
        if category:
            if category == 'Unbilled Cash' or category == 'Cash':
                query = query.filter(PendingBill.is_cash == True)
            elif category == 'Cash Paid':
                query = query.filter(
                    PendingBill.is_paid == True,
                    or_(
                        PendingBill.client_code == 'OPEN-KHATA',
                        func.upper(PendingBill.client_name) == 'OPEN KHATA'
                    )
                )
            elif category == 'Open Khata':
                query = query.filter(
                    or_(
                        PendingBill.client_code == 'OPEN-KHATA',
                        func.upper(PendingBill.client_name) == 'OPEN KHATA'
                    )
                )
            else:
                query = query.join(Client, PendingBill.client_code == Client.code).filter(
                    func.lower(func.trim(Client.category)) == category.lower().strip()
                )
            
        bills = query.order_by(PendingBill.id.desc()).all()
        data = [{
            'client_code': x.client_code,
            'client_name': x.client_name,
            'bill_no': x.bill_no,
            'bill_kind': x.bill_kind,
            'amount': x.amount,
            'reason': x.reason,
            'nimbus': x.nimbus_no,
            'is_paid': x.is_paid,
            'created_at': x.created_at
        } for x in bills]
    elif dataset == 'unpaid_transactions':
        query = PendingBill.query.filter(PendingBill.is_void == False)
        
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        material = request.args.get('material')
        bill_no = request.args.get('bill_no')
        status = request.args.get('status', 'unpaid')
        include_booking = request.args.get('include_booking', '0')

        if start_date:
            query = query.filter(PendingBill.created_at >= start_date)
        if end_date:
            query = query.filter(PendingBill.created_at <= f"{end_date} 23:59:59")
        if material:
            query = query.filter(PendingBill.reason.ilike(f'%{material}%'))
        if bill_no:
            query = query.filter((PendingBill.bill_no.ilike(f'%{bill_no}%')) | (PendingBill.nimbus_no.ilike(f'%{bill_no}%')))
        
        if status == 'paid':
            query = query.filter(PendingBill.is_paid == True)
        elif status == 'unpaid':
            query = query.filter(PendingBill.is_paid == False)
            
        query = query.filter(or_(PendingBill.amount > 0, PendingBill.is_paid == True))

        if include_booking not in ['1', 'true', 'on', 'yes']:
            booked_names = [r[0] for r in db.session.query(Booking.client_name).filter(Booking.is_void == False).distinct().all() if r[0]]
            booked_codes = set()
            if booked_names:
                booked_codes = {c.code for c in Client.query.filter(Client.name.in_(booked_names)).all()}
            if booked_codes:
                query = query.filter(~PendingBill.client_code.in_(booked_codes))
            if booked_names:
                query = query.filter(~PendingBill.client_name.in_(booked_names))
        
        bills = query.order_by(PendingBill.id.desc()).all()
        data = [{
            'client_code': x.client_code,
            'client_name': x.client_name,
            'bill_no': x.bill_no,
            'amount': x.amount,
            'reason': x.reason,
            'nimbus': x.nimbus_no,
            'is_paid': x.is_paid,
            'created_at': x.created_at
        } for x in bills]
    else:
        return "Invalid dataset", 400
        
    df = pd.DataFrame(data)
    filename = f"{dataset}_export_{pk_today()}"
    
    if fmt == 'csv':
        csv_text = df.to_csv(index=False)
        _archive_artifact_bytes(csv_text, f"{filename}.csv", kind='exports')
        return Response(
            csv_text,
            mimetype="text/csv",
            headers={"Content-disposition": f"attachment; filename={filename}.csv"}
        )
    elif fmt == 'pdf':
        # Basic HTML to PDF using weasyprint if available
        html = f"""
        <html><head><style>
            @page {{ size: 14.8cm 21cm; margin: 1cm; }}
            body {{ font-family: sans-serif; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 10px; }}
            th, td {{ border: 1px solid #ddd; padding: 4px; }}
            th {{ background: #f2f2f2; }}
        </style></head><body>
        <h2>{dataset.upper()} EXPORT</h2>
        <p>Generated: {pk_now()}</p>
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
        _archive_artifact_bytes(output.getvalue(), f"{filename}.xlsx", kind='exports')
        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"{filename}.xlsx")

@import_export_bp.route('/email_file', methods=['POST'])
@login_required
def email_file():
    kind = (request.form.get('kind') or '').strip().lower()
    dataset = (request.form.get('dataset') or '').strip()
    fmt = (request.form.get('format') or 'excel').strip().lower()

    filename = None
    mime = None
    content = None

    if kind == 'template':
        filename, mime, content = _build_template_attachment(dataset, fmt)
        if not filename:
            flash('Invalid template selection for email.', 'warning')
            return redirect(url_for('import_export.import_export_page'))
    elif kind == 'master':
        try:
            scope_ctx = _resolve_scope_context(
                scope_raw=request.form.get('scope'),
                tenant_id_raw=request.form.get('tenant_id'),
            )
        except ValueError as e:
            flash(str(e), 'danger')
            return redirect(url_for('import_export.import_export_page'))
        content = _build_master_export_bytes(scope_ctx=scope_ctx)
        filename = f"master_backup_{pk_today()}.xlsx"
        mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    elif kind == 'export':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        if dataset == 'clients':
            clients = Client.query.all()
            data = [{k: getattr(x, k) for k in CLIENT_SCHEMA if hasattr(x, k)} for x in clients]
            for d, x in zip(data, clients):
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
                if x.bill_no and PendingBill.query.filter_by(bill_no=x.bill_no, client_code=x.client_code).first():
                    match_status = "MATCHED"
                data.append({
                    'CLIENT_CODE': x.client_code, 'CLIENT_NAME': x.client, 'CLIENT_CATEGORY': x.client_category,
                    'TRANSACTION_CATEGORY': 'CEMENT+BILL' if x.bill_no else 'CEMENT', 'BILL_NO': x.bill_no,
                    'BILL_DATE': x.date, 'CEMENT_BRAND': x.material, 'QTY': x.qty, 'NIMBUS': x.nimbus_no,
                    'NOTES': '', 'SOURCE': 'CEMENT', 'MATCH_STATUS': match_status
                })
        elif dataset == 'pending_bills':
            query = PendingBill.query.filter(PendingBill.is_void == False)
            if start_date:
                query = query.filter(PendingBill.created_at >= start_date)
            if end_date:
                query = query.filter(PendingBill.created_at <= f"{end_date} 23:59:59")
            bills = query.order_by(PendingBill.id.desc()).all()
            data = [{
                'client_code': x.client_code, 'client_name': x.client_name, 'bill_no': x.bill_no,
                'bill_kind': x.bill_kind,
                'amount': x.amount, 'reason': x.reason, 'nimbus': x.nimbus_no,
                'is_paid': x.is_paid, 'created_at': x.created_at
            } for x in bills]
        else:
            flash('Invalid export dataset for email.', 'warning')
            return redirect(url_for('import_export.import_export_page'))

        df = pd.DataFrame(data)
        base = f"{dataset}_export_{pk_today()}"
        if fmt == 'csv':
            filename = f"{base}.csv"
            mime = 'text/csv'
            content = df.to_csv(index=False).encode('utf-8')
        else:
            filename = f"{base}.xlsx"
            mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            content = out.getvalue()
    else:
        flash('Invalid email request.', 'warning')
        return redirect(url_for('import_export.import_export_page'))

    ok, msg = _smtp_send_attachments(
        subject=f"Import/Export File - {filename}",
        body=f"Attached file from Import/Export Center: {filename}",
        attachments=[(filename, mime, content)]
    )
    flash(msg, 'success' if ok else 'warning')
    return redirect(url_for('import_export.import_export_page'))


@import_export_bp.route('/transfer/export', methods=['POST'])
@login_required
def transfer_export():
    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.form.get('scope'),
            tenant_id_raw=request.form.get('tenant_id'),
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('import_export.import_export_page'))

    sections = [str(x).strip().lower() for x in request.form.getlist('sections') if str(x).strip()]
    if not sections:
        sections = ['all_business']

    if 'literal_all' in sections:
        content = _build_full_raw_export_bytes(scope_ctx=scope_ctx)
        fname = f"literal_full_export_{pk_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    elif 'all_business' in sections:
        content = _build_master_export_bytes(scope_ctx=scope_ctx)
        fname = f"master_backup_{pk_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    else:
        sheet_names = _selected_master_sheets(sections)
        if not sheet_names:
            flash('Select at least one section for export.', 'warning')
            return redirect(url_for('import_export.import_export_page'))
        content = _filter_excel_bytes_to_sheets(
            _build_master_export_bytes(scope_ctx=scope_ctx),
            sheet_names,
        )
        fname = f"section_export_{pk_now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    _archive_artifact_bytes(content, fname, kind='exports')
    output = io.BytesIO(content)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=fname,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@import_export_bp.route('/transfer/import', methods=['POST'])
@login_required
def transfer_import():
    file = request.files.get('file')
    if not file:
        flash('Please upload an Excel file.', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.form.get('scope'),
            tenant_id_raw=request.form.get('tenant_id'),
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('import_export.import_export_page'))

    sections = [str(x).strip().lower() for x in request.form.getlist('sections') if str(x).strip()]
    if not sections:
        sections = ['all_business']

    try:
        file_bytes = file.read()
        if hasattr(file, 'stream'):
            file.stream.seek(0)
        _archive_artifact_bytes(file_bytes, f"transfer_import_{file.filename}", kind='imports')
    except Exception as e:
        flash(f'Invalid file: {e}', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    if 'literal_all' in sections:
        if not _full_raw_import_enabled():
            flash('Literal Full Import is disabled by safety toggle.', 'warning')
            return redirect(url_for('import_export.import_export_page'))
        mode = (request.form.get('mode') or 'append').strip().lower()
        if mode not in ['append', 'replace_tenant_data']:
            mode = 'append'
        if scope_ctx.get('scope') == 'all_tenants' and mode == 'replace_tenant_data':
            flash('Replace mode is blocked for all-tenants scope. Use append mode.', 'danger')
            return redirect(url_for('import_export.import_export_page'))
        try:
            report, report_name = _run_full_raw_import_bytes(
                file_bytes=file_bytes,
                scope_ctx=scope_ctx,
                mode=mode,
                source_file_name=file.filename,
            )
            flash(
                f"Literal full import complete ({mode}). Inserted: {report['inserted']}, "
                f"Skipped: {report['skipped']}, Tables: {report['tables']}",
                'success'
            )
            return redirect(url_for('import_export.import_export_page', full_raw_import_report=report_name))
        except Exception as e:
            db.session.rollback()
            flash(f'Literal full import failed: {e}', 'danger')
            return redirect(url_for('import_export.import_export_page'))

    if getattr(current_user, 'role', None) == 'root' and scope_ctx.get('scope') == 'all_tenants':
        flash('Root all-tenant master import is blocked. Use Literal Full Import for all-tenant restore.', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    try:
        run_bytes = file_bytes
        if 'all_business' not in sections:
            sheet_names = _selected_master_sheets(sections)
            if not sheet_names:
                flash('Select at least one section for import.', 'warning')
                return redirect(url_for('import_export.import_export_page'))
            run_bytes = _filter_excel_bytes_to_sheets(file_bytes, sheet_names)

        _set_import_actor_context(
            username=getattr(current_user, 'username', None),
            tenant_id=scope_ctx.get('target_tenant_id'),
            role=getattr(current_user, 'role', None),
        )
        g.enforce_tenant = (scope_ctx.get('scope') == 'tenant' and scope_ctx.get('target_tenant_id') is not None)
        g.tenant_id = scope_ctx.get('target_tenant_id')
        report = _run_master_import_bytes(
            file_bytes=run_bytes,
            actor_username=getattr(current_user, 'username', None),
            progress_cb=None,
        )
        flash(
            f"Import complete. Imported: {report.get('imported', 0)}, "
            f"Updated: {report.get('updated', 0)}, Skipped: {report.get('skipped', 0)}.",
            'success'
        )
    except Exception as e:
        db.session.rollback()
        flash(f'Import failed: {e}', 'danger')
    finally:
        _clear_import_actor_context()
    return redirect(url_for('import_export.import_export_page'))

@import_export_bp.route('/master/export')
@login_required
def export_master():
    """Export all datasets into a single Excel file with multiple sheets."""
    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.args.get('scope'),
            tenant_id_raw=request.args.get('tenant_id'),
        )
    except ValueError as e:
        flash(str(e), 'danger')
        return redirect(url_for('import_export.import_export_page'))
    content = _build_master_export_bytes(scope_ctx=scope_ctx)
    _archive_artifact_bytes(content, f"master_backup_{pk_today()}.xlsx", kind='exports')
    output = io.BytesIO(content)
    output.seek(0)
    return send_file(output, as_attachment=True, download_name=f"master_backup_{pk_today()}.xlsx")


@import_export_bp.route('/export_excel_all')
@login_required
def export_excel_all():
    """Legacy endpoint compatibility."""
    return redirect(url_for('import_export.export_master'))

def _run_master_import_bytes(file_bytes, actor_username=None, progress_cb=None):
    ok, msg = backup_database()
    if not ok:
        raise RuntimeError(f"Backup failed: {msg}")

    xls = pd.ExcelFile(io.BytesIO(file_bytes))
    report = {'imported': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
    steps = [
        'Clients', 'MaterialCategories', 'Materials', 'PendingBills',
        'Dispatch', 'Bookings', 'BookingItems', 'Payments', 'Sales',
        'SaleItems', 'GRN', 'GRNItems', 'DeliveryPersons', 'DeliveryRents'
    ]
    total = len(steps)

    def _read_sheet(name):
        d = pd.read_excel(xls, name)
        d.columns = [c.lower().strip().replace(' ', '_') for c in d.columns]
        return d.fillna('')

    for idx, sheet_name in enumerate(steps, start=1):
        pct = 8 + int(((idx - 1) / total) * 84)
        exists = sheet_name in xls.sheet_names
        if progress_cb:
            msg = f"Processing sheet {idx}/{total}: {sheet_name}" if exists else f"Skipping missing sheet {idx}/{total}: {sheet_name}"
            progress_cb(pct, msg)
        if not exists:
            continue

        if sheet_name == 'Clients':
            _process_clients(_read_sheet('Clients'), 'update', report)
        elif sheet_name == 'MaterialCategories':
            _process_material_categories(_read_sheet('MaterialCategories'), report)
        elif sheet_name == 'Materials':
            _process_materials(_read_sheet('Materials'), report)
        elif sheet_name == 'PendingBills':
            _process_pending_bills(_read_sheet('PendingBills'), 'update', 'create', report)
        elif sheet_name == 'Dispatch':
            df = _read_sheet('Dispatch')
            df.rename(columns={'cement_brand': 'item', 'client_name': 'customer', 'bill_date': 'date', 'nimbus': 'nimbus_no'}, inplace=True)
            _process_dispatch(df, 'skip', 'create', report)
        elif sheet_name == 'Bookings':
            _process_bookings(_read_sheet('Bookings'), 'update', report)
        elif sheet_name == 'BookingItems':
            _process_booking_items(_read_sheet('BookingItems'), 'update', report)
        elif sheet_name == 'Payments':
            _process_payments(_read_sheet('Payments'), 'update', report)
        elif sheet_name == 'Sales':
            _process_sales(_read_sheet('Sales'), 'update', report)
        elif sheet_name == 'SaleItems':
            _process_sale_items(_read_sheet('SaleItems'), 'update', report)
        elif sheet_name == 'GRN':
            _process_grn(_read_sheet('GRN'), 'update', report)
        elif sheet_name == 'GRNItems':
            _process_grn_items(_read_sheet('GRNItems'), 'update', report)
        elif sheet_name == 'DeliveryPersons':
            df = _read_sheet('DeliveryPersons')
            for _, row in df.iterrows():
                name = str(row.get('name', '')).strip()
                if not name:
                    continue
                phone = str(row.get('phone', '')).strip()
                existing = DeliveryPerson.query.filter(
                    func.lower(func.trim(DeliveryPerson.name)) == name.lower()
                ).first()
                is_active_raw = str(row.get('is_active', '')).strip().lower()
                is_active = is_active_raw not in ['false', '0', 'no', 'off']
                if existing:
                    existing.is_active = is_active
                    if phone:
                        existing.phone = phone
                    report['updated'] += 1
                else:
                    db.session.add(DeliveryPerson(name=name, phone=phone or None, is_active=is_active))
                    report['imported'] += 1
        elif sheet_name == 'DeliveryRents':
            df = _read_sheet('DeliveryRents')
            for _, row in df.iterrows():
                driver_name = str(row.get('delivery_person_name', '')).strip()
                bill_no = str(row.get('bill_no', '')).strip()
                if not driver_name:
                    report['skipped'] += 1
                    continue
                try:
                    amount = float(row.get('amount', 0) or 0)
                except Exception:
                    amount = 0

                sale_id = None
                raw_sale_id = str(row.get('sale_id', '')).strip()
                if raw_sale_id:
                    try:
                        sale_id = int(float(raw_sale_id))
                    except Exception:
                        sale_id = None

                if not sale_id and bill_no:
                    sale = DirectSale.query.filter(
                        or_(DirectSale.manual_bill_no == bill_no, DirectSale.auto_bill_no == bill_no)
                    ).first()
                    if sale:
                        sale_id = sale.id

                existing = None
                if sale_id:
                    existing = DeliveryRent.query.filter_by(sale_id=sale_id).first()
                if not existing and bill_no:
                    existing = DeliveryRent.query.filter(
                        DeliveryRent.bill_no == bill_no,
                        func.lower(func.trim(DeliveryRent.delivery_person_name)) == driver_name.lower()
                    ).first()

                is_void_raw = str(row.get('is_void', '')).strip().lower()
                is_void = is_void_raw in ['true', '1', 'yes', 'on']
                note = str(row.get('note', '')).strip()
                created_by = str(row.get('created_by', '')).strip() or (actor_username or _actor_username())

                if existing:
                    existing.delivery_person_name = driver_name
                    existing.bill_no = bill_no
                    existing.amount = amount
                    existing.note = note
                    existing.created_by = created_by
                    existing.is_void = is_void
                    report['updated'] += 1
                else:
                    db.session.add(DeliveryRent(
                        sale_id=sale_id,
                        delivery_person_name=driver_name,
                        bill_no=bill_no,
                        amount=amount,
                        note=note,
                        created_by=created_by,
                        is_void=is_void
                    ))
                    report['imported'] += 1

    if progress_cb:
        progress_cb(97, 'Finalizing import...')
    db.session.commit()
    return report


def _master_import_worker(flask_app, job_id, file_bytes, username, tenant_id, role, scope):
    with flask_app.app_context():
        with flask_app.test_request_context('/import_export/master/import'):
            g.user = None
            g.is_root = (role == 'root')
            g.tenant_id = tenant_id
            g.enforce_tenant = (tenant_id is not None) and (scope == 'tenant' or not g.is_root)
            _set_import_actor_context(username=username, tenant_id=tenant_id, role=role)
            try:
                _set_master_import_progress(job_id, percent=2, message='Started master import...', done=False, success=False, user=username)
                report = _run_master_import_bytes(
                    file_bytes=file_bytes,
                    actor_username=username,
                    progress_cb=lambda p, m: _set_master_import_progress(job_id, percent=p, message=m, done=False, success=False, user=username),
                )
                _set_master_import_progress(
                    job_id,
                    percent=100,
                    message='Master import completed.',
                    done=True,
                    success=True,
                    user=username,
                    report=report,
                )
            except Exception as e:
                db.session.rollback()
                _set_master_import_progress(
                    job_id,
                    percent=100,
                    message='Master import failed.',
                    done=True,
                    success=False,
                    user=username,
                    error=str(e),
                )
            finally:
                _clear_import_actor_context()


@import_export_bp.route('/master/import/start', methods=['POST'])
@login_required
def master_import_start():
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400
    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        return jsonify({'error': 'Joint Master Import requires Excel file (.xlsx/.xls).'}), 400

    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.form.get('scope'),
            tenant_id_raw=request.form.get('tenant_id'),
        )
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    if getattr(current_user, 'role', None) == 'root' and scope_ctx.get('scope') == 'all_tenants':
        return jsonify({
            'error': 'Root all-tenant master import is blocked. Use Literal Full Import for all-tenant restore.'
        }), 400

    job_id = uuid.uuid4().hex
    username = getattr(current_user, 'username', None)
    tenant_id = scope_ctx.get('target_tenant_id')
    role = getattr(current_user, 'role', None)
    _set_master_import_progress(job_id, percent=0, message='Queued...', done=False, success=False, user=username)
    file_bytes = file.read()
    _archive_artifact_bytes(file_bytes, f"master_import_{file.filename}", kind='imports')

    flask_app = current_app._get_current_object()
    t = threading.Thread(
        target=_master_import_worker,
        args=(flask_app, job_id, file_bytes, username, tenant_id, role, scope_ctx.get('scope')),
        daemon=True,
        name=f"master-import-{job_id[:8]}",
    )
    t.start()
    return jsonify({'job_id': job_id})


@import_export_bp.route('/master/import/status/<job_id>', methods=['GET'])
@login_required
def master_import_status(job_id):
    state = _get_master_import_progress(job_id)
    if not state:
        return jsonify({'error': 'Unknown job id'}), 404

    owner = state.get('user')
    if owner and owner != getattr(current_user, 'username', None) and getattr(current_user, 'role', None) not in ['admin', 'root']:
        return jsonify({'error': 'Forbidden'}), 403
    return jsonify(state)


@import_export_bp.route('/master/import', methods=['POST'])
@login_required
def import_master():
    """Import multiple datasets from a single Excel file (sync endpoint for compatibility)."""
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'No file uploaded'}), 400
    try:
        scope_ctx = _resolve_scope_context(
            scope_raw=request.form.get('scope'),
            tenant_id_raw=request.form.get('tenant_id'),
        )
        if getattr(current_user, 'role', None) == 'root' and scope_ctx.get('scope') == 'all_tenants':
            return jsonify({
                'error': 'Root all-tenant master import is blocked. Use Literal Full Import for all-tenant restore.'
            }), 400
        file_bytes = file.read()
        _archive_artifact_bytes(file_bytes, f"master_import_sync_{file.filename}", kind='imports')
        _set_import_actor_context(
            username=getattr(current_user, 'username', None),
            tenant_id=scope_ctx.get('target_tenant_id'),
            role=getattr(current_user, 'role', None)
        )
        g.enforce_tenant = (scope_ctx.get('scope') == 'tenant' and scope_ctx.get('target_tenant_id') is not None)
        g.tenant_id = scope_ctx.get('target_tenant_id')
        report = _run_master_import_bytes(
            file_bytes=file_bytes,
            actor_username=getattr(current_user, 'username', None),
            progress_cb=None,
        )
        return jsonify({'success': True, 'report': report})
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        _clear_import_actor_context()

