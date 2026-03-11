import os
import io
import secrets
import json
import calendar
import threading
import time
import smtplib
import shutil
import zipfile
import urllib.request
import urllib.error
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file, Response, make_response, send_from_directory
from flask import session
from flask_login import LoginManager, login_user, login_required, logout_user, current_user
import re
import logging
import importlib
from itertools import zip_longest
from urllib.parse import unquote
from contextlib import redirect_stderr
from email.message import EmailMessage
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import func, case, text, or_, and_, exists, not_
from types import SimpleNamespace
from models import db, Tenant, TenantFeature, AuditLog, User, Client, Material, MaterialCategory, Entry, PendingBill, Booking, BookingItem, Payment, WaiveOff, RootRecoveryCode, Invoice, BillCounter, DirectSale, DirectSaleItem, GRN, GRNItem, Delivery, DeliveryItem, Settings, ReconBasket, StaffEmail, FollowUpReminder, FollowUpContact, DeliveryPerson, DeliveryRent, Supplier, SupplierPayment, TenantWipeBackupHistory, RootBackupSettings, RootBackupEmailHistory, get_or_create_material_category
from tenancy import init_tenancy, bootstrap_tenancy, require_root, audit_log, ensure_user_table_tenant_unique, ensure_material_table_tenant_unique, ensure_client_table_tenant_unique, hard_delete_tenant, can_hard_delete_tenant, TEST_TENANT_NAME, DEFAULT_TENANT_NAME

app = Flask(__name__)
# Allow embedding and open CORS (use with caution)
@app.after_request
def allow_iframe_and_cors(response):
    if os.environ.get('ALLOW_OPEN_CORS', '').lower() in ('1', 'true', 'yes'):
        response.headers["X-Frame-Options"] = "ALLOWALL"
        response.headers["Content-Security-Policy"] = "frame-ancestors *"
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
        response.headers["Access-Control-Allow-Credentials"] = "false"
        return response

    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Content-Security-Policy"] = "frame-ancestors 'self'"
    return response


@app.errorhandler(RequestEntityTooLarge)
def _handle_request_too_large(_err):
    max_mb = max(1, int((app.config.get('MAX_CONTENT_LENGTH') or 0) / (1024 * 1024)))
    msg = f"Uploaded file is too large. Max allowed size is {max_mb} MB."
    if request.path.startswith('/import_export/app_upgrade/start') or request.path.startswith('/import_export/master/import/start'):
        return jsonify({'error': msg}), 413
    flash(msg, 'danger')
    return redirect(request.referrer or url_for('index'))
# Canonical marker for unknown-credit transactions kept on ledger for long-term tracking.
OPEN_KHATA_CODE = 'OPEN-KHATA'
OPEN_KHATA_NAME = 'OPEN KHATA'
_NOTIFY_WORKER_STARTED = False
_NOTIFY_LAST_SENT_DATE = None
_HOURLY_BACKUP_WORKER_STARTED = False
_HOURLY_BACKUP_LAST_SLOT = None
AMS_ASSISTANT_EXPORT_CACHE = {}
AMS_ASSISTANT_CONTEXT_CACHE = {}

PK_TZ = ZoneInfo('Asia/Karachi')


def pk_now():
    """Current Pakistan local datetime (naive) for app-wide timestamps."""
    return datetime.now(PK_TZ).replace(tzinfo=None)


def pk_today():
    """Current Pakistan local date for app-wide date defaults."""
    return pk_now().date()


def resolve_posted_datetime(date_str=None, fallback_dt=None):
    """
    Normalize transaction timestamps to PKT:
    - No date: current PK time
    - datetime-local: keep selected PK date+time
    - Selected today (date-only): current PK time
    - Selected past date (date-only): selected date at 00:00:00
    """
    if not date_str:
        return fallback_dt or pk_now()
    try:
        raw = str(date_str).strip()
        for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
            try:
                return datetime.strptime(raw, fmt)
            except Exception:
                pass
        parsed = datetime.strptime(raw, '%Y-%m-%d')
        if parsed.date() == pk_today():
            return pk_now()
        return parsed
    except Exception:
        return fallback_dt or pk_now()


def _to_float_or_zero(value):
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _resolve_opening_balance_date(date_str=None, fallback_dt=None):
    """Normalize opening-balance date from form input with stable fallback."""
    if not date_str:
        return fallback_dt or pk_now()
    try:
        return datetime.strptime(str(date_str).strip(), '%Y-%m-%d')
    except Exception:
        return fallback_dt or pk_now()

SALE_CATEGORY_CHOICES = ['Booking Delivery', 'Mixed Transaction', 'Credit Customer', 'Open Khata', 'Cash']
_SALE_CATEGORY_ALIASES = {
    'booked sale': 'Booking Delivery',
    'booked': 'Booking Delivery',
    'booking delivery': 'Booking Delivery',
    'booking': 'Booking Delivery',
    'booked +credit': 'Mixed Transaction',
    'booked+credit': 'Mixed Transaction',
    'booked + credit': 'Mixed Transaction',
    'mixed': 'Mixed Transaction',
    'mixed transaction': 'Mixed Transaction',
    'credit': 'Credit Customer',
    'credit sale': 'Credit Customer',
    'credit customer': 'Credit Customer',
    'open khata': 'Open Khata',
    'cash': 'Cash',
    'cash sale': 'Cash',
}
# Allow larger uploads for release ZIP and master Excel imports.
_max_upload_mb = int(os.environ.get('MAX_UPLOAD_MB', '256') or '256')
app.config['MAX_CONTENT_LENGTH'] = _max_upload_mb * 1024 * 1024

# Use environment variable for secret key or generate a secure one
app.secret_key = os.environ.get('SECRET_KEY') or secrets.token_hex(32)

# Backup policy: default to OFF (user-managed manual backups only).
_AUTO_BACKUP_ENABLED = os.environ.get('AUTO_BACKUP_ENABLED', '0').strip() == '1'
_WIPE_BACKUP_ENABLED = os.environ.get('WIPE_BACKUP_ENABLED', '0').strip() == '1'

basedir = os.path.abspath(os.path.dirname(__file__))
legacy_instance_dir = os.path.join(basedir, 'instance')
os.makedirs(legacy_instance_dir, exist_ok=True)
legacy_db_path = os.path.join(legacy_instance_dir, 'ahmed_cement.db')
db_path = os.environ.get('APP_DB_PATH') or legacy_db_path

app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['REMEMBER_COOKIE_DURATION'] = timedelta(days=30)
app.config['FULL_RAW_IMPORT_ENABLED'] = '1'
app.config['DEPLOY_BASE_DIR'] = "/home/rehmanahmed/releases"
app.config['DEPLOY_CURRENT_PATH'] = "/home/rehmanahmed/app_current"
app.config['WSGI_RELOAD_PATH'] = "/var/www/rehmanahmed_pythonanywhere_com_wsgi.py"
app.config['MIGRATIONS_DIR'] = "/home/rehmanahmed/migrations"
db.init_app(app)

@app.before_request
def _protect_against_csrf():
    return None

login_manager = LoginManager()
login_manager.login_view = 'login'
login_manager.init_app(app)
init_tenancy(app)

# Configure logging
logging.basicConfig(filename='errorlog.txt', level=logging.ERROR,
                    format='%(asctime)s %(levelname)s: %(message)s')

_WEASYPRINT_MODULE = None


def _safe_download_name(name, default='document.pdf'):
    raw = (name or '').strip()
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', raw).strip('._')
    if not safe:
        safe = default
    if '.' not in safe:
        safe = f"{safe}.pdf"
    return safe


def _try_render_weasy_pdf(rendered_html, download_name, disposition='attachment'):
    """
    Render PDF with WeasyPrint only when available.
    On first failure, disable future attempts to avoid repeated noisy warnings.
    """
    global _WEASYPRINT_MODULE

    try:
        if _WEASYPRINT_MODULE is None:
            # WeasyPrint prints dependency warnings to stderr on import; silence them here.
            with redirect_stderr(io.StringIO()):
                _WEASYPRINT_MODULE = importlib.import_module('flask_weasyprint')
        # Enforce consistent PDF paper format across all exports.
        # Required layout: width 14.8cm, height 21cm with 1cm margins on all sides.
        forced_page_css = (
            "<style>"
            "@page { size: 14.8cm 21cm; margin: 1cm; }"
            "</style>"
        )
        html_for_pdf = f"{forced_page_css}{rendered_html}"
        safe_name = _safe_download_name(download_name, default='document.pdf')
        response = _WEASYPRINT_MODULE.render_pdf(
            _WEASYPRINT_MODULE.HTML(string=html_for_pdf),
            download_name=safe_name
        )
        response.headers['Content-Disposition'] = f'{disposition}; filename={safe_name}'
        return response
    except Exception:
        # Keep retrying on future requests in case dependencies are fixed at runtime.
        _WEASYPRINT_MODULE = None
        return None


def _start_notification_worker():
    global _NOTIFY_WORKER_STARTED
    if _NOTIFY_WORKER_STARTED:
        return
    t = threading.Thread(target=_notification_worker_loop, daemon=True, name='notify-daily-mailer')
    t.start()
    _NOTIFY_WORKER_STARTED = True


def _start_hourly_backup_worker():
    global _HOURLY_BACKUP_WORKER_STARTED
    if _HOURLY_BACKUP_WORKER_STARTED:
        return
    t = threading.Thread(target=_hourly_backup_worker_loop, daemon=True, name='hourly-backup-mailer')
    t.start()
    _HOURLY_BACKUP_WORKER_STARTED = True


@app.before_request
def _ensure_notify_worker_started():
    _start_notification_worker()
    if _AUTO_BACKUP_ENABLED:
        _start_hourly_backup_worker()


def _smtp_send_attachments_to(recipients, subject, body, attachments):
    recipients = [x.strip() for x in (recipients or []) if str(x or '').strip()]
    if not recipients:
        return False, 'No recipients configured'

    settings_obj = Settings.query.first()
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
        return False, 'SMTP settings missing'
    if smtp_user and not smtp_pass:
        return False, 'SMTP password missing'

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
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=60) as server:
                if smtp_user:
                    server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
                if use_tls:
                    server.starttls()
                if smtp_user:
                    server.login(smtp_user, smtp_pass)
                server.send_message(msg)
        return True, f'Sent to {len(recipients)} email(s)'
    except Exception as e:
        return False, f'SMTP send failed: {e}'


def _root_backup_dir():
    root_dir = os.path.join(basedir, 'instance', 'root_hourly_backups')
    os.makedirs(root_dir, exist_ok=True)
    return root_dir


def _normalize_csv_emails(raw_value):
    return [x.strip() for x in str(raw_value or '').split(',') if x.strip()]


def _get_or_create_root_backup_settings():
    row = RootBackupSettings.query.first()
    if row:
        return row
    row = RootBackupSettings(
        enabled=False,
        frequency='hourly',
        include_full_raw_xlsx=True,
        include_sqlite_db=True,
        keep_history_count=200,
        subject_prefix='PWARE Root Backup'
    )
    settings_obj = Settings.query.first()
    if settings_obj and (settings_obj.company_email or '').strip():
        row.recipient_emails = (settings_obj.company_email or '').strip()
    db.session.add(row)
    db.session.commit()
    return row


def _build_root_backup_zip(settings_row):
    from blueprints.import_export import _build_full_raw_export_bytes

    now = pk_now()
    stamp = now.strftime('%Y%m%d_%H%M%S')
    scope_ctx = {
        'scope': 'all_tenants',
        'target_tenant_id': None,
        'target_tenant_name': 'All Tenants',
        'role': 'root',
    }

    zip_io = io.BytesIO()
    zip_name = f"all_tenants_backup_{stamp}.zip"
    with zipfile.ZipFile(zip_io, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        if bool(getattr(settings_row, 'include_full_raw_xlsx', True)):
            xlsx_content = _build_full_raw_export_bytes(scope_ctx=scope_ctx)
            zf.writestr(f"all_tenants_full_raw_{stamp}.xlsx", xlsx_content or b'')

        if bool(getattr(settings_row, 'include_sqlite_db', True)) and os.path.exists(db_path):
            with open(db_path, 'rb') as dbf:
                zf.writestr(f"ahmed_cement_{stamp}.db", dbf.read())

        meta = {
            'generated_at': now.strftime('%Y-%m-%d %H:%M:%S'),
            'tenant_count': Tenant.query.count(),
            'db_source_path': db_path,
        }
        zf.writestr('backup_meta.json', json.dumps(meta, ensure_ascii=True, indent=2))

    zip_bytes = zip_io.getvalue()
    save_path = os.path.join(_root_backup_dir(), zip_name)
    with open(save_path, 'wb') as f:
        f.write(zip_bytes)
    return zip_name, save_path, zip_bytes


def _cleanup_root_backup_history(keep_count):
    keep_count = max(1, int(keep_count or 200))
    rows = RootBackupEmailHistory.query.order_by(RootBackupEmailHistory.created_at.desc()).all()
    if len(rows) <= keep_count:
        return
    for row in rows[keep_count:]:
        fpath = (row.backup_path or '').strip()
        if fpath and os.path.exists(fpath):
            try:
                os.remove(fpath)
            except Exception:
                pass
        db.session.delete(row)
    db.session.commit()


def _log_root_backup_history(settings_row, trigger_type, status, recipients, subject, attachment_name, attachment_size_bytes, backup_path, message):
    db.session.add(RootBackupEmailHistory(
        trigger_type=(trigger_type or 'auto'),
        status=(status or 'failed'),
        recipient_emails=', '.join(recipients or []),
        subject=subject,
        attachment_name=attachment_name,
        attachment_size_kb=(int(attachment_size_bytes / 1024) if attachment_size_bytes else 0),
        backup_path=(backup_path or ''),
        message=(message or '')[:1000]
    ))
    settings_row.last_sent_at = pk_now()
    settings_row.last_status = status
    settings_row.last_message = (message or '')[:500]
    db.session.commit()
    _cleanup_root_backup_history(settings_row.keep_history_count or 200)


def _send_hourly_all_tenants_backup_email(trigger_type='auto-hourly', force_send=False):
    settings_row = _get_or_create_root_backup_settings()
    if not force_send and not settings_row.enabled:
        return False, 'Root backup automation disabled'

    recipients = _normalize_csv_emails(settings_row.recipient_emails)
    if not recipients:
        return False, 'No recipient emails configured'

    zip_name = ''
    zip_path = ''
    zip_bytes = b''
    try:
        zip_name, zip_path, zip_bytes = _build_root_backup_zip(settings_row=settings_row)
        ts = pk_now().strftime('%Y-%m-%d %H:%M')
        subject_prefix = (settings_row.subject_prefix or 'PWARE Root Backup').strip()
        subject = f"{subject_prefix} - {ts}"
        body = (
            "Attached is the root automatic backup ZIP for all tenants.\n"
            f"Generated at: {pk_now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Tenant count: {Tenant.query.count()}"
        )
        ok, msg = _smtp_send_attachments_to(
            recipients,
            subject,
            body,
            [(zip_name, 'application/zip', zip_bytes)]
        )
        _log_root_backup_history(
            settings_row=settings_row,
            trigger_type=trigger_type,
            status=('sent' if ok else 'failed'),
            recipients=recipients,
            subject=subject,
            attachment_name=zip_name,
            attachment_size_bytes=len(zip_bytes or b''),
            backup_path=zip_path,
            message=msg
        )
        return ok, msg
    except Exception as e:
        try:
            _log_root_backup_history(
                settings_row=settings_row,
                trigger_type=trigger_type,
                status='failed',
                recipients=recipients,
                subject='Root Backup Failed',
                attachment_name=zip_name,
                attachment_size_bytes=len(zip_bytes or b''),
                backup_path=zip_path,
                message=str(e)
            )
        except Exception:
            db.session.rollback()
        return False, f'Backup send failed: {e}'


def _hourly_backup_worker_loop():
    global _HOURLY_BACKUP_LAST_SLOT
    while True:
        try:
            with app.app_context():
                now = pk_now()
                slot = now.strftime('%Y%m%d%H')
                if now.minute == 0 and _HOURLY_BACKUP_LAST_SLOT != slot:
                    _send_hourly_all_tenants_backup_email(trigger_type='auto-hourly', force_send=False)
                    _HOURLY_BACKUP_LAST_SLOT = slot
            time.sleep(30)
        except Exception:
            time.sleep(60)


USER_PERMISSION_DEFAULTS = {
    'can_view_stock': True,
    'can_view_daily': True,
    'can_view_history': True,
    'can_import_export': False,
    'can_manage_directory': False,
    'can_view_dashboard': True,
    'can_manage_grn': True,
    'can_manage_bookings': True,
    'can_manage_payments': True,
    'can_manage_sales': True,
    'can_view_delivery_rent': True,
    'can_manage_pending_bills': True,
    'can_view_reports': True,
    'can_manage_notifications': True,
    'can_view_client_ledger': True,
    'can_view_supplier_ledger': True,
    'can_view_decision_ledger': True,
    'can_manage_clients': False,
    'can_manage_suppliers': False,
    'can_manage_materials': False,
    'can_manage_delivery_persons': False,
    'can_access_settings': False,
}

PERMISSION_LEGACY_FALLBACKS = {
    'can_manage_grn': 'can_view_stock',
    'can_manage_bookings': 'can_view_history',
    'can_manage_payments': 'can_view_history',
    'can_manage_sales': 'can_view_history',
    'can_view_delivery_rent': 'can_view_history',
    'can_manage_pending_bills': 'can_view_history',
    'can_view_reports': 'can_view_history',
    'can_manage_notifications': 'can_view_history',
    'can_view_client_ledger': 'can_view_history',
    'can_view_supplier_ledger': 'can_view_history',
    'can_view_decision_ledger': 'can_view_history',
    'can_manage_clients': 'can_manage_directory',
    'can_manage_suppliers': 'can_manage_directory',
    'can_manage_materials': 'can_manage_directory',
    'can_manage_delivery_persons': 'can_manage_directory',
}

ENDPOINT_PERMISSION_MAP = {
    'index': 'can_view_dashboard',
    'grn': 'can_manage_grn',
    'edit_grn': 'can_manage_grn',
    'export_grn': 'can_manage_grn',
    'bookings_page': 'can_manage_bookings',
    'add_booking': 'can_manage_bookings',
    'client_booking_cancel': 'can_manage_bookings',
    'client_booking_cancel_revert': 'can_manage_bookings',
    'edit_booking': 'can_manage_bookings',
    'payments_page': 'can_manage_payments',
    'add_payment': 'can_manage_payments',
    'edit_payment': 'can_manage_payments',
    'direct_sales_page': 'can_manage_sales',
    'add_direct_sale': 'can_manage_sales',
    'add_sale': 'can_manage_sales',
    'edit_direct_sale': 'can_manage_sales',
    'void_transaction': 'can_manage_sales',
    'unvoid_transaction': 'can_manage_sales',
    'delete_bill': 'can_manage_sales',
    'view_bill': 'can_view_history',
    'download_invoice': 'can_view_history',
    'view_bill_detail': 'can_view_history',
    'dispatching': 'can_view_daily',
    'add_record': 'can_view_daily',
    'edit_entry': 'can_view_daily',
    'delete_entry': 'can_view_daily',
    'import_dispatch_data': 'can_view_daily',
    'tracking': 'can_view_history',
    'ledger_page': 'can_view_client_ledger',
    'financial_ledger': 'can_view_client_ledger',
    'financial_ledger_details': 'can_view_client_ledger',
    'client_ledger': 'can_view_client_ledger',
    'download_client_ledger': 'can_view_client_ledger',
    'api_client_booking_status': 'can_view_client_ledger',
    'api_client_financial_summary': 'can_view_client_ledger',
    'decision_ledger': 'can_view_decision_ledger',
    'clients': 'can_view_client_ledger',
    'add_client': 'can_manage_clients',
    'edit_client': 'can_manage_clients',
    'delete_client': 'can_manage_clients',
    'transfer_client': 'can_manage_clients',
    'reclaim_client': 'can_manage_clients',
    'client_toggle_active': 'can_manage_clients',
    'activate_all_clients': 'can_manage_clients',
    'suppliers': 'can_view_supplier_ledger',
    'supplier_ledger': 'can_view_supplier_ledger',
    'api_supplier_balance': 'can_view_supplier_ledger',
    'add_supplier': 'can_manage_suppliers',
    'edit_supplier': 'can_manage_suppliers',
    'delete_supplier': 'can_manage_suppliers',
    'add_supplier_payment': 'can_manage_suppliers',
    'edit_supplier_payment': 'can_manage_suppliers',
    'delete_supplier_payment': 'can_manage_suppliers',
    'restore_supplier_payment': 'can_manage_suppliers',
    'delivery_rents_page': 'can_view_delivery_rent',
    'void_delivery_rent': 'can_manage_sales',
    'delivery_persons_page': 'can_manage_delivery_persons',
    'add_delivery_person': 'can_manage_delivery_persons',
    'toggle_delivery_person': 'can_manage_delivery_persons',
    'edit_delivery_person': 'can_manage_delivery_persons',
    'materials': 'can_manage_materials',
    'api_material_next_code': 'can_manage_materials',
    'merge_materials': 'can_manage_materials',
    'add_material': 'can_manage_materials',
    'edit_material': 'can_manage_materials',
    'bulk_update_material_unit': 'can_manage_materials',
    'add_material_category': 'can_manage_materials',
    'rename_material_category': 'can_manage_materials',
    'toggle_material_category': 'can_manage_materials',
    'delete_material': 'can_manage_materials',
    'activate_all_materials': 'can_manage_materials',
    'pending_bills': 'can_manage_pending_bills',
    'add_pending_bill': 'can_manage_pending_bills',
    'edit_pending_bill': 'can_manage_pending_bills',
    'delete_pending_bill': 'can_manage_pending_bills',
    'toggle_bill_paid': 'can_manage_pending_bills',
    'export_pending_bills': 'can_manage_pending_bills',
    'import_pending_bills': 'can_manage_pending_bills',
    'unpaid_transactions_page': 'can_view_reports',
    'export_unpaid_transactions': 'can_view_reports',
    'financial_details': 'can_view_reports',
    'profit_reports': 'can_view_reports',
    'mixed_transactions': 'can_view_reports',
    'ams_assistant_page': 'can_view_reports',
    'ams_assistant_chat_api': 'can_view_reports',
    'ams_assistant_export_api': 'can_view_reports',
    'notifications_page': 'can_manage_notifications',
    'notifications_upcoming': 'can_manage_notifications',
    'notifications_add_email': 'can_manage_notifications',
    'notifications_toggle_email': 'can_manage_notifications',
    'notifications_delete_email': 'can_manage_notifications',
    'notifications_set_reminder': 'can_manage_notifications',
    'notifications_log_contact': 'can_manage_notifications',
    'notifications_close_reminder': 'can_manage_notifications',
    'notifications_set_severity': 'can_manage_notifications',
    'notifications_bill_detail': 'can_manage_notifications',
    'api_notifications_contact_history': 'can_manage_notifications',
    'notifications_ack_reminder': 'can_manage_notifications',
    'api_notifications_due': 'can_manage_notifications',
    'notifications_send_daily_now': 'can_manage_notifications',
    'settings': 'can_access_settings',
    'change_password': 'can_access_settings',
    'void_audit_page': 'can_access_settings',
    'restore_audit_record': 'can_access_settings',
    'data_lab.upload': 'can_import_export',
    'data_lab.view_basket': 'can_import_export',
    'data_lab.correct_bill': 'can_import_export',
    'data_lab.legacy_import': 'can_import_export',
    'import_export.import_export_page': 'can_import_export',
    'import_export.get_template': 'can_import_export',
    'import_export.preview_import': 'can_import_export',
    'import_export.execute_import': 'can_import_export',
    'import_export.full_raw_export': 'can_import_export',
    'import_export.full_raw_import': 'can_import_export',
    'import_export.export_data': 'can_import_export',
    'import_export.email_file': 'can_import_export',
    'import_export.export_master': 'can_import_export',
    'import_export.export_excel_all': 'can_import_export',
    'import_export.import_master': 'can_import_export',
    'import_export.master_import_start': 'can_import_export',
    'import_export.master_import_status': 'can_import_export',
    'inventory.stock_summary': 'can_view_stock',
    'inventory.daily_transactions': 'can_view_daily',
    'inventory.inventory_log': 'can_view_history',
}


def _user_can(permission_name):
    if not current_user.is_authenticated:
        return False
    if current_user.role in ('admin', 'root'):
        return True

    default_value = USER_PERMISSION_DEFAULTS.get(permission_name, False)
    val = getattr(current_user, permission_name, None)
    if val is None:
        legacy_name = PERMISSION_LEGACY_FALLBACKS.get(permission_name)
        if legacy_name:
            legacy_val = getattr(current_user, legacy_name, None)
            if legacy_val is not None:
                return bool(legacy_val)
        return bool(default_value)
    return bool(val)


@app.before_request
def _enforce_user_permissions():
    if not current_user.is_authenticated:
        return None
    if current_user.role in ('admin', 'root'):
        return None
    endpoint = request.endpoint or ''
    needed = ENDPOINT_PERMISSION_MAP.get(endpoint)
    if not needed:
        return None
    if _user_can(needed):
        return None
    flash('Permission denied for this module.', 'danger')
    return redirect(url_for('index'))

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


def generate_material_code():
    """Generate next material code in format tmpm-00001"""
    last_material = Material.query.filter(
        Material.code.like('tmpm-%')).order_by(Material.code.desc()).first()
    if last_material and last_material.code:
        try:
            num = int(last_material.code.split('-')[1]) + 1
        except:
            num = 1
    else:
        num = 1
    return f"tmpm-{num:05d}"


def _material_category_code_segment(category):
    """Return category segment for material code (e.g., CEM, ST)."""
    raw_name = ((category.name if category else 'General') or 'General').strip().upper()
    normalized = re.sub(r'[^A-Z0-9]+', '', raw_name)
    if not normalized:
        return 'GEN'

    static_map = {
        'CEMENT': 'CEM',
        'STEEL': 'ST',
    }
    if normalized in static_map:
        return static_map[normalized]

    words = [w for w in re.split(r'[^A-Z0-9]+', raw_name) if w]
    if len(words) >= 2:
        return ''.join(w[0] for w in words[:3]).upper()
    return normalized[:3].upper()


def _material_code_profile(material_name):
    """Return company prefix and serial width based on material name."""
    nm = (material_name or '').strip().upper()
    if nm.startswith('FT-'):
        return ('FTP', 4)
    return ('FBM', 6)


def _next_material_code_for_category(category, material_name=''):
    """Generate next category-based code like FBMCEM-000000 or FTPCEM-0000."""
    company_prefix, serial_width = _material_code_profile(material_name)
    cat_segment = _material_category_code_segment(category)
    prefix = f"{company_prefix}{cat_segment}"
    max_num = 0
    code_rx = re.compile(rf"^{re.escape(prefix)}-(\d+)$", re.IGNORECASE)

    q = Material.query
    if category and getattr(category, 'id', None):
        q = q.filter(Material.category_id == category.id)

    for mat in q.with_entities(Material.code).all():
        code = (mat[0] or '').strip()
        match = code_rx.match(code)
        if not match:
            continue
        try:
            max_num = max(max_num, int(match.group(1)))
        except Exception:
            continue

    return f"{prefix}-{(max_num + 1):0{serial_width}d}"


def _get_default_material_category_id():
    try:
        cat = get_or_create_material_category(current_user.tenant_id, 'General')
        return cat.id if cat else None
    except Exception:
        return None


AUTO_BILL_NS_DEFAULT = 'GEN'
AUTO_BILL_NAMESPACES = {
    'BOOKING': 'BK',
    'PAYMENT': 'CP',
    'SUPPLIER_PAYMENT': 'SP',
    'DIRECT_SALE': 'SL',
    'GRN': 'GRN',
    'ENTRY': 'EN',
}


def _normalize_namespace(namespace):
    ns = (namespace or AUTO_BILL_NS_DEFAULT).strip().upper()
    if not ns:
        ns = AUTO_BILL_NS_DEFAULT
    if not re.fullmatch(r'[A-Z][A-Z0-9]{1,7}', ns):
        ns = AUTO_BILL_NS_DEFAULT
    return ns


def _extract_sb_parts(value):
    raw = (value or '').strip()
    if not raw:
        return (None, None)
    txt = raw.upper()
    if txt.startswith('MB NO.'):
        return (None, None)

    m = re.match(r'^SB\s*-\s*([A-Z][A-Z0-9]{1,7})\s*-\s*(\d+)$', txt)
    if m:
        return (_normalize_namespace(m.group(1)), int(m.group(2)))

    body = raw
    if txt.startswith('SB NO.'):
        body = raw.split('.', 1)[1].strip() if '.' in raw else ''
    elif txt.startswith('SB '):
        body = raw[2:].strip()
    elif txt.startswith('AUTO '):
        body = raw[5:].strip()
        body_up = body.upper()
        if body_up.startswith('SB NO.'):
            body = body.split('.', 1)[1].strip() if '.' in body else ''
        elif body_up.startswith('SB '):
            body = body[2:].strip()

    if body.startswith('#'):
        body = body[1:].strip()
    if re.fullmatch(r'\d+\.0+', body or ''):
        body = body.split('.', 1)[0]
    if re.fullmatch(r'\d+', body or ''):
        return (None, int(body))
    return (None, None)


def _extract_sb_seq(value, namespace=None):
    parsed_ns, seq = _extract_sb_parts(value)
    if seq is None:
        return None
    if namespace and parsed_ns and _normalize_namespace(namespace) != parsed_ns:
        return None
    return seq


def parse_bill_namespace(value):
    parsed_ns, seq = _extract_sb_parts(value)
    if seq is None:
        return None
    return parsed_ns


def _bill_counter_sources():
    return [
        (Booking, 'auto_bill_no', AUTO_BILL_NAMESPACES['BOOKING']),
        (Payment, 'auto_bill_no', AUTO_BILL_NAMESPACES['PAYMENT']),
        (SupplierPayment, 'auto_bill_no', AUTO_BILL_NAMESPACES['SUPPLIER_PAYMENT']),
        (DirectSale, 'auto_bill_no', AUTO_BILL_NAMESPACES['DIRECT_SALE']),
        (GRN, 'auto_bill_no', AUTO_BILL_NAMESPACES['GRN']),
        (Entry, 'auto_bill_no', AUTO_BILL_NAMESPACES['ENTRY']),
    ]


def _get_or_create_bill_counter(namespace=AUTO_BILL_NS_DEFAULT):
    ns = _normalize_namespace(namespace)
    counter = BillCounter.query.filter_by(namespace=ns).first()
    if not counter:
        counter = BillCounter(count=1000, namespace=ns)
        db.session.add(counter)
        db.session.flush()
    return counter


def _max_used_auto_bill_seq(namespace=AUTO_BILL_NS_DEFAULT):
    ns = _normalize_namespace(namespace)
    max_seq = 0
    for model, col, source_ns in _bill_counter_sources():
        if source_ns != ns:
            continue
        rows = model.query.with_entities(getattr(model, col)).all()
        for (ref,) in rows:
            parsed_ns, seq = _extract_sb_parts(ref)
            if seq is None:
                continue
            if parsed_ns and parsed_ns != ns:
                continue
            if seq > max_seq:
                max_seq = seq
    return max_seq


def _sync_bill_counter_with_db(namespace=AUTO_BILL_NS_DEFAULT):
    """
    Keep module-scoped auto-bill counter ahead of all existing SB refs.
    This protects against collisions after imports/manual DB changes.
    """
    ns = _normalize_namespace(namespace)
    counter = _get_or_create_bill_counter(ns)
    current = int(counter.count or 1000)
    max_used = _max_used_auto_bill_seq(ns)
    required_next = max(1000, max_used + 1)
    if current < required_next:
        counter.count = required_next
        db.session.flush()
        return required_next
    return current


def parse_bill_kind(value):
    txt = (value or '').strip().upper()
    if not txt:
        return 'UNKNOWN'
    if txt.startswith('SB NO.') or txt.startswith('SB-'):
        return 'SB'
    if txt.startswith('MB NO.'):
        return 'MB'
    _, seq = _extract_sb_parts(txt)
    if seq is not None:
        return 'SB'
    return 'UNKNOWN'


def normalize_auto_bill(value, namespace=AUTO_BILL_NS_DEFAULT):
    raw = (value or '').strip()
    if not raw:
        return ''
    ns_default = _normalize_namespace(namespace)
    parsed_ns, seq = _extract_sb_parts(raw)
    if seq is None:
        return ''
    ns = parsed_ns or ns_default
    return f"SB-{ns}-{int(seq)}"


def normalize_manual_bill(value):
    raw = (value or '').strip()
    if not raw:
        return ''
    upper = raw.upper()
    if upper.startswith('MB NO.'):
        body = raw.split('.', 1)[1].strip() if '.' in raw else ''
    elif upper.startswith('SB NO.'):
        body = raw.split('.', 1)[1].strip() if '.' in raw else ''
    else:
        body = raw
    if body.startswith('#'):
        body = body[1:].strip()
    if re.fullmatch(r'\d+\.0+', body or ''):
        body = body.split('.', 1)[0]
    if not body:
        return ''
    if re.fullmatch(r'\d+', body):
        body = str(int(body))
    return f"MB NO.{body}"


def _format_bill_no(count, namespace=AUTO_BILL_NS_DEFAULT):
    return normalize_auto_bill(str(int(count or 0)), namespace=namespace)


def peek_next_bill_no(namespace=AUTO_BILL_NS_DEFAULT):
    ns = _normalize_namespace(namespace)
    current = _sync_bill_counter_with_db(ns)
    return _format_bill_no(current or 1000, namespace=ns)


def get_next_bill_no(namespace=AUTO_BILL_NS_DEFAULT):
    """Generate and increment the next auto bill number."""
    ns = _normalize_namespace(namespace)
    counter = _get_or_create_bill_counter(ns)
    current = _sync_bill_counter_with_db(ns)
    bill_no = _format_bill_no(current, namespace=ns)
    # Auto bills must be globally unique per tenant across main bill-bearing modules.
    while find_bill_conflict(bill_no):
        current += 1
        bill_no = _format_bill_no(current, namespace=ns)
    counter.count = current + 1
    db.session.flush()
    return bill_no


def save_photo(file):
    """Save uploaded photo and return filename"""
    if file and file.filename != '':
        filename = secure_filename(
            f"{pk_now().strftime('%Y%m%d%H%M%S')}_{file.filename}")
        upload_folder = os.path.join(basedir, 'static', 'uploads')
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)
        return filename
    return None


def _ensure_user_password_column():
    """Ensure `password_hash` column exists on `user` table and copy legacy `password` values."""
    try:
        rows = db.session.execute(text("PRAGMA table_info('user')")).fetchall()
        cols = [r[1] for r in rows]
        if 'password_hash' not in cols:
            db.session.execute(text("ALTER TABLE user ADD COLUMN password_hash VARCHAR(200);"))
            if 'password' in cols:
                db.session.execute(text("UPDATE user SET password_hash = password WHERE password_hash IS NULL;"))
            db.session.commit()
    except Exception:
        db.session.rollback()


def _ensure_model_columns():
    """Add any missing columns declared in models but missing in the DB."""
    from sqlalchemy import String, Integer, Float, Date, DateTime, Boolean, Text, Boolean

    try:
        for table in db.metadata.sorted_tables:
            rows = db.session.execute(text(f"PRAGMA table_info('{table.name}')")).fetchall()
            existing_cols = [r[1] for r in rows]
            for col in table.columns:
                if col.name not in existing_cols:
                    coltype = col.type
                    sqltype = 'VARCHAR(200)'
                    if isinstance(coltype, (String, Text)):
                        sqltype = 'VARCHAR(200)'
                    elif isinstance(coltype, (Integer, Boolean)) or str(coltype) == 'BOOLEAN':
                        sqltype = 'INTEGER'
                    elif isinstance(coltype, Float):
                        sqltype = 'REAL'
                    elif isinstance(coltype, Date):
                        sqltype = 'DATE'
                    elif isinstance(coltype, DateTime):
                        sqltype = 'DATETIME'

                    try:
                        db.session.execute(text(f"ALTER TABLE {table.name} ADD COLUMN {col.name} {sqltype};"))
                    except Exception:
                        db.session.rollback()
        db.session.commit()
    except Exception:
        db.session.rollback()

def _ensure_material_categories():
    try:
        tenant_ids = [r[0] for r in db.session.query(Material.tenant_id).distinct().all()]
        if not tenant_ids:
            return
        for tenant_id in tenant_ids:
            default_cat = get_or_create_material_category(tenant_id, 'General')
            if tenant_id is None:
                mats = Material.query.filter(Material.tenant_id.is_(None), Material.category_id.is_(None)).all()
                assigned = Material.query.filter(Material.tenant_id.is_(None), Material.category_id.isnot(None)).all()
            else:
                mats = Material.query.filter(Material.tenant_id == tenant_id, Material.category_id.is_(None)).all()
                assigned = Material.query.filter(Material.tenant_id == tenant_id, Material.category_id.isnot(None)).all()
            for m in mats:
                m.category_id = default_cat.id
            valid_ids = {c.id for c in MaterialCategory.query.filter_by(tenant_id=tenant_id).all()}
            for m in assigned:
                if m.category_id not in valid_ids:
                    m.category_id = default_cat.id
        db.session.commit()
    except Exception:
        db.session.rollback()

def _ensure_discount_columns():
    """Ensure discount and discount_reason columns exist on relevant tables."""
    tables = {
        'direct_sale': ['discount', 'discount_reason'],
        'booking': ['discount', 'discount_reason'],
        'payment': ['discount', 'discount_reason']
    }
    try:
        for table, cols in tables.items():
            rows = db.session.execute(text(f"PRAGMA table_info('{table}')")).fetchall()
            existing = [r[1] for r in rows]
            for col in cols:
                if col not in existing:
                    col_type = 'REAL DEFAULT 0' if col == 'discount' else 'VARCHAR(200)'
                    db.session.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {col_type};"))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _ensure_bill_counter_namespace_defaults():
    """Backfill namespace for legacy bill_counter rows after schema upgrades."""
    try:
        rows = db.session.execute(text("PRAGMA table_info('bill_counter')")).fetchall()
        existing = {r[1] for r in rows}
        if 'namespace' not in existing:
            return
        db.session.execute(text(
            "UPDATE bill_counter SET namespace = 'GEN' "
            "WHERE namespace IS NULL OR TRIM(namespace) = ''"
        ))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _ensure_waive_off_table():
    """Ensure dedicated waive_off table exists for loss/write-off events."""
    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS waive_off (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id VARCHAR(36),
                payment_id INTEGER,
                client_code VARCHAR(50),
                client_name VARCHAR(100),
                bill_no VARCHAR(50),
                amount REAL DEFAULT 0,
                reason VARCHAR(300),
                date_posted DATETIME,
                created_by VARCHAR(80),
                note VARCHAR(500),
                is_void INTEGER DEFAULT 0
            )
        """))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _backfill_legacy_payment_discounts_to_waive_off():
    """
    Backfill legacy Payment.discount values into waive_off rows.
    Keep Payment.discount for compatibility; downstream logic avoids double counting.
    """
    try:
        existing_payment_ids = {
            r[0] for r in WaiveOff.query.filter(
                WaiveOff.payment_id.isnot(None),
                WaiveOff.is_void == False
            ).with_entities(WaiveOff.payment_id).distinct().all()
            if r and r[0] is not None
        }
        legacy_rows = Payment.query.filter(
            Payment.is_void == False,
            Payment.discount > 0
        ).all()

        for pay in legacy_rows:
            if pay.id in existing_payment_ids:
                continue
            client_obj = get_client_by_input(pay.client_name or '')
            bill_ref = (pay.manual_bill_no or pay.auto_bill_no or f"PAY-{pay.id}")
            db.session.add(WaiveOff(
                payment_id=pay.id,
                client_code=(client_obj.code if client_obj else None),
                client_name=(client_obj.name if client_obj else pay.client_name),
                bill_no=bill_ref,
                amount=float(pay.discount or 0),
                reason=(pay.discount_reason or 'Legacy waive-off migration'),
                date_posted=pay.date_posted or pk_now(),
                created_by=None,
                note=pay.note,
                is_void=bool(pay.is_void)
            ))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _ensure_user_permission_defaults():
    """Backfill NULL permission values so newly added columns remain usable."""
    try:
        rows = db.session.execute(text("PRAGMA table_info('user')")).fetchall()
        existing = {r[1] for r in rows}
        for col, default_value in USER_PERMISSION_DEFAULTS.items():
            if col in existing:
                db.session.execute(
                    text(f'UPDATE "user" SET {col} = :v WHERE {col} IS NULL'),
                    {'v': 1 if default_value else 0}
                )
        db.session.commit()
    except Exception:
        db.session.rollback()


def _bootstrap_database():
    db.create_all()
    try:
        _ensure_user_password_column()
    except Exception:
        pass
    try:
        _ensure_model_columns()
    except Exception:
        pass
    try:
        _ensure_material_categories()
    except Exception:
        pass
    try:
        _ensure_discount_columns()
    except Exception:
        pass
    try:
        _ensure_bill_counter_namespace_defaults()
    except Exception:
        pass
    try:
        _ensure_waive_off_table()
    except Exception:
        pass
    try:
        _backfill_legacy_payment_discounts_to_waive_off()
    except Exception:
        pass
    try:
        _ensure_user_permission_defaults()
    except Exception:
        pass
    try:
        bootstrap_tenancy()
    except Exception:
        db.session.rollback()

def get_client_by_input(input_str):
    """Helper to find client by name, code, or mixed string."""
    if not input_str:
        return None
    input_str = input_str.strip()

    # 1. Exact Code Match
    client = Client.query.filter_by(code=input_str).first()
    if client: return client

    # 2. Exact Name Match
    client = Client.query.filter_by(name=input_str).first()
    if client: return client

    # 3. Try to extract code from format "Name (Code)" or "Code - Name"
    match = re.search(r'\((tmpc-\d+|FBM-\d+|FBMCL-\d+)\)$', input_str, re.IGNORECASE)
    if match:
        code = match.group(1)
        client = Client.query.filter_by(code=code).first()
        if client: return client

    # 4. Case-insensitive Code/Name
    client = Client.query.filter(or_(Client.code.ilike(input_str), Client.name.ilike(input_str))).first()
    if client: return client

    return None

def get_material_by_input(input_str):
    """Helper to find material by name or code."""
    if not input_str:
        return None
    input_str = input_str.strip()

    mat = Material.query.filter(or_(Material.name == input_str, Material.code == input_str)).first()
    if mat: return mat

    mat = Material.query.filter(or_(Material.name.ilike(input_str), Material.code.ilike(input_str))).first()
    return mat

def get_supplier_by_input(input_str):
    """Helper to find supplier by name."""
    if not input_str:
        return None
    input_str = input_str.strip()
    return Supplier.query.filter(func.lower(Supplier.name) == input_str.lower()).first()


def _client_waive_off_total(client_name_norm, cutoff_dt=None):
    """Return total waive-off(loss) for a client, with legacy fallback from Payment.discount."""
    if not client_name_norm:
        return 0.0

    waive_q = WaiveOff.query.filter(
        func.lower(func.trim(WaiveOff.client_name)) == client_name_norm,
        WaiveOff.is_void == False
    )
    # DirectSale discounts are accounted from DirectSale.discount separately.
    waive_q = waive_q.filter(
        ~func.lower(func.coalesce(WaiveOff.note, '')).like('[direct_sale_discount:%')
    )
    # Ignore orphan rows that reference a deleted payment.
    waive_q = waive_q.filter(
        or_(
            WaiveOff.payment_id.is_(None),
            exists().where(and_(Payment.id == WaiveOff.payment_id, Payment.tenant_id == WaiveOff.tenant_id))
        )
    )
    if cutoff_dt:
        waive_q = waive_q.filter(WaiveOff.date_posted <= cutoff_dt)
    waive_total = float(waive_q.with_entities(func.sum(WaiveOff.amount)).scalar() or 0)

    represented_payment_ids = {
        r[0] for r in waive_q.filter(WaiveOff.payment_id.isnot(None))
        .with_entities(WaiveOff.payment_id).distinct().all()
        if r and r[0] is not None
    }

    legacy_payments_q = Payment.query.filter(
        func.lower(func.trim(Payment.client_name)) == client_name_norm,
        Payment.is_void == False,
        Payment.discount > 0
    )
    if cutoff_dt:
        legacy_payments_q = legacy_payments_q.filter(Payment.date_posted <= cutoff_dt)

    legacy_total = 0.0
    for p in legacy_payments_q.all():
        if p.id in represented_payment_ids:
            continue
        legacy_total += float(p.discount or 0)

    return waive_total + legacy_total


def _sync_payment_waive_off(payment):
    """Keep dedicated waive_off rows in sync with Payment.discount for phase-2 rollout."""
    if not payment:
        return

    amount = float(getattr(payment, 'discount', 0) or 0)
    existing_rows = WaiveOff.query.filter_by(payment_id=payment.id).all()

    if amount <= 0:
        for row in existing_rows:
            db.session.delete(row)
        return

    client_obj = get_client_by_input(payment.client_name or '')
    bill_ref = (payment.manual_bill_no or payment.auto_bill_no or f"PAY-{payment.id}")
    reason = (payment.discount_reason or '').strip() or 'Payment waive-off (loss)'

    if existing_rows:
        row = existing_rows[0]
        row.client_code = (client_obj.code if client_obj else row.client_code)
        row.client_name = (client_obj.name if client_obj else payment.client_name)
        row.bill_no = bill_ref
        row.amount = amount
        row.reason = reason
        row.date_posted = payment.date_posted or pk_now()
        row.note = payment.note
        row.is_void = bool(payment.is_void)
        for extra in existing_rows[1:]:
            db.session.delete(extra)
    else:
        db.session.add(WaiveOff(
            payment_id=payment.id,
            client_code=(client_obj.code if client_obj else None),
            client_name=(client_obj.name if client_obj else payment.client_name),
            bill_no=bill_ref,
            amount=amount,
            reason=reason,
            date_posted=payment.date_posted or pk_now(),
            created_by=(current_user.username if current_user and current_user.is_authenticated else None),
            note=payment.note,
            is_void=bool(payment.is_void)
        ))


def _direct_sale_waive_marker(sale_id):
    return f"[direct_sale_discount:{sale_id}]"


def _sync_direct_sale_waive_off(sale):
    """Keep dedicated waive_off rows in sync with DirectSale.discount."""
    if not sale:
        return

    marker = _direct_sale_waive_marker(sale.id)
    existing_rows = WaiveOff.query.filter(
        WaiveOff.payment_id.is_(None),
        WaiveOff.note == marker
    ).all()

    amount = max(0.0, float(getattr(sale, 'discount', 0) or 0))
    if amount <= 0:
        for row in existing_rows:
            db.session.delete(row)
        return

    client_name = (getattr(sale, 'client_name', '') or '').strip()
    client_obj = get_client_by_input(client_name) if client_name else None
    client_code = client_obj.code if client_obj else (OPEN_KHATA_CODE if normalize_sale_category(getattr(sale, 'category', None)) == 'Open Khata' else None)
    client_display_name = client_obj.name if client_obj else client_name
    bill_ref = _direct_sale_default_bill_ref(sale)
    reason = (getattr(sale, 'discount_reason', '') or '').strip() or 'Direct sale waive-off (loss)'

    created_by = None
    try:
        if current_user and current_user.is_authenticated:
            created_by = current_user.username
    except Exception:
        created_by = None

    if existing_rows:
        row = existing_rows[0]
        row.client_code = client_code
        row.client_name = client_display_name
        row.bill_no = bill_ref
        row.amount = amount
        row.reason = reason
        row.date_posted = sale.date_posted or pk_now()
        row.note = marker
        row.is_void = bool(sale.is_void)
        for extra in existing_rows[1:]:
            db.session.delete(extra)
    else:
        db.session.add(WaiveOff(
            payment_id=None,
            client_code=client_code,
            client_name=client_display_name,
            bill_no=bill_ref,
            amount=amount,
            reason=reason,
            date_posted=sale.date_posted or pk_now(),
            created_by=created_by,
            note=marker,
            is_void=bool(sale.is_void)
        ))


def _generate_root_recovery_codes_plain(count=10):
    alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    codes = []
    for _ in range(max(1, int(count or 1))):
        chunks = [''.join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3)]
        codes.append('-'.join(chunks))
    return codes


def _create_root_recovery_codes(username='root', count=10, note=''):
    # Rotate: remove unused codes before creating a fresh set.
    RootRecoveryCode.query.filter(
        RootRecoveryCode.username == username,
        RootRecoveryCode.used_at.is_(None)
    ).delete(synchronize_session=False)

    plain_codes = _generate_root_recovery_codes_plain(count=count)
    for code in plain_codes:
        db.session.add(RootRecoveryCode(
            username=username,
            code_hash=generate_password_hash(code),
            generated_by=(current_user.username if current_user.is_authenticated else None),
            note=note or ''
        ))
    db.session.flush()
    return plain_codes


def _consume_root_recovery_code(username, code_plain):
    candidates = RootRecoveryCode.query.filter(
        RootRecoveryCode.username == username,
        RootRecoveryCode.used_at.is_(None)
    ).order_by(RootRecoveryCode.created_at.desc()).all()
    for row in candidates:
        try:
            if check_password_hash(row.code_hash, code_plain):
                row.used_at = pk_now()
                return row
        except Exception:
            continue
    return None


def _compute_client_financial_summary(client):
    """Return a lightweight financial summary for decision-making (balance, totals)."""
    if not client:
        return {
            'balance': 0,
            'debit_total': 0,
            'credit_total': 0,
            'cash_received_total': 0,
            'waive_off_total': 0,
            'status': 'settled'
        }
    client_name_norm = (client.name or '').strip().lower()

    b_debit = db.session.query(func.sum(Booking.amount)).filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False
    ).scalar() or 0
    b_credit = db.session.query(func.sum(Booking.paid_amount)).filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False
    ).scalar() or 0
    p_credit = db.session.query(func.sum(Payment.amount)).filter(
        func.lower(func.trim(Payment.client_name)) == client_name_norm,
        Payment.is_void == False,
        Payment.amount >= 0
    ).scalar() or 0
    p_debit = db.session.query(func.sum(-Payment.amount)).filter(
        func.lower(func.trim(Payment.client_name)) == client_name_norm,
        Payment.is_void == False,
        Payment.amount < 0
    ).scalar() or 0
    ds_debit = db.session.query(func.sum(DirectSale.amount)).filter(
        func.lower(func.trim(DirectSale.client_name)) == client_name_norm,
        DirectSale.is_void == False
    ).scalar() or 0
    ds_credit = db.session.query(func.sum(DirectSale.paid_amount)).filter(
        func.lower(func.trim(DirectSale.client_name)) == client_name_norm,
        DirectSale.is_void == False
    ).scalar() or 0
    
    b_discount = 0
    try:
        b_discount = db.session.query(func.sum(Booking.discount)).filter(
            func.lower(func.trim(Booking.client_name)) == client_name_norm,
            Booking.is_void == False
        ).scalar() or 0
    except Exception:
        pass

    p_discount = 0
    try:
        p_discount = _client_waive_off_total(client_name_norm)
    except Exception:
        pass

    ds_discount = 0
    try:
        ds_discount = db.session.query(func.sum(DirectSale.discount)).filter(
            func.lower(func.trim(DirectSale.client_name)) == client_name_norm,
            DirectSale.is_void == False
        ).scalar() or 0
    except Exception:
        pass

    opening_balance = _to_float_or_zero(getattr(client, 'opening_balance', 0))
    opening_debit = opening_balance if opening_balance > 0 else 0
    opening_credit = abs(opening_balance) if opening_balance < 0 else 0

    debit_total = (opening_debit + b_debit + ds_debit + p_debit)
    cash_received_total = (opening_credit + b_credit + p_credit + ds_credit)
    waive_off_total = (ds_discount + b_discount + p_discount)
    credit_total = cash_received_total + waive_off_total
    balance = debit_total - credit_total
    status = 'debit' if balance > 0 else ('credit' if balance < 0 else 'settled')

    return {
        'balance': balance,
        'debit_total': debit_total,
        'credit_total': credit_total,
        'cash_received_total': cash_received_total,
        'waive_off_total': waive_off_total,
        'status': status
    }


def _parse_ledger_entry_dt(date_val, time_val=None):
    """Parse Entry(date,time) to datetime for stable ledger ordering."""
    if isinstance(date_val, datetime):
        return date_val
    if isinstance(date_val, date):
        return datetime.combine(date_val, datetime.min.time())
    s_date = str(date_val or '').strip()
    s_time = str(time_val or '').strip()
    try:
        if s_date and s_time:
            return datetime.strptime(f"{s_date} {s_time}", '%Y-%m-%d %H:%M:%S')
    except Exception:
        pass
    try:
        if s_date:
            return datetime.strptime(s_date, '%Y-%m-%d')
    except Exception:
        pass
    return datetime.min


def _parse_cancel_amount_from_note(note):
    """Extract cancellation amount encoded in note as 'amount=<number>'."""
    text_note = str(note or '')
    m = re.search(r'amount=([-+]?\d+(?:\.\d+)?)', text_note, re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _parse_cancel_rate_from_note(note):
    """Extract cancellation rate encoded in note as 'rate=<number>'."""
    text_note = str(note or '')
    m = re.search(r'rate=([-+]?\d+(?:\.\d+)?)', text_note, re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _resolve_cancel_display_amount(client_name_norm, bill_ref, mat_ref, qty, note):
    """Best-effort cancel amount for ledger display.

    Priority:
    1) Matched booking item rate (case-insensitive material match).
    2) Encoded note rate.
    3) Encoded note amount (legacy fallback).
    """
    try:
        q = float(qty or 0)
    except Exception:
        q = 0
    if q <= 0:
        return None

    bill = (bill_ref or '').strip()
    mat = (mat_ref or '').strip()

    if bill and mat:
        bk = Booking.query.filter(
            func.lower(func.trim(Booking.client_name)) == client_name_norm,
            or_(Booking.manual_bill_no == bill, Booking.auto_bill_no == bill)
        ).order_by(Booking.id.desc()).first()
        if bk:
            bi = BookingItem.query.filter(
                BookingItem.booking_id == bk.id,
                func.lower(func.trim(BookingItem.material_name)) == mat.lower()
            ).order_by(BookingItem.id.desc()).first()
            if bi:
                try:
                    unit_rate = float(getattr(bi, 'price_at_time', 0) or 0)
                except Exception:
                    unit_rate = 0
                if unit_rate > 0:
                    return unit_rate * q

    parsed_rate = _parse_cancel_rate_from_note(note)
    if parsed_rate is not None:
        return float(parsed_rate) * q

    return _parse_cancel_amount_from_note(note)


def _booking_ledger_gross_due(booking, cancel_value=0.0, allow_legacy_lift=True):
    """Booking due displayed in ledger (before discount row).

    Legacy-safe rule:
    - Prefer stored booking amount.
    - If discount exists and historical data has amount lower than paid+discount,
      lift due to (paid + discount) so the sequence is:
      booking pending -> discount row -> corrected pending.
    """
    amount = float(getattr(booking, 'amount', 0) or 0) + max(0.0, float(cancel_value or 0))
    paid = float(getattr(booking, 'paid_amount', 0) or 0)
    discount = max(0.0, float(getattr(booking, 'discount', 0) or 0))
    if allow_legacy_lift and discount > 0:
        corrected_due = paid + discount
        if corrected_due > amount:
            return corrected_due
    return amount


def _pending_client_key(client_code, client_name):
    code = (client_code or '').strip()
    if code:
        return f"code:{code}"
    return f"name:{(client_name or '').strip().lower()}"


def _pending_cancel_credit_for_client(client_code, client_name):
    """Ledger-aligned cancel credit pool for a client (from active CANCEL entries)."""
    code = (client_code or '').strip()
    name_norm = (client_name or '').strip().lower()
    if not code and not name_norm:
        return 0.0

    filt = []
    if code:
        filt.append(Entry.client_code == code)
    if name_norm:
        filt.append(func.lower(func.trim(Entry.client)) == name_norm)
    if not filt:
        return 0.0

    cancel_rows = Entry.query.filter(
        or_(*filt),
        Entry.type == 'CANCEL',
        Entry.is_void == False
    ).all()

    total_credit = 0.0
    for ce in cancel_rows:
        qty = float(getattr(ce, 'qty', 0) or 0)
        bill_ref = (getattr(ce, 'bill_no', None) or getattr(ce, 'auto_bill_no', None) or '').strip()
        mat_ref = (getattr(ce, 'material', None) or getattr(ce, 'booked_material', None) or '').strip()
        amt = _resolve_cancel_display_amount(
            client_name_norm=name_norm,
            bill_ref=bill_ref,
            mat_ref=mat_ref,
            qty=qty,
            note=getattr(ce, 'note', None)
        )
        if amt is not None and float(amt) > 0:
            total_credit += float(amt)
    return total_credit


def _compute_pending_effective_amount_map(pending_rows):
    """
    Compute ledger-aligned effective pending amounts by allocating booking-cancel
    credit against unpaid pending bills per client (oldest first).
    """
    if not pending_rows:
        return {}

    effective = {}
    groups = {}
    for pb in pending_rows:
        raw_amt = float(getattr(pb, 'amount', 0) or 0)
        key = _pending_client_key(getattr(pb, 'client_code', ''), getattr(pb, 'client_name', ''))
        groups.setdefault(key, []).append(pb)
        # default fallback
        effective[pb.id] = raw_amt

    for rows in groups.values():
        unpaid_rows = [
            r for r in rows
            if (not bool(getattr(r, 'is_void', False))) and (not bool(getattr(r, 'is_paid', False)))
        ]
        if not unpaid_rows:
            continue

        sample = unpaid_rows[0]
        cancel_credit_pool = _pending_cancel_credit_for_client(
            getattr(sample, 'client_code', ''),
            getattr(sample, 'client_name', '')
        )
        if cancel_credit_pool <= 0:
            continue

        # Apply to oldest unpaid first to mirror running-balance reduction.
        for pb in sorted(unpaid_rows, key=lambda x: int(getattr(x, 'id', 0) or 0)):
            amt = max(0.0, float(getattr(pb, 'amount', 0) or 0))
            if cancel_credit_pool <= 0:
                effective[pb.id] = amt
                continue
            applied = min(amt, cancel_credit_pool)
            effective[pb.id] = amt - applied
            cancel_credit_pool -= applied

    return effective


def get_or_create_delivery_person(name_input, phone=None):
    name = (name_input or '').strip()
    if not name:
        return None
    phone_value = (phone or '').strip()
    existing = DeliveryPerson.query.filter(func.lower(func.trim(DeliveryPerson.name)) == name.lower()).first()
    if existing:
        if not existing.is_active:
            existing.is_active = True
        if phone_value:
            existing.phone = phone_value
        return existing
    # DeliveryPerson.name is globally unique in current schema.
    # Under tenant-scoped ORM filters, a name that exists in another tenant can be invisible here.
    # Use raw SQL fallback to avoid duplicate insert integrity errors.
    global_row = db.session.execute(
        text("SELECT id FROM delivery_person WHERE lower(trim(name)) = :n LIMIT 1"),
        {'n': name.lower()}
    ).fetchone()
    if global_row:
        return None
    dp = DeliveryPerson(name=name, phone=phone_value or None, is_active=True)
    db.session.add(dp)
    db.session.flush()
    return dp

def find_bill_conflict(bill_no, exclude_sale_id=None):
    """Return a tuple (source, id) if bill_no is already used."""
    if not bill_no:
        return None
    base = (bill_no or '').strip()
    if not base:
        return None
    candidates = _bill_no_variants(base)
    if not candidates:
        return None

    # Direct Sales
    q = DirectSale.query.filter(
        DirectSale.is_void == False,
        or_(
            DirectSale.manual_bill_no.in_(candidates),
            DirectSale.auto_bill_no.in_(candidates)
        )
    )
    if exclude_sale_id:
        q = q.filter(DirectSale.id != exclude_sale_id)
    ds = q.first()
    if ds:
        return ("DirectSale", ds.id)

    # Bookings
    bk = Booking.query.filter(
        Booking.is_void == False,
        or_(
            Booking.manual_bill_no.in_(candidates),
            Booking.auto_bill_no.in_(candidates)
        )
    ).first()
    if bk:
        return ("Booking", bk.id)

    # Payments
    pay = Payment.query.filter(
        Payment.is_void == False,
        or_(
            Payment.manual_bill_no.in_(candidates),
            Payment.auto_bill_no.in_(candidates)
        )
    ).first()
    if pay:
        return ("Payment", pay.id)

    # GRN
    grn = GRN.query.filter(
        GRN.is_void == False,
        or_(
            GRN.manual_bill_no.in_(candidates),
            GRN.auto_bill_no.in_(candidates)
        )
    ).first()
    if grn:
        return ("GRN", grn.id)

    # Invoices
    inv = Invoice.query.filter(Invoice.is_void == False, Invoice.invoice_no.in_(candidates)).first()
    if inv:
        return ("Invoice", inv.id)

    # Pending Bills (fallback)
    pb = PendingBill.query.filter(PendingBill.is_void == False, PendingBill.bill_no.in_(candidates)).first()
    if pb:
        return ("PendingBill", pb.id)

    return None


def normalize_sale_category(raw_value, default='Credit Customer'):
    key = (raw_value or '').strip().lower()
    if not key:
        return default
    return _SALE_CATEGORY_ALIASES.get(key, default)


def _direct_sale_default_bill_ref(sale):
    if sale.manual_bill_no:
        return sale.manual_bill_no
    if sale.auto_bill_no:
        return sale.auto_bill_no
    if getattr(sale, 'invoice', None) and sale.invoice and sale.invoice.invoice_no:
        return sale.invoice.invoice_no
    if (sale.category or '') == 'Cash':
        return f"CSH-{sale.id}"
    return f"DS-{sale.id}"


def _direct_sale_bill_refs(sale):
    refs = {f"DS-{sale.id}", f"UNBILLED-{sale.id}", f"CSH-{sale.id}"}
    if sale.manual_bill_no:
        refs.add(sale.manual_bill_no)
    if sale.auto_bill_no:
        refs.add(sale.auto_bill_no)
    if getattr(sale, 'invoice', None) and sale.invoice and sale.invoice.invoice_no:
        refs.add(sale.invoice.invoice_no)
    return [r for r in refs if r]


def _direct_sale_item_category(sale_category, price_at_time):
    cat = normalize_sale_category(sale_category)
    price = float(price_at_time or 0)
    if cat == 'Mixed Transaction':
        return 'Booking Delivery' if price <= 0 else 'Credit Customer'
    if cat == 'Booking Delivery':
        return 'Booking Delivery' if price <= 0 else 'Credit Customer'
    return cat


def _is_rent_material_name(name):
    txt = (name or '').strip().lower()
    if not txt:
        return False
    normalized = txt.replace('-', ' ').replace('_', ' ')
    return 'rent' in normalized


def _material_norm_key(v):
    txt = (v or '').strip().lower()
    return re.sub(r'[^a-z0-9]+', '', txt)


def _client_booking_unit_price_map(client_name=None, client_code=None):
    client = get_client_by_input((client_code or '').strip() or (client_name or '').strip())
    if not client and client_name:
        norm = (client_name or '').strip().lower()
        if norm:
            client = Client.query.filter(func.lower(func.trim(Client.name)) == norm).first()
    if not client:
        return {}

    bookings = Booking.query.filter_by(client_name=client.name, is_void=False).all()
    booking_ids = [b.id for b in bookings]
    if not booking_ids:
        return {}

    latest_price = {}
    latest_price_dt = {}
    for item in BookingItem.query.filter(BookingItem.booking_id.in_(booking_ids)).all():
        raw_mat = (item.material_name or '').strip()
        key = _material_norm_key(raw_mat)
        if not key:
            continue
        bk = item.booking
        bk_dt = bk.date_posted if bk and getattr(bk, 'date_posted', None) else None
        if key not in latest_price_dt or (bk_dt and latest_price_dt[key] and bk_dt > latest_price_dt[key]) or (bk_dt and not latest_price_dt[key]):
            latest_price_dt[key] = bk_dt
            latest_price[key] = float(item.price_at_time or 0)
        elif key not in latest_price:
            latest_price[key] = float(item.price_at_time or 0)
    return latest_price


def _rent_reconciliation_from_items(items, delivery_rent_cost=0, client_name=None, client_code=None):
    booking_rate_map = _client_booking_unit_price_map(client_name=client_name, client_code=client_code)
    rent_revenue = 0.0
    for item in (items or []):
        mat_name = (item.get('product_name') or item.get('name') or '').strip()
        if not _is_rent_material_name(mat_name):
            continue
        qty = float(item.get('qty') or 0)
        rate = float(item.get('price_at_time') or 0)
        if rate <= 0:
            lookup_name = (item.get('booked_material') or mat_name or '').strip()
            rate = float(booking_rate_map.get(_material_norm_key(lookup_name), 0) or 0)
        if qty <= 0 or rate <= 0:
            continue
        rent_revenue += qty * rate
    delivery_cost = max(0.0, float(delivery_rent_cost or 0))
    variance_loss = max(0.0, delivery_cost - rent_revenue)
    return {
        'rent_item_revenue': float(rent_revenue),
        'delivery_rent_cost': float(delivery_cost),
        'rent_variance_loss': float(variance_loss),
    }


def _sync_delivery_rent_for_sale(sale, include_in_bill=False, rent_amount=0, rent_note=''):
    """Upsert delivery-rent ledger row for a sale when an actual rent amount is provided."""
    if not sale:
        return
    include = float(rent_amount or 0) > 0
    row = DeliveryRent.query.filter_by(sale_id=sale.id).order_by(DeliveryRent.id.desc()).first()

    if not include:
        if row:
            db.session.delete(row)
        return

    if not (sale.driver_name or '').strip():
        return

    bill_ref = _direct_sale_default_bill_ref(sale)
    created_by = None
    try:
        if current_user and current_user.is_authenticated:
            created_by = current_user.username
    except Exception:
        created_by = None

    if not row:
        row = DeliveryRent(sale_id=sale.id, created_by=created_by)
        db.session.add(row)
    elif not row.created_by and created_by:
        row.created_by = created_by

    row.delivery_person_name = (sale.driver_name or '').strip()
    row.bill_no = bill_ref
    row.amount = float(rent_amount or 0)
    row.note = (rent_note or '').strip()
    row.date_posted = sale.date_posted or pk_now()
    row.is_void = bool(sale.is_void)


def _void_direct_sale_entries_and_restore_stock(sale, refs=None):
    refs = refs or _direct_sale_bill_refs(sale)
    entries = Entry.query.filter(Entry.bill_no.in_(refs), Entry.nimbus_no == 'Direct Sale').all()
    for e in entries:
        if e.is_void:
            continue
        e.is_void = True
        mat = Material.query.filter_by(name=e.material).first()
        if mat:
            if e.type == 'OUT':
                mat.total = (mat.total or 0) + (e.qty or 0)
            elif e.type == 'IN':
                mat.total = (mat.total or 0) - (e.qty or 0)


def _unvoid_direct_sale_entries_and_apply_stock(sale, refs=None):
    refs = refs or _direct_sale_bill_refs(sale)
    entries = Entry.query.filter(Entry.bill_no.in_(refs), Entry.nimbus_no == 'Direct Sale').all()
    for e in entries:
        if not e.is_void:
            continue
        mat = Material.query.filter_by(name=e.material).first()
        if mat:
            if e.type == 'OUT':
                mat.total = (mat.total or 0) - (e.qty or 0)
            elif e.type == 'IN':
                mat.total = (mat.total or 0) + (e.qty or 0)
        e.is_void = False


def _booking_bill_refs(booking):
    if not booking:
        return []
    refs = [f"BK-{booking.id}"]
    if booking.manual_bill_no:
        refs.append(booking.manual_bill_no)
    if booking.auto_bill_no:
        refs.append(booking.auto_bill_no)
    return [r for r in dict.fromkeys(refs) if r]


def _payment_receipt_refs(payment):
    manual_ref = (getattr(payment, 'manual_bill_no', None) or '').strip()
    if not manual_ref:
        return []
    return _bill_no_variants(manual_ref)


def _set_payment_receipt_pending_bill_void_state(payment, is_void):
    refs = _payment_receipt_refs(payment)
    if not refs:
        return 0

    reason_filter = func.lower(func.coalesce(PendingBill.reason, '')).like('payment received%')
    bill_filter = or_(*[PendingBill.bill_no.ilike(r) for r in refs])
    client_obj = get_client_by_input(payment.client_name or '')
    if client_obj:
        client_filter = or_(
            PendingBill.client_code == client_obj.code,
            func.lower(func.coalesce(PendingBill.client_name, '')) == client_obj.name.lower(),
            func.coalesce(PendingBill.client_code, '') == ''
        )
    else:
        client_filter = func.lower(func.coalesce(PendingBill.client_name, '')) == (payment.client_name or '').strip().lower()

    q = PendingBill.query.filter(reason_filter, bill_filter, client_filter)
    if is_void:
        q = q.filter(PendingBill.is_void == False)
    rows = q.all()
    for pb in rows:
        pb.is_void = bool(is_void)
    return len(rows)


def _set_entry_void_state(entry, is_void):
    if not entry:
        return False
    target = bool(is_void)
    if entry.is_void == target:
        return False

    mat = Material.query.filter_by(name=entry.material).first()
    if mat:
        qty = float(entry.qty or 0)
        if target:
            if entry.type == 'IN':
                mat.total = (mat.total or 0) - qty
            elif entry.type == 'OUT':
                mat.total = (mat.total or 0) + qty
        else:
            if entry.type == 'IN':
                mat.total = (mat.total or 0) + qty
            elif entry.type == 'OUT':
                mat.total = (mat.total or 0) - qty

    entry.is_void = target
    return True


def _set_direct_sale_void_state(sale, is_void):
    if not sale:
        return False
    target = bool(is_void)
    if sale.is_void == target:
        return False

    sale.is_void = target
    refs = _direct_sale_bill_refs(sale)
    if target:
        _void_direct_sale_entries_and_restore_stock(sale, refs=refs)
    else:
        _unvoid_direct_sale_entries_and_apply_stock(sale, refs=refs)
    PendingBill.query.filter(PendingBill.bill_no.in_(refs)).update({'is_void': target}, synchronize_session=False)
    DeliveryRent.query.filter_by(sale_id=sale.id).update({'is_void': target}, synchronize_session=False)
    _sync_direct_sale_waive_off(sale)
    return True


def _set_booking_void_state(booking, is_void):
    if not booking:
        return False
    target = bool(is_void)
    if booking.is_void == target:
        return False
    booking.is_void = target
    refs = _booking_bill_refs(booking)
    PendingBill.query.filter(PendingBill.bill_no.in_(refs)).update({'is_void': target}, synchronize_session=False)
    # Keep booking-cancel audit rows in the same lifecycle as their parent booking.
    if refs:
        Entry.query.filter(
            Entry.type == 'CANCEL',
            Entry.bill_no.in_(refs)
        ).update({'is_void': target}, synchronize_session=False)
    return True


def _set_payment_void_state(payment, is_void):
    if not payment:
        return False
    target = bool(is_void)
    if payment.is_void == target:
        return False
    payment.is_void = target
    _set_payment_receipt_pending_bill_void_state(payment, is_void=target)
    WaiveOff.query.filter_by(payment_id=payment.id).update({'is_void': target}, synchronize_session=False)
    return True


def _sync_direct_sale_pending_bill(sale, primary_material='', extra_void_refs=None):
    category = normalize_sale_category(sale.category)
    sale.category = category
    discount = float(getattr(sale, 'discount', 0) or 0)
    pending_amount = max(0.0, float(sale.amount or 0) - discount - float(sale.paid_amount or 0))

    client_obj = get_client_by_input((sale.client_name or '').strip())
    client_code = client_obj.code if client_obj else None
    client_name = client_obj.name if client_obj else sale.client_name

    if category == 'Open Khata':
        client_code = OPEN_KHATA_CODE
        if not client_name:
            client_name = OPEN_KHATA_NAME

    refs = set(_direct_sale_bill_refs(sale))
    for r in (extra_void_refs or []):
        if r:
            refs.add(r)
    refs = list(refs)
    reason_filter = func.lower(func.coalesce(PendingBill.reason, '')).like('direct sale%')
    PendingBill.query.filter(
        PendingBill.is_void == False,
        PendingBill.bill_no.in_(refs),
        reason_filter
    ).update({'is_void': True}, synchronize_session=False)

    bill_ref = _direct_sale_default_bill_ref(sale)
    should_track = (
        bool(bill_ref) or
        pending_amount > 0 or
        (category in ['Cash', 'Mixed Transaction', 'Credit Customer', 'Open Khata'] and float(sale.amount or 0) > 0)
    )
    if not should_track:
        return

    reason = f"Direct Sale ({category}): {primary_material}".strip()
    if reason.endswith(':'):
        reason = reason[:-1]
    is_paid_status = (pending_amount <= 0 and float(sale.amount or 0) > 0)

    existing_pb = PendingBill.query.filter_by(
        bill_no=bill_ref,
        client_code=client_code,
        is_void=False
    ).order_by(PendingBill.id.desc()).first()

    if existing_pb:
        existing_pb.client_name = client_name
        existing_pb.amount = pending_amount
        existing_pb.reason = reason
        existing_pb.is_cash = (category == 'Cash')
        existing_pb.is_manual = bool(sale.manual_bill_no)
        existing_pb.bill_kind = parse_bill_kind(bill_ref)
        existing_pb.is_paid = is_paid_status
        existing_pb.note = sale.note
    else:
        db.session.add(PendingBill(
            client_code=client_code,
            client_name=client_name,
            bill_no=bill_ref,
            bill_kind=parse_bill_kind(bill_ref),
            amount=pending_amount,
            reason=reason,
            is_cash=(category == 'Cash'),
            is_manual=bool(sale.manual_bill_no),
            is_paid=is_paid_status,
            created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
            created_by=current_user.username,
            note=sale.note
        ))


def _parse_dt_safe(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    txt = str(value).strip()
    if not txt:
        return None
    # Support both browser datetime-local format (with "T") and standard space-separated values.
    for fmt in (
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d'
    ):
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None


def _material_ledger_recent(client_obj, only_booking=True, limit_per_material=5, cutoff_dt=None):
    if not client_obj:
        return []
    events_by_material = {}
    client_name_norm = (client_obj.name or '').strip().lower()

    bookings = Booking.query.filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False
    ).order_by(Booking.date_posted.asc()).all()

    for b in bookings:
        bill_ref = b.manual_bill_no or b.auto_bill_no or f"BK-{b.id}"
        for item in b.items:
            mat = (item.material_name or '').strip()
            if not mat:
                continue
            events_by_material.setdefault(mat, []).append({
                'date_dt': b.date_posted,
                'date_str': b.date_posted.strftime('%Y-%m-%d') if b.date_posted else '',
                'bill_no': bill_ref,
                'material': mat,
                'material_display': mat,
                'qty_added': float(item.qty or 0),
                'qty_dispatched': 0,
                'source': 'Booking'
            })

    delivery_q = Entry.query.filter(
        Entry.type == 'OUT',
        Entry.is_void == False,
        or_(
            Entry.client_code == client_obj.code,
            func.lower(func.trim(Entry.client)) == client_name_norm
        )
    )
    if only_booking:
        delivery_q = delivery_q.filter(Entry.client_category == 'Booking Delivery')
    deliveries = delivery_q.all()

    for d in deliveries:
        mat = (d.booked_material or d.material or '').strip()
        if not mat:
            continue
        dt_val = _parse_dt_safe(f"{d.date} {d.time}") or _parse_dt_safe(d.date) or datetime.min
        bill_ref = d.bill_no or d.auto_bill_no or ''
        material_display = mat
        if d.booked_material and d.material and d.booked_material != d.material:
            material_display = f"{d.booked_material}>ALT>{d.material}"
        events_by_material.setdefault(mat, []).append({
            'date_dt': dt_val,
            'date_str': d.date or '',
            'bill_no': bill_ref,
            'material': mat,
            'material_display': material_display,
            'qty_added': 0,
            'qty_dispatched': float(d.qty or 0),
            'source': d.nimbus_no or 'Dispatch'
        })

    grouped = []
    for mat, events in sorted(events_by_material.items(), key=lambda x: x[0].lower()):
        events_sorted = sorted(events, key=lambda e: e['date_dt'] or datetime.min)
        if cutoff_dt:
            events_sorted = [e for e in events_sorted if (e['date_dt'] or datetime.min) <= cutoff_dt]
        running = 0
        for e in events_sorted:
            running += (e['qty_added'] - e['qty_dispatched'])
            e['remaining'] = running
        tail = events_sorted[-limit_per_material:] if limit_per_material else events_sorted
        tail_display = tail
        grouped.append({
            'material': mat,
            'rows': tail_display
        })
    return grouped


def _invoice_cutoff_dt(invoice_obj):
    if not invoice_obj:
        return None
    if getattr(invoice_obj, 'date', None):
        return datetime.combine(invoice_obj.date, datetime.max.time())
    created_at = getattr(invoice_obj, 'created_at', None)
    if created_at:
        dt = _parse_dt_safe(created_at)
        if dt:
            return dt
    inv_no = getattr(invoice_obj, 'invoice_no', None) or ''
    if isinstance(inv_no, str) and inv_no.startswith('INV-') and len(inv_no) >= 18:
        suffix = inv_no[4:]
        try:
            return datetime.strptime(suffix, '%Y%m%d%H%M%S')
        except ValueError:
            return None
    return None


def _client_balance_as_of(client_obj, cutoff_dt=None):
    """Return client pending balance using transactions up to cutoff_dt (inclusive)."""
    if not client_obj:
        return 0.0

    opening_effect = 0.0
    opening_balance = _to_float_or_zero(getattr(client_obj, 'opening_balance', 0))
    if opening_balance != 0:
        opening_dt = (
            _parse_dt_safe(getattr(client_obj, 'opening_balance_date', None))
            or _parse_dt_safe(getattr(client_obj, 'created_at', None))
            or datetime.min
        )
        if (not cutoff_dt) or (opening_dt <= cutoff_dt):
            opening_effect = opening_balance

    client_name_norm = (client_obj.name or '').strip().lower()

    booking_q = Booking.query.filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False
    )
    payment_q = Payment.query.filter(
        func.lower(func.trim(Payment.client_name)) == client_name_norm,
        Payment.is_void == False
    )
    sale_q = DirectSale.query.filter(
        func.lower(func.trim(DirectSale.client_name)) == client_name_norm,
        DirectSale.is_void == False
    )

    if cutoff_dt:
        booking_q = booking_q.filter(Booking.date_posted <= cutoff_dt)
        payment_q = payment_q.filter(Payment.date_posted <= cutoff_dt)
        sale_q = sale_q.filter(DirectSale.date_posted <= cutoff_dt)

    b_debit = booking_q.with_entities(func.sum(Booking.amount)).scalar() or 0
    b_credit = booking_q.with_entities(func.sum(Booking.paid_amount)).scalar() or 0
    p_credit = payment_q.with_entities(func.sum(Payment.amount)).scalar() or 0
    ds_debit = sale_q.with_entities(func.sum(DirectSale.amount)).scalar() or 0
    ds_credit = sale_q.with_entities(func.sum(DirectSale.paid_amount)).scalar() or 0

    b_discount = booking_q.with_entities(func.sum(Booking.discount)).scalar() or 0
    p_discount = _client_waive_off_total(client_name_norm, cutoff_dt=cutoff_dt)
    ds_discount = sale_q.with_entities(func.sum(DirectSale.discount)).scalar() or 0

    movement_effect = (b_debit + ds_debit) - (b_credit + p_credit + ds_credit + ds_discount + b_discount + p_discount)
    return float(opening_effect + movement_effect)


def _bill_cutoff_dt_for_snapshot(booking=None, payment=None, invoice=None, sale=None, pending=None):
    """Resolve bill datetime for historical pending snapshot."""
    if booking:
        return _parse_dt_safe(getattr(booking, 'date_posted', None))
    if payment:
        return _parse_dt_safe(getattr(payment, 'date_posted', None))
    if sale:
        return _parse_dt_safe(getattr(sale, 'date_posted', None))
    if invoice:
        return _invoice_cutoff_dt(invoice)
    if pending:
        return _parse_dt_safe(getattr(pending, 'created_at', None))
    return None


def _material_ledger_current_summary(material_ledger_recent, bill_refs):
    if not material_ledger_recent or not bill_refs:
        return []
    refs = {str(r).strip() for r in bill_refs if r}
    if not refs:
        return []
    summary = []
    for group in material_ledger_recent:
        rows = group.get('rows') or []
        if not rows:
            continue
        matched = [r for r in rows if str(r.get('bill_no') or '').strip() in refs]
        if not matched:
            continue
        matched_sorted = sorted(matched, key=lambda r: r.get('date_dt') or datetime.min)
        last_row = matched_sorted[-1]
        dispatched = sum(float(r.get('qty_dispatched') or 0) for r in matched_sorted)
        added = sum(float(r.get('qty_added') or 0) for r in matched_sorted)
        last_stock = float(last_row.get('remaining') or 0) - float(last_row.get('qty_added') or 0) + float(last_row.get('qty_dispatched') or 0)
        remaining = float(rows[-1].get('remaining') or 0)
        summary.append({
            'material': group.get('material') or '',
            'dispatched': dispatched,
            'added': added,
            'last_stock': last_stock,
            'remaining': remaining
        })
    return summary


def _pending_bill_category(pb):
    is_open_khata = pb.client_code == OPEN_KHATA_CODE or (pb.client_name or '').strip().upper() == OPEN_KHATA_NAME
    if is_open_khata and pb.is_paid and pb.is_cash:
        return 'Cash Paid'
    if is_open_khata:
        return 'Open Khata'
    if pb.is_cash:
        return 'Unbilled Cash'
    if pb.bill_no:
        return 'Billed'
    return 'Unbilled'


def _pending_bill_age_days(pb):
    created_dt = _parse_dt_safe(pb.created_at)
    if not created_dt:
        return 0
    return max(0, (pk_now() - created_dt).days)


def _normalize_risk_label(value):
    txt = (value or '').strip().lower().replace(' ', '_')
    if txt == 'veryhigh':
        txt = 'very_high'
    return txt


def _risk_label_pretty(value):
    mapping = {
        'low': 'Low',
        'medium': 'Medium',
        'high': 'High',
        'very_high': 'Very High',
    }
    return mapping.get(_normalize_risk_label(value), 'Low')


def _pending_bill_risk(pb, contact_count=0):
    amt = float(pb.amount or 0)
    valid_overrides = {'low', 'medium', 'high', 'very_high'}
    override_key = _normalize_risk_label(getattr(pb, 'risk_override', None))

    if override_key in valid_overrides:
        level_key = override_key
    else:
        if pb.is_paid:
            level_key = 'low'
        elif amt > 10000:
            level_key = 'very_high'
        elif amt > 5000:
            level_key = 'high'
        elif amt > 0:
            level_key = 'medium'
        else:
            level_key = 'low'

        if (not pb.is_paid) and contact_count >= 2:
            level_key = 'very_high'

    severity_rank = {'low': 1, 'medium': 2, 'high': 3, 'very_high': 4}
    score = (severity_rank.get(level_key, 1) * 1000000.0) + amt + _pending_bill_age_days(pb)
    return score, _risk_label_pretty(level_key)


def _build_notifications_pdf_bytes(rows):
    from reportlab.lib.units import cm
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    page_width = 14.8 * cm
    page_height = 21 * cm
    margin = 1 * cm
    c = canvas.Canvas(buf, pagesize=(page_width, page_height))
    width, height = page_width, page_height

    y = height - margin
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margin, y, f"Pending Credit Notifications - {pk_now().strftime('%Y-%m-%d')}")
    y -= 24
    c.setFont("Helvetica", 9)
    c.drawString(margin, y, "Client")
    c.drawString(210, y, "Bill")
    c.drawString(300, y, "Category")
    c.drawString(390, y, "Days")
    c.drawString(440, y, "Amount")
    c.drawString(510, y, "Risk")
    y -= 14

    for r in rows[:120]:
        if y < margin:
            c.showPage()
            y = height - margin
        c.setFont("Helvetica", 8)
        c.drawString(margin, y, str(r['client'])[:30])
        c.drawString(210, y, str(r['bill_no'])[:14])
        c.drawString(300, y, str(r['category'])[:12])
        c.drawRightString(420, y, str(r['age_days']))
        c.drawRightString(500, y, f"{r['amount']:.0f}")
        c.drawString(510, y, str(r['risk_level']))
        y -= 12

    c.save()
    buf.seek(0)
    return buf.read()


def _build_notifications_xlsx_bytes(rows):
    import pandas as pd
    data = [{
        'Client': r['client'],
        'Bill No': r['bill_no'],
        'Category': r['category'],
        'Age Days': r['age_days'],
        'Amount': r['amount'],
        'Risk': r['risk_level'],
        'Risk Score': r['risk_score'],
    } for r in rows]
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        pd.DataFrame(data).to_excel(writer, index=False, sheet_name='PendingCredit')
    out.seek(0)
    return out.read()


def _build_pending_rows_for_report():
    pending = PendingBill.query.filter(
        PendingBill.is_void == False,
        PendingBill.is_paid == False,
        PendingBill.amount > 0
    ).all()
    rows = []
    for pb in pending:
        score, level = _pending_bill_risk(pb)
        rows.append({
            'client': pb.client_name or pb.client_code or '',
            'bill_no': pb.bill_no or '',
            'category': _pending_bill_category(pb),
            'age_days': _pending_bill_age_days(pb),
            'amount': float(pb.amount or 0),
            'risk_score': score,
            'risk_level': level,
        })
    rows.sort(key=lambda x: (x['risk_score'], x['age_days'], x['amount']), reverse=True)
    return rows


def _send_daily_notifications_email():
    emails = [x.email for x in StaffEmail.query.filter_by(is_active=True).all() if x.email]
    if not emails:
        return False, 'No active staff emails configured'

    settings_obj = Settings.query.first()
    rows = _build_pending_rows_for_report()
    pdf_bytes = _build_notifications_pdf_bytes(rows)
    xlsx_bytes = _build_notifications_xlsx_bytes(rows)
    attachments = [
        (f"pending_credit_{pk_today().isoformat()}.pdf", 'application/pdf', pdf_bytes),
        (f"pending_credit_{pk_today().isoformat()}.xlsx", 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', xlsx_bytes),
    ]
    subject = f"Daily Pending Credit Report - {pk_today().isoformat()}"
    body = f"Attached: daily pending credit report (PDF + Excel).\nTotal pending records: {len(rows)}"

    smtp_host = (settings_obj.smtp_host if settings_obj and settings_obj.smtp_host else os.environ.get('SMTP_HOST', '')).strip()
    smtp_user = (settings_obj.smtp_user if settings_obj and settings_obj.smtp_user else os.environ.get('SMTP_USER', '')).strip()
    smtp_pass = (settings_obj.smtp_pass if settings_obj and settings_obj.smtp_pass else os.environ.get('SMTP_PASS', '')).strip()
    # Gmail app passwords are often pasted with spaces; normalize to avoid auth failures.
    smtp_pass = smtp_pass.replace(' ', '')
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
        return False, 'SMTP settings missing. Configure in Settings -> General Settings (SMTP) or env.'
    if smtp_user and not smtp_pass:
        return False, 'SMTP password missing. Enter SMTP App Password in Settings.'

    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ', '.join(emails)
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
        return True, f'Sent via SMTP to {len(emails)} email(s)'
    except smtplib.SMTPAuthenticationError as e:
        detail = ''
        try:
            detail = (e.smtp_error or b'').decode(errors='ignore').strip()
        except Exception:
            detail = str(e)
        msg = 'SMTP login failed. For Gmail, use a 16-character App Password.'
        if detail:
            msg = f'{msg} Server says: {detail}'
        return False, msg
    except Exception as e:
        return False, f'SMTP send failed: {e}'


def _notification_worker_loop():
    global _NOTIFY_LAST_SENT_DATE
    while True:
        try:
            with app.app_context():
                now = pk_now()
                settings_obj = Settings.query.first()
                target_time = (settings_obj.notify_daily_time if settings_obj and settings_obj.notify_daily_time else os.environ.get('NOTIFY_DAILY_TIME', '08:00'))
                hh, mm = 8, 0
                try:
                    hh, mm = [int(x) for x in target_time.split(':', 1)]
                except Exception:
                    pass
                if now.hour == hh and now.minute == mm and _NOTIFY_LAST_SENT_DATE != now.date():
                        _send_daily_notifications_email()
                        _NOTIFY_LAST_SENT_DATE = now.date()
            time.sleep(30)
        except Exception:
            time.sleep(60)

with app.app_context():
    _bootstrap_database()


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

@app.context_processor
def inject_dropdown_data():
    if current_user.is_authenticated:
        return dict(
            clients=Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all(),
            materials=Material.query.order_by(Material.name.asc()).all(),
            delivery_persons=DeliveryPerson.query.filter_by(is_active=True).order_by(DeliveryPerson.name.asc()).all(),
            settings=Settings.query.first(),
            user_can=_user_can,
            ui_theme_preference=session.get('ui_theme')
        )
    return dict(ui_theme_preference=session.get('ui_theme'))


@app.route('/api/ui/theme', methods=['GET', 'POST'])
def ui_theme_preference_api():
    if request.method == 'GET':
        stored = (session.get('ui_theme') or '').strip().lower()
        theme = stored if stored in ('light', 'dark') else None
        return jsonify({'theme': theme})

    payload = request.get_json(silent=True) or {}
    theme = str(payload.get('theme', '')).strip().lower()
    if theme not in ('light', 'dark'):
        return jsonify({'ok': False, 'error': 'Invalid theme'}), 400
    session['ui_theme'] = theme
    return jsonify({'ok': True, 'theme': theme})

# ==================== BOOKING ROUTES ====================

@app.route('/bookings')
@login_required
def bookings_page():
    show_mode = (request.args.get('show', 'active') or 'active').strip().lower()
    client_filter = (request.args.get('client') or '').strip()
    resolved_filter_client = get_client_by_input(client_filter) if client_filter else None
    bill_filter = (request.args.get('bill_no') or '').strip()
    date_from = (request.args.get('date_from') or '').strip()
    date_to = (request.args.get('date_to') or '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    per_page = min(max(per_page, 10), 100)

    if date_from and date_to and date_to < date_from:
        date_to = date_from

    bookings_q = Booking.query
    if show_mode == 'voided':
        bookings_q = bookings_q.filter(Booking.is_void == True)
    elif show_mode == 'all':
        bookings_q = bookings_q
    else:
        show_mode = 'active'
        bookings_q = bookings_q.filter(Booking.is_void == False)

    if client_filter:
        if resolved_filter_client:
            bookings_q = bookings_q.filter(
                func.lower(func.trim(Booking.client_name)) == resolved_filter_client.name.strip().lower()
            )
        else:
            bookings_q = bookings_q.filter(Booking.client_name.ilike(f"%{client_filter}%"))
    if bill_filter:
        bookings_q = bookings_q.filter(or_(
            Booking.manual_bill_no.ilike(f"%{bill_filter}%"),
            Booking.auto_bill_no.ilike(f"%{bill_filter}%")
        ))
    if date_from:
        bookings_q = bookings_q.filter(func.date(Booking.date_posted) >= date_from)
    if date_to:
        bookings_q = bookings_q.filter(func.date(Booking.date_posted) <= date_to)

    bookings_pagination = bookings_q.order_by(Booking.date_posted.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    bookings = bookings_pagination.items
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()
    next_auto = peek_next_bill_no(AUTO_BILL_NAMESPACES['BOOKING'])
    return render_template('bookings.html',
                           bookings=bookings,
                           clients=clients,
                           materials=materials,
                           next_auto=next_auto,
                           show_mode=show_mode,
                           client_filter=client_filter,
                           client_filter_display=(resolved_filter_client.name if resolved_filter_client else ''),
                           bill_filter=bill_filter,
                           date_from=date_from,
                           date_to=date_to,
                           pagination=bookings_pagination,
                           per_page=per_page)


@app.route('/add_booking', methods=['POST'])
@login_required
def add_booking():
    client_input = request.form.get('client_code', '').strip() or request.form.get('client_name', '').strip()
    materials_list = request.form.getlist('material_name[]')
    qtys = request.form.getlist('qty[]')
    rates = request.form.getlist('unit_rate[]')
    amount = _to_float_or_zero(request.form.get('amount', 0))
    paid_amount = _to_float_or_zero(request.form.get('paid_amount', 0))
    discount = 0.0
    discount_reason = ''
    # Safety: booking discount should only apply when explicit discount fields are provided.
    if current_user.role == 'admin' and ('discount' in request.form or 'discount_reason' in request.form):
        discount = _to_float_or_zero(request.form.get('discount', 0))
        discount_reason = request.form.get('discount_reason', '').strip()
    manual_bill_raw = request.form.get('manual_bill_no', '').strip()
    manual_bill_no = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
    note = request.form.get('note', '').strip()
    date_str = (request.form.get('date') or '').strip()

    photo_path = save_photo(request.files.get('photo'))
    photo_url = request.form.get('photo_url', '').strip()

    # Find client by name or code
    client = get_client_by_input(client_input)

    if not client:
        flash(f'Client "{client_input}" not found. Please add client first.', 'danger')
        return redirect(url_for('bookings_page'))

    if manual_bill_no:
        conflict = find_bill_conflict(manual_bill_no)
        if conflict:
            flash(f"Manual bill '{manual_bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
            return redirect(url_for('bookings_page'))

    # Calculate pending amount (what's still owed)
    pending_amount = max(0.0, amount - discount - paid_amount)

    auto_bill_no = get_next_bill_no(AUTO_BILL_NAMESPACES['BOOKING'])

    booking_posted_at = resolve_posted_datetime(date_str)

    # Create the booking
    booking = Booking(client_name=client.name,
                      amount=amount,
                      paid_amount=paid_amount,
                      discount=discount,
                      discount_reason=discount_reason,
                      manual_bill_no=manual_bill_no,
                      auto_bill_no=auto_bill_no,
                      photo_path=photo_path,
                      photo_url=photo_url,
                      date_posted=booking_posted_at,
                      note=note)
    db.session.add(booking)
    db.session.flush()

    # Add booking items; auto-create material master if missing.
    for mat, qty, rate in zip(materials_list, qtys, rates):
        mat_obj = get_material_by_input(mat)
        mat_name = str(mat or '').strip()
        if _to_float_or_zero(qty) > 0 and _to_float_or_zero(rate) <= 0:
            flash(f'Unit rate is required and must be greater than 0 for "{mat_name}".', 'danger')
            return redirect(url_for('bookings_page'))
        if not mat_obj and mat_name:
            mat_obj = Material(
                code=generate_material_code(),
                name=mat_name,
                unit_price=_to_float_or_zero(rate),
                category_id=_get_default_material_category_id()
            )
            db.session.add(mat_obj)
            db.session.flush()
        if mat_obj:
            db.session.add(
                BookingItem(booking_id=booking.id,
                            material_name=mat_obj.name,
                            qty=_to_float_or_zero(qty),
                            price_at_time=_to_float_or_zero(rate)))

    # Auto-add to PendingBill (Use manual bill or auto bill reference)
    bill_ref = manual_bill_no or booking.auto_bill_no or f"BK-{booking.id}"

    if pending_amount > 0:
        existing_pb = PendingBill.query.filter_by(bill_no=bill_ref, client_code=client.code).first()
        if existing_pb:
            existing_pb.amount += pending_amount
            existing_pb.reason = f"Booking: {materials_list[0] if materials_list else ''}"
            existing_pb.bill_kind = parse_bill_kind(bill_ref)
        else:
            db.session.add(PendingBill(
                client_code=client.code,
                client_name=client.name,
                bill_no=bill_ref,
                amount=pending_amount,
                reason=f"Booking: {materials_list[0] if materials_list else ''}",
                is_manual=bool(manual_bill_no),
                bill_kind=('MB' if manual_bill_no else 'SB'),
                created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                created_by=current_user.username,
                note=note
            ))

    db.session.commit()

    msg = f'Booking added successfully'
    if manual_bill_no:
        msg += f' (Bill: {manual_bill_no})'
    if pending_amount > 0:
        msg += f' â€” Pending amount: {pending_amount}'
    flash(msg, 'success')

    bill_ref = manual_bill_no or booking.auto_bill_no or f"BK-{booking.id}"
    return redirect(url_for(
        'bookings_page',
        download_bill=bill_ref,
        download_src='booking',
        download_src_id=booking.id,
        download_client_code=(client.code if client else None),
        download_client_name=(client.name if client else booking.client_name)
    ))


@app.route('/edit_bill/Booking/<int:id>', methods=['POST'])
@login_required
def edit_booking(id):
    booking = Booking.query.get_or_404(id)

    old_bill_no = booking.manual_bill_no
    old_client = Client.query.filter_by(name=booking.client_name).first()
    old_client_code = old_client.code if old_client else None
    old_pending_amount = max(0.0, (booking.amount or 0) - (booking.discount or 0) - (booking.paid_amount or 0))

    client_code = request.form.get('client_code', '').strip()
    client_name_input = request.form.get('client_name', '').strip()

    # Find client by name or code
    client = get_client_by_input(client_code) or get_client_by_input(client_name_input)
    if client:
        booking.client_name = client.name

    materials_list = request.form.getlist('material_name[]')
    qtys = request.form.getlist('qty[]')
    rates = request.form.getlist('unit_rate[]')
    booking.amount = _to_float_or_zero(request.form.get('amount', 0))
    booking.paid_amount = _to_float_or_zero(request.form.get('paid_amount', 0))
    # Do not overwrite booking discount unless explicit fields are submitted.
    if current_user.role == 'admin' and ('discount' in request.form or 'discount_reason' in request.form):
        booking.discount = _to_float_or_zero(request.form.get('discount', 0))
        booking.discount_reason = request.form.get('discount_reason', '').strip()
    new_manual_raw = request.form.get('manual_bill_no', '').strip()
    booking.manual_bill_no = normalize_manual_bill(new_manual_raw) if new_manual_raw else ''
    booking.note = request.form.get('note', '').strip()
    date_str = (request.form.get('date') or '').strip()
    if date_str:
        parsed_posted_at = resolve_posted_datetime(date_str, fallback_dt=booking.date_posted or pk_now())
        # Keep original timestamp if the submitted datetime is unchanged at minute precision.
        if booking.date_posted:
            old_minute = booking.date_posted.replace(second=0, microsecond=0)
            new_minute = parsed_posted_at.replace(second=0, microsecond=0)
            if new_minute != old_minute:
                booking.date_posted = parsed_posted_at
        else:
            booking.date_posted = parsed_posted_at

    booking.photo_url = request.form.get('photo_url', '').strip()
    new_photo = save_photo(request.files.get('photo'))
    if new_photo:
        booking.photo_path = new_photo

    if booking.manual_bill_no:
        conflict = find_bill_conflict(booking.manual_bill_no)
        if conflict and not (conflict[0] == 'Booking' and conflict[1] == booking.id):
            flash(f"Manual bill '{booking.manual_bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
            return redirect(url_for('bookings_page'))

    # Update booking items
    BookingItem.query.filter_by(booking_id=id).delete()

    for mat, qty, rate in zip(materials_list, qtys, rates):
        mat_obj = get_material_by_input(mat)
        mat_name = str(mat or '').strip()
        if _to_float_or_zero(qty) > 0 and _to_float_or_zero(rate) <= 0:
            flash(f'Unit rate is required and must be greater than 0 for "{mat_name}".', 'danger')
            return redirect(url_for('bookings_page'))
        if not mat_obj and mat_name:
            mat_obj = Material(
                code=generate_material_code(),
                name=mat_name,
                unit_price=_to_float_or_zero(rate),
                category_id=_get_default_material_category_id()
            )
            db.session.add(mat_obj)
            db.session.flush()
        if mat_obj:
            db.session.add(
                BookingItem(booking_id=booking.id,
                            material_name=mat_obj.name,
                            qty=_to_float_or_zero(qty),
                            price_at_time=_to_float_or_zero(rate)))

    # Update PendingBill
    new_bill_no = booking.manual_bill_no
    new_bill_ref = new_bill_no or booking.auto_bill_no or f"BK-{id}"
    new_pending_amount = max(0.0, booking.amount - booking.discount - booking.paid_amount)
    new_client = Client.query.filter_by(name=booking.client_name).first()
    new_client_code = new_client.code if new_client else None

    # Remove old pending bill if exists
    old_bill_ref = old_bill_no or booking.auto_bill_no or f"BK-{id}"
    if old_bill_ref and old_client_code:
        old_pb = PendingBill.query.filter_by(bill_no=old_bill_ref, client_code=old_client_code).first()
        if old_pb:
            old_pb.amount -= old_pending_amount
            if old_pb.amount <= 0:
                db.session.delete(old_pb)

    # Add/update new pending bill
    if new_pending_amount > 0 and new_client_code:
        new_pb = PendingBill.query.filter_by(bill_no=new_bill_ref, client_code=new_client_code).first()
        if new_pb:
            new_pb.amount += new_pending_amount
            new_pb.client_name = booking.client_name
            new_pb.note = booking.note
            new_pb.bill_kind = parse_bill_kind(new_bill_ref)
        else:
            db.session.add(PendingBill(
                client_code=new_client_code,
                client_name=booking.client_name,
                bill_no=new_bill_ref,
                amount=new_pending_amount,
                reason=f"Booking: {materials_list[0] if materials_list else ''}",
                is_manual=bool(new_bill_no),
                bill_kind=('MB' if new_bill_no else 'SB'),
                created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                created_by=current_user.username,
                note=booking.note
            ))

    db.session.commit()
    flash('Booking updated', 'success')

    bill_ref = booking.manual_bill_no or booking.auto_bill_no or f"BK-{id}"
    return redirect(url_for(
        'bookings_page',
        download_bill=bill_ref,
        download_src='booking',
        download_src_id=booking.id,
        download_client_code=(new_client_code if new_client_code else None),
        download_client_name=booking.client_name
    ))


# ==================== PAYMENT ROUTES ====================

@app.route('/payments')
@login_required
def payments_page():
    party = (request.args.get('party', 'customer') or 'customer').strip().lower()
    if party not in ['customer', 'supplier', 'all']:
        party = 'customer'
    show_mode = (request.args.get('show', 'active') or 'active').strip().lower()
    payments = []
    supplier_payments = []

    if party in ['customer', 'all']:
        payments_q = Payment.query
        if show_mode == 'voided':
            payments_q = payments_q.filter(Payment.is_void == True)
        elif show_mode == 'all':
            payments_q = payments_q
        else:
            show_mode = 'active'
            payments_q = payments_q.filter(Payment.is_void == False)
        payments = payments_q.order_by(Payment.date_posted.desc()).all()

    if party in ['supplier', 'all']:
        supplier_q = SupplierPayment.query
        if show_mode == 'voided':
            supplier_q = supplier_q.filter(SupplierPayment.is_void == True)
        elif show_mode == 'all':
            supplier_q = supplier_q
        else:
            show_mode = 'active'
            supplier_q = supplier_q.filter(SupplierPayment.is_void == False)
        supplier_payments = supplier_q.order_by(SupplierPayment.date_posted.desc()).all()

    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    suppliers = Supplier.query.filter_by(is_active=True).order_by(Supplier.name.asc()).all()
    next_auto = peek_next_bill_no(AUTO_BILL_NAMESPACES['PAYMENT'])
    return render_template('payments.html',
                           payments=payments,
                           supplier_payments=supplier_payments,
                           clients=clients,
                           suppliers=suppliers,
                           next_auto=next_auto,
                           show_mode=show_mode,
                           party=party,
                           today_date=pk_today().strftime('%Y-%m-%d'))


@app.route('/add_payment', methods=['POST'])
@login_required
def add_payment():
    def _as_bool(val):
        return str(val).strip().lower() in ['1', 'true', 'on', 'yes']

    # Form submits `client_code` (search input), keep legacy fallback for `client_name`.
    client_input = (request.form.get('client_code') or request.form.get('client_name') or '').strip()
    client_name = client_input
    amount = float(request.form.get('amount', 0) or 0)
    if _user_can('can_manage_payments'):
        discount = float(request.form.get('discount', 0) or 0)
        discount_reason = request.form.get('discount_reason', '').strip()
        settle_leftover_with_discount = _as_bool(request.form.get('settle_leftover_with_discount'))
    else:
        discount = 0
        discount_reason = ''
        settle_leftover_with_discount = False
    method = request.form.get('method', 'Cash')
    bank_name = request.form.get('bank_name', '').strip()
    account_name = request.form.get('account_name', '').strip()
    account_no = request.form.get('account_no', '').strip()
    if method not in ['Bank', 'Bank Transfer', 'Check', 'Cheque']:
        bank_name = ''
        account_name = ''
        account_no = ''
    manual_bill_raw = request.form.get('manual_bill_no', '').strip()
    manual_bill_no = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
    note = request.form.get('note', '').strip()
    date_str = (request.form.get('date') or '').strip()
    photo_path = save_photo(request.files.get('photo'))
    photo_url = request.form.get('photo_url', '').strip()
    payment_posted_at = resolve_posted_datetime(date_str)
    auto_bill_no = get_next_bill_no(AUTO_BILL_NAMESPACES['PAYMENT'])

    # Find client by name or code
    client = get_client_by_input(client_input)

    auto_discount_applied = 0.0
    if client:
        client_name = client.name

    if manual_bill_no:
        conflict = find_bill_conflict(manual_bill_no)
        if conflict:
            flash(f"Manual bill '{manual_bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
            return redirect(url_for('payments_page'))

    payment = Payment(client_name=client_name,
                      amount=amount,
                      discount=discount,
                      discount_reason=discount_reason,
                      bank_name=bank_name,
                      account_name=account_name,
                      account_no=account_no,
                      method=method,
                      manual_bill_no=manual_bill_no,
                      auto_bill_no=auto_bill_no,
                      photo_path=photo_path,
                      photo_url=photo_url,
                      date_posted=payment_posted_at,
                      note=note)
    db.session.add(payment)
    db.session.flush()

    # Apply payment to matching pending bills when possible
    remaining = float(amount) + float(discount)
    applied = []
    bill_candidates = []
    if manual_bill_no:
        bill_candidates = _bill_no_variants(manual_bill_no)

    def _is_open_khata_bill(pb):
        return pb and (pb.client_code == OPEN_KHATA_CODE or (pb.client_name or '').strip().upper() == OPEN_KHATA_NAME)

    if client:
        # Prefer matching by manual_bill_no when provided
        if manual_bill_no:
            filters = []
            for candidate in bill_candidates:
                filters.append(PendingBill.bill_no.ilike(candidate))
                filters.append(PendingBill.nimbus_no.ilike(candidate))

            pending_q = PendingBill.query.filter(
                PendingBill.client_code == client.code,
                PendingBill.is_paid == False,
                or_(*filters)
            ).order_by(PendingBill.id.asc()).all()

            # Fallback for imported data where pending bill has missing/misaligned client code.
            if not pending_q:
                fallback_q = PendingBill.query.filter(
                    PendingBill.is_paid == False,
                    or_(*filters)
                ).order_by(PendingBill.id.asc()).all()
                compatible = [
                    pb for pb in fallback_q
                    if (
                        (not (pb.client_code or '').strip()) or
                        (pb.client_code == client.code) or
                        ((pb.client_name or '').strip().lower() == client.name.strip().lower())
                    )
                ]
                # Only auto-attach on an unambiguous single match.
                if len(compatible) == 1:
                    pb = compatible[0]
                    pb.client_code = client.code
                    if not (pb.client_name or '').strip():
                        pb.client_name = client.name
                    pending_q = [pb]
        else:
            # Otherwise apply to oldest unpaid bills for this client
            pending_q = PendingBill.query.filter_by(client_code=client.code, is_paid=False).order_by(PendingBill.id.asc()).all()

        for pb in pending_q:
            if remaining <= 0:
                break
            if pb.is_paid:
                continue
            if remaining >= (pb.amount or 0):
                remaining -= (pb.amount or 0)
                applied.append((pb.bill_no, f'paid Rs.{pb.amount}'))
                pb.amount = 0
                pb.is_paid = True
                if _is_open_khata_bill(pb):
                    pb.is_cash = True
            else:
                applied.append((pb.bill_no, f'partial Rs.{remaining:.2f}'))
                pb.amount = (pb.amount or 0) - remaining
                remaining = 0

        if settle_leftover_with_discount and manual_bill_no:
            for pb in pending_q:
                if pb.is_paid:
                    continue
                outstanding = float(pb.amount or 0)
                if outstanding <= 0:
                    continue
                pb.amount = 0
                pb.is_paid = True
                auto_discount_applied += outstanding
                applied.append((pb.bill_no, f'waived off (loss) Rs.{outstanding:.2f}'))
                if _is_open_khata_bill(pb):
                    pb.is_cash = True
                break
    elif manual_bill_no:
        # Open Khata / unknown-client fallback: apply by bill number even without client match.
        filters = []
        for candidate in bill_candidates:
            filters.append(PendingBill.bill_no.ilike(candidate))
            filters.append(PendingBill.nimbus_no.ilike(candidate))

        pending_q = PendingBill.query.filter(
            PendingBill.is_paid == False,
            or_(*filters)
        ).order_by(PendingBill.id.asc()).all()

        for pb in pending_q:
            if remaining <= 0:
                break
            if pb.is_paid:
                continue
            if remaining >= (pb.amount or 0):
                remaining -= (pb.amount or 0)
                applied.append((pb.bill_no, f'paid Rs.{pb.amount}'))
                pb.amount = 0
                pb.is_paid = True
                if _is_open_khata_bill(pb):
                    pb.is_cash = True
            else:
                applied.append((pb.bill_no, f'partial Rs.{remaining:.2f}'))
                pb.amount = (pb.amount or 0) - remaining
                remaining = 0

    # If manual_bill_no is provided but didn't match an existing bill,
    # create a Receipt record in PendingBill so it appears in the list.
    created_receipt = False
    if manual_bill_no and client:
        existing_target_filters = [PendingBill.bill_no.ilike(x) for x in bill_candidates]
        existing_target = PendingBill.query.filter(
            PendingBill.client_code == client.code,
            or_(*existing_target_filters)
        ).first()
        if not existing_target:
            # Fallback: attach imported pending bill with same bill no if client code was blank/misaligned.
            global_target = PendingBill.query.filter(or_(*existing_target_filters)).order_by(PendingBill.id.desc()).first()
            if global_target and (
                (not (global_target.client_code or '').strip()) or
                (global_target.client_code == client.code) or
                ((global_target.client_name or '').strip().lower() == client.name.strip().lower())
            ):
                global_target.client_code = client.code
                if not (global_target.client_name or '').strip():
                    global_target.client_name = client.name
                existing_target = global_target
        if not existing_target:
            receipt_pb = PendingBill(
                client_code=client.code,
                client_name=client.name,
                bill_no=manual_bill_no,
                amount=0, # Zero balance for receipts
                reason=f"Payment Received ({method})",
                is_paid=True,
                is_manual=True,
                bill_kind='MB',
                created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                created_by=current_user.username,
                note=note
            )
            db.session.add(receipt_pb)
            created_receipt = True

    if auto_discount_applied > 0:
        payment.discount = float(payment.discount or 0) + auto_discount_applied
        auto_reason = f'Auto waive-off (loss) settlement for bill {manual_bill_no}'
        if payment.discount_reason:
            if auto_reason not in payment.discount_reason:
                payment.discount_reason = f"{payment.discount_reason}; {auto_reason}"
        else:
            payment.discount_reason = auto_reason

    _sync_payment_waive_off(payment)
    db.session.commit()

    msg = 'Payment received successfully'
    if applied:
        details = ', '.join([f"{b}: {s}" for b, s in applied])
        msg += f" - applied to: {details}"
    if created_receipt:
        msg += f" - Receipt #{manual_bill_no} recorded"
    if auto_discount_applied > 0:
        msg += f" - leftover waived off as loss Rs.{auto_discount_applied:.2f}"
    elif remaining > 0 and amount > 0:
        msg += f" - Rs.{remaining:.2f} unapplied (advance)"

    if manual_bill_no and not applied and not created_receipt and amount > 0:
        # Diagnostic check
        reason = "check number or client"
        filters_global = []
        for candidate in bill_candidates:
            filters_global.append(PendingBill.bill_no.ilike(candidate))
            filters_global.append(PendingBill.nimbus_no.ilike(candidate))
        global_match = PendingBill.query.filter(or_(*filters_global)).first()
        if global_match:
            if not client:
                 reason = f"Bill belongs to {global_match.client_name} (Client not identified)"
            elif global_match.client_code != client.code:
                reason = f"Bill belongs to {global_match.client_name}"
            elif global_match.is_paid:
                reason = "Bill is already paid"
        else:
            reason = "Bill number not found"
        flash(msg + f" (Warning: Could not link to Bill '{manual_bill_no}' - {reason})", 'warning')
    else:
        flash(msg, 'success')

    bill_ref = manual_bill_no or payment.auto_bill_no or f"PAY-{payment.id}"
    return redirect(url_for(
        'payments_page',
        download_bill=bill_ref,
        download_src='payment',
        download_src_id=payment.id,
        download_client_code=(client.code if client else None),
        download_client_name=payment.client_name
    ))


@app.route('/edit_bill/Payment/<int:id>', methods=['POST'])
@login_required
def edit_payment(id):
    payment = Payment.query.get_or_404(id)

    client_code = request.form.get('client_code', '').strip()
    client_name_input = request.form.get('client_name', '').strip()

    # Find client by name or code
    client = get_client_by_input(client_code) or get_client_by_input(client_name_input)
    if client:
        payment.client_name = client.name

    payment.amount = float(request.form.get('amount', 0) or 0)
    if _user_can('can_manage_payments'):
        payment.discount = float(request.form.get('discount', 0) or 0)
        payment.discount_reason = request.form.get('discount_reason', '').strip()
    payment.method = request.form.get('method', 'Cash')
    payment.bank_name = request.form.get('bank_name', '').strip()
    payment.account_name = request.form.get('account_name', '').strip()
    payment.account_no = request.form.get('account_no', '').strip()
    if payment.method not in ['Bank', 'Bank Transfer', 'Check', 'Cheque']:
        payment.bank_name = ''
        payment.account_name = ''
        payment.account_no = ''
    manual_bill_raw = request.form.get('manual_bill_no', '').strip()
    payment.manual_bill_no = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
    payment.note = request.form.get('note', '').strip()

    payment.photo_url = request.form.get('photo_url', '').strip()
    new_photo = save_photo(request.files.get('photo'))
    if new_photo:
        payment.photo_path = new_photo

    if payment.manual_bill_no:
        conflict = find_bill_conflict(payment.manual_bill_no)
        if conflict and not (conflict[0] == 'Payment' and conflict[1] == payment.id):
            flash(f"Manual bill '{payment.manual_bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
            return redirect(url_for('payments_page'))

    _sync_payment_waive_off(payment)
    db.session.commit()
    flash('Payment updated', 'success')

    bill_ref = payment.manual_bill_no or payment.auto_bill_no or f"PAY-{id}"
    payment_client = get_client_by_input(payment.client_name or '')
    return redirect(url_for(
        'payments_page',
        download_bill=bill_ref,
        download_src='payment',
        download_src_id=payment.id,
        download_client_code=(payment_client.code if payment_client else None),
        download_client_name=payment.client_name
    ))


# ==================== DIRECT SALES ROUTES ====================

@app.route('/direct_sales')
@login_required
def direct_sales_page():
    show_mode = (request.args.get('show', 'active') or 'active').strip().lower()
    filter_client = (request.args.get('client') or '').strip()
    filter_bill_no = (request.args.get('bill_no') or '').strip()
    filter_bill_state = (request.args.get('bill_state') or '').strip().lower()  # all|billed|unbilled
    filter_category = (request.args.get('category') or '').strip()
    filter_material = (request.args.get('material') or '').strip()

    sales_q = DirectSale.query
    if show_mode == 'voided':
        sales_q = sales_q.filter(DirectSale.is_void == True)
    elif show_mode == 'all':
        sales_q = sales_q
    else:
        show_mode = 'active'
        sales_q = sales_q.filter(DirectSale.is_void == False)

    if filter_client:
        sales_q = sales_q.filter(DirectSale.client_name.ilike(f'%{filter_client}%'))

    if filter_bill_no:
        sales_q = sales_q.filter(or_(
            DirectSale.manual_bill_no.ilike(f'%{filter_bill_no}%'),
            DirectSale.auto_bill_no.ilike(f'%{filter_bill_no}%'),
            DirectSale.invoice.has(Invoice.invoice_no.ilike(f'%{filter_bill_no}%'))
        ))

    if filter_bill_state == 'billed':
        sales_q = sales_q.filter(or_(
            func.length(func.trim(func.coalesce(DirectSale.manual_bill_no, ''))) > 0,
            DirectSale.invoice_id.isnot(None)
        ))
    elif filter_bill_state == 'unbilled':
        sales_q = sales_q.filter(
            func.length(func.trim(func.coalesce(DirectSale.manual_bill_no, ''))) == 0,
            DirectSale.invoice_id.is_(None)
        )

    if filter_category and filter_category in SALE_CATEGORY_CHOICES:
        sales_q = sales_q.filter(DirectSale.category == filter_category)

    if filter_material:
        sales_q = sales_q.filter(
            DirectSale.items.any(DirectSaleItem.product_name.ilike(f'%{filter_material}%'))
        )

    sales = sales_q.order_by(DirectSale.date_posted.desc()).all()
    active_sales = DirectSale.query.filter_by(is_void=False).all()
    materials = Material.query.order_by(Material.name.asc()).all()
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    delivery_persons = DeliveryPerson.query.filter_by(is_active=True).order_by(DeliveryPerson.name.asc()).all()
    # Keep sales categories concise and business-focused.
    categories = SALE_CATEGORY_CHOICES
    client_name_prefill = request.args.get('client_name', '').strip()
    next_auto = peek_next_bill_no(AUTO_BILL_NAMESPACES['DIRECT_SALE'])

    # Keep counters consistent with row status logic in templates/direct_sales.html
    # BILLED: has manual bill no or linked invoice (except Open Khata)
    # UNBILLED: no manual bill no and no linked invoice (except Open Khata)
    billed_count = 0
    unbilled_count = 0
    for s in active_sales:
        if s.category == 'Open Khata':
            continue
        if s.manual_bill_no or s.invoice_id:
            billed_count += 1
        else:
            unbilled_count += 1

    stats = {
        'billed': billed_count,
        'unbilled': unbilled_count
    }

    settings = Settings.query.first()
    rent_rows = DeliveryRent.query.filter_by(is_void=False).all()
    rents_by_sale = {}
    for rr in rent_rows:
        if rr.sale_id:
            rents_by_sale[rr.sale_id] = rr
    booked_client_codes = []
    for c in clients:
        norm_name = (c.name or '').strip().lower()
        booked_rows = db.session.query(
            BookingItem.material_name,
            func.sum(BookingItem.qty)
        ).join(Booking, BookingItem.booking_id == Booking.id).filter(
            Booking.is_void == False,
            func.lower(func.trim(Booking.client_name)) == norm_name
        ).group_by(BookingItem.material_name).all()
        if not booked_rows:
            continue

        delivered_rows = db.session.query(
            func.coalesce(Entry.booked_material, Entry.material),
            func.sum(Entry.qty)
        ).filter(
            Entry.type == 'OUT',
            Entry.is_void == False,
            or_(
                Entry.client_code == c.code,
                func.lower(func.trim(Entry.client)) == norm_name
            ),
            not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
        ).group_by(func.coalesce(Entry.booked_material, Entry.material)).all()
        delivered_map = {m: (q or 0) for m, q in delivered_rows if m}

        has_balance = any((booked_qty or 0) - delivered_map.get(mat, 0) > 0 for mat, booked_qty in booked_rows)
        if has_balance:
            booked_client_codes.append(c.code)

    sale_form_draft = session.pop('direct_sale_form_draft', None)
    resume_mode = (request.args.get('resume') or '').strip().lower()

    return render_template('direct_sales.html',
                           sales=sales,
                           materials=materials,
                           clients=clients,
                           booked_client_codes=booked_client_codes,
                           delivery_persons=delivery_persons,
                           categories=categories,
                           next_auto=next_auto,
                           client_name_prefill=client_name_prefill,
                           stats=stats,
                           settings=settings,
                           rents_by_sale=rents_by_sale,
                           sale_form_draft=sale_form_draft,
                           resume_mode=resume_mode,
                           show_mode=show_mode,
                           filters={
                               'client': filter_client,
                               'bill_no': filter_bill_no,
                               'bill_state': filter_bill_state or 'all',
                               'category': filter_category,
                               'material': filter_material
                           })


def _collect_direct_sale_form_draft(form_data, mode='add', sale_id=None):
    return {
        'mode': mode,
        'sale_id': sale_id,
        'category': (form_data.get('category') or '').strip(),
        'client_code': (form_data.get('client_code') or '').strip(),
        'client_name': (form_data.get('client_name') or '').strip(),
        'manual_client_name': (form_data.get('manual_client_name') or '').strip(),
        'driver_name': (form_data.get('driver_name') or '').strip(),
        'sale_date': (form_data.get('sale_date') or '').strip(),
        'manual_bill_no': (form_data.get('manual_bill_no') or '').strip(),
        'note': (form_data.get('note') or '').strip(),
        'photo_url': (form_data.get('photo_url') or '').strip(),
        'paid_amount': (form_data.get('paid_amount') or '').strip(),
        'discount': (form_data.get('discount') or '').strip(),
        'discount_reason': (form_data.get('discount_reason') or '').strip(),
        'delivery_rent': (form_data.get('delivery_rent') or '').strip(),
        'allow_negative_stock': str(form_data.get('allow_negative_stock') or '').strip().lower() in ['1', 'true', 'on', 'yes'],
        'has_bill': str(form_data.get('has_bill') or '').strip().lower() in ['1', 'true', 'on', 'yes'],
        'create_invoice': str(form_data.get('create_invoice') or '').strip().lower() in ['1', 'true', 'on', 'yes'],
        'track_as_cash': str(form_data.get('track_as_cash') or '').strip().lower() in ['1', 'true', 'on', 'yes'],
        'items': [
            {
                'product_name': (p or '').strip(),
                'alternate_material': (a or '').strip(),
                'qty': (q or '').strip(),
                'unit_rate': (r or '').strip()
            }
            for p, a, q, r in zip_longest(
                form_data.getlist('product_name[]'),
                form_data.getlist('alternate_material[]'),
                form_data.getlist('qty[]'),
                form_data.getlist('unit_rate[]'),
                fillvalue=''
            )
            if (p or '').strip() or (q or '').strip() or (r or '').strip()
        ]
    }


def _stash_direct_sale_form_draft(form_data, mode='add', sale_id=None):
    session['direct_sale_form_draft'] = _collect_direct_sale_form_draft(
        form_data,
        mode=mode,
        sale_id=sale_id
    )


@app.route('/add_direct_sale', methods=['POST'])
@login_required
def add_direct_sale():
  try:
    def as_bool(val):
        return str(val).strip().lower() in ['1', 'true', 'on', 'yes']

    def _fail_sale(msg):
        flash(msg, 'danger')
        _stash_direct_sale_form_draft(request.form, mode='add')
        return redirect(url_for('direct_sales_page', resume='add'))

    client_name = request.form.get('client_name', '').strip() or request.form.get('client_code', '').strip()
    driver_name = (request.form.get('driver_name') or '').strip()
    materials_list = request.form.getlist('product_name[]')
    alternate_list = request.form.getlist('alternate_material[]')
    qtys = request.form.getlist('qty[]')
    rates = request.form.getlist('unit_rate[]')
    # amount = float(request.form.get('amount', 0) or 0) # Recalculated below
    paid_amount = _to_float_or_zero(request.form.get('paid_amount', 0))
    if _user_can('can_manage_sales'):
        discount = _to_float_or_zero(request.form.get('discount', 0))
        discount_reason = request.form.get('discount_reason', '').strip()
    else:
        discount = 0
        discount_reason = ''
    manual_bill_raw = request.form.get('manual_bill_no', '').strip()
    manual_bill_no = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
    allow_negative_stock = as_bool(request.form.get('allow_negative_stock'))
    note = request.form.get('note', '').strip()
    create_invoice = as_bool(request.form.get('create_invoice'))
    track_as_cash = as_bool(request.form.get('track_as_cash'))
    delivery_rent = _to_float_or_zero(request.form.get('delivery_rent', 0))

    # Check for global setting
    settings = Settings.query.first()
    global_negative_stock_allowed = settings.allow_global_negative_stock if settings else False

    photo_path = save_photo(request.files.get('photo'))
    photo_url = request.form.get('photo_url', '').strip()

    category_input = request.form.get('category', '').strip()
    sale_posted_at = resolve_posted_datetime(request.form.get('sale_date', '').strip(), fallback_dt=pk_now())

    if not driver_name:
        return _fail_sale('Delivery person is required for sale dispatch.')
    get_or_create_delivery_person(driver_name)
    if delivery_rent < 0:
        return _fail_sale('Delivery rent cannot be negative.')
    if discount < 0:
        return _fail_sale('Discount cannot be negative.')

    # Find client by name or code
    client = get_client_by_input(client_name)

    if client:
        client_name = client.name

    # 1. Calculate Booking Balances
    booking_balances = {}
    if client:
        for mat_in in set(materials_list):
            mat_obj = get_material_by_input(mat_in)
            mat_name_in = str(mat_in or '').strip()
            if not mat_obj and mat_name_in:
                mat_obj = Material(code=generate_material_code(), name=mat_name_in, category_id=_get_default_material_category_id())
                db.session.add(mat_obj)
                db.session.flush()
            if not mat_obj:
                continue
            mat = mat_obj.name
            booked = db.session.query(func.sum(BookingItem.qty)).join(Booking).filter(
                Booking.client_name == client.name,
                BookingItem.material_name == mat_obj.name,
                Booking.is_void == False
            ).scalar() or 0
            dispatched = db.session.query(func.sum(Entry.qty)).filter(
                (Entry.client_code == client.code) | (Entry.client == client.name),
                or_(Entry.material == mat, Entry.booked_material == mat),
                Entry.type == 'OUT',
                Entry.is_void == False,
                # Exclude Direct Sales that are NOT Booking Deliveries (i.e. Cash/Credit sales)
                # This prevents regular sales from reducing the booking balance
                not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
            ).scalar() or 0
            booking_balances[mat] = max(0, booked - dispatched)

    # 2. Process Items (Auto-Split Booking vs Sale)
    processed_items = []
    calculated_amount = 0
    missing_rate_non_booked = []

    for idx, mat in enumerate(materials_list):
        qty_val = qtys[idx] if idx < len(qtys) else ''
        rate_val = rates[idx] if idx < len(rates) else ''
        mat_obj = get_material_by_input(mat)
        mat_name_in = str(mat or '').strip()
        if not mat_obj and mat_name_in:
            mat_obj = Material(
                code=generate_material_code(),
                name=mat_name_in,
                unit_price=_to_float_or_zero(rate_val),
                category_id=_get_default_material_category_id()
            )
            db.session.add(mat_obj)
            db.session.flush()
        if not mat_obj or not qty_val:
            continue
        mat_name = mat_obj.name
        qty = _to_float_or_zero(qty_val)
        rate = _to_float_or_zero(rate_val)
        if qty <= 0:
            continue

        balance = booking_balances.get(mat_name, 0)
        qty_booking = 0
        qty_sale = qty

        alt_input = (alternate_list[idx] if idx < len(alternate_list) else '').strip()
        alt_obj = None
        if alt_input:
            alt_obj = get_material_by_input(alt_input)
            if not alt_obj:
                alt_obj = Material(code=generate_material_code(), name=alt_input, category_id=_get_default_material_category_id())
                db.session.add(alt_obj)
                db.session.flush()

        if balance > 0:
            qty_booking = min(qty, balance)
            qty_sale = qty - qty_booking
            booking_balances[mat_name] -= qty_booking

        if alt_input and qty_booking <= 0:
            return _fail_sale(f'Alternate material is only allowed for booked items. "{mat_name}" has no booking balance.')

        if qty_booking > 0:
            delivered_mat = alt_obj.name if alt_obj else mat_name
            processed_items.append({
                'product_name': delivered_mat,
                'booked_material': mat_name,
                'qty': qty_booking,
                'price_at_time': 0,
                'is_booking': True,
                'is_alternate': bool(alt_obj and delivered_mat != mat_name)
            })

        if qty_sale > 0:
            if rate <= 0:
                missing_rate_non_booked.append(mat_name)
            processed_items.append({
                'product_name': mat_name,
                'booked_material': None,
                'qty': qty_sale,
                'price_at_time': rate,
                'is_booking': False,
                'is_alternate': False
            })
            calculated_amount += (qty_sale * rate)

    if not processed_items:
        return _fail_sale('No valid material items were captured. Add at least one item with qty > 0.')

    if missing_rate_non_booked:
        mats = ', '.join(sorted(set(missing_rate_non_booked)))
        return _fail_sale(f'Rate is required for non-booked items: {mats}')

    # 3. Stock Validation (Only for non-booked items)
    # Aggregate required quantities first to prevent cumulative overrun
    required_stock = {}
    required_alt_stock = {}
    for item in processed_items:
        if not item['is_booking']:
            mat = item['product_name']
            required_stock[mat] = required_stock.get(mat, 0) + item['qty']
        elif item.get('is_alternate'):
            mat = item.get('booked_material') or item['product_name']
            required_alt_stock[mat] = required_alt_stock.get(mat, 0) + item['qty']

    for mat, req_qty in required_stock.items():
        mat_obj = Material.query.filter_by(name=mat).first()
        if mat_obj:
            available = mat_obj.total or 0
            if not allow_negative_stock and not global_negative_stock_allowed and available < req_qty:
                raise ValueError(f"Insufficient stock for {mat}. Available: {available}, Required: {req_qty} (Non-booked). Enable 'Allow Negative Stock' or global setting to bypass.")
    for mat, req_qty in required_alt_stock.items():
        mat_obj = Material.query.filter_by(name=mat).first()
        if mat_obj:
            available = mat_obj.total or 0
            if not allow_negative_stock and not global_negative_stock_allowed and available < req_qty:
                raise ValueError(f"Insufficient stock for {mat}. Available: {available}, Required: {req_qty} (Alternate booking from original). Enable 'Allow Negative Stock' or global setting to bypass.")

    # Compute whether this client has any active booking balance (across all materials).
    has_client_booking_balance = False
    if client:
        norm_name = (client.name or '').strip().lower()
        booked_rows_all = db.session.query(
            BookingItem.material_name,
            func.sum(BookingItem.qty)
        ).join(Booking, BookingItem.booking_id == Booking.id).filter(
            Booking.is_void == False,
            func.lower(func.trim(Booking.client_name)) == norm_name
        ).group_by(BookingItem.material_name).all()
        if booked_rows_all:
            delivered_rows_all = db.session.query(
                func.coalesce(Entry.booked_material, Entry.material),
                func.sum(Entry.qty)
            ).filter(
                Entry.type == 'OUT',
                Entry.is_void == False,
                or_(
                    Entry.client_code == client.code,
                    func.lower(func.trim(Entry.client)) == norm_name
                ),
                not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
            ).group_by(func.coalesce(Entry.booked_material, Entry.material)).all()
            delivered_map_all = {m: (q or 0) for m, q in delivered_rows_all if m}
            has_client_booking_balance = any(
                (booked_qty or 0) - delivered_map_all.get(mat, 0) > 0
                for mat, booked_qty in booked_rows_all
            )

    # 4. Determine Final Category & Amount
    amount = calculated_amount
    all_booking = all(item['is_booking'] for item in processed_items)
    any_booking = any(item['is_booking'] for item in processed_items)
    category = normalize_sale_category(category_input)
    category_input_l = category.lower()

    # Enforce selected sale-type policy.
    if category_input_l in ['booking delivery', 'mixed transaction', 'credit customer'] and not client:
        return _fail_sale('Select a registered client from the client list for this sale type.')

    if category == 'Booking Delivery':
        if not has_client_booking_balance or not all_booking:
            return _fail_sale('Booked Sale is only for clients with booking balance and booked materials only.')
        paid_amount = 0
    elif category == 'Mixed Transaction':
        if not has_client_booking_balance or not any_booking:
            return _fail_sale('Booked + Credit is only for clients with booking balance and must include booked items.')
        if all_booking or amount <= 0:
            return _fail_sale('Booked + Credit must include a non-booked credit portion with amount > 0.')
    elif category == 'Credit Customer':
        # Due sale can be used for any registered client; it must remain pure chargeable.
        if any_booking:
            return _fail_sale('Credit Sale cannot include booked-material fulfillment.')
    elif category == 'Open Khata':
        # Open Khata is for unregistered walk-in style credit.
        if client and client.code != OPEN_KHATA_CODE:
            return _fail_sale('Open Khata is only for unregistered customers (not selected from client list).')
    else:
        category = 'Cash'

    if discount > (amount + 0.01):
        return _fail_sale('Discount cannot exceed total amount.')

    # Validation: Unbilled Cash Sale must be fully paid
    if category == 'Cash' and (paid_amount + discount) < (amount - 0.01):
        return _fail_sale('Cash Sale must be fully paid. Transaction not complete.')
    
    # Validation: Billed/credit sales must have manual bill
    if category in ['Credit Customer', 'Mixed Transaction', 'Open Khata'] and not manual_bill_no:
        return _fail_sale('Manual Bill No is required for billed sales. Please enter it before saving.')

    # Validation: Manual bill must be unique
    if manual_bill_no:
        conflict = find_bill_conflict(manual_bill_no)
        if conflict:
            return _fail_sale(f"Bill No '{manual_bill_no}' already exists. Please open the existing bill and edit it instead.")

    # Handle Cash category (Manual overrides)
    if category == 'Cash':
        manual_client_name = request.form.get('manual_client_name', '').strip()
        if manual_client_name:
            client_name = manual_client_name
    elif category == 'Open Khata':
        manual_client_name = request.form.get('manual_client_name', '').strip()
        if not manual_client_name:
            return _fail_sale('Open Khata requires manual customer name.')
        client_name = manual_client_name
        create_invoice = False
        track_as_cash = False

    # Force manual bill requirement for non-cash sales if not provided
    if category != 'Cash' and not manual_bill_no and not create_invoice:
        # We allow it but it will be auto-generated or marked as system bill
        pass

    hv = request.form.get('has_bill')
    has_bill = True if hv is None else hv in ['on', '1', 'true', 'True']

    pending_amount = max(0.0, amount - discount - paid_amount)
    sale_client_code = client.code if client else None
    if category == 'Open Khata' and not sale_client_code:
        sale_client_code = OPEN_KHATA_CODE
    rent_rec = _rent_reconciliation_from_items(
        processed_items,
        delivery_rent_cost=delivery_rent,
        client_name=client_name,
        client_code=sale_client_code
    )

    if (
        category in ['Mixed Transaction', 'Credit Customer', 'Open Khata']
        and pending_amount <= 0
        and discount <= 0
    ):
        return _fail_sale('This sale type is for credit only. Use Cash Sale if fully paid.')

    # Handle invoice creation
    inv = None
    invoice_no = None
    if create_invoice:
        if manual_bill_no:
            invoice_no = manual_bill_no
            is_manual = True
        else:
            # Invoice without manual bill no (auto)
            invoice_no = f"INV-{pk_now().strftime('%Y%m%d%H%M%S')}"
            is_manual = False

        existing_global = Invoice.query.filter_by(invoice_no=invoice_no).first()
        if existing_global:
            if is_manual:
                return _fail_sale(f'Invoice number "{invoice_no}" is already used. Please use a different number.')
            # Auto invoice: ensure uniqueness instead of reusing/updating
            while Invoice.query.filter_by(invoice_no=invoice_no).first():
                invoice_no = f"INV-{pk_now().strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(2).upper()}"

        balance = max(0.0, amount - discount - paid_amount)
        status = 'PAID' if balance <= 0 else ('PARTIAL' if paid_amount > 0 else 'OPEN')

        inv = Invoice(client_code=sale_client_code,
                      client_name=client.name if client else client_name,
                      invoice_no=invoice_no,
                      is_manual=is_manual,
                      date=sale_posted_at.date(),
                      total_amount=amount,
                      # Note: Invoice model might not have discount column, so we reflect it in balance
                      balance=balance,
                      status=status,
                      is_cash=track_as_cash,
                      note=note,
                      created_at=sale_posted_at.strftime('%Y-%m-%d %H:%M'),
                      created_by=current_user.username)
        db.session.add(inv)
        db.session.flush()

    auto_bill_no = get_next_bill_no(AUTO_BILL_NAMESPACES['DIRECT_SALE'])

    sale = DirectSale(client_name=client_name,
                      amount=amount,
                      paid_amount=paid_amount,
                      discount=discount,
                      discount_reason=discount_reason,
                      manual_bill_no=manual_bill_no,
                      auto_bill_no=auto_bill_no,
                      photo_path=photo_path,
                      photo_url=photo_url,
                      category=category,
                      note=note,
                      driver_name=driver_name,
                      rent_item_revenue=rent_rec['rent_item_revenue'],
                      delivery_rent_cost=rent_rec['delivery_rent_cost'],
                      rent_variance_loss=rent_rec['rent_variance_loss'],
                      date_posted=sale_posted_at)
    db.session.add(sale)
    db.session.flush()

    if create_invoice and inv:
        sale.invoice_id = inv.id

    # Create DirectSaleItems and Entries
    now = sale_posted_at
    for item in processed_items:
        # Create Sale Item
        dsi = DirectSaleItem(sale_id=sale.id,
                           product_name=item['product_name'],
                           qty=item['qty'],
                           price_at_time=item['price_at_time'])
        db.session.add(dsi)

        # Create Entry
        ledger_bill_ref = manual_bill_no or (inv.invoice_no if (create_invoice and inv) else (sale.auto_bill_no or ("UNBILLED-" + str(sale.id))))

        # Determine category per item for mixed transactions
        item_category = category
        if item['is_booking']:
            item_category = 'Booking Delivery'
        elif category == 'Mixed Transaction':
            item_category = 'Credit Customer'
        elif category == 'Booking Delivery': # Fallback if main cat is Booking but this item isn't (shouldn't happen with split logic)
            item_category = 'Credit Customer'

        entry_note = note
        if item.get('is_booking') and item.get('is_alternate') and item.get('booked_material'):
            extra = f"Alternate Material for Booked Sale (Original: {item['booked_material']})"
            entry_note = f"{note} | {extra}" if note else extra
        entry = Entry(date=now.strftime('%Y-%m-%d'),
                      time=now.strftime('%H:%M:%S'),
                      type='OUT',
                      material=item['product_name'],
                      booked_material=(item.get('booked_material') if item.get('is_alternate') else None),
                      client=client_name,
                      client_code=sale_client_code,
                      qty=item['qty'],
                      bill_no=ledger_bill_ref,
                      nimbus_no='Direct Sale',
                      created_by=current_user.username,
                      client_category=item_category,
                      transaction_category=('Unbilled' if category == 'Cash' else 'Billed'),
                      driver_name=driver_name,
                      note=entry_note,
                      is_alternate=bool(item.get('is_alternate')))
        db.session.add(entry)

        # Update Material stock (reduce In Hand)
        stock_name = item.get('booked_material') if item.get('is_alternate') else item['product_name']
        mat_obj = Material.query.filter_by(name=stock_name).first()
        if mat_obj:
            mat_obj.total = (mat_obj.total or 0) - item['qty']

    _sync_direct_sale_pending_bill(sale, materials_list[0] if materials_list else '')
    _sync_delivery_rent_for_sale(
        sale,
        rent_amount=delivery_rent,
        rent_note=''
    )
    _sync_direct_sale_waive_off(sale)

    db.session.commit()
    msg = 'Direct sale added successfully'
    if create_invoice and inv:
        msg += f" â€” Invoice: {inv.invoice_no}"
    flash(msg, 'success')
  except Exception as e:
    db.session.rollback()
    logging.error(f"Direct Sale Error: {str(e)}")
    flash(f"Error processing sale: {str(e)}", "danger")
    _stash_direct_sale_form_draft(request.form, mode='add')
    return redirect(url_for('direct_sales_page', resume='add'))

  # Success redirect
  if manual_bill_no:
      bill_ref = manual_bill_no
  elif create_invoice and inv:
      bill_ref = inv.invoice_no
  elif sale.auto_bill_no:
      bill_ref = sale.auto_bill_no
  else:
      bill_ref = f"CSH-{sale.id}" if category == 'Cash' else f"DS-{sale.id}"

  return redirect(url_for(
      'direct_sales_page',
      download_bill=bill_ref,
      download_src='direct_sale',
      download_src_id=sale.id,
      download_client_code=sale_client_code,
      download_client_name=sale.client_name
  ))


@app.route('/add_sale', methods=['POST'])
@login_required
def add_sale():
    return add_direct_sale()


@app.route('/edit_bill/DirectSale/<int:id>', methods=['POST'])
@login_required
def edit_direct_sale(id):
    try:
        def as_bool(val):
            return str(val).strip().lower() in ['1', 'true', 'on', 'yes']
        def _fail_edit(msg):
            flash(msg, 'danger')
            _stash_direct_sale_form_draft(request.form, mode='edit', sale_id=id)
            return redirect(url_for('direct_sales_page', resume='edit', sale_id=id))
        def _is_self_owned_sale_conflict(conflict_row, sale_obj, candidate_bill_no):
            """
            Allow edit when conflict points to records that belong to this same sale
            (linked invoice or derived direct-sale pending row).
            """
            if not conflict_row:
                return False
            src, row_id = conflict_row
            bill_variants = set(_bill_no_variants(candidate_bill_no))
            bill_variants.update(_direct_sale_bill_refs(sale_obj))

            if src == 'Invoice':
                return bool(getattr(sale_obj, 'invoice_id', None) and sale_obj.invoice_id == row_id)

            if src == 'PendingBill':
                pb = db.session.get(PendingBill, row_id)
                if not pb:
                    return False
                pb_bill = (pb.bill_no or '').strip()
                pb_reason = (pb.reason or '').strip().lower()
                same_bill = pb_bill in bill_variants
                # Direct-sale pending row is a derived tracker, not a true duplicate owner.
                return bool(same_bill and pb_reason.startswith('direct sale'))

            return False

        sale = DirectSale.query.get_or_404(id)
        old_refs = _direct_sale_bill_refs(sale)
        old_active_entries = Entry.query.filter(
            Entry.bill_no.in_(old_refs),
            Entry.nimbus_no == 'Direct Sale',
            Entry.is_void == False
        ).all()

        category = normalize_sale_category(request.form.get('category', ''), default='Credit Customer')
        client_code = request.form.get('client_code', '').strip()
        client_name_input = request.form.get('client_name', '').strip()
        manual_client_name = request.form.get('manual_client_name', '').strip()
        client = get_client_by_input(client_code) or get_client_by_input(client_name_input)
        if category == 'Open Khata':
            if not manual_client_name:
                return _fail_edit('Open Khata requires manual customer name.')
            sale.client_name = manual_client_name
            client = None
        elif category == 'Cash' and manual_client_name:
            sale.client_name = manual_client_name
            client = None
        else:
            if client:
                sale.client_name = client.name
            elif client_name_input:
                sale.client_name = client_name_input

        # For registered-client sale types, force selection from client master.
        if category in ['Booking Delivery', 'Mixed Transaction', 'Credit Customer'] and not client:
            return _fail_edit('Select a registered client from the client list. Partial/manual client text is not allowed for this sale type.')

        driver_name = (request.form.get('driver_name') or sale.driver_name or '').strip()
        manual_bill_raw = request.form.get('manual_bill_no', '').strip()
        manual_bill_no = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
        paid_amount = _to_float_or_zero(request.form.get('paid_amount', 0))
        if _user_can('can_manage_sales'):
            discount = _to_float_or_zero(request.form.get('discount', 0))
            discount_reason = request.form.get('discount_reason', '').strip()
        else:
            discount = sale.discount or 0
            discount_reason = sale.discount_reason or ''
        note = request.form.get('note', '').strip()
        # Normalize posted timestamp via PK resolver for consistent timezone handling.
        sale_posted_at = resolve_posted_datetime(
            request.form.get('sale_date', '').strip(),
            fallback_dt=sale.date_posted or pk_now()
        )
        delivery_rent = _to_float_or_zero(request.form.get('delivery_rent', 0))

        if not driver_name:
            return _fail_edit('Delivery person is required for sale dispatch.')
        get_or_create_delivery_person(driver_name)
        if delivery_rent < 0:
            return _fail_edit('Delivery rent cannot be negative.')
        if discount < 0:
            return _fail_edit('Discount cannot be negative.')

        if category in ['Credit Customer', 'Mixed Transaction', 'Open Khata'] and not manual_bill_no:
            return _fail_edit('Manual Bill No is required for billed sales. Please enter it before saving.')

        if manual_bill_no:
            conflict = find_bill_conflict(manual_bill_no, exclude_sale_id=sale.id)
            if conflict and not _is_self_owned_sale_conflict(conflict, sale, manual_bill_no):
                return _fail_edit(f"Bill No '{manual_bill_no}' already exists. Please open the existing bill and edit it instead.")

        materials_list = request.form.getlist('product_name[]')
        alternate_list = request.form.getlist('alternate_material[]')
        qtys = request.form.getlist('qty[]')
        rates = request.form.getlist('unit_rate[]')

        parsed_items = []
        max_len = max(len(materials_list), len(alternate_list), len(qtys), len(rates))
        for idx in range(max_len):
            mat = materials_list[idx] if idx < len(materials_list) else ''
            alt = alternate_list[idx] if idx < len(alternate_list) else ''
            qty = qtys[idx] if idx < len(qtys) else ''
            rate = rates[idx] if idx < len(rates) else ''
            mat_name_in = str(mat or '').strip()
            if not mat_name_in:
                continue
            mat_obj = get_material_by_input(mat_name_in)
            if not mat_obj:
                return _fail_edit(f'Select a valid material from list. "{mat_name_in}" was not found.')
            qty_val = _to_float_or_zero(qty)
            if qty_val <= 0:
                continue
            rate_val = _to_float_or_zero(rate)
            if rate_val < 0:
                rate_val = 0
            alt_name_in = str(alt or '').strip()
            alt_obj = None
            if alt_name_in:
                alt_obj = get_material_by_input(alt_name_in)
                if not alt_obj:
                    return _fail_edit(f'Select a valid alternate material from list. "{alt_name_in}" was not found.')
            if alt_obj and rate_val > 0:
                return _fail_edit('Alternate material is only allowed for booked items (rate 0).')
            delivered_name = alt_obj.name if alt_obj else mat_obj.name
            parsed_items.append({
                'product_name': delivered_name,
                'booked_material': (mat_obj.name if alt_obj and delivered_name != mat_obj.name else None),
                'is_alternate': bool(alt_obj and delivered_name != mat_obj.name),
                'qty': qty_val,
                'price_at_time': rate_val
            })

        if not parsed_items:
            return _fail_edit('No valid material items were captured. Add at least one item with qty > 0.')

        any_booking_item = any(float(i['price_at_time'] or 0) <= 0 for i in parsed_items)
        any_chargeable_item = any(float(i['price_at_time'] or 0) > 0 for i in parsed_items)
        amount = sum((float(i['qty'] or 0) * float(i['price_at_time'] or 0)) for i in parsed_items)
        if discount > (amount + 0.01):
            return _fail_edit('Discount cannot exceed total amount.')

        if category == 'Booking Delivery':
            if any_chargeable_item:
                return _fail_edit('Booked Sale can only contain reserved items (rate 0).')
            amount = 0
            paid_amount = 0
        elif category == 'Mixed Transaction':
            if not any_booking_item or not any_chargeable_item:
                return _fail_edit('Booked + Credit must contain both booked (rate 0) and non-booked (rate > 0) items.')
        else:
            if any_booking_item:
                return _fail_edit('This sale type cannot include booked items (rate 0).')
            if not any_chargeable_item:
                return _fail_edit('This sale type requires chargeable items with rate > 0.')

        if category == 'Cash' and (paid_amount + discount) < (amount - 0.01):
            return _fail_edit('Cash Sale must be fully paid. Transaction not complete.')

        if (
            category in ['Credit Customer', 'Mixed Transaction', 'Open Khata']
            and max(0.0, amount - discount - paid_amount) <= 0
            and discount <= 0
        ):
            return _fail_edit('This sale type is for credit only. Use Cash Sale if fully paid.')

        if category == 'Open Khata' and not sale.client_name:
            sale.client_name = OPEN_KHATA_NAME
        rent_rec = _rent_reconciliation_from_items(
            parsed_items,
            delivery_rent_cost=delivery_rent,
            client_name=sale.client_name,
            client_code=(client.code if client else (OPEN_KHATA_CODE if category == 'Open Khata' else None))
        )

        sale.category = category
        sale.driver_name = driver_name
        sale.amount = amount
        sale.discount = discount
        sale.discount_reason = discount_reason
        sale.paid_amount = paid_amount
        sale.rent_item_revenue = rent_rec['rent_item_revenue']
        sale.delivery_rent_cost = rent_rec['delivery_rent_cost']
        sale.rent_variance_loss = rent_rec['rent_variance_loss']
        sale.manual_bill_no = manual_bill_no
        sale.note = note
        sale.date_posted = sale_posted_at

        sale.photo_url = request.form.get('photo_url', '').strip()
        new_photo = save_photo(request.files.get('photo'))
        if new_photo:
            sale.photo_path = new_photo

        # Preserve alternate-booking mapping from previous active rows when edit form
        # does not explicitly carry alternate source fields.
        old_alt_candidates = []
        for e in old_active_entries:
            bm = (e.booked_material or '').strip()
            if not bm:
                continue
            old_alt_candidates.append({
                'material': (e.material or '').strip(),
                'qty': float(e.qty or 0),
                'booked_material': bm,
                'used': False
            })

        def _take_old_booked_material(delivered_material, qty_val):
            delivered = (delivered_material or '').strip()
            try:
                q = float(qty_val or 0)
            except Exception:
                q = 0.0
            for row in old_alt_candidates:
                if row['used']:
                    continue
                if row['material'] != delivered:
                    continue
                if abs(float(row['qty'] or 0) - q) > 0.0001:
                    continue
                row['used'] = True
                return row['booked_material']
            return None

        _void_direct_sale_entries_and_restore_stock(sale, refs=old_refs)
        DirectSaleItem.query.filter_by(sale_id=id).delete()

        bill_ref = _direct_sale_default_bill_ref(sale)
        entry_client_obj = client or get_client_by_input(sale.client_name or '')
        for item in parsed_items:
            db.session.add(DirectSaleItem(
                sale_id=sale.id,
                product_name=item['product_name'],
                qty=item['qty'],
                price_at_time=item['price_at_time']
            ))
            inferred_booked_material = item.get('booked_material')
            if (not inferred_booked_material) and float(item.get('price_at_time') or 0) <= 0:
                inferred_booked_material = _take_old_booked_material(item['product_name'], item['qty'])
            is_alt = bool(
                inferred_booked_material and
                inferred_booked_material != item['product_name']
            )
            entry = Entry(
                date=sale_posted_at.strftime('%Y-%m-%d'),
                time=sale_posted_at.strftime('%H:%M:%S'),
                type='OUT',
                material=item['product_name'],
                booked_material=(inferred_booked_material if is_alt else None),
                client=sale.client_name,
                client_code=(entry_client_obj.code if entry_client_obj else (OPEN_KHATA_CODE if category == 'Open Khata' else None)),
                qty=item['qty'],
                bill_no=bill_ref,
                nimbus_no='Direct Sale',
                created_by=current_user.username,
                client_category=_direct_sale_item_category(category, item['price_at_time']),
                transaction_category=('Unbilled' if category == 'Cash' else 'Billed'),
                driver_name=driver_name,
                note=note,
                is_alternate=is_alt
            )
            db.session.add(entry)
            stock_name = inferred_booked_material if is_alt else item['product_name']
            mat_obj = Material.query.filter_by(name=stock_name).first()
            if mat_obj:
                mat_obj.total = (mat_obj.total or 0) - item['qty']

        _sync_direct_sale_pending_bill(
            sale,
            parsed_items[0]['product_name'] if parsed_items else '',
            extra_void_refs=old_refs
        )
        _sync_delivery_rent_for_sale(
            sale,
            rent_amount=delivery_rent,
            rent_note=''
        )
        _sync_direct_sale_waive_off(sale)
        db.session.commit()
        flash('Direct sale updated and resynced', 'success')
        resolved_client_code = (entry_client_obj.code if entry_client_obj else (OPEN_KHATA_CODE if category == 'Open Khata' else None))
        return redirect(url_for(
            'direct_sales_page',
            download_bill=bill_ref,
            download_src='direct_sale',
            download_src_id=sale.id,
            download_client_code=resolved_client_code,
            download_client_name=sale.client_name
        ))
    except Exception as e:
        db.session.rollback()
        logging.error(f"Direct Sale Edit Error: {str(e)}")
        flash(f"Error updating sale: {str(e)}", "danger")
        _stash_direct_sale_form_draft(request.form, mode='edit', sale_id=id)
        return redirect(url_for('direct_sales_page', resume='edit', sale_id=id))


@app.route('/void_transaction/<string:type>/<int:id>', methods=['POST'])
@login_required
def void_transaction(type, id):
    if not _user_can('can_manage_sales'):
        flash('Permission denied', 'danger')
        return redirect(request.referrer or url_for('index'))

    if type == 'Entry':
        entry = db.session.get(Entry, id)
        if _set_entry_void_state(entry, True):
            flash('Entry voided and stock reversed', 'success')

    elif type == 'DirectSale':
        sale = db.session.get(DirectSale, id)
        if _set_direct_sale_void_state(sale, True):
            flash('Sale voided', 'success')

    elif type == 'Booking':
        bk = db.session.get(Booking, id)
        if _set_booking_void_state(bk, True):
            flash('Booking voided', 'success')

    elif type == 'Payment':
        pay = db.session.get(Payment, id)
        if _set_payment_void_state(pay, True):
            flash('Payment voided', 'success')

    db.session.commit()
    return redirect(request.referrer or url_for('index'))


@app.route('/unvoid_transaction/<string:type>/<int:id>', methods=['POST'])
@login_required
def unvoid_transaction(type, id):
    if not _user_can('can_manage_sales'):
        flash('Permission denied', 'danger')
        return redirect(request.referrer or url_for('index'))

    if type == 'Entry':
        entry = db.session.get(Entry, id)
        if _set_entry_void_state(entry, False):
            flash('Entry restored and stock reapplied', 'success')

    elif type == 'DirectSale':
        sale = db.session.get(DirectSale, id)
        if _set_direct_sale_void_state(sale, False):
            flash('Sale restored', 'success')

    elif type == 'Booking':
        bk = db.session.get(Booking, id)
        if _set_booking_void_state(bk, False):
            flash('Booking restored', 'success')

    elif type == 'Payment':
        pay = db.session.get(Payment, id)
        if _set_payment_void_state(pay, False):
            flash('Payment restored', 'success')

    db.session.commit()
    return redirect(request.referrer or url_for('index'))


# ==================== BILL ROUTES ====================

def _bill_no_variants(ref):
    out = []
    val = (ref or '').strip()
    if not val:
        return out

    # Some routes/templates can double-encode '#' as %2523, so decode progressively.
    seed_values = [val]
    if '%' in val:
        decoded = val
        for _ in range(3):
            next_decoded = unquote(decoded).strip()
            if not next_decoded or next_decoded == decoded:
                break
            seed_values.append(next_decoded)
            decoded = next_decoded

    for seed in [x for x in dict.fromkeys(seed_values) if x]:
        out.append(seed)
        kind = parse_bill_kind(seed)
        if kind == 'SB':
            parsed_ns, parsed_seq = _extract_sb_parts(seed)
            if parsed_seq is not None:
                ns = parsed_ns or AUTO_BILL_NS_DEFAULT
                out.append(normalize_auto_bill(str(parsed_seq), namespace=ns))
                # Keep numeric aliases only for legacy no-namespace values.
                if parsed_ns is None:
                    out.append(str(parsed_seq))
                    out.append(f'#{parsed_seq}')
        elif kind == 'MB':
            body = seed.split('.', 1)[1].strip() if '.' in seed else ''
            if body:
                out.append(normalize_manual_bill(body))
                out.append(body)
                out.append(f'#{body}')
        else:
            maybe_auto = normalize_auto_bill(seed, namespace=AUTO_BILL_NS_DEFAULT)
            maybe_manual = normalize_manual_bill(seed)
            if maybe_auto:
                out.append(maybe_auto)
            if maybe_manual:
                out.append(maybe_manual)
        # Legacy/imported rows can carry integer bill numbers as float-like text (e.g. "6230.0").
        # Normalize those to integer-style variants so lookup remains stable across sources.
        if re.fullmatch(r'\d+\.0+', seed):
            int_like = seed.split('.', 1)[0]
            if int_like:
                out.append(int_like)
                out.append(f'#{int_like}')
                out.append(normalize_auto_bill(int_like, namespace=AUTO_BILL_NS_DEFAULT))
                out.append(normalize_manual_bill(int_like))
        if seed.startswith('#') and re.fullmatch(r'\d+\.0+', seed[1:]):
            int_like = seed[1:].split('.', 1)[0]
            if int_like:
                out.append(int_like)
                out.append(f'#{int_like}')
                out.append(normalize_auto_bill(int_like, namespace=AUTO_BILL_NS_DEFAULT))
                out.append(normalize_manual_bill(int_like))
        if seed.startswith('#') and len(seed) > 1:
            out.append(seed[1:])
            out.append(normalize_auto_bill(seed[1:], namespace=AUTO_BILL_NS_DEFAULT))
            out.append(normalize_manual_bill(seed[1:]))
        elif seed.isdigit():
            out.append(f'#{seed}')
            out.append(normalize_auto_bill(seed, namespace=AUTO_BILL_NS_DEFAULT))
            out.append(normalize_manual_bill(seed))

    return [x for x in dict.fromkeys(out) if x]


def _collect_bill_refs_for_lookup(bill_type, bill_obj):
    if not bill_obj:
        return []
    refs = set()
    t = (bill_type or '').strip()
    if t == 'Booking':
        refs.update([getattr(bill_obj, 'manual_bill_no', None), getattr(bill_obj, 'auto_bill_no', None), f"BK-{bill_obj.id}"])
    elif t == 'Payment':
        refs.update([getattr(bill_obj, 'manual_bill_no', None), getattr(bill_obj, 'auto_bill_no', None), f"PAY-{bill_obj.id}"])
    elif t == 'DirectSale':
        refs.update([getattr(bill_obj, 'manual_bill_no', None), getattr(bill_obj, 'auto_bill_no', None), f"DS-{bill_obj.id}", f"CSH-{bill_obj.id}", f"UNBILLED-{bill_obj.id}"])
        if getattr(bill_obj, 'invoice', None) and bill_obj.invoice and bill_obj.invoice.invoice_no:
            refs.add(bill_obj.invoice.invoice_no)
    elif t == 'Invoice':
        refs.update([getattr(bill_obj, 'invoice_no', None)])
    elif t == 'GRN':
        refs.update([getattr(bill_obj, 'manual_bill_no', None), getattr(bill_obj, 'auto_bill_no', None)])
    elif t == 'PendingBill':
        refs.update([getattr(bill_obj, 'bill_no', None), getattr(bill_obj, 'manual_bill_no', None), getattr(bill_obj, 'auto_bill_no', None)])
    refs = {r for r in refs if r}
    all_refs = set()
    for r in refs:
        all_refs.update(_bill_no_variants(r))
    return [r for r in all_refs if r]


def _latest_entry_for_bill_refs(bill_refs, client_code=None, client_name=None):
    refs = [r for r in (bill_refs or []) if r]
    if not refs:
        return None
    q = Entry.query.filter(
        Entry.is_void == False,
        Entry.bill_no.in_(refs)
    )
    if client_code:
        q = q.filter(Entry.client_code == client_code)
    elif client_name:
        q = q.filter(func.lower(func.trim(func.coalesce(Entry.client, ''))) == client_name.strip().lower())
    return q.order_by(Entry.date.desc(), Entry.time.desc(), Entry.id.desc()).first()


def _entry_best_bill_ref(entry_obj):
    if not entry_obj:
        return ''
    primary = (getattr(entry_obj, 'bill_no', None) or '').strip()
    auto = (getattr(entry_obj, 'auto_bill_no', None) or '').strip()
    if primary and not primary.upper().startswith('UNBILLED'):
        return primary
    if auto and not auto.upper().startswith('UNBILLED'):
        return auto
    inv_id = getattr(entry_obj, 'invoice_id', None)
    if inv_id:
        inv = db.session.get(Invoice, inv_id)
        if inv and not inv.is_void and inv.invoice_no:
            return (inv.invoice_no or '').strip()
    return ''


def _resolve_transaction_type(bill_type, bill_obj, entry_hint_id=None):
    default = ('general_transaction', 'General Transaction', '')
    if not bill_obj:
        return default

    t = (bill_type or '').strip()
    client_code = getattr(bill_obj, 'client_code', None)
    client_name = (getattr(bill_obj, 'client_name', None) or '').strip()
    bill_refs = _collect_bill_refs_for_lookup(t, bill_obj)

    hinted_entry = None
    if entry_hint_id:
        try:
            hinted_entry = db.session.get(Entry, int(entry_hint_id))
        except Exception:
            hinted_entry = None
        if hinted_entry and hinted_entry.is_void:
            hinted_entry = None
        if hinted_entry and bill_refs and (hinted_entry.bill_no not in bill_refs):
            hinted_entry = None

    latest_entry = hinted_entry or _latest_entry_for_bill_refs(bill_refs, client_code=client_code, client_name=client_name)

    if t == 'GRN':
        return ('grn_purchase', 'GRN / Purchase', 'Stock receiving purchase bill')

    if t == 'Payment':
        method = (getattr(bill_obj, 'method', '') or '').strip()
        note = f"Method: {method}" if method else ''
        return ('payment_only', 'Payment Only', note)

    if t == 'DirectSale':
        cat = normalize_sale_category(getattr(bill_obj, 'category', None))
        mapping = {
            'Cash': ('direct_sale_cash', 'Direct Sale (Cash)'),
            'Credit Customer': ('direct_sale_credit', 'Direct Sale (Credit)'),
            'Mixed Transaction': ('direct_sale_mixed', 'Direct Sale (Booked + Due)'),
            'Booking Delivery': ('direct_sale_booked', 'Direct Sale (Booked Delivery)'),
            'Open Khata': ('direct_sale_open_khata', 'Direct Sale (Open Khata)'),
        }
        code, label = mapping.get(cat, ('direct_sale', 'Direct Sale'))
        display_cat = 'Booked + Due' if cat == 'Mixed Transaction' else cat
        return (code, label, f"Sale Category: {display_cat}")

    # Invoice linked to direct sale must keep direct-sale labeling, not entry fallback.
    if t == 'Invoice':
        if getattr(bill_obj, 'direct_sales', None):
            ds = bill_obj.direct_sales[0] if bill_obj.direct_sales else None
            if ds:
                return _resolve_transaction_type('DirectSale', ds, entry_hint_id=entry_hint_id)
        return ('invoice', 'Invoice', 'General invoice record')

    if latest_entry:
        e_type = (latest_entry.type or '').strip().upper()
        nimbus = (latest_entry.nimbus_no or '').strip().lower()
        tcat = (latest_entry.transaction_category or '').strip().lower()
        if e_type == 'CANCEL' or 'cancel' in nimbus or 'cancel' in tcat:
            return ('cancellation', 'Cancellation', 'Booking cancellation / reversal')
        if e_type == 'OUT' or 'delivery' in nimbus:
            return ('delivery', 'Delivery', 'Material delivery transaction')

    if t == 'Booking':
        return ('booking', 'Booking', 'Booked / reserved material bill')

    if t == 'PendingBill':
        reason = (getattr(bill_obj, 'reason', '') or '').strip().lower()
        if reason.startswith('booking'):
            return ('booking', 'Booking', getattr(bill_obj, 'reason', ''))
        if 'payment received' in reason:
            return ('payment_only', 'Payment Only', getattr(bill_obj, 'reason', ''))
        if 'direct sale' in reason or reason.startswith('auto sale'):
            return ('direct_sale', 'Direct Sale', getattr(bill_obj, 'reason', ''))
        if 'cancel' in reason:
            return ('cancellation', 'Cancellation', getattr(bill_obj, 'reason', ''))
        return ('pending_adjustment', 'Pending / Adjustment', getattr(bill_obj, 'reason', ''))

    return default


def _lookup_bill(bill_no, hint_type=None, hint_id=None, hint_client_code=None, hint_client_name=None, hint_entry_id=None):
    """Resolve a bill number to an object; use optional hints to avoid collisions on imported legacy data."""
    hint_type = (hint_type or '').strip().lower()
    hint_client_name_norm = (hint_client_name or '').strip().lower()
    bill_variants = _bill_no_variants(bill_no)

    if hint_entry_id:
        try:
            hinted_entry = db.session.get(Entry, int(hint_entry_id))
        except Exception:
            hinted_entry = None
        if hinted_entry and not hinted_entry.is_void and hinted_entry.bill_no in bill_variants:
            if not hint_client_code:
                hint_client_code = hinted_entry.client_code
            if not hint_client_name:
                hint_client_name = hinted_entry.client

    def _bill_or_expr(model_manual, model_auto=None):
        clauses = []
        for b in bill_variants:
            clauses.append(model_manual == b)
            if model_auto is not None:
                clauses.append(model_auto == b)
        return or_(*clauses) if clauses else (model_manual == bill_no)

    booking = None
    payment = None
    invoice = None
    sale = None
    grn = None
    pending = None

    # If caller gives exact source + id, trust that first to avoid bill_no collisions.
    if hint_id:
        try:
            hid = int(hint_id)
        except Exception:
            hid = None
        if hid:
            if hint_type in ['booking', 'booked', 'bk']:
                row = db.session.get(Booking, hid)
                if row and not row.is_void:
                    refs = {row.manual_bill_no, row.auto_bill_no, f"BK-{row.id}"}
                    if bill_no in refs or any(v in refs for v in bill_variants):
                        booking = row
            elif hint_type in ['payment', 'pay']:
                row = db.session.get(Payment, hid)
                if row and not row.is_void:
                    refs = {row.manual_bill_no, row.auto_bill_no, f"PAY-{row.id}"}
                    if bill_no in refs or any(v in refs for v in bill_variants):
                        payment = row
            elif hint_type in ['direct_sale', 'sale', 'directsale', 'ds', 'cash']:
                row = db.session.get(DirectSale, hid)
                if row and not row.is_void:
                    refs = {row.manual_bill_no, row.auto_bill_no, f"DS-{row.id}", f"CSH-{row.id}", f"UNBILLED-{row.id}"}
                    if row.invoice and row.invoice.invoice_no:
                        refs.add(row.invoice.invoice_no)
                    if bill_no in refs or any(v in refs for v in bill_variants):
                        sale = row
            elif hint_type in ['invoice', 'inv']:
                row = db.session.get(Invoice, hid)
                if row and not row.is_void:
                    refs = {row.invoice_no}
                    if bill_no in refs or any(v in refs for v in bill_variants):
                        invoice = row
            elif hint_type in ['grn', 'purchase']:
                row = db.session.get(GRN, hid)
                if row and not row.is_void:
                    refs = {row.manual_bill_no, row.auto_bill_no}
                    if bill_no in refs or any(v in refs for v in bill_variants):
                        grn = row
            elif hint_type in ['pending', 'pending_bill', 'pb']:
                row = db.session.get(PendingBill, hid)
                if row and not row.is_void:
                    refs = {row.bill_no}
                    if bill_no in refs or any(v in refs for v in bill_variants):
                        pending = row

    # If caller provided an explicit source hint that resolved successfully, trust it
    # and avoid filling other object types that may share the same bill reference.
    if hint_id and (
        booking is not None or
        payment is not None or
        invoice is not None or
        sale is not None or
        grn is not None or
        pending is not None
    ):
        return booking, payment, invoice, sale, grn, pending

    if not booking:
        booking_q = Booking.query.filter(Booking.is_void == False, _bill_or_expr(Booking.manual_bill_no, Booking.auto_bill_no))
        if hint_client_name_norm:
            booking_q = booking_q.filter(func.lower(func.trim(Booking.client_name)) == hint_client_name_norm)
        booking = booking_q.order_by(Booking.id.desc()).first()
    if not payment:
        payment_q = Payment.query.filter(Payment.is_void == False, _bill_or_expr(Payment.manual_bill_no, Payment.auto_bill_no))
        if hint_client_name_norm:
            payment_q = payment_q.filter(func.lower(func.trim(Payment.client_name)) == hint_client_name_norm)
        payment = payment_q.order_by(Payment.id.desc()).first()
    if not invoice:
        invoice_q = Invoice.query.filter(Invoice.is_void == False, _bill_or_expr(Invoice.invoice_no))
        if hint_client_code:
            invoice_q = invoice_q.filter(Invoice.client_code == hint_client_code)
        if hint_client_name_norm:
            invoice_q = invoice_q.filter(func.lower(func.trim(Invoice.client_name)) == hint_client_name_norm)
        invoice = invoice_q.order_by(Invoice.id.desc()).first()
    if not sale:
        sale_q = DirectSale.query.filter(DirectSale.is_void == False, _bill_or_expr(DirectSale.manual_bill_no, DirectSale.auto_bill_no))
        if hint_client_name_norm:
            sale_q = sale_q.filter(func.lower(func.trim(DirectSale.client_name)) == hint_client_name_norm)
        sale = sale_q.order_by(DirectSale.id.desc()).first()
    if not grn:
        grn = GRN.query.filter(
            GRN.is_void == False,
            _bill_or_expr(GRN.manual_bill_no, GRN.auto_bill_no)
        ).order_by(GRN.id.desc()).first()
    if not pending:
        pending_q = PendingBill.query.filter(
            PendingBill.is_void == False,
            _bill_or_expr(PendingBill.bill_no)
        )
        if hint_client_code:
            pending_q = pending_q.filter(PendingBill.client_code == hint_client_code)
        if hint_client_name_norm:
            pending_q = pending_q.filter(func.lower(func.trim(PendingBill.client_name)) == hint_client_name_norm)
        pending = pending_q.order_by(PendingBill.id.desc()).first()

    # Handle generated IDs (BK-ID, DS-ID, CSH-ID) if not found by direct match
    if not (booking or payment or invoice or sale or grn):
        if bill_no.startswith('BK-'):
            try:
                booking = db.session.get(Booking, int(bill_no.split('-')[1]))
            except: pass
        elif bill_no.startswith('DS-') or bill_no.startswith('CSH-') or bill_no.startswith('UNBILLED-'):
            try:
                sale = db.session.get(DirectSale, int(bill_no.split('-')[1]))
            except: pass
        elif bill_no.startswith('PAY-'):
            try:
                payment = db.session.get(Payment, int(bill_no.split('-')[1]))
            except: pass
    
    return booking, payment, invoice, sale, grn, pending


def _bill_lookup_candidates_map(booking=None, payment=None, invoice=None, sale=None, grn=None, pending=None):
    candidates = {}
    if booking:
        candidates['booking'] = {'id': booking.id, 'label': f"Booking #{booking.id}", 'bill_no': booking.manual_bill_no or booking.auto_bill_no or f"BK-{booking.id}"}
    if payment:
        candidates['payment'] = {'id': payment.id, 'label': f"Payment #{payment.id}", 'bill_no': payment.manual_bill_no or payment.auto_bill_no or f"PAY-{payment.id}"}
    if invoice:
        candidates['invoice'] = {'id': invoice.id, 'label': f"Invoice #{invoice.id}", 'bill_no': invoice.invoice_no}
    if sale:
        candidates['direct_sale'] = {'id': sale.id, 'label': f"Direct Sale #{sale.id}", 'bill_no': sale.manual_bill_no or sale.auto_bill_no or f"DS-{sale.id}"}
    if grn:
        candidates['grn'] = {'id': grn.id, 'label': f"GRN #{grn.id}", 'bill_no': grn.manual_bill_no or grn.auto_bill_no}
    if pending:
        candidates['pending_bill'] = {'id': pending.id, 'label': f"Pending Bill #{pending.id}", 'bill_no': pending.bill_no}
    return candidates


def _effective_collision_candidates(candidates_map):
    """
    PendingBill is a derivative tracker and should not trigger collision prompt
    when a primary bill source exists.
    """
    if not candidates_map:
        return {}
    primary_keys = [k for k in candidates_map.keys() if k != 'pending_bill']
    if primary_keys:
        return {k: candidates_map[k] for k in primary_keys}
    return candidates_map

@app.route('/view_bill/<path:bill_no>')
@login_required
def view_bill(bill_no):
    all_clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    all_materials = Material.query.order_by(Material.name.asc()).all()
    material_ledger_recent = []

    booking, payment, invoice, sale, grn, pending = _lookup_bill(
        bill_no,
        hint_type=request.args.get('src'),
        hint_id=request.args.get('src_id'),
        hint_client_code=request.args.get('client_code'),
        hint_client_name=request.args.get('client_name'),
        hint_entry_id=request.args.get('entry_id')
    )
    entry_hint_id = request.args.get('entry_id')
    hint_type = (request.args.get('src') or '').strip().lower()
    hint_id = (request.args.get('src_id') or '').strip()

    # Entry-driven fallback: if original ref is weak/legacy, try best bill ref from that entry.
    if not (booking or payment or invoice or sale or grn or pending) and entry_hint_id:
        hinted_entry = None
        try:
            hinted_entry = db.session.get(Entry, int(entry_hint_id))
        except Exception:
            hinted_entry = None
        fallback_ref = _entry_best_bill_ref(hinted_entry)
        if fallback_ref and fallback_ref != (bill_no or '').strip():
            booking, payment, invoice, sale, grn, pending = _lookup_bill(
                fallback_ref,
                hint_type='entry',
                hint_id=None,
                hint_client_code=(getattr(hinted_entry, 'client_code', None) if hinted_entry else None),
                hint_client_name=(getattr(hinted_entry, 'client', None) if hinted_entry else None),
                hint_entry_id=entry_hint_id
            )

    # If no explicit source hint was provided and the same ref matches multiple bill types,
    # force user to choose instead of opening a potentially wrong document.
    candidates_map = _bill_lookup_candidates_map(booking, payment, invoice, sale, grn, pending)
    effective_candidates = _effective_collision_candidates(candidates_map)
    if not hint_type and not hint_id and len(effective_candidates) > 1:
        return render_template(
            'bill_collision_resolution.html',
            bill_no=bill_no,
            candidates=effective_candidates
        )

    client = None
    client_balance = 0
    previous_balance = 0
    recent_deliveries = []
    material_ledger_recent = []
    material_stock_summary = []
    direct_sale_rent_reconciliation = None

    if booking or payment or invoice or sale or pending:
        bill_obj_temp = booking or payment or invoice or sale or pending
        c_name = getattr(bill_obj_temp, 'client_name', None)
        c_code = getattr(bill_obj_temp, 'client_code', None)
        if c_code: client = Client.query.filter_by(code=c_code).first()
        if not client and c_name: client = Client.query.filter_by(name=c_name).first()

        if client:
            cutoff_dt = _bill_cutoff_dt_for_snapshot(
                booking=booking,
                payment=payment,
                invoice=invoice,
                sale=sale,
                pending=pending
            )
            client_balance = _client_balance_as_of(client, cutoff_dt=cutoff_dt)

            effect = 0
            if booking: effect = (booking.amount or 0) - (booking.paid_amount or 0)
            elif sale: effect = (sale.amount or 0) - (getattr(sale, 'discount', 0) or 0) - (sale.paid_amount or 0)
            elif payment: effect = -(payment.amount or 0)
            elif invoice: effect = invoice.balance or 0
            elif pending: effect = pending.amount or 0

            previous_balance = client_balance - effect

            is_booking_flow = False
            if booking:
                is_booking_flow = True
            elif sale and normalize_sale_category(getattr(sale, 'category', None)) in ['Booking Delivery', 'Mixed Transaction']:
                is_booking_flow = True
            elif invoice and getattr(invoice, 'direct_sales', None):
                for ds in invoice.direct_sales:
                    if normalize_sale_category(ds.category) in ['Booking Delivery', 'Mixed Transaction']:
                        is_booking_flow = True
                        break

            delivery_query = Entry.query.filter(
                (Entry.client_code == client.code) | (Entry.client == client.name),
                Entry.type == 'OUT',
                Entry.is_void == False
            )
            if is_booking_flow:
                delivery_query = delivery_query.filter(Entry.client_category == 'Booking Delivery')
            delivery_rows = delivery_query.all()
            if cutoff_dt:
                filtered_rows = []
                for d in delivery_rows:
                    d_dt = _parse_dt_safe(f"{d.date} {d.time}") or _parse_dt_safe(d.date) or datetime.min
                    if d_dt <= cutoff_dt:
                        filtered_rows.append(d)
                delivery_rows = filtered_rows
            recent_deliveries = sorted(
                delivery_rows,
                key=lambda d: (
                    _parse_dt_safe(f"{d.date} {d.time}") or _parse_dt_safe(d.date) or datetime.min,
                    d.id or 0
                ),
                reverse=True
            )[:5]
            if is_booking_flow:
                material_ledger_recent = _material_ledger_recent(
                    client,
                    only_booking=True,
                    limit_per_material=5,
                    cutoff_dt=cutoff_dt
                )

            bill_refs = []
            if booking:
                bill_refs = [booking.manual_bill_no, booking.auto_bill_no, f"BK-{booking.id}"]
            elif payment:
                bill_refs = [payment.manual_bill_no, payment.auto_bill_no, f"PAY-{payment.id}"]
            elif sale:
                bill_refs = [sale.manual_bill_no, sale.auto_bill_no, f"DS-{sale.id}", f"CSH-{sale.id}"]
                if getattr(sale, 'invoice', None) and sale.invoice and getattr(sale.invoice, 'invoice_no', None):
                    bill_refs.append(sale.invoice.invoice_no)
            elif invoice:
                bill_refs = [invoice.invoice_no]
                if getattr(invoice, 'direct_sales', None):
                    for ds in invoice.direct_sales:
                        bill_refs.extend([ds.manual_bill_no, ds.auto_bill_no, f"DS-{ds.id}", f"CSH-{ds.id}"])
            elif pending:
                bill_refs = [pending.bill_no]

            if is_booking_flow:
                material_stock_summary = _material_ledger_current_summary(material_ledger_recent, bill_refs)
            else:
                material_stock_summary = []

    if booking:
        tx_code, tx_label, tx_note = _resolve_transaction_type('Booking', booking, entry_hint_id=entry_hint_id)
        return render_template('view_bill.html', bill=booking, type='Booking', items=booking.items, client=client, client_balance=client_balance, previous_balance=previous_balance, recent_deliveries=recent_deliveries, material_ledger_recent=material_ledger_recent, material_stock_summary=material_stock_summary, clients=all_clients, materials=all_materials, transaction_type_code=tx_code, transaction_type_label=tx_label, transaction_type_note=tx_note, pk_now=pk_now)
    if payment:
        tx_code, tx_label, tx_note = _resolve_transaction_type('Payment', payment, entry_hint_id=entry_hint_id)
        return render_template('view_bill.html', bill=payment, type='Payment', items=[], client=client, client_balance=client_balance, previous_balance=previous_balance, recent_deliveries=recent_deliveries, material_ledger_recent=material_ledger_recent, material_stock_summary=material_stock_summary, clients=all_clients, materials=all_materials, transaction_type_code=tx_code, transaction_type_label=tx_label, transaction_type_note=tx_note, pk_now=pk_now)
    if sale:
        rent_row = DeliveryRent.query.filter_by(sale_id=sale.id, is_void=False).order_by(DeliveryRent.id.desc()).first()
        sale_items_payload = [
            {
                'product_name': it.product_name,
                'qty': it.qty,
                'price_at_time': it.price_at_time
            }
            for it in (sale.items or [])
        ]
        fallback_delivery_cost = float(rent_row.amount or 0) if rent_row else 0.0
        effective_delivery_cost = float(getattr(sale, 'delivery_rent_cost', 0) or 0)
        if effective_delivery_cost <= 0:
            effective_delivery_cost = fallback_delivery_cost
        calc_rec = _rent_reconciliation_from_items(
            sale_items_payload,
            delivery_rent_cost=effective_delivery_cost,
            client_name=sale.client_name
        )
        direct_sale_rent_reconciliation = {
            'rent_item_revenue': float(getattr(sale, 'rent_item_revenue', 0) or calc_rec['rent_item_revenue']),
            'delivery_rent_cost': float(getattr(sale, 'delivery_rent_cost', 0) or calc_rec['delivery_rent_cost']),
            'rent_variance_loss': float(getattr(sale, 'rent_variance_loss', 0) or calc_rec['rent_variance_loss'])
        }
        tx_code, tx_label, tx_note = _resolve_transaction_type('DirectSale', sale, entry_hint_id=entry_hint_id)
        return render_template('view_bill.html', bill=sale, type='DirectSale', items=sale.items, client=client, client_balance=client_balance, previous_balance=previous_balance, recent_deliveries=recent_deliveries, material_ledger_recent=material_ledger_recent, material_stock_summary=material_stock_summary, clients=all_clients, materials=all_materials, transaction_type_code=tx_code, transaction_type_label=tx_label, transaction_type_note=tx_note, direct_sale_rent_reconciliation=direct_sale_rent_reconciliation, pk_now=pk_now)
    if invoice:
        invoice.amount = invoice.total_amount
        # Calculate discount from linked sales
        invoice_discount = 0
        if getattr(invoice, 'direct_sales', None):
            invoice_discount = sum((getattr(ds, 'discount', 0) or 0) for ds in invoice.direct_sales)
        invoice.discount = invoice_discount
        invoice.paid_amount = max(0, (invoice.total_amount or 0) - invoice_discount - (invoice.balance or 0))
        invoice.date_posted = _parse_dt_safe(getattr(invoice, 'created_at', None)) or (datetime.combine(invoice.date, datetime.min.time()) if invoice.date else None)

        items = []
        if getattr(invoice, 'direct_sales', None) and invoice.direct_sales:
            ds = invoice.direct_sales[0]
            # Preserve item rates so line amount matches invoice totals on first-open.
            items = [
                {
                    'name': it.product_name,
                    'qty': it.qty,
                    'price_at_time': (it.price_at_time or 0)
                }
                for it in ds.items
            ]
        if not items and getattr(invoice, 'entries', None):
            entry_total_qty = sum(float(e.qty or 0) for e in invoice.entries)
            inferred_rate = (float(invoice.total_amount or 0) / entry_total_qty) if entry_total_qty > 0 else 0
            items = [
                {
                    'name': e.material,
                    'qty': e.qty,
                    'price_at_time': inferred_rate
                }
                for e in invoice.entries
            ]
        tx_code, tx_label, tx_note = _resolve_transaction_type('Invoice', invoice, entry_hint_id=entry_hint_id)
        return render_template('view_bill.html', bill=invoice, type='Invoice', items=items, client=client, client_balance=client_balance, previous_balance=previous_balance, recent_deliveries=recent_deliveries, material_ledger_recent=material_ledger_recent, material_stock_summary=material_stock_summary, clients=all_clients, materials=all_materials, transaction_type_code=tx_code, transaction_type_label=tx_label, transaction_type_note=tx_note, pk_now=pk_now)
    if grn:
        grn.amount = calculate_grn_total(grn) + (grn.discount or 0)
        grn.paid_amount = grn.paid_amount or 0
        tx_code, tx_label, tx_note = _resolve_transaction_type('GRN', grn, entry_hint_id=entry_hint_id)
        return render_template('view_bill.html', bill=grn, type='GRN', items=grn.items, client=None, client_balance=0, previous_balance=0, recent_deliveries=[], material_ledger_recent=[], material_stock_summary=[], clients=all_clients, materials=all_materials, transaction_type_code=tx_code, transaction_type_label=tx_label, transaction_type_note=tx_note, pk_now=pk_now)
    if pending:
        pending_bill_view = SimpleNamespace(
            manual_bill_no=pending.bill_no,
            auto_bill_no='',
            invoice_no='',
            date_posted=_parse_dt_safe(pending.created_at),
            client_name=pending.client_name,
            client_code=pending.client_code,
            amount=pending.amount or 0,
            paid_amount=0,
            reason=pending.reason or '',
            nimbus_no=pending.nimbus_no or '',
            method='',
            photo_path=''
        )
        tx_code, tx_label, tx_note = _resolve_transaction_type('PendingBill', pending_bill_view, entry_hint_id=entry_hint_id)
        return render_template('view_bill.html', bill=pending_bill_view, type='PendingBill', items=[], client=client, client_balance=client_balance, previous_balance=previous_balance, recent_deliveries=recent_deliveries, material_ledger_recent=material_ledger_recent, material_stock_summary=material_stock_summary, clients=all_clients, materials=all_materials, transaction_type_code=tx_code, transaction_type_label=tx_label, transaction_type_note=tx_note, pk_now=pk_now)

    flash('Bill not found', 'danger')
    return redirect(url_for('index'))


@app.route('/download_invoice/<path:bill_no>')
@login_required
def download_invoice(bill_no):
    booking, payment, invoice, sale, grn, pending = _lookup_bill(
        bill_no,
        hint_type=request.args.get('src'),
        hint_id=request.args.get('src_id'),
        hint_client_code=request.args.get('client_code'),
        hint_client_name=request.args.get('client_name'),
        hint_entry_id=request.args.get('entry_id')
    )
    entry_hint_id = request.args.get('entry_id')
    hint_type = (request.args.get('src') or '').strip().lower()
    hint_id = (request.args.get('src_id') or '').strip()

    candidates_map = _bill_lookup_candidates_map(booking, payment, invoice, sale, grn, pending)
    effective_candidates = _effective_collision_candidates(candidates_map)
    if not hint_type and not hint_id and len(effective_candidates) > 1:
        flash('Multiple records match this bill reference. Please choose the exact document type first.', 'warning')
        return redirect(url_for('view_bill', bill_no=bill_no))

    bill_obj = None
    bill_type = ''
    items = []

    client = None
    client_balance = 0
    previous_balance = 0
    recent_deliveries = []
    material_ledger_recent = []
    material_stock_summary = []
    direct_sale_rent_reconciliation = None

    if booking or payment or invoice or sale or pending:
        bill_obj_temp = booking or payment or invoice or sale or pending
        c_name = getattr(bill_obj_temp, 'client_name', None)
        c_code = getattr(bill_obj_temp, 'client_code', None)
        if c_code: client = Client.query.filter_by(code=c_code).first()
        if not client and c_name: client = Client.query.filter_by(name=c_name).first()

        if client:
            cutoff_dt = _bill_cutoff_dt_for_snapshot(
                booking=booking,
                payment=payment,
                invoice=invoice,
                sale=sale,
                pending=pending
            )
            client_balance = _client_balance_as_of(client, cutoff_dt=cutoff_dt)

            effect = 0
            if booking: effect = (booking.amount or 0) - (booking.paid_amount or 0)
            elif sale: effect = (sale.amount or 0) - (getattr(sale, 'discount', 0) or 0) - (sale.paid_amount or 0)
            elif payment: effect = -(payment.amount or 0)
            elif invoice: effect = invoice.balance or 0
            elif pending: effect = pending.amount or 0

            previous_balance = client_balance - effect

            is_booking_flow = False
            if booking:
                is_booking_flow = True
            elif sale and normalize_sale_category(getattr(sale, 'category', None)) in ['Booking Delivery', 'Mixed Transaction']:
                is_booking_flow = True
            elif invoice and getattr(invoice, 'direct_sales', None):
                for ds in invoice.direct_sales:
                    if normalize_sale_category(ds.category) in ['Booking Delivery', 'Mixed Transaction']:
                        is_booking_flow = True
                        break

            delivery_query = Entry.query.filter(
                (Entry.client_code == client.code) | (Entry.client == client.name),
                Entry.type == 'OUT',
                Entry.is_void == False
            )
            if is_booking_flow:
                delivery_query = delivery_query.filter(Entry.client_category == 'Booking Delivery')
            delivery_rows = delivery_query.all()
            if cutoff_dt:
                filtered_rows = []
                for d in delivery_rows:
                    d_dt = _parse_dt_safe(f"{d.date} {d.time}") or _parse_dt_safe(d.date) or datetime.min
                    if d_dt <= cutoff_dt:
                        filtered_rows.append(d)
                delivery_rows = filtered_rows
            recent_deliveries = sorted(
                delivery_rows,
                key=lambda d: (
                    _parse_dt_safe(f"{d.date} {d.time}") or _parse_dt_safe(d.date) or datetime.min,
                    d.id or 0
                ),
                reverse=True
            )[:5]
            if is_booking_flow:
                material_ledger_recent = _material_ledger_recent(
                    client,
                    only_booking=True,
                    limit_per_material=5,
                    cutoff_dt=cutoff_dt
                )

            bill_refs = []
            if booking:
                bill_refs = [booking.manual_bill_no, booking.auto_bill_no, f"BK-{booking.id}"]
            elif payment:
                bill_refs = [payment.manual_bill_no, payment.auto_bill_no, f"PAY-{payment.id}"]
            elif sale:
                bill_refs = [sale.manual_bill_no, sale.auto_bill_no, f"DS-{sale.id}", f"CSH-{sale.id}"]
                if getattr(sale, 'invoice', None) and sale.invoice and getattr(sale.invoice, 'invoice_no', None):
                    bill_refs.append(sale.invoice.invoice_no)
            elif invoice:
                bill_refs = [invoice.invoice_no]
                if getattr(invoice, 'direct_sales', None):
                    for ds in invoice.direct_sales:
                        bill_refs.extend([ds.manual_bill_no, ds.auto_bill_no, f"DS-{ds.id}", f"CSH-{ds.id}"])
            elif pending:
                bill_refs = [pending.bill_no]

            if is_booking_flow:
                material_stock_summary = _material_ledger_current_summary(material_ledger_recent, bill_refs)
            else:
                material_stock_summary = []

    if booking:
        bill_obj = booking
        bill_type = 'Booking'
        items = booking.items
    elif payment:
        bill_obj = payment
        bill_type = 'Payment'
    elif sale:
        bill_obj = sale
        bill_type = 'DirectSale'
        items = sale.items
        rent_row = DeliveryRent.query.filter_by(sale_id=sale.id, is_void=False).order_by(DeliveryRent.id.desc()).first()
        sale_items_payload = [
            {
                'product_name': it.product_name,
                'qty': it.qty,
                'price_at_time': it.price_at_time
            }
            for it in (sale.items or [])
        ]
        fallback_delivery_cost = float(rent_row.amount or 0) if rent_row else 0.0
        effective_delivery_cost = float(getattr(sale, 'delivery_rent_cost', 0) or 0)
        if effective_delivery_cost <= 0:
            effective_delivery_cost = fallback_delivery_cost
        calc_rec = _rent_reconciliation_from_items(
            sale_items_payload,
            delivery_rent_cost=effective_delivery_cost,
            client_name=sale.client_name
        )
        direct_sale_rent_reconciliation = {
            'rent_item_revenue': float(getattr(sale, 'rent_item_revenue', 0) or calc_rec['rent_item_revenue']),
            'delivery_rent_cost': float(getattr(sale, 'delivery_rent_cost', 0) or calc_rec['delivery_rent_cost']),
            'rent_variance_loss': float(getattr(sale, 'rent_variance_loss', 0) or calc_rec['rent_variance_loss'])
        }
    elif invoice:
        bill_obj = invoice
        bill_type = 'Invoice'
        invoice.amount = invoice.total_amount
        # Calculate discount from linked sales
        invoice_discount = 0
        if getattr(invoice, 'direct_sales', None):
            invoice_discount = sum((getattr(ds, 'discount', 0) or 0) for ds in invoice.direct_sales)
        invoice.discount = invoice_discount
        invoice.paid_amount = max(0, (invoice.total_amount or 0) - invoice_discount - (invoice.balance or 0))
        invoice.date_posted = _parse_dt_safe(getattr(invoice, 'created_at', None)) or (datetime.combine(invoice.date, datetime.min.time()) if invoice.date else None)
        if getattr(invoice, 'direct_sales', None) and invoice.direct_sales:
            ds = invoice.direct_sales[0]
            # Preserve item rates so line amount matches invoice totals on first-open.
            items = [
                {
                    'name': it.product_name,
                    'qty': it.qty,
                    'price_at_time': (it.price_at_time or 0)
                }
                for it in ds.items
            ]
        if not items and getattr(invoice, 'entries', None):
            entry_total_qty = sum(float(e.qty or 0) for e in invoice.entries)
            inferred_rate = (float(invoice.total_amount or 0) / entry_total_qty) if entry_total_qty > 0 else 0
            items = [
                {
                    'name': e.material,
                    'qty': e.qty,
                    'price_at_time': inferred_rate
                }
                for e in invoice.entries
            ]
    elif grn:
        bill_obj = grn
        bill_type = 'GRN'
        grn.amount = calculate_grn_total(grn) + (grn.discount or 0)
        grn.paid_amount = grn.paid_amount or 0
        items = grn.items
    elif pending:
        bill_obj = SimpleNamespace(
            manual_bill_no=pending.bill_no,
            auto_bill_no='',
            invoice_no='',
            date_posted=_parse_dt_safe(pending.created_at),
            client_name=pending.client_name,
            client_code=pending.client_code,
            amount=pending.amount or 0,
            paid_amount=0,
            reason=pending.reason or '',
            nimbus_no=pending.nimbus_no or '',
            method='',
            photo_path=''
        )
        bill_type = 'PendingBill'
        items = []

    if not bill_obj:
        flash('Bill not found for download', 'danger')
        return redirect(url_for('index'))

    action = (request.args.get('action') or 'download').lower()
    disposition = 'inline' if action in ['print', 'view'] else 'attachment'
    tx_code, tx_label, tx_note = _resolve_transaction_type(bill_type, bill_obj, entry_hint_id=entry_hint_id)
    rendered = render_template(
        'view_bill.html',
        bill=bill_obj,
        type=bill_type,
        items=items,
        client=client,
        client_balance=client_balance,
        previous_balance=previous_balance,
        recent_deliveries=recent_deliveries,
        material_ledger_recent=material_ledger_recent,
        material_stock_summary=material_stock_summary,
        transaction_type_code=tx_code,
        transaction_type_label=tx_label,
        transaction_type_note=tx_note,
        direct_sale_rent_reconciliation=direct_sale_rent_reconciliation,
        pk_now=pk_now,
        auto_print=(action == 'print')
    )

    if action != 'print':
        pdf_response = _try_render_weasy_pdf(
            rendered,
            f'{bill_type}-{bill_no}.pdf',
            disposition=disposition
        )
        if pdf_response:
            return pdf_response

    response = make_response(rendered)
    fallback_name = _safe_download_name(f'{bill_type}-{bill_no}.html', default='document.html')
    response.headers['Content-Disposition'] = f'{disposition}; filename={fallback_name}'
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    return response


@app.route('/delete_bill/<string:type>/<int:id>', methods=['POST'])
@login_required
def delete_bill(type, id):
    if current_user.role != 'admin':
        flash('Unauthorized', 'danger')
        return redirect(url_for('index'))

    bill = None
    changed = False
    if type == 'Booking':
        bill = db.session.get(Booking, id)
        if bill:
            changed = _set_booking_void_state(bill, True)
    elif type == 'Payment':
        bill = db.session.get(Payment, id)
        if bill:
            changed = _set_payment_void_state(bill, True)
    elif type == 'DirectSale':
        bill = db.session.get(DirectSale, id)
        if bill:
            changed = _set_direct_sale_void_state(bill, True)

    if bill:
        if changed:
            db.session.commit()
            flash(f'{type} voided successfully', 'success')
        else:
            flash(f'{type} already voided', 'warning')
    else:
        flash('Bill not found', 'danger')

    if type == 'Booking':
        return redirect(url_for('bookings_page'))
    if type == 'Payment':
        return redirect(url_for('payments_page'))
    if type == 'DirectSale':
        return redirect(url_for('direct_sales_page'))
    return redirect(url_for('index'))


@app.route('/view_bill_detail/<string:type>/<int:id>')
@login_required
def view_bill_detail(type, id):
    bill = None
    items = []
    if type == 'Booking':
        bill = Booking.query.get_or_404(id)
        items = bill.items
    elif type == 'Payment':
        bill = Payment.query.get_or_404(id)
    elif type == 'DirectSale':
        bill = DirectSale.query.get_or_404(id)
        items = bill.items
    else:
        return "Invalid Bill Type", 400

    all_clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    all_materials = Material.query.order_by(Material.name.asc()).all()
    return render_template('view_bill.html', bill=bill, type=type, items=items, clients=all_clients, materials=all_materials, pk_now=pk_now)


# ==================== LEDGER ROUTES ====================

@app.route('/ledger')
@login_required
def ledger_page():
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()
    return render_template('ledger.html', clients=clients, materials=materials)


@app.route('/ledger/<int:client_id>')
@login_required
def financial_ledger(client_id):
    client = Client.query.get_or_404(client_id)
    client_name_norm = (client.name or '').strip().lower()

    def _fmt_dt(dt_val):
        if not dt_val:
            return ''
        if isinstance(dt_val, str):
            return dt_val
        try:
            return dt_val.strftime('%Y-%m-%d %H:%M')
        except Exception:
            return str(dt_val)

    def _parse_dt(dt_val):
        if isinstance(dt_val, datetime):
            return dt_val
        if isinstance(dt_val, date):
            return datetime.combine(dt_val, datetime.min.time())
        if isinstance(dt_val, str):
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    return datetime.strptime(dt_val, fmt)
                except ValueError:
                    continue
        return datetime.min

    # 1. Fetch Pending Bills
    pending_bills = PendingBill.query.filter_by(client_code=client.code, is_void=False).order_by(PendingBill.id.desc()).all()

    # Sanitize pending bills for template to avoid NoneType error
    for pb in pending_bills:
        if pb.reason is None: pb.reason = ""

    # 2. Financial Ledger
    bookings = Booking.query.filter(func.lower(func.trim(Booking.client_name)) == client_name_norm).all()
    payments = Payment.query.filter(func.lower(func.trim(Payment.client_name)) == client_name_norm).all()
    # Use case-insensitive match for Direct Sales to ensure we catch them all
    direct_sales = DirectSale.query.filter(func.lower(func.trim(DirectSale.client_name)) == client_name_norm).all()

    # Financial History (Bookings, Payments, Direct Sales) - NO Material Entries
    financial_history = []
    booking_bill_refs = set()
    direct_sale_bill_refs = set()

    cancel_bill_refs = set()
    cancel_amount_by_bill = {}
    cancel_bill_rows = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type == 'CANCEL',
        Entry.is_void == False
    ).all()
    for ce in cancel_bill_rows:
        bno = (ce.bill_no or '').strip()
        ano = (ce.auto_bill_no or '').strip()
        if bno:
            cancel_bill_refs.add(bno)
        if ano:
            cancel_bill_refs.add(ano)
        bill_ref = bno or ano
        if bill_ref:
            qty = float(ce.qty or 0)
            mat_ref = (ce.material or ce.booked_material or '').strip()
            amount = _resolve_cancel_display_amount(
                client_name_norm=client_name_norm,
                bill_ref=bill_ref,
                mat_ref=mat_ref,
                qty=qty,
                note=getattr(ce, 'note', None)
            )
            if amount is not None and amount > 0:
                cancel_amount_by_bill[bill_ref] = float(cancel_amount_by_bill.get(bill_ref, 0) or 0) + float(amount)

    for b in bookings:
        if b.is_void: continue
        if b.manual_bill_no:
            booking_bill_refs.add(b.manual_bill_no)
        if b.auto_bill_no:
            booking_bill_refs.add(b.auto_bill_no)
        booking_bill_refs.add(f"BK-{b.id}")
        booking_bill_ref = b.manual_bill_no or b.auto_bill_no or f"BK-{b.id}"
        debit = _booking_ledger_gross_due(
            b,
            cancel_value=cancel_amount_by_bill.get(booking_bill_ref, 0),
            allow_legacy_lift=(booking_bill_ref not in cancel_bill_refs)
        )
        credit = b.paid_amount or 0
        discount = getattr(b, 'discount', 0) or 0
        financial_history.append({
            'date': b.date_posted,
            'date_display': _fmt_dt(b.date_posted),
            'description': 'Booking',
            'bill_no': booking_bill_ref,
            'debit': debit,
            'credit': credit,
            'type': 'Booking',
            'id': b.id
        })
        if float(discount or 0) > 0:
            discount_reason = (getattr(b, 'discount_reason', None) or '').strip()
            discount_desc = 'DISCOUNT WAIVE OFF'
            if discount_reason:
                discount_desc = f'DISCOUNT WAIVE OFF ({discount_reason})'
            financial_history.append({
                'date': b.date_posted,
                'date_display': _fmt_dt(b.date_posted),
                'description': discount_desc,
                'bill_no': booking_bill_ref,
                'debit': 0,
                'credit': float(discount or 0),
                'type': None,
                'id': None
            })

    waive_rows = WaiveOff.query.filter(
        func.lower(func.trim(WaiveOff.client_name)) == client_name_norm,
        WaiveOff.is_void == False
    ).filter(
        ~func.lower(func.coalesce(WaiveOff.note, '')).like('[direct_sale_discount:%')
    ).order_by(WaiveOff.date_posted.asc(), WaiveOff.id.asc()).all()
    waive_by_payment = {}
    standalone_waive_rows = []
    for w in waive_rows:
        if w.payment_id:
            waive_by_payment.setdefault(w.payment_id, []).append(w)
        else:
            standalone_waive_rows.append(w)

    for p in payments:
        if p.is_void: continue
        amt = p.amount or 0
        method_label = p.method or "Cash"
        pay_details = []
        if getattr(p, 'bank_name', None):
            pay_details.append(f"Bank: {p.bank_name}")
        if getattr(p, 'account_name', None):
            pay_details.append(f"A/C Name: {p.account_name}")
        if getattr(p, 'account_no', None):
            pay_details.append(f"A/C No: {p.account_no}")
        details_suffix = f" - {' | '.join(pay_details)}" if pay_details else ''
        if amt >= 0:
            debit = 0
            credit = amt
            payment_desc = f'Payment ({method_label}){details_suffix}'
        else:
            debit = abs(amt)
            credit = 0
            payment_desc = f'Repayment ({method_label}){details_suffix}'

        # Payment row: only actual cash/bank amount.
        financial_history.append({
            'date': p.date_posted,
            'date_display': _fmt_dt(p.date_posted),
            'description': payment_desc,
            'bill_no': p.manual_bill_no or p.auto_bill_no or f"PAY-{p.id}",
            'debit': debit,
            'credit': credit,
            'type': 'Payment',
            'id': p.id
        })

        linked_waive_rows = waive_by_payment.get(p.id, [])
        if linked_waive_rows:
            for w in linked_waive_rows:
                w_desc = 'Waive-Off (Loss)'
                if (w.reason or '').strip():
                    w_desc = f'Waive-Off (Loss) ({w.reason.strip()})'
                financial_history.append({
                    'date': w.date_posted or p.date_posted,
                    'date_display': _fmt_dt(w.date_posted or p.date_posted),
                    'description': w_desc,
                    'bill_no': w.bill_no or p.manual_bill_no or p.auto_bill_no or f"PAY-{p.id}",
                    'debit': 0,
                    'credit': float(w.amount or 0),
                    'type': None,
                    'id': None
                })
        else:
            # Legacy fallback for older records where waive_off row does not exist.
            p_discount = float(getattr(p, 'discount', 0) or 0)
            if p_discount > 0:
                discount_reason = (getattr(p, 'discount_reason', None) or '').strip()
                discount_desc = 'Waive-Off (Loss)'
                if discount_reason:
                    discount_desc = f'Waive-Off (Loss) ({discount_reason})'
                financial_history.append({
                    'date': p.date_posted,
                    'date_display': _fmt_dt(p.date_posted),
                    'description': discount_desc,
                    'bill_no': p.manual_bill_no or p.auto_bill_no or f"PAY-{p.id}",
                    'debit': 0,
                    'credit': p_discount,
                    'type': None,
                    'id': None
                })

    def _waive_bill_ref(row):
        ref = (getattr(row, 'bill_no', None) or '').strip()
        if ref:
            return ref
        marker = (getattr(row, 'note', None) or '').strip()
        m = re.match(r'^\[direct_sale_discount:(\d+)\]$', marker, re.IGNORECASE)
        if m:
            sale = db.session.get(DirectSale, int(m.group(1)))
            if sale:
                return (sale.manual_bill_no or sale.auto_bill_no or f"DS-{sale.id}")
        return ''

    for w in standalone_waive_rows:
        w_desc = 'Waive-Off (Loss)'
        if (w.reason or '').strip():
            w_desc = f'Waive-Off (Loss) ({w.reason.strip()})'
        financial_history.append({
            'date': w.date_posted,
            'date_display': _fmt_dt(w.date_posted),
            'description': w_desc,
            'bill_no': _waive_bill_ref(w),
            'debit': 0,
            'credit': float(w.amount or 0),
            'type': None,
            'id': None
        })

    for s in direct_sales:
        if s.is_void: continue
        sale_bill_ref = (
            (s.invoice.invoice_no if getattr(s, 'invoice', None) else None)
            or s.manual_bill_no
            or s.auto_bill_no
            or f"DS-{s.id}"
        )
        if s.manual_bill_no:
            direct_sale_bill_refs.add(s.manual_bill_no)
        if s.auto_bill_no:
            direct_sale_bill_refs.add(s.auto_bill_no)
        if getattr(s, 'invoice', None) and s.invoice and s.invoice.invoice_no:
            direct_sale_bill_refs.add(s.invoice.invoice_no)
        direct_sale_bill_refs.add(f"UNBILLED-{s.id}")
        direct_sale_bill_refs.add(f"DS-{s.id}")
        direct_sale_bill_refs.add(f"CSH-{s.id}")
        debit = s.amount or 0
        credit = s.paid_amount or 0
        discount = getattr(s, 'discount', 0) or 0
        # A Direct Sale with no financial value is just a dispatch, not a financial event.
        # It should only appear in the material ledger.
        if debit > 0 or credit > 0:
            financial_history.append({
                'date': s.date_posted,
                'date_display': _fmt_dt(s.date_posted),
                'description': 'Direct Sale',
                'bill_no': sale_bill_ref,
                'debit': debit,
                'credit': credit,
                'type': 'DirectSale',
                'id': s.id
            })
            if float(discount or 0) > 0:
                discount_reason = (getattr(s, 'discount_reason', None) or '').strip()
                discount_desc = 'DISCOUNT WAIVE OFF (Direct Sale)'
                if discount_reason:
                    discount_desc = f'DISCOUNT WAIVE OFF (Direct Sale) ({discount_reason})'
                financial_history.append({
                    'date': s.date_posted,
                    'date_display': _fmt_dt(s.date_posted),
                    'description': discount_desc,
                    'bill_no': sale_bill_ref,
                    'debit': 0,
                    'credit': float(discount or 0),
                    'type': None,
                    'id': None
                })
        # Company-side delivery rent variance should be visible in client financial timeline
        # as an informational row, but must not alter client running balance.
        rent_loss = float(getattr(s, 'rent_variance_loss', 0) or 0)
        if rent_loss > 0:
            financial_history.append({
                'date': s.date_posted,
                'date_display': _fmt_dt(s.date_posted),
                'description': f'Delivery Rent Variance (Company Loss) Rs.{rent_loss:.2f}',
                'bill_no': sale_bill_ref,
                'debit': 0,
                'credit': 0,
                'type': None,
                'id': None
            })

    # Explicit booking-cancellation rows for readability in financial ledger.
    cancel_entries = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type == 'CANCEL',
        Entry.is_void == False
    ).order_by(Entry.date.asc(), Entry.time.asc(), Entry.id.asc()).all()
    for ce in cancel_entries:
        qty = float(ce.qty or 0)
        bill_ref = (ce.bill_no or ce.auto_bill_no or '').strip()
        mat_ref = (ce.material or ce.booked_material or '').strip()
        amount = _resolve_cancel_display_amount(
            client_name_norm=client_name_norm,
            bill_ref=bill_ref,
            mat_ref=mat_ref,
            qty=qty,
            note=getattr(ce, 'note', None)
        )
        desc = f"Booking Cancel ({(ce.material or ce.booked_material or '-')} x {qty:.3f})"
        financial_history.append({
            'date': _parse_ledger_entry_dt(ce.date, ce.time),
            'date_display': _fmt_dt(_parse_ledger_entry_dt(ce.date, ce.time)),
            'description': desc,
            'bill_no': ce.bill_no or '',
            'debit': 0,
            'credit': float(amount or 0),
            'type': 'Entry',
            'id': ce.id,
            'is_cancel_entry': True,
            'cancel_amount': amount
        })

    # Sort by date (oldest first)
    opening_balance = _to_float_or_zero(getattr(client, 'opening_balance', 0))
    if opening_balance != 0:
        opening_dt = (
            getattr(client, 'opening_balance_date', None)
            or getattr(client, 'created_at', None)
            or datetime.min
        )
        financial_history.append({
            'date': opening_dt,
            'date_display': _fmt_dt(opening_dt),
            'description': 'Opening Balance',
            'bill_no': 'OPENING',
            'debit': opening_balance if opening_balance > 0 else 0,
            'credit': abs(opening_balance) if opening_balance < 0 else 0,
            'type': None,
            'id': None
        })

    for idx, row in enumerate(financial_history):
        row['_sort_idx'] = idx

    def _ledger_sort_key(row):
        dt = _parse_dt(row.get('date'))
        opening_first = -1 if row.get('bill_no') == 'OPENING' else 0
        return (dt, opening_first, row.get('_sort_idx', 0))

    financial_history.sort(key=_ledger_sort_key)

    for row in financial_history:
        if '_sort_idx' in row:
            del row['_sort_idx']

    # Initial running balance for baseline financial events.
    running_balance = 0
    for item in financial_history:
        running_balance += (item['debit'] - item['credit'])
        item['balance'] = running_balance

    # 3. Material Ledger
    deliveries = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type.in_(['OUT', 'CANCEL'])
    ).order_by(Entry.date.asc(), Entry.time.asc()).all()

    material_history = []
    seen_material_bills = set()
    unresolved_dispatches = []

    # Add Bookings to Material Ledger
    bookings = Booking.query.filter(func.lower(func.trim(Booking.client_name)) == client_name_norm).order_by(Booking.date_posted.asc()).all()
    for b in bookings:
        if b.is_void: continue
        for item in b.items:
            created_at = getattr(b, 'created_at', None)
            date_sort = b.date_posted if b.date_posted else None
            if not date_sort and created_at:
                try:
                    date_sort = datetime.strptime(created_at[:19], '%Y-%m-%d %H:%M:%S')
                except Exception:
                    try:
                        date_sort = datetime.strptime(created_at[:10], '%Y-%m-%d')
                    except Exception:
                        date_sort = None
            material_history.append({
                'date': b.date_posted.strftime('%Y-%m-%d') if b.date_posted else (created_at[:10] if created_at else ''),
                'date_sort': date_sort,
                'material': item.material_name,
                'material_group': item.material_name,
                'material_display': item.material_name,
                'qty_added': item.qty,
                'qty_dispatched': 0,
                'bill_no': b.manual_bill_no or b.auto_bill_no or f"BK-{b.id}",
                'nimbus_no': 'Booking',
                'type': 'Booking'
            })

    # Process Deliveries/Entries
    for d in deliveries:
        if d.is_void:
            continue
        bill_ref = d.bill_no or d.auto_bill_no
        if d.type == 'CANCEL':
            mat_name = d.material or d.booked_material
            if not mat_name:
                continue
            date_sort = None
            try:
                if d.date and d.time:
                    date_sort = datetime.strptime(f"{d.date} {d.time}", '%Y-%m-%d %H:%M:%S')
                elif d.date:
                    date_sort = datetime.strptime(d.date, '%Y-%m-%d')
            except Exception:
                date_sort = None
            material_history.append({
                'date': d.date,
                'date_sort': date_sort,
                'material': mat_name,
                'material_group': mat_name,
                'material_display': mat_name,
                'qty_added': 0,
                'qty_dispatched': d.qty,
                'bill_no': bill_ref,
                'nimbus_no': d.nimbus_no or 'Booking Cancel',
                'type': 'Cancel'
            })
            continue

        is_booking_delivery = (d.client_category == 'Booking Delivery') or (bill_ref in booking_bill_refs)

        if is_booking_delivery:
            group_mat = d.booked_material or d.material
            display_mat = group_mat
            if d.booked_material and d.material and d.booked_material != d.material:
                display_mat = f"{d.booked_material}>ALT>{d.material}"
            date_sort = None
            try:
                if d.date and d.time:
                    date_sort = datetime.strptime(f"{d.date} {d.time}", '%Y-%m-%d %H:%M:%S')
                elif d.date:
                    date_sort = datetime.strptime(d.date, '%Y-%m-%d')
            except Exception:
                date_sort = None
            material_history.append({
                'date': d.date,
                'date_sort': date_sort,
                'material': group_mat,
                'material_group': group_mat,
                'material_display': display_mat,
                'qty_added': 0,
                'qty_dispatched': d.qty,
                'bill_no': bill_ref,
                'nimbus_no': d.nimbus_no,
                'type': 'Dispatch'
            })
            if bill_ref:
                seen_material_bills.add(bill_ref)
        else:
            # Material ledger is booking-reserved only.
            # Non-booking dispatches (cash/credit direct sales etc.) must never appear here.
            # They remain visible in financial views/reports as applicable.
            continue

    for s in direct_sales:
        if s.is_void: continue
        sale_ref_candidates = set()
        if s.manual_bill_no:
            sale_ref_candidates.add(s.manual_bill_no)
        if s.auto_bill_no:
            sale_ref_candidates.add(s.auto_bill_no)
        if getattr(s, 'invoice', None) and s.invoice and s.invoice.invoice_no:
            sale_ref_candidates.add(s.invoice.invoice_no)

        # If any sale reference is already present from Entry rows, this sale dispatch is
        # already represented and must not be appended again under another bill number.
        if sale_ref_candidates & seen_material_bills:
            continue

        # Also skip standalone Direct Sales in this fallback loop
        if s.category != 'Booking Delivery':
            continue

        bill_ref = (
            (s.invoice.invoice_no if getattr(s, 'invoice', None) and s.invoice else None)
            or s.manual_bill_no
            or s.auto_bill_no
            or f"DS-{s.id}"
        )

        for item in s.items:
            # Skip non-booked items (Price > 0) in mixed transactions
            if item.price_at_time > 0:
                continue

            date_sort = s.date_posted if s.date_posted else None
            material_history.append({
                'date': s.date_posted.strftime('%Y-%m-%d') if s.date_posted else '',
                'date_sort': date_sort,
                'material': item.product_name,
                'material_group': item.product_name,
                'material_display': item.product_name,
                'qty_added': 0,
                'qty_dispatched': item.qty,
                'bill_no': bill_ref,
                'nimbus_no': 'Direct Sale',
                'type': 'Dispatch'
            })

    # Financial rows can also be appended during delivery processing (e.g., non-booking
    # dispatches). Recompute running balances so every rendered row has `balance`.
    financial_history.sort(key=lambda x: _parse_dt(x.get('date')))
    running_balance = 0
    for item in financial_history:
        running_balance += (item.get('debit', 0) - item.get('credit', 0))
        item['balance'] = running_balance

    # Sort by date (oldest first)
    # Sort by date, then by type priority (Booking/Add before Dispatch) to ensure balance doesn't dip
    def mat_sort_key(x):
        d = x.get('date_sort') or datetime.min
        t = x['type']
        # When timestamps exist, sort strictly by time to preserve real order.
        # Only use type priority for rows missing a timestamp.
        if d != datetime.min:
            p = 0
        else:
            if t == 'Booking':
                p = 0
            elif t == 'Cancel':
                p = 1
            elif t == 'Direct Sale':
                p = 2
            else:
                p = 3
        return (d, p)

    material_history.sort(key=mat_sort_key)

    # Running balance per material
    mat_balances = {}
    for item in material_history:
        mat = item.get('material_group') or item['material']
        if mat not in mat_balances:
            mat_balances[mat] = 0
        # Cancellation rows are informational only; do not alter running balance.
        if item.get('type') != 'Cancel':
            mat_balances[mat] += (item.get('qty_added', 0) - item.get('qty_dispatched', 0))
        item['balance'] = mat_balances[mat]

    # Group material history by material so UI can render separate sections.
    material_history_grouped = {}
    for item in material_history:
        mat_name = item.get('material_group') or item.get('material') or 'Unknown'
        material_history_grouped.setdefault(mat_name, []).append(item)

    # Calculate totals
    total_debit = sum(item['debit'] for item in financial_history)
    total_credit = sum(item['credit'] for item in financial_history)
    total_balance = total_debit - total_credit

    # Cancellation preview for remaining bookings (LIFO by booking date)
    cancel_rows = []
    cancel_total = 0
    cancel_total_qty = 0
    delivered_totals = {}

    delivered_entries = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type == 'OUT',
        Entry.is_void == False,
        not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
    ).all()
    for e in delivered_entries:
        key = e.booked_material or e.material
        delivered_totals[key] = delivered_totals.get(key, 0) + (e.qty or 0)

    booking_items = BookingItem.query.join(Booking).filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False
    ).all()

    items_by_material = {}
    for item in booking_items:
        mat_name = item.material_name or ''
        items_by_material.setdefault(mat_name, []).append(item)

    def _fmt_date_short(dt_val):
        if not dt_val:
            return ''
        if isinstance(dt_val, str):
            return dt_val
        try:
            return dt_val.strftime('%Y-%m-%d')
        except Exception:
            return str(dt_val)

    for mat_name, items in items_by_material.items():
        items.sort(
            key=lambda x: (
                x.booking.date_posted or datetime.min,
                x.booking.id or 0,
                x.id or 0
            ),
            reverse=True
        )
        remaining_delivered = float(delivered_totals.get(mat_name, 0) or 0)

        for item in items:
            booked_qty = float(item.qty or 0)
            consumed = min(booked_qty, remaining_delivered) if remaining_delivered > 0 else 0
            remaining_delivered = max(0, remaining_delivered - consumed)
            remaining_qty = booked_qty - consumed
            if remaining_qty <= 0:
                continue

            rate = float(item.price_at_time or 0)
            amount = remaining_qty * rate
            cancel_total += amount
            cancel_total_qty += remaining_qty

            booking_ref = item.booking.manual_bill_no or item.booking.auto_bill_no or f"BK-{item.booking.id}"
            cancel_rows.append({
                'item_id': item.id,
                'material': mat_name,
                'booking_date': _fmt_date_short(item.booking.date_posted),
                'bill_no': booking_ref,
                'qty_remaining': remaining_qty,
                'rate': rate,
                'amount': amount
            })

    cancel_rows.sort(
        key=lambda x: (x.get('material') or '', x.get('booking_date') or ''),
    )
    cancel_new_balance = total_balance - cancel_total
    cancel_client_due = max(0, cancel_new_balance)
    cancel_company_due = max(0, -cancel_new_balance)

    all_clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()

    return render_template('client_ledger.html',
                           client=client,
                           pending_bills=pending_bills,
                           financial_history=financial_history,
                           material_history=material_history,
                           material_history_grouped=material_history_grouped,
                           unresolved_dispatches=unresolved_dispatches,
                           total_debit=total_debit,
                           total_credit=total_credit,
                           total_balance=total_balance,
                           cancel_rows=cancel_rows,
                           cancel_total=cancel_total,
                           cancel_total_qty=cancel_total_qty,
                           cancel_new_balance=cancel_new_balance,
                           cancel_client_due=cancel_client_due,
                           cancel_company_due=cancel_company_due,
                           clients=all_clients,
                           materials=materials)


def _build_client_ledger_rows(client):
    client_name_norm = (client.name or '').strip().lower()

    def _fmt_dt(dt_val):
        if not dt_val:
            return ''
        if isinstance(dt_val, str):
            return dt_val
        try:
            return dt_val.strftime('%Y-%m-%d %H:%M')
        except Exception:
            return str(dt_val)

    def _parse_dt(dt_val):
        if isinstance(dt_val, datetime):
            return dt_val
        if isinstance(dt_val, date):
            return datetime.combine(dt_val, datetime.min.time())
        if isinstance(dt_val, str):
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    return datetime.strptime(dt_val, fmt)
                except ValueError:
                    continue
        return datetime.min

    def _fmt_qty(qty_val):
        try:
            q = float(qty_val or 0)
            return f"{q:.2f}".rstrip('0').rstrip('.')
        except Exception:
            return str(qty_val or '0')

    def _fmt_money(val):
        try:
            return f"{float(val or 0):,.2f}".rstrip('0').rstrip('.')
        except Exception:
            return str(val or '0')

    def _line_items_text(items, name_attr, qty_attr='qty', rate_attr=None, max_items=3):
        parts = []
        for it in (items or []):
            name = str(getattr(it, name_attr, '') or '').strip()
            if not name:
                continue
            qty = _fmt_qty(getattr(it, qty_attr, 0))
            if rate_attr:
                rate = _fmt_money(getattr(it, rate_attr, 0))
                parts.append(f"{name} ({qty} x {rate})")
            else:
                parts.append(f"{name} ({qty})")
        if not parts:
            return ''
        if len(parts) > max_items:
            shown = ' | '.join(parts[:max_items])
            return f"{shown} | +{len(parts) - max_items} more"
        return ' | '.join(parts)

    pending_bills = PendingBill.query.filter_by(
        client_code=client.code,
        is_void=False
    ).order_by(PendingBill.id.desc()).all()
    for pb in pending_bills:
        if pb.reason is None:
            pb.reason = ''

    bookings = Booking.query.filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm
    ).all()
    payments = Payment.query.filter(
        func.lower(func.trim(Payment.client_name)) == client_name_norm
    ).all()
    direct_sales = DirectSale.query.filter(
        func.lower(func.trim(DirectSale.client_name)) == client_name_norm
    ).all()

    financial_history = []
    cancel_bill_refs = set()
    cancel_amount_by_bill = {}
    cancel_bill_rows = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type == 'CANCEL',
        Entry.is_void == False
    ).all()
    for ce in cancel_bill_rows:
        bno = (ce.bill_no or '').strip()
        ano = (ce.auto_bill_no or '').strip()
        if bno:
            cancel_bill_refs.add(bno)
        if ano:
            cancel_bill_refs.add(ano)
        bill_ref = bno or ano
        if bill_ref:
            qty = float(ce.qty or 0)
            mat_ref = (ce.material or ce.booked_material or '').strip()
            amount = _resolve_cancel_display_amount(
                client_name_norm=client_name_norm,
                bill_ref=bill_ref,
                mat_ref=mat_ref,
                qty=qty,
                note=getattr(ce, 'note', None)
            )
            if amount is not None and amount > 0:
                cancel_amount_by_bill[bill_ref] = float(cancel_amount_by_bill.get(bill_ref, 0) or 0) + float(amount)

    for b in bookings:
        if b.is_void:
            continue
        booking_bill_ref = b.manual_bill_no or b.auto_bill_no or f"BK-{b.id}"
        debit = _booking_ledger_gross_due(
            b,
            cancel_value=cancel_amount_by_bill.get(booking_bill_ref, 0),
            allow_legacy_lift=(booking_bill_ref not in cancel_bill_refs)
        )
        credit = b.paid_amount or 0
        discount = getattr(b, 'discount', 0) or 0
        booking_items_text = _line_items_text(
            getattr(b, 'items', []),
            'material_name',
            qty_attr='qty',
            rate_attr='price_at_time'
        )
        booking_desc = 'Booking'
        if booking_items_text:
            booking_desc = f"Booking: {booking_items_text}"
        financial_history.append({
            'date': b.date_posted,
            'date_display': _fmt_dt(b.date_posted),
            'description': booking_desc,
            'bill_no': booking_bill_ref,
            'debit': debit,
            'credit': credit,
            'type': 'Booking',
            'id': b.id
        })
        if float(discount or 0) > 0:
            discount_reason = (getattr(b, 'discount_reason', None) or '').strip()
            discount_desc = 'DISCOUNT WAIVE OFF'
            if discount_reason:
                discount_desc = f'DISCOUNT WAIVE OFF ({discount_reason})'
            financial_history.append({
                'date': b.date_posted,
                'date_display': _fmt_dt(b.date_posted),
                'description': discount_desc,
                'bill_no': booking_bill_ref,
                'debit': 0,
                'credit': float(discount or 0),
                'type': None,
                'id': None
            })

    waive_rows = WaiveOff.query.filter(
        func.lower(func.trim(WaiveOff.client_name)) == client_name_norm,
        WaiveOff.is_void == False
    ).filter(
        ~func.lower(func.coalesce(WaiveOff.note, '')).like('[direct_sale_discount:%')
    ).order_by(WaiveOff.date_posted.asc(), WaiveOff.id.asc()).all()
    waive_by_payment = {}
    standalone_waive_rows = []
    for w in waive_rows:
        if w.payment_id:
            waive_by_payment.setdefault(w.payment_id, []).append(w)
        else:
            standalone_waive_rows.append(w)

    for p in payments:
        if p.is_void:
            continue
        amt = p.amount or 0
        method_label = p.method or "Cash"
        pay_details = []
        if getattr(p, 'bank_name', None):
            pay_details.append(f"Bank: {p.bank_name}")
        if getattr(p, 'account_name', None):
            pay_details.append(f"A/C Name: {p.account_name}")
        if getattr(p, 'account_no', None):
            pay_details.append(f"A/C No: {p.account_no}")
        details_suffix = f" - {' | '.join(pay_details)}" if pay_details else ''
        if amt >= 0:
            debit = 0
            credit = amt
            payment_desc = f'Payment ({method_label}){details_suffix}'
        else:
            debit = abs(amt)
            credit = 0
            payment_desc = f'Repayment ({method_label}){details_suffix}'

        payment_bill_ref = p.manual_bill_no or p.auto_bill_no or f"PAY-{p.id}"
        financial_history.append({
            'date': p.date_posted,
            'date_display': _fmt_dt(p.date_posted),
            'description': payment_desc,
            'bill_no': payment_bill_ref,
            'debit': debit,
            'credit': credit,
            'type': 'Payment',
            'id': p.id
        })

        linked_waive_rows = waive_by_payment.get(p.id, [])
        if linked_waive_rows:
            for w in linked_waive_rows:
                w_desc = 'Waive-Off (Loss)'
                if (w.reason or '').strip():
                    w_desc = f'Waive-Off (Loss) ({w.reason.strip()})'
                financial_history.append({
                    'date': w.date_posted or p.date_posted,
                    'date_display': _fmt_dt(w.date_posted or p.date_posted),
                    'description': w_desc,
                    'bill_no': w.bill_no or payment_bill_ref,
                    'debit': 0,
                    'credit': float(w.amount or 0),
                    'type': None,
                    'id': None
                })
        else:
            p_discount = float(getattr(p, 'discount', 0) or 0)
            if p_discount > 0:
                discount_reason = (getattr(p, 'discount_reason', None) or '').strip()
                discount_desc = 'Waive-Off (Loss)'
                if discount_reason:
                    discount_desc = f'Waive-Off (Loss) ({discount_reason})'
                financial_history.append({
                    'date': p.date_posted,
                    'date_display': _fmt_dt(p.date_posted),
                    'description': discount_desc,
                    'bill_no': payment_bill_ref,
                    'debit': 0,
                    'credit': p_discount,
                    'type': None,
                    'id': None
                })

    def _waive_bill_ref(row):
        ref = (getattr(row, 'bill_no', None) or '').strip()
        if ref:
            return ref
        marker = (getattr(row, 'note', None) or '').strip()
        m = re.match(r'^\[direct_sale_discount:(\d+)\]$', marker, re.IGNORECASE)
        if m:
            sale = db.session.get(DirectSale, int(m.group(1)))
            if sale:
                return (sale.manual_bill_no or sale.auto_bill_no or f"DS-{sale.id}")
        return ''

    for w in standalone_waive_rows:
        w_desc = 'Waive-Off (Loss)'
        if (w.reason or '').strip():
            w_desc = f'Waive-Off (Loss) ({w.reason.strip()})'
        financial_history.append({
            'date': w.date_posted,
            'date_display': _fmt_dt(w.date_posted),
            'description': w_desc,
            'bill_no': _waive_bill_ref(w),
            'debit': 0,
            'credit': float(w.amount or 0),
            'type': None,
            'id': None
        })

    for s in direct_sales:
        if s.is_void:
            continue
        debit = s.amount or 0
        credit = s.paid_amount or 0
        discount = getattr(s, 'discount', 0) or 0
        sale_items_text = _line_items_text(
            getattr(s, 'items', []),
            'product_name',
            qty_attr='qty',
            rate_attr='price_at_time'
        )
        sale_desc = 'Direct Sale'
        if sale_items_text:
            sale_desc = f"Direct Sale: {sale_items_text}"
        sale_bill_ref = (
            (s.invoice.invoice_no if getattr(s, 'invoice', None) else None)
            or s.manual_bill_no
            or s.auto_bill_no
            or f"DS-{s.id}"
        )
        if debit > 0 or credit > 0:
            financial_history.append({
                'date': s.date_posted,
                'date_display': _fmt_dt(s.date_posted),
                'description': sale_desc,
                'bill_no': sale_bill_ref,
                'debit': debit,
                'credit': credit,
                'type': 'DirectSale',
                'id': s.id
            })
            if float(discount or 0) > 0:
                discount_reason = (getattr(s, 'discount_reason', None) or '').strip()
                discount_desc = 'DISCOUNT WAIVE OFF (Direct Sale)'
                if discount_reason:
                    discount_desc = f'DISCOUNT WAIVE OFF (Direct Sale) ({discount_reason})'
                financial_history.append({
                    'date': s.date_posted,
                    'date_display': _fmt_dt(s.date_posted),
                    'description': discount_desc,
                    'bill_no': sale_bill_ref,
                    'debit': 0,
                    'credit': float(discount or 0),
                    'type': None,
                    'id': None
                })
        # Informational row only: keep client balance unchanged while showing why P/L changed.
        rent_loss = float(getattr(s, 'rent_variance_loss', 0) or 0)
        if rent_loss > 0:
            financial_history.append({
                'date': s.date_posted,
                'date_display': _fmt_dt(s.date_posted),
                'description': f'Delivery Rent Variance (Company Loss) Rs.{rent_loss:.2f}',
                'bill_no': sale_bill_ref,
                'debit': 0,
                'credit': 0,
                'type': None,
                'id': None
            })

    # Explicit booking-cancellation rows for readability in financial ledger.
    cancel_entries = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type == 'CANCEL',
        Entry.is_void == False
    ).order_by(Entry.date.asc(), Entry.time.asc(), Entry.id.asc()).all()
    for ce in cancel_entries:
        qty = float(ce.qty or 0)
        bill_ref = (ce.bill_no or ce.auto_bill_no or '').strip()
        mat_ref = (ce.material or ce.booked_material or '').strip()
        amount = _resolve_cancel_display_amount(
            client_name_norm=client_name_norm,
            bill_ref=bill_ref,
            mat_ref=mat_ref,
            qty=qty,
            note=getattr(ce, 'note', None)
        )
        desc = f"Booking Cancel ({(ce.material or ce.booked_material or '-')} x {qty:.3f})"
        cancel_dt = _parse_ledger_entry_dt(ce.date, ce.time)
        financial_history.append({
            'date': cancel_dt,
            'date_display': _fmt_dt(cancel_dt),
            'description': desc,
            'bill_no': ce.bill_no or '',
            'debit': 0,
            'credit': float(amount or 0),
            'type': 'Entry',
            'id': ce.id,
            'is_cancel_entry': True,
            'cancel_amount': amount
        })

    financial_history.sort(key=lambda x: _parse_dt(x.get('date')))

    opening_balance = _to_float_or_zero(getattr(client, 'opening_balance', 0))
    if opening_balance != 0:
        opening_dt = (
            getattr(client, 'opening_balance_date', None)
            or getattr(client, 'created_at', None)
            or datetime.min
        )
        financial_history.insert(0, {
            'date': opening_dt,
            'date_display': _fmt_dt(opening_dt),
            'description': 'Opening Balance',
            'bill_no': 'OPENING',
            'debit': opening_balance if opening_balance > 0 else 0,
            'credit': abs(opening_balance) if opening_balance < 0 else 0,
            'type': None,
            'id': None
        })

    running_balance = 0
    for row in financial_history:
        running_balance += (row.get('debit', 0) - row.get('credit', 0))
        row['balance'] = running_balance

    total_debit = sum(float(x.get('debit') or 0) for x in financial_history)
    total_credit = sum(float(x.get('credit') or 0) for x in financial_history)
    total_balance = total_debit - total_credit

    deliveries = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type.in_(['OUT', 'CANCEL']),
        Entry.is_void == False,
        not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
    ).order_by(Entry.date.asc(), Entry.time.asc()).all()

    material_history = []

    bookings_for_material = Booking.query.filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False
    ).order_by(Booking.date_posted.asc()).all()
    for b in bookings_for_material:
        for item in b.items:
            created_at = getattr(b, 'created_at', None)
            date_sort = b.date_posted if b.date_posted else None
            if not date_sort and created_at:
                try:
                    date_sort = datetime.strptime(created_at[:19], '%Y-%m-%d %H:%M:%S')
                except Exception:
                    try:
                        date_sort = datetime.strptime(created_at[:10], '%Y-%m-%d')
                    except Exception:
                        date_sort = None
            material_history.append({
                'date': b.date_posted.strftime('%Y-%m-%d') if b.date_posted else (created_at[:10] if created_at else ''),
                'date_sort': date_sort,
                'material': item.material_name,
                'material_group': item.material_name,
                'material_display': item.material_name,
                'qty_added': item.qty,
                'qty_dispatched': 0,
                'bill_no': b.manual_bill_no or b.auto_bill_no or f"BK-{b.id}",
                'nimbus_no': 'Booking',
                'type': 'Booking',
                'source_type': 'Booking',
                'source_id': b.id
            })

    for d in deliveries:
        bill_ref = d.bill_no or d.auto_bill_no
        mat_name = d.booked_material or d.material
        if not mat_name:
            continue
        date_sort = None
        try:
            if d.date and d.time:
                date_sort = datetime.strptime(f"{d.date} {d.time}", '%Y-%m-%d %H:%M:%S')
            elif d.date:
                date_sort = datetime.strptime(d.date, '%Y-%m-%d')
        except Exception:
            date_sort = None

        row_type = 'Cancel' if d.type == 'CANCEL' else 'Dispatch'
        material_history.append({
            'date': d.date,
            'date_sort': date_sort,
            'material': mat_name,
            'material_group': mat_name,
            'material_display': mat_name,
            'qty_added': 0,
            'qty_dispatched': d.qty,
            'bill_no': bill_ref,
            'nimbus_no': d.nimbus_no or 'Booking Cancel',
            'type': row_type,
            'source_type': 'Entry',
            'source_id': d.id
        })

    def _mat_sort_key(x):
        d = x.get('date_sort') or datetime.min
        t = x.get('type')
        if d != datetime.min:
            p = 0
        else:
            if t == 'Booking':
                p = 0
            elif t == 'Cancel':
                p = 1
            else:
                p = 2
        return (d, p)

    material_history.sort(key=_mat_sort_key)

    mat_balances = {}
    for item in material_history:
        mat = item.get('material_group') or item.get('material') or 'Unknown'
        if mat not in mat_balances:
            mat_balances[mat] = 0
        if item.get('type') != 'Cancel':
            mat_balances[mat] += (item.get('qty_added', 0) - item.get('qty_dispatched', 0))
        item['balance'] = mat_balances[mat]

    material_history_grouped = {}
    for item in material_history:
        mat_name = item.get('material_group') or item.get('material') or 'Unknown'
        material_history_grouped.setdefault(mat_name, []).append(item)

    return (
        financial_history,
        pending_bills,
        total_debit,
        total_credit,
        total_balance,
        material_history_grouped
    )


@app.route('/download_client_ledger/<int:id>')
@login_required
def download_client_ledger(id):
    client = Client.query.get_or_404(id)
    action = (request.args.get('action') or 'download').lower()
    disposition = 'inline' if action == 'print' else 'attachment'

    (
        financial_history,
        pending_bills,
        total_debit,
        total_credit,
        total_balance,
        material_history_grouped
    ) = _build_client_ledger_rows(client)

    rendered = render_template(
        'client_ledger_print.html',
        client=client,
        financial_history=financial_history,
        pending_bills=pending_bills,
        total_debit=total_debit,
        total_credit=total_credit,
        total_balance=total_balance,
        material_history_grouped=material_history_grouped,
        generated_at=pk_now(),
        auto_print=(action == 'print')
    )

    if action != 'print':
        safe_code = _safe_download_name(str(client.code or client.id), default='client')
        pdf_response = _try_render_weasy_pdf(
            rendered,
            f'ClientLedger-{safe_code}.pdf',
            disposition=disposition
        )
        if pdf_response:
            return pdf_response

    response = make_response(rendered)
    fallback_name = _safe_download_name(
        f'ClientLedger-{client.code or client.id}.html',
        default='client-ledger.html'
    )
    response.headers['Content-Disposition'] = f'{disposition}; filename={fallback_name}'
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    return response


@app.route('/client_booking_cancel/<int:client_id>', methods=['POST'])
@login_required
def client_booking_cancel(client_id):
    client = db.session.get(Client, client_id)
    if not client:
        flash('Client not found', 'danger')
        return redirect(url_for('clients'))

    client_name_norm = (client.name or '').strip().lower()

    delivered_totals = {}
    delivered_entries = Entry.query.filter(
        (Entry.client_code == client.code) | (func.lower(func.trim(Entry.client)) == client_name_norm),
        Entry.type == 'OUT',
        Entry.is_void == False,
        not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
    ).all()
    for e in delivered_entries:
        key = e.booked_material or e.material
        delivered_totals[key] = delivered_totals.get(key, 0) + (e.qty or 0)

    booking_items = BookingItem.query.join(Booking).filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False
    ).all()

    items_by_material = {}
    for item in booking_items:
        mat_name = item.material_name or ''
        items_by_material.setdefault(mat_name, []).append(item)

    cancel_plan = []
    cancel_total = 0
    cancel_total_qty = 0

    for mat_name, items in items_by_material.items():
        items.sort(
            key=lambda x: (
                x.booking.date_posted or datetime.min,
                x.booking.id or 0,
                x.id or 0
            ),
            reverse=True
        )
        remaining_delivered = float(delivered_totals.get(mat_name, 0) or 0)

        for item in items:
            booked_qty = float(item.qty or 0)
            consumed = min(booked_qty, remaining_delivered) if remaining_delivered > 0 else 0
            remaining_delivered = max(0, remaining_delivered - consumed)
            remaining_qty = booked_qty - consumed
            if remaining_qty <= 0:
                continue

            rate = float(item.price_at_time or 0)
            amount = remaining_qty * rate
            cancel_total += amount
            cancel_total_qty += remaining_qty
            cancel_plan.append({
                'item': item,
                'remaining_qty': remaining_qty
            })

    if not cancel_plan:
        flash('No remaining booking items to cancel.', 'info')
        return redirect(url_for('client_ledger', id=client.id))

    selected_item_ids_raw = request.form.getlist('selected_item_ids')
    selected_item_ids_csv = (request.form.get('selected_item_ids_csv') or '').strip()
    if selected_item_ids_csv:
        selected_item_ids_raw = [x.strip() for x in selected_item_ids_csv.split(',') if x.strip()]
    has_selection_ui = request.form.get('has_selection_ui') == '1'
    selected_item_ids = set()
    for raw in selected_item_ids_raw:
        try:
            selected_item_ids.add(int(raw))
        except Exception:
            continue

    if has_selection_ui:
        if not selected_item_ids:
            flash('Select at least one material row to cancel.', 'warning')
            return redirect(url_for('client_ledger', id=client.id))
        cancel_plan = [r for r in cancel_plan if r.get('item') and r['item'].id in selected_item_ids]
        if not cancel_plan:
            flash('Selected material rows are no longer available. Please review and retry.', 'warning')
            return redirect(url_for('client_ledger', id=client.id))

    touched_bookings = set()
    now = pk_now()
    for row in cancel_plan:
        item = row.get('item')
        remaining_qty = float(row.get('remaining_qty') or 0)
        if not item or remaining_qty <= 0:
            continue
        booking = item.booking
        rate = float(item.price_at_time or 0)
        amount = remaining_qty * rate
        bill_ref = booking.manual_bill_no or booking.auto_bill_no or f"BK-{booking.id}" if booking else ''
        db.session.add(Entry(
            date=now.strftime('%Y-%m-%d'),
            time=now.strftime('%H:%M:%S'),
            type='CANCEL',
            material=item.material_name,
            client=client.name,
            client_code=client.code,
            qty=remaining_qty,
            bill_no=bill_ref,
            nimbus_no='Booking Cancel',
            created_by=current_user.username,
            client_category='Booking Delivery',
            transaction_category='Cancel',
            note=f"Booking cancellation|rate={rate:.6f}|amount={amount:.6f}"
        ))
        new_qty = float(item.qty or 0) - remaining_qty
        if new_qty <= 0:
            db.session.delete(item)
        else:
            item.qty = new_qty
        if booking:
            touched_bookings.add(booking)

    for booking in touched_bookings:
        items = BookingItem.query.filter_by(booking_id=booking.id).all()
        new_amount = sum((i.qty or 0) * (i.price_at_time or 0) for i in items)
        booking.amount = new_amount

        bill_ref = booking.manual_bill_no or booking.auto_bill_no or f"BK-{booking.id}"
        new_pending = max(0.0, (booking.amount or 0) - (booking.discount or 0) - (booking.paid_amount or 0))
        pb = PendingBill.query.filter_by(bill_no=bill_ref, client_code=client.code).first()
        if new_pending <= 0:
            if pb:
                db.session.delete(pb)
        else:
            if pb:
                pb.amount = new_pending
                pb.client_name = booking.client_name
            else:
                db.session.add(PendingBill(
                    client_code=client.code,
                    client_name=booking.client_name,
                    bill_no=bill_ref,
                    bill_kind=parse_bill_kind(bill_ref),
                    amount=new_pending,
                    reason='Booking (Adjusted)',
                    is_manual=bool(booking.manual_bill_no),
                    created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                    created_by=current_user.username
                ))

    db.session.commit()
    flash(f'Booking cancellation applied. Total cancelled: {cancel_total_qty:.2f} items, value {cancel_total:.2f}', 'success')
    return redirect(url_for('client_ledger', id=client.id))


@app.route('/client_booking_cancel_revert/<int:client_id>/<int:entry_id>', methods=['POST'])
@login_required
def client_booking_cancel_revert(client_id, entry_id):
    client = db.session.get(Client, client_id)
    if not client:
        flash('Client not found', 'danger')
        return redirect(url_for('clients'))

    entry = db.session.get(Entry, entry_id)
    if not entry:
        flash('Cancellation entry not found', 'danger')
        return redirect(url_for('client_ledger', id=client.id))
    if entry.is_void:
        flash('This cancellation is already reverted.', 'info')
        return redirect(url_for('client_ledger', id=client.id))
    if (entry.type or '').upper() != 'CANCEL':
        flash('Selected row is not a booking cancellation entry.', 'warning')
        return redirect(url_for('client_ledger', id=client.id))

    client_name_norm = (client.name or '').strip().lower()
    entry_client_norm = (entry.client or '').strip().lower()
    if (entry.client_code and entry.client_code != client.code) and (entry_client_norm != client_name_norm):
        flash('Cancellation row does not belong to this client.', 'danger')
        return redirect(url_for('client_ledger', id=client.id))

    bill_ref = (entry.bill_no or entry.auto_bill_no or '').strip()
    if not bill_ref:
        flash('Cannot revert: cancellation row has no bill reference.', 'warning')
        return redirect(url_for('client_ledger', id=client.id))

    booking = Booking.query.filter(
        func.lower(func.trim(Booking.client_name)) == client_name_norm,
        Booking.is_void == False,
        or_(Booking.manual_bill_no == bill_ref, Booking.auto_bill_no == bill_ref)
    ).order_by(Booking.id.desc()).first()
    if not booking:
        flash('Cannot revert: original booking for this bill was not found.', 'warning')
        return redirect(url_for('client_ledger', id=client.id))

    material_name = (entry.material or entry.booked_material or '').strip()
    qty = float(entry.qty or 0)
    if not material_name or qty <= 0:
        flash('Cannot revert: cancellation row has invalid material/qty.', 'warning')
        return redirect(url_for('client_ledger', id=client.id))

    rate = _parse_cancel_rate_from_note(entry.note)
    if rate is None:
        amount = _parse_cancel_amount_from_note(entry.note)
        if amount is not None and qty > 0:
            rate = float(amount) / float(qty)
    if rate is None:
        existing_item = BookingItem.query.filter_by(booking_id=booking.id, material_name=material_name).first()
        rate = float(existing_item.price_at_time or 0) if existing_item else 0.0

    item = BookingItem.query.filter_by(booking_id=booking.id, material_name=material_name).first()
    if item:
        item.qty = float(item.qty or 0) + qty
        if float(item.price_at_time or 0) <= 0 and rate > 0:
            item.price_at_time = rate
    else:
        db.session.add(BookingItem(
            booking_id=booking.id,
            material_name=material_name,
            qty=qty,
            price_at_time=float(rate or 0)
        ))

    # Recompute booking amount and pending due.
    items = BookingItem.query.filter_by(booking_id=booking.id).all()
    booking.amount = sum((float(i.qty or 0) * float(i.price_at_time or 0)) for i in items)
    bill_ref_booking = booking.manual_bill_no or booking.auto_bill_no or f"BK-{booking.id}"
    new_pending = max(0.0, (booking.amount or 0) - (booking.discount or 0) - (booking.paid_amount or 0))
    pb = PendingBill.query.filter_by(bill_no=bill_ref_booking, client_code=client.code).first()
    if new_pending <= 0:
        if pb:
            db.session.delete(pb)
    else:
        if pb:
            pb.amount = new_pending
            pb.client_name = booking.client_name
            pb.is_void = False
        else:
            db.session.add(PendingBill(
                client_code=client.code,
                client_name=booking.client_name,
                bill_no=bill_ref_booking,
                bill_kind=parse_bill_kind(bill_ref_booking),
                amount=new_pending,
                reason='Booking (Adjusted)',
                is_manual=bool(booking.manual_bill_no),
                created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                created_by=current_user.username
            ))

    # Mark cancellation row voided (audit-preserving revert).
    entry.is_void = True
    db.session.commit()
    flash(f'Cancellation reverted for {material_name} ({qty:.2f}).', 'success')
    return redirect(url_for('client_ledger', id=client.id))


@app.route('/client_toggle_active/<int:client_id>', methods=['POST'])
@login_required
def client_toggle_active(client_id):
    client = db.session.get(Client, client_id)
    if not client:
        flash('Client not found', 'danger')
        return redirect(url_for('clients'))
    client.is_active = not client.is_active
    db.session.commit()
    if client.is_active:
        flash('Client reactivated. Deliveries are allowed.', 'success')
    else:
        flash('Client suspended. Deliveries are blocked.', 'warning')
    return redirect(request.referrer or url_for('clients'))


@app.route('/financial_ledger/<int:client_id>')
@login_required
def financial_ledger_details(client_id):
    return redirect(url_for('financial_ledger', client_id=client_id))


@app.route('/decision_ledger')
@login_required
def decision_ledger():
    # --- Part 1: Per-Client Financial Summary ---
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    client_financial_summary = []

    for client in clients:
        # Financial totals (void-safe)
        b_debit = db.session.query(func.sum(Booking.amount)).filter_by(client_name=client.name, is_void=False).scalar() or 0
        b_credit = db.session.query(func.sum(Booking.paid_amount)).filter_by(client_name=client.name, is_void=False).scalar() or 0
        p_credit = db.session.query(func.sum(Payment.amount)).filter_by(client_name=client.name, is_void=False).scalar() or 0
        ds_debit = db.session.query(func.sum(DirectSale.amount)).filter(func.lower(DirectSale.client_name) == client.name.lower(), DirectSale.is_void==False).scalar() or 0
        ds_credit = db.session.query(func.sum(DirectSale.paid_amount)).filter(func.lower(DirectSale.client_name) == client.name.lower(), DirectSale.is_void==False).scalar() or 0
        
        b_discount = 0
        try:
            b_discount = db.session.query(func.sum(Booking.discount)).filter(
                func.lower(func.trim(Booking.client_name)) == client.name.lower(),
                Booking.is_void == False
            ).scalar() or 0
        except Exception:
            pass

        p_discount = 0
        try:
            p_discount = _client_waive_off_total((client.name or '').strip().lower())
        except Exception:
            pass

        ds_discount = 0
        try:
            ds_discount = db.session.query(func.sum(DirectSale.discount)).filter(
                func.lower(func.trim(DirectSale.client_name)) == client.name.lower(),
                DirectSale.is_void == False
            ).scalar() or 0
        except Exception:
            pass

        total_debit = b_debit + ds_debit
        total_credit = b_credit + p_credit + ds_credit + ds_discount + b_discount + p_discount
        balance = total_debit - total_credit

        # --- Per-Client Material Summary ---
        booked_res = db.session.query(BookingItem.material_name, func.sum(BookingItem.qty))\
            .join(Booking).filter(Booking.client_name == client.name, Booking.is_void == False)\
            .group_by(BookingItem.material_name).all()
        booked_map = {r[0]: (r[1] or 0) for r in booked_res if r[0]}

        # Latest unit price per material (from booking items)
        latest_price = {}
        latest_price_dt = {}
        booking_items = BookingItem.query.join(Booking).filter(
            Booking.client_name == client.name,
            Booking.is_void == False
        ).all()
        for item in booking_items:
            mat_name = item.material_name
            if not mat_name:
                continue
            bk = item.booking
            bk_dt = bk.date_posted if bk and getattr(bk, 'date_posted', None) else None
            if mat_name not in latest_price_dt or (bk_dt and latest_price_dt[mat_name] and bk_dt > latest_price_dt[mat_name]) or (bk_dt and not latest_price_dt[mat_name]):
                latest_price_dt[mat_name] = bk_dt
                latest_price[mat_name] = float(item.price_at_time or 0)
            elif mat_name not in latest_price:
                latest_price[mat_name] = float(item.price_at_time or 0)

        entries = Entry.query.filter(
            (Entry.client_code == client.code) | (Entry.client == client.name),
            Entry.type == 'OUT',
            Entry.is_void == False
        ).filter(
            or_(
                func.coalesce(Entry.nimbus_no, '') != 'Direct Sale',
                Entry.client_category == 'Booking Delivery'
            )
        ).all()

        dispatched_map = {}
        for e in entries:
            key = e.booked_material or e.material
            if key:
                dispatched_map[key] = dispatched_map.get(key, 0) + e.qty

        materials_summary = []
        total_remaining_qty = 0
        total_reserved_cost = 0
        total_booked_cost = 0
        total_dispatched_cost = 0
        all_mats = set(booked_map.keys()) | set(dispatched_map.keys())

        for m in sorted(all_mats, key=lambda x: str(x).lower()):
            b = booked_map.get(m, 0)
            d = dispatched_map.get(m, 0)
            rem = b - d
            unit_price = latest_price.get(m, 0)
            booked_cost = b * unit_price
            dispatched_cost = d * unit_price
            remaining_cost = rem * unit_price
            if b > 0 or d > 0 or rem != 0:
                materials_summary.append({
                    'name': m,
                    'booked': b,
                    'dispatched': d,
                    'remaining': rem,
                    'unit_price': unit_price,
                    'booked_cost': booked_cost,
                    'dispatched_cost': dispatched_cost,
                    'remaining_cost': remaining_cost
                })
                total_remaining_qty += rem
                total_reserved_cost += remaining_cost
                total_booked_cost += booked_cost
                total_dispatched_cost += dispatched_cost

        client_financial_summary.append({
            'client': client,
            'financial': {
                'debit': total_debit,
                'credit': total_credit,
                'balance': balance
            },
            'materials': materials_summary,
            'material_totals': {
                'total_remaining_qty': total_remaining_qty,
                'total_reserved_cost': total_reserved_cost,
                'total_booked_cost': total_booked_cost,
                'total_dispatched_cost': total_dispatched_cost
            }
        })

    # --- Filters & Pagination (Client Summary) ---
    q = request.args.get('q', '').strip()
    category_filter = request.args.get('category', '').strip()
    balance_filter = request.args.get('balance', 'all').strip().lower()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    per_page = min(max(per_page, 5), 100)

    def _match(row):
        if q:
            ql = q.lower()
            if ql not in (row['client'].name or '').lower() and ql not in (row['client'].code or '').lower():
                return False
        if category_filter and (row['client'].category or '') != category_filter:
            return False
        bal = row['financial']['balance']
        if balance_filter == 'debit' and bal <= 0:
            return False
        if balance_filter == 'credit' and bal >= 0:
            return False
        if balance_filter == 'zero' and bal != 0:
            return False
        return True

    filtered = [r for r in client_financial_summary if _match(r)]
    total = len(filtered)
    total_pages = max(1, (total + per_page - 1) // per_page)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages
    start = (page - 1) * per_page
    end = start + per_page
    paged = filtered[start:end]

    # --- Part 2: Overall Material Summary ---
    total_booked_q = db.session.query(
        BookingItem.material_name,
        func.sum(BookingItem.qty).label('total_booked')
    ).join(Booking).filter(Booking.is_void == False).group_by(BookingItem.material_name).all()
    total_booked_map = {r.material_name: r.total_booked for r in total_booked_q}

    all_dispatches = db.session.query(
        func.coalesce(Entry.booked_material, Entry.material).label('booked_mat'),
        func.sum(Entry.qty).label('total_dispatched')
    ).filter(
        Entry.type == 'OUT', Entry.is_void == False
    ).filter(
        or_(
            func.coalesce(Entry.nimbus_no, '') != 'Direct Sale',
            Entry.client_category == 'Booking Delivery'
        )
    ).group_by(func.coalesce(Entry.booked_material, Entry.material)).all()
    total_dispatched_map = {r.booked_mat: r.total_dispatched for r in all_dispatches if r.booked_mat}

    all_materials = {m for m in (set(total_booked_map.keys()) | set(total_dispatched_map.keys())) if m}
    overall_material_summary = []
    overall_remaining_total = 0
    for m in sorted(list(all_materials), key=lambda x: str(x).lower()):
        booked = total_booked_map.get(m, 0)
        dispatched = total_dispatched_map.get(m, 0)
        remaining = booked - dispatched
        overall_material_summary.append({
            'name': m,
            'booked': booked,
            'dispatched': dispatched,
            'remaining': remaining
        })
        overall_remaining_total += remaining

    category_options = sorted({c.category for c in clients if c.category})
    for default_cat in ['General', 'Open Khata', 'Walking-Customer', 'Misc']:
        if default_cat not in category_options:
            category_options.append(default_cat)
    category_options = sorted(category_options, key=lambda x: str(x).lower())

    return render_template('decision_ledger.html',
                           overall_material_summary=overall_material_summary,
                           data=paged,
                           q=q,
                           category_filter=category_filter,
                           balance_filter=balance_filter,
                           page=page,
                           per_page=per_page,
                           total=total,
                           total_pages=total_pages,
                           categories=category_options,
                           overall_remaining_total=overall_remaining_total)


@app.route('/material_ledger/<int:mat_id>')
@login_required
def material_ledger_page(mat_id):
    material = Material.query.get_or_404(mat_id)

    # Fetch all entries
    entries = Entry.query.filter_by(material=material.name, is_void=False).all()

    # Helper to parse date for sorting
    def parse_entry_datetime(e):
        d_str = e.date or ""
        t_str = e.time or "00:00:00"
        try:
            return datetime.strptime(f"{d_str} {t_str}", '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
        try:
            return datetime.strptime(f"{d_str} {t_str}", '%d-%m-%Y %H:%M:%S')
        except ValueError:
            pass
        return datetime.min

    # Sort by Date/Time, then ID to ensure stable sort
    entries.sort(key=lambda x: (parse_entry_datetime(x), x.id))

    history = []
    running_balance = 0

    for e in entries:
        qty_add = e.qty if e.type == 'IN' else 0
        qty_delivered = e.qty if e.type == 'OUT' else 0
        running_balance += (qty_add - qty_delivered)

        date_display = e.date
        try:
            dt = datetime.strptime(e.date, '%Y-%m-%d')
            date_display = dt.strftime('%d-%m-%Y')
        except (ValueError, TypeError):
            pass

        history.append({
            'date': date_display,
            'item': e.material,
            'bill_no': e.bill_no or e.auto_bill_no or '',
            'add': qty_add,
            'delivered': qty_delivered,
            'balance': running_balance
        })

    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    all_materials = Material.query.order_by(Material.name.asc()).all()

    return render_template('material_ledger.html',
                           material=material,
                           history=history,
                           clients=clients,
                           materials=all_materials)


@app.route('/client_ledger/<int:id>')
@login_required
def client_ledger(id):
    client = db.session.get(Client, id)
    if client:
        page = request.args.get('page', 1, type=int)
        pagination = Entry.query.filter_by(client=client.name, is_void=False).order_by(
            Entry.date.desc()).paginate(page=page, per_page=10)
        summary_query = db.session.query(
            Entry.material,
            func.sum(Entry.qty).label('total')).filter_by(
                client=client.name).group_by(Entry.material).all()
        summary = {row.material: row.total for row in summary_query}
        total_qty = db.session.query(func.sum(Entry.qty)).filter_by(client=client.name).scalar() or 0

        pending_photos = {
            b.bill_no: b.photo_url
            for b in PendingBill.query.filter(PendingBill.photo_url != '').all() if b.bill_no
        }

        clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
        materials = Material.query.order_by(Material.name.asc()).all()

        return render_template('ledger.html',
                               client=client,
                               entries=pagination.items,
                               pagination=pagination,
                               total_qty=total_qty,
                               summary=summary,
                               pending_photos=pending_photos,
                               clients=clients,
                               materials=materials)
    return redirect(url_for('clients'))


# ==================== INVENTORY ROUTES ====================

@app.route('/dispatching')
@login_required
def dispatching():
    mats = Material.query.order_by(Material.name.asc()).all()
    cls = Client.query.filter(Client.is_active == True).order_by(Client.name.asc()).all()
    dps = DeliveryPerson.query.filter_by(is_active=True).order_by(DeliveryPerson.name.asc()).all()
    today = pk_today().strftime('%Y-%m-%d')
    return render_template('dispatching.html',
                           materials=mats,
                           clients=cls,
                           delivery_persons=dps,
                           today_date=today)


@app.route('/api/client_booking_status/<client_code>')
@login_required
def api_client_booking_status(client_code):
    client = get_client_by_input(client_code)
    if not client:
        return jsonify([])

    # Get bookings by client name (Booking model uses name)
    bookings = Booking.query.filter_by(client_name=client.name, is_void=False).all()
    booking_ids = [b.id for b in bookings]

    def _material_key(v):
        txt = (v or '').strip().lower()
        return re.sub(r'[^a-z0-9]+', '', txt)

    booked_totals = {}
    material_labels = {}
    latest_price = {}
    latest_price_dt = {}
    if booking_ids:
        items = BookingItem.query.filter(BookingItem.booking_id.in_(booking_ids)).all() # BookingItem doesn't have is_void, parent Booking does
        for item in items:
            raw_mat = (item.material_name or '').strip()
            key = _material_key(raw_mat)
            if not key:
                continue
            booked_totals[key] = booked_totals.get(key, 0) + (item.qty or 0)
            if key not in material_labels:
                material_labels[key] = raw_mat
            bk = item.booking
            bk_dt = bk.date_posted if bk and getattr(bk, 'date_posted', None) else None
            if key not in latest_price_dt or (bk_dt and latest_price_dt[key] and bk_dt > latest_price_dt[key]) or (bk_dt and not latest_price_dt[key]):
                latest_price_dt[key] = bk_dt
                latest_price[key] = float(item.price_at_time or 0)
            elif key not in latest_price:
                latest_price[key] = float(item.price_at_time or 0)

    # Get delivered totals from Entry (OUT)
    entries = Entry.query.filter(
        (Entry.client_code == client_code) | (Entry.client == client.name),
        Entry.type == 'OUT'
    ).filter(
        Entry.is_void == False,
        # Direct Sale credit/cash rows must not consume booking balance.
        not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
    ).all()

    delivered_totals = {}
    for e in entries:
        key = _material_key(e.booked_material or e.material)
        if not key:
            continue
        delivered_totals[key] = delivered_totals.get(key, 0) + e.qty

    status_data = []
    for key, booked_qty in booked_totals.items():
        delivered_qty = delivered_totals.get(key, 0)
        status_data.append({
            'material': material_labels.get(key, key),
            'booked': booked_qty,
            'delivered': delivered_qty,
            'balance': booked_qty - delivered_qty,
            'unit_price': latest_price.get(key, 0)
        })

    return jsonify(status_data)

@app.route('/api/client_financial_summary/<client_code>')
@login_required
def api_client_financial_summary(client_code):
    client = get_client_by_input(client_code)
    if not client:
        resp = jsonify({'found': False, 'generated_at': pk_now().strftime('%Y-%m-%d %H:%M:%S')})
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        resp.headers['Pragma'] = 'no-cache'
        resp.headers['Expires'] = '0'
        return resp
    summary = _compute_client_financial_summary(client)
    resp = jsonify({
        'found': True,
        'client_name': client.name,
        'client_code': client.code,
        'generated_at': pk_now().strftime('%Y-%m-%d %H:%M:%S'),
        **summary
    })
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp

@app.route('/add_record', methods=['POST'])
@login_required
def add_record():
    entry_date = request.form.get('date') or pk_now().strftime('%Y-%m-%d')

    if current_user.role == 'user' and entry_date != pk_now().strftime('%Y-%m-%d'):
        flash('Permission Denied: Standard users cannot add back-dated records.', 'danger')
        return redirect(url_for('index'))

    now = pk_now()
    client_name = request.form.get('client', '').strip()
    client_code = None
    client_obj = None
    note = request.form.get('note', '').strip()
    driver_name = (request.form.get('driver_name') or '').strip()

    client_obj = get_client_by_input(client_name)
    if client_obj:
        client_code = client_obj.code
        client_name = client_obj.name

    entry_type = request.form.get('type', 'IN')
    if entry_type == 'OUT' and not driver_name:
        flash('Driver name is required for delivery dispatch.', 'danger')
        return redirect(url_for('dispatching'))
    if entry_type == 'OUT':
        get_or_create_delivery_person(driver_name)

    # For OUT dispatches to unknown clients, redirect to Direct Sale
    if entry_type == 'OUT' and not client_obj:
        flash('Unknown client: For cash customers, please use the Direct Sale form.', 'warning')
        return redirect(url_for('direct_sales_page', client_name=client_name or ''))

    if entry_type == 'OUT' and client_obj:
        mat_input = request.form.get('material', '')
        mat_obj = get_material_by_input(mat_input)
        mat_name = mat_obj.name if mat_obj else mat_input

        try:
            req_qty = float(request.form.get('qty', 0) or 0)
        except ValueError:
            req_qty = 0

        booked = db.session.query(func.sum(BookingItem.qty))\
            .join(Booking)\
            .filter(Booking.client_name == client_obj.name)\
            .filter(BookingItem.material_name == mat_name, Booking.is_void == False)\
            .scalar() or 0

        dispatched = db.session.query(func.sum(Entry.qty))\
            .filter((Entry.client_code == client_obj.code) | (Entry.client == client_obj.name))\
            .filter(or_(Entry.material == mat_name, Entry.booked_material == mat_name), Entry.is_void == False)\
            .filter(Entry.type == 'OUT')\
            .filter(not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery')))\
            .scalar() or 0

        remaining = booked - dispatched

        if req_qty > remaining:
            flash(f"Cannot dispatch {req_qty} bags. Only {remaining} bags available from booking. (Booked: {booked}, Dispatched: {dispatched})", 'danger')
            return redirect(url_for('dispatching'))

    # Auto-mark as Booking Delivery if fulfilling a booking
    nimbus_no_val = request.form.get('nimbus_no', '').strip()
    if entry_type == 'OUT' and client_obj and not nimbus_no_val:
        nimbus_no_val = "Booking Delivery"

    # Resolve material for entry
    mat_input = request.form.get('material', '')
    mat_obj = get_material_by_input(mat_input)
    entry_material_name = mat_obj.name if mat_obj else mat_input
    entry = Entry(date=entry_date,
                  time=now.strftime('%H:%M:%S'),
                  type=entry_type,
                  material=entry_material_name,
                  client=client_name,
                  client_code=client_code,
                  qty=float(request.form.get('qty', 0) or 0),
                  bill_no=request.form.get('bill_no', '').strip(),
                  nimbus_no=nimbus_no_val,
                  created_by=current_user.username,
                  client_category=client_obj.category if client_obj else None,
                  driver_name=(driver_name if entry_type == 'OUT' else None),
                  note=note)
    db.session.add(entry)
    db.session.flush()

    # Update material stock with correct direction:
    # IN increases stock, OUT decreases stock.
    if mat_obj:
        if entry_type == 'IN':
            mat_obj.total = (mat_obj.total or 0) + entry.qty
        elif entry_type == 'OUT':
            mat_obj.total = (mat_obj.total or 0) - entry.qty

    hv = request.form.get('has_bill')
    has_bill = True if hv is None else hv in ['on', '1', 'true', 'True']

    unit_price = (mat_obj.unit_price if mat_obj else 0) or 0
    amount = float(entry.qty) * float(unit_price)

    create_invoice = bool(request.form.get('create_invoice'))

    if client_obj and getattr(client_obj, 'require_manual_invoice', False) and entry_type == 'OUT' and not entry.bill_no and not create_invoice:
        db.session.rollback()
        flash('Manual invoice required for this client.', 'danger')
        return redirect(url_for('dispatching'))

    invoice_no = None
    inv = None

    # Only create Invoice for non-OUT entries (e.g. IN/Receiving)
    # For OUT entries (Dispatching), we are fulfilling bookings which already have financial records.
    if entry_type != 'OUT' and has_bill and (create_invoice or entry.bill_no):
        if entry.bill_no:
            invoice_no = entry.bill_no
            is_manual = True
        else:
            invoice_no = get_next_bill_no(AUTO_BILL_NAMESPACES['ENTRY'])
            entry.auto_bill_no = invoice_no
            is_manual = False

        existing_global = Invoice.query.filter_by(invoice_no=invoice_no).first()
        if existing_global and not is_manual:
            while Invoice.query.filter_by(invoice_no=invoice_no).first():
                invoice_no = get_next_bill_no(AUTO_BILL_NAMESPACES['ENTRY'])
            entry.auto_bill_no = invoice_no
        elif existing_global and is_manual:
            if existing_global.client_code != entry.client_code:
                db.session.rollback()
                flash(f'Invoice number "{invoice_no}" is already used by another client.', 'danger')
                return redirect(url_for('dispatching'))

        inv = Invoice.query.filter_by(invoice_no=invoice_no, client_code=entry.client_code).first()
        if inv:
            inv.client_name = entry.client
            inv.total_amount = amount
            inv.balance = amount
            inv.is_cash = bool(request.form.get('track_as_cash'))
            inv.date = datetime.strptime(entry.date, '%Y-%m-%d').date() if entry.date else pk_now().date()
            inv.note = note
        else:
            inv = Invoice(client_code=entry.client_code,
                          client_name=entry.client,
                          invoice_no=invoice_no,
                          is_manual=is_manual,
                          date=datetime.strptime(entry.date, '%Y-%m-%d').date() if entry.date else pk_now().date(),
                          total_amount=amount,
                          balance=amount,
                          is_cash=bool(request.form.get('track_as_cash')),
                          note=note,
                          created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                          created_by=current_user.username)
            db.session.add(inv)
            db.session.flush()
        entry.invoice_id = inv.id

    # Pending Bill logic for OUT entries REMOVED
    # Dispatching (OUT) is now strictly for booking fulfillment, so no new financial bills are created.
    # The financial obligation is tracked via the original Booking and its PendingBill.

    db.session.commit()
    flash("Record Saved", "success")
    return redirect(url_for('index'))


@app.route('/edit_entry/<int:id>', methods=['POST'])
@login_required
def edit_entry(id):
    e = db.session.get(Entry, id)
    if not e:
        return redirect(url_for('index'))

    today_str = pk_today().strftime('%Y-%m-%d')
    if current_user.role != 'admin' and e.date != today_str:
        flash('Permission Denied: Only Admins can edit back-dated records.', 'danger')
        return redirect(url_for('index'))

    old_bill_no = e.bill_no
    old_client_code = e.client_code
    old_qty = e.qty
    old_material = e.material

    e.date = request.form.get('date') or e.date
    e.time = request.form.get('time') or e.time
    e.type = request.form.get('type') or e.type

    mat_obj = get_material_by_input(request.form.get('material'))
    e.material = mat_obj.name if mat_obj else (request.form.get('material') or e.material)

    client_input = request.form.get('client', '').strip()
    if client_input:
        client_obj = get_client_by_input(client_input)
        if client_obj:
            e.client = client_obj.name
            e.client_code = client_obj.code
        else:
            e.client = client_input
            e.client_code = None
    else:
        e.client = None
        e.client_code = None

    e.qty = float(request.form.get('qty', e.qty) or e.qty)
    e.bill_no = request.form.get('bill_no', '').strip() or None
    e.nimbus_no = request.form.get('nimbus_no', '').strip() or None
    driver_name = (request.form.get('driver_name') or '').strip()
    if e.type == 'OUT' and not driver_name:
        flash('Driver name is required for delivery dispatch.', 'danger')
        return redirect(request.referrer or url_for('tracking'))
    if e.type == 'OUT' and driver_name:
        get_or_create_delivery_person(driver_name)
    e.driver_name = driver_name or None
    e.note = request.form.get('note', '').strip()

    # Update Material Totals if qty or material changed
    if e.type == 'OUT' or e.type == 'IN':
        # Revert old qty from old material
        old_mat_obj = Material.query.filter_by(name=old_material).first()
        if old_mat_obj:
            if e.type == 'IN': old_mat_obj.total -= old_qty
            else: old_mat_obj.total += old_qty

        # Apply new qty to new material
        new_mat_obj = mat_obj if mat_obj else Material.query.filter_by(name=e.material).first()
        if new_mat_obj:
            if e.type == 'IN': new_mat_obj.total += e.qty
            else: new_mat_obj.total -= e.qty

    # Synchronize PendingBill - REMOVED DANGEROUS LOGIC
    # We do NOT want to auto-update PendingBills for OUT entries here because
    # it risks overwriting Booking bills with partial dispatch amounts.
    # Only update bill reference if changed.
    if e.type == 'OUT':
        pass

    db.session.commit()
    flash('Entry Updated', 'success')

    redirect_to = request.form.get('redirect_to')
    if redirect_to == 'tracking':
        return redirect(url_for('tracking'))
    if redirect_to == 'daily_transactions':
        return redirect(url_for('inventory.daily_transactions', date=e.date))
    return redirect(url_for('index'))


@app.route('/import_dispatch_data', methods=['POST'])
@login_required
def import_dispatch_data():
    if current_user.role not in ['admin', 'root']:
        flash('Only tenant admin or root can run import/export operations.', 'danger')
        return redirect(url_for('index'))
    import pandas as pd
    from datetime import datetime
    file = request.files.get('file')
    if not file or not file.filename:
        flash('No file selected', 'danger')
        return redirect(url_for('import_export.import_export_page'))

    try:
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file, low_memory=False)
        else:
            df = pd.read_excel(file)

        count = 0
        for _, row in df.iterrows():
            code = str(row.get('CLIENT_CODE', '')).strip()
            name = str(row.get('CLIENT_NAME', '')).strip()
            c_cat = str(row.get('CLIENT_CATEGORY', '')).strip()
            t_cat = str(row.get('TRANSACTION_CATEGORY', '')).strip()
            bill_no = str(row.get('BILL_NO', '')).strip()
            b_date = str(row.get('BILL_DATE', '')).strip()
            brand = str(row.get('CEMENT_BRAND', '')).strip()
            qty_val = row.get('QTY', 0)
            try:
                qty = float(qty_val)
            except:
                qty = 0
            nimbus = str(row.get('NIMBUS', '')).strip()
            notes = str(row.get('NOTES', '')).strip()

            if not brand or qty <= 0: continue

            # Date conversion
            final_date = None
            date_formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']
            for fmt in date_formats:
                try:
                    dt_obj = datetime.strptime(b_date, fmt)
                    final_date = dt_obj.strftime('%Y-%m-%d')
                    break
                except:
                    continue

            if not final_date:
                final_date = pk_now().strftime('%Y-%m-%d')

            # Ensure client exists
            if code and name:
                client = Client.query.filter_by(code=code).first()
                if not client:
                    client = Client(code=code, name=name, category=(c_cat or 'General'))
                    db.session.add(client)
                elif c_cat:
                    client.category = c_cat

            # Ensure material exists
            mat = Material.query.filter_by(name=brand).first()
            if not mat:
                mat = Material(name=brand, code=f"MAT-{brand[:3].upper()}", category_id=_get_default_material_category_id())
                db.session.add(mat)

            # Create entry
            entry = Entry(
                date=final_date,
                time="00:00:00",
                type='OUT',
                material=brand,
                client=name,
                client_code=code,
                qty=qty,
                bill_no=bill_no if bill_no != 'UNBILLED' else None,
                nimbus_no=nimbus,
                client_category=c_cat,
                transaction_category=t_cat,
                created_by=current_user.username
            )
            db.session.add(entry)

            # If billed and has bill_no, add to PendingBill if not exists
            if t_cat == 'BILLED' and bill_no and bill_no != 'UNBILLED':
                existing_bill = PendingBill.query.filter_by(bill_no=bill_no, client_code=code).first()
                if not existing_bill:
                    pb = PendingBill(
                        client_code=code,
                        client_name=name,
                        bill_no=bill_no,
                        bill_kind=parse_bill_kind(bill_no),
                        nimbus_no=nimbus,
                        amount=0,
                        reason=notes,
                        created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                        created_by=current_user.username
                    )
                    db.session.add(pb)

            count += 1

        db.session.commit()
        flash(f'Imported {count} dispatching entries successfully.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Import failed: {str(e)}', 'danger')

    return redirect(url_for('import_export.import_export_page'))


@app.route('/delete_entry/<int:id>', methods=['POST'])
@login_required
def delete_entry(id):
    e = db.session.get(Entry, id)
    if not e:
        return redirect(url_for('index'))

    today_str = pk_today().strftime('%Y-%m-%d')
    if current_user.role != 'admin' and e.date != today_str:
        flash('Permission Denied: Only Admins can delete back-dated records.', 'danger')
        return redirect(url_for('index'))

    changed = _set_entry_void_state(e, True)
    if changed:
        db.session.commit()
        flash('Transaction voided', 'warning')
    else:
        flash('Transaction already voided', 'info')
    return redirect(url_for('index'))


# ==================== TRACKING ROUTES ====================

@app.route('/tracking')
@login_required
def tracking():
    s = request.args.get('start_date')
    end = request.args.get('end_date')
    cl = request.args.get('client')
    m = request.args.get('material')
    bill_no = request.args.get('bill_no', '').strip()
    category = request.args.get('category', '').strip()
    search = request.args.get('search', '').strip()
    page = request.args.get('page', 1, type=int)
    type_filter = request.args.get('type', '').strip()
    has_bill_filter = request.args.get('has_bill', '').strip()

    has_filter = bool(s or end or cl or m or search or bill_no or category or type_filter or has_bill_filter in ['0', '1'])

    entries = []
    pagination = None
    summary = {}
    total_qty = 0

    if has_filter:
        query = Entry.query
        if s:
            query = query.filter(Entry.date >= s) # Show voided in tracking? Yes, but maybe filterable. For now show all.
        if end:
            query = query.filter(Entry.date <= end)
        if cl:
            query = query.filter(Entry.client == cl)
        if m:
            query = query.filter(Entry.material == m)
        if bill_no:
            query = query.filter(db.or_(Entry.bill_no.ilike(f'%{bill_no}%'), Entry.auto_bill_no.ilike(f'%{bill_no}%')))
        if category:
            query = query.outerjoin(Client, Entry.client_code == Client.code).filter(
                or_(Entry.client_category == category, Client.category == category)
            )
        if type_filter and type_filter != 'PAYMENT':
            query = query.filter(Entry.type == type_filter)
        if has_bill_filter == '1':
            query = query.filter(db.or_(Entry.bill_no != None, Entry.auto_bill_no != None))\
                         .filter(db.or_(Entry.bill_no != '', Entry.auto_bill_no != ''))\
                         .filter(db.or_(Entry.bill_no == None, db.not_(Entry.bill_no.like('UNBILLED%'))))\
                         .filter(db.or_(Entry.bill_no == None, db.not_(Entry.bill_no.like('#%'))))
        if has_bill_filter == '0':
            query = query.filter(db.or_(
                db.and_(
                    db.or_(Entry.bill_no == None, Entry.bill_no == ''),
                    db.or_(Entry.auto_bill_no == None, Entry.auto_bill_no == '')
                ),
                Entry.bill_no.like('UNBILLED%'),
                Entry.bill_no.like('#%')
            ))
        if search:
            query = query.filter(
                db.or_(Entry.material.ilike(f'%{search}%'),
                       Entry.client.ilike(f'%{search}%'),
                       Entry.client_code.ilike(f'%{search}%'),
                       Entry.bill_no.ilike(f'%{search}%'),
                       Entry.nimbus_no.ilike(f'%{search}%'),
                       Entry.note.ilike(f'%{search}%')))

        entry_rows = query.order_by(Entry.date.desc(), Entry.time.desc()).all()
        for e in entry_rows:
            e.bill_ref = _entry_best_bill_ref(e)
            e.source_type = 'Entry'

        payment_rows = []
        if not type_filter or type_filter == 'PAYMENT':
            pay_query = Payment.query
            if s:
                pay_query = pay_query.filter(func.date(Payment.date_posted) >= s)
            if end:
                pay_query = pay_query.filter(func.date(Payment.date_posted) <= end)
            if cl:
                pay_query = pay_query.filter(Payment.client_name == cl)
            if bill_no:
                pay_query = pay_query.filter(or_(
                    Payment.manual_bill_no.ilike(f'%{bill_no}%'),
                    Payment.auto_bill_no.ilike(f'%{bill_no}%')
                ))
            if category:
                category_names = [x[0] for x in db.session.query(Client.name).filter(Client.category == category).all() if x[0]]
                if category_names:
                    pay_query = pay_query.filter(Payment.client_name.in_(category_names))
                else:
                    pay_query = pay_query.filter(Payment.id == -1)
            if has_bill_filter == '1':
                pay_query = pay_query.filter(
                    or_(Payment.manual_bill_no != None, Payment.auto_bill_no != None)
                ).filter(
                    or_(Payment.manual_bill_no != '', Payment.auto_bill_no != '')
                )
            if has_bill_filter == '0':
                pay_query = pay_query.filter(
                    and_(
                        or_(Payment.manual_bill_no == None, Payment.manual_bill_no == ''),
                        or_(Payment.auto_bill_no == None, Payment.auto_bill_no == '')
                    )
                )
            if search:
                pay_query = pay_query.filter(or_(
                    Payment.client_name.ilike(f'%{search}%'),
                    Payment.manual_bill_no.ilike(f'%{search}%'),
                    Payment.auto_bill_no.ilike(f'%{search}%'),
                    Payment.method.ilike(f'%{search}%'),
                    Payment.note.ilike(f'%{search}%')
                ))

            code_by_client = {c.name: c.code for c in Client.query.with_entities(Client.name, Client.code).all()}
            for p in pay_query.order_by(Payment.date_posted.desc(), Payment.id.desc()).all():
                dt = p.date_posted or pk_now()
                payment_rows.append(SimpleNamespace(
                    id=p.id,
                    date=dt.strftime('%Y-%m-%d'),
                    time=dt.strftime('%H:%M:%S'),
                    type='PAYMENT',
                    client=(p.client_name or ''),
                    client_code=(code_by_client.get(p.client_name, '') or ''),
                    material='-',
                    qty=float(p.amount or 0),
                    auto_bill_no=(p.auto_bill_no or ''),
                    bill_no=(p.manual_bill_no or ''),
                    bill_ref=(p.manual_bill_no or p.auto_bill_no or f'PAY-{p.id}'),
                    nimbus_no='Payment',
                    created_by='System',
                    note=(p.note or ''),
                    is_void=bool(p.is_void),
                    source_type='Payment'
                ))

        combined_rows = payment_rows if type_filter == 'PAYMENT' else (entry_rows + payment_rows)
        combined_rows.sort(
            key=lambda r: _parse_dt_safe(f"{(getattr(r, 'date', '') or '').strip()} {(getattr(r, 'time', '') or '').strip()}".strip()) or datetime.min,
            reverse=True
        )

        per_page = 15
        total = len(combined_rows)
        pages = max(1, (total + per_page - 1) // per_page)
        if page > pages:
            page = pages
        start_idx = (page - 1) * per_page
        entries = combined_rows[start_idx:start_idx + per_page]
        pagination = SimpleNamespace(
            page=page,
            pages=pages,
            total=total,
            has_prev=(page > 1),
            has_next=(page < pages),
            prev_num=(page - 1),
            next_num=(page + 1)
        )

        # Summary calculation
        base_query = db.session.query(
            Entry.material,
            func.sum(case((Entry.type == 'IN', Entry.qty), else_=-Entry.qty)).label('net'))

        # Ensure summary excludes voided transactions
        base_query = base_query.filter(Entry.is_void == False)

        if category:
            base_query = base_query.outerjoin(Client, Entry.client_code == Client.code).filter(
                or_(Entry.client_category == category, Client.category == category)
            )
        if s:
            base_query = base_query.filter(Entry.date >= s)
        if end:
            base_query = base_query.filter(Entry.date <= end)
        if cl:
            base_query = base_query.filter(Entry.client == cl)
        if m:
            base_query = base_query.filter(Entry.material == m)
        if bill_no:
            base_query = base_query.filter(db.or_(Entry.bill_no.ilike(f'%{bill_no}%'), Entry.auto_bill_no.ilike(f'%{bill_no}%')))
        if type_filter and type_filter != 'PAYMENT':
            base_query = base_query.filter(Entry.type == type_filter)
        if has_bill_filter == '1':
            base_query = base_query.filter(db.or_(Entry.bill_no != None, Entry.auto_bill_no != None))\
                         .filter(db.or_(Entry.bill_no != '', Entry.auto_bill_no != ''))\
                         .filter(db.or_(Entry.bill_no == None, db.not_(Entry.bill_no.like('UNBILLED%'))))
        if has_bill_filter == '0':
            base_query = base_query.filter(db.or_(
                db.and_(
                    db.or_(Entry.bill_no == None, Entry.bill_no == ''),
                    db.or_(Entry.auto_bill_no == None, Entry.auto_bill_no == '')
                ),
                Entry.bill_no.like('UNBILLED%')
            ))
        if search:
            base_query = base_query.filter(
                db.or_(Entry.material.ilike(f'%{search}%'),
                       Entry.client.ilike(f'%{search}%'),
                       Entry.client_code.ilike(f'%{search}%'),
                       Entry.bill_no.ilike(f'%{search}%'),
                       Entry.nimbus_no.ilike(f'%{search}%'),
                       Entry.note.ilike(f'%{search}%')))

        summary_query = base_query.group_by(Entry.material).all()
        summary = {row.material: row.net for row in summary_query}
        total_qty = sum(summary.values()) if summary else 0

    today_str = pk_today().strftime('%Y-%m-%d')
    pending_photos = {
        b.bill_no: b.photo_url
        for b in PendingBill.query.filter(PendingBill.photo_url != '').all()
        if b.bill_no
    }

    return render_template(
        'tracking.html',
        entries=entries,
        pagination=pagination,
        clients=Client.query.filter(Client.is_active == True).order_by(Client.name.asc()).all(),
        materials=Material.query.order_by(Material.name.asc()).all(),
        start_date=s,
        end_date=end,
        client_filter=cl,
        material_filter=m,
        bill_no_filter=bill_no,
        category_filter=category,
        search_query=search,
        now_date=today_str,
        total_qty=total_qty,
        summary=summary,
        has_filter=has_filter,
        pending_photos=pending_photos,
        type_filter=type_filter,
        has_bill_filter=has_bill_filter)


@app.route('/unpaid_transactions')
@login_required
def unpaid_transactions_page():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    material = request.args.get('material')
    bill_no = request.args.get('bill_no')
    status = request.args.get('status', 'unpaid')
    include_booking = request.args.get('include_booking', '0')

    query = PendingBill.query.filter(PendingBill.is_void == False)

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

    # Hide 0-amount unpaid bills (Booking Deliveries)
    query = query.filter(or_(PendingBill.amount > 0, PendingBill.is_paid == True))

    # Exclude clients who have bookings unless explicitly included
    if include_booking not in ['1', 'true', 'on', 'yes']:
        booked_names = [r[0] for r in db.session.query(Booking.client_name).filter(Booking.is_void == False).distinct().all() if r[0]]
        booked_codes = set()
        if booked_names:
            booked_codes = {c.code for c in Client.query.filter(Client.name.in_(booked_names)).all()}
        if booked_codes:
            query = query.filter(~PendingBill.client_code.in_(booked_codes))
        if booked_names:
            query = query.filter(~PendingBill.client_name.in_(booked_names))

    transactions = query.order_by(PendingBill.id.desc()).all()
    effective_map = _compute_pending_effective_amount_map(transactions)
    for t in transactions:
        t.effective_amount = float(effective_map.get(t.id, float(t.amount or 0)) or 0)

    # For unpaid view, hide rows fully neutralized by cancellation credits.
    if status == 'unpaid':
        transactions = [t for t in transactions if float(getattr(t, 'effective_amount', 0) or 0) > 0]

    materials = Material.query.order_by(Material.name.asc()).all()
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()

    return render_template('unpaid_transactions.html',
                           transactions=transactions,
                           materials=materials,
                           clients=clients,
                           filters={
                               'start_date': start_date,
                               'end_date': end_date,
                               'material': material,
                               'bill_no': bill_no,
                               'status': status,
                               'include_booking': include_booking
                           })


@app.route('/financial_details')
@login_required
def financial_details():
    type_filter = request.args.get('type', 'cash')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    client_query = request.args.get('client', '').strip()
    min_price = request.args.get('min_price', type=float)
    max_price = request.args.get('max_price', type=float)

    if not start_date: start_date = pk_today().strftime('%Y-%m-%d')
    if not end_date: end_date = pk_today().strftime('%Y-%m-%d')

    # Resolve client code to name if applicable
    if client_query and (
        client_query.lower().startswith('tmpc-') or
        client_query.lower().startswith('fbmcl-') or
        client_query.lower().startswith('fbm-') or
        client_query[0].isdigit()
    ):
         c = Client.query.filter(Client.code.ilike(f'%{client_query}%')).first()
         if c:
             client_query = c.name

    transactions = []

    if type_filter == 'cash':
        # 1. Payments
        q_pay = Payment.query.filter(func.date(Payment.date_posted) >= start_date,
                                   func.date(Payment.date_posted) <= end_date, Payment.is_void == False)
        if client_query:
            q_pay = q_pay.filter(Payment.client_name.ilike(f'%{client_query}%'))
        if min_price is not None: q_pay = q_pay.filter(Payment.amount >= min_price)
        if max_price is not None: q_pay = q_pay.filter(Payment.amount <= max_price)

        for p in q_pay.all():
            transactions.append({
                'date': p.date_posted,
                'client': p.client_name,
                'amount': p.amount,
                'type': 'Payment',
                'ref': p.manual_bill_no or p.auto_bill_no or f'PAY-{p.id}'
            })

        # 2. Booking Advances
        q_book = Booking.query.filter(func.date(Booking.date_posted) >= start_date,
                                    func.date(Booking.date_posted) <= end_date,
                                    Booking.paid_amount > 0, Booking.is_void == False)
        if client_query:
            q_book = q_book.filter(Booking.client_name.ilike(f'%{client_query}%'))
        if min_price is not None: q_book = q_book.filter(Booking.paid_amount >= min_price)
        if max_price is not None: q_book = q_book.filter(Booking.paid_amount <= max_price)

        for b in q_book.all():
            transactions.append({
                'date': b.date_posted,
                'client': b.client_name,
                'amount': b.paid_amount,
                'type': 'Booking Advance',
                'ref': b.manual_bill_no or b.auto_bill_no or f'BK-{b.id}'
            })

        # 3. Direct Sale Cash
        q_sale = DirectSale.query.filter(func.date(DirectSale.date_posted) >= start_date,
                                       func.date(DirectSale.date_posted) <= end_date,
                                       DirectSale.paid_amount > 0, DirectSale.is_void == False)
        if client_query:
            q_sale = q_sale.filter(DirectSale.client_name.ilike(f'%{client_query}%'))
        if min_price is not None: q_sale = q_sale.filter(DirectSale.paid_amount >= min_price)
        if max_price is not None: q_sale = q_sale.filter(DirectSale.paid_amount <= max_price)

        for s in q_sale.all():
            transactions.append({
                'date': s.date_posted,
                'client': s.client_name,
                'amount': s.paid_amount,
                'type': 'Direct Sale',
                'ref': s.manual_bill_no or s.auto_bill_no or f'DS-{s.id}'
            })

    elif type_filter == 'credit':
        # 1. Booking Credit
        q_book = Booking.query.filter(func.date(Booking.date_posted) >= start_date,
                                    func.date(Booking.date_posted) <= end_date,
                                    (Booking.amount - Booking.paid_amount) > 0, Booking.is_void == False)
        if client_query:
            q_book = q_book.filter(Booking.client_name.ilike(f'%{client_query}%'))

        for b in q_book.all():
            credit = b.amount - b.paid_amount
            if min_price is not None and credit < min_price: continue
            if max_price is not None and credit > max_price: continue
            transactions.append({
                'date': b.date_posted,
                'client': b.client_name,
                'amount': credit,
                'type': 'Booking Credit',
                'ref': b.manual_bill_no or b.auto_bill_no or f'BK-{b.id}'
            })

        # 2. Direct Sale Credit
        q_sale = DirectSale.query.filter(func.date(DirectSale.date_posted) >= start_date,
                                       func.date(DirectSale.date_posted) <= end_date,
                                       (DirectSale.amount - DirectSale.paid_amount) > 0, DirectSale.is_void == False)
        if client_query:
            q_sale = q_sale.filter(DirectSale.client_name.ilike(f'%{client_query}%'))

        for s in q_sale.all():
            credit = s.amount - s.paid_amount
            if min_price is not None and credit < min_price: continue
            if max_price is not None and credit > max_price: continue
            transactions.append({
                'date': s.date_posted,
                'client': s.client_name,
                'amount': credit,
                'type': 'Direct Sale Credit',
                'ref': s.manual_bill_no or s.auto_bill_no or f'DS-{s.id}'
            })

    transactions.sort(key=lambda x: x['date'], reverse=True)

    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()

    return render_template('financial_details.html',
                           transactions=transactions,
                           type=type_filter,
                           start_date=start_date,
                           end_date=end_date,
                           client=client_query,
                           min_price=min_price,
                           max_price=max_price,
                           clients=clients,
                           materials=materials)


@app.route('/profit_reports')
@login_required
def profit_reports():
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()
    material_query = request.args.get('material', '').strip()
    client_query = request.args.get('client', '').strip()
    entry_metric = (request.args.get('metric') or '').strip().lower()
    view_mode = (request.args.get('view') or '').strip().lower()
    template_name = 'profit_entries.html' if view_mode == 'entries' else 'profit_reports.html'

    today_str = pk_today().strftime('%Y-%m-%d')
    month_start_str = pk_today().replace(day=1).strftime('%Y-%m-%d')
    if not start_date:
        start_date = month_start_str
    if not end_date:
        end_date = today_str

    def _norm_text(value):
        value = (value or '').strip().lower()
        return ' '.join(value.split())

    def _safe_parse_date(value):
        try:
            return datetime.strptime(value, '%Y-%m-%d').date()
        except Exception:
            return None

    start_dt = _safe_parse_date(start_date)
    end_dt = _safe_parse_date(end_date)

    if not start_dt or not end_dt:
        flash('Invalid date format. Please use YYYY-MM-DD.', 'danger')
        return redirect(url_for('profit_reports'))

    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
        start_date = start_dt.strftime('%Y-%m-%d')
        end_date = end_dt.strftime('%Y-%m-%d')

    resolved_client = client_query
    if client_query:
        code_match = Client.query.filter(Client.code.ilike(f'%{client_query}%')).first()
        if code_match:
            resolved_client = code_match.name

    purchase_query = db.session.query(GRNItem, GRN).join(GRN, GRNItem.grn_id == GRN.id).filter(
        GRN.is_void == False,
        func.date(GRN.date_posted) <= end_date
    )
    if material_query:
        purchase_query = purchase_query.filter(GRNItem.mat_name.ilike(f'%{material_query}%'))

    purchase_index = {}
    material_unit_cost_index = {}
    for item, grn in purchase_query.all():
        mat_name = (item.mat_name or '').strip()
        if not mat_name:
            continue
        mat_key = _norm_text(mat_name)
        posted_dt = grn.date_posted or datetime.min
        rate = float(item.price_at_time or 0)
        purchase_index.setdefault(mat_key, []).append((posted_dt, rate))

    for key in purchase_index:
        purchase_index[key].sort(key=lambda x: x[0])

    for m in Material.query.with_entities(Material.name, Material.unit_price).all():
        mk = _norm_text(m.name)
        if mk and float(m.unit_price or 0) > 0:
            material_unit_cost_index[mk] = float(m.unit_price or 0)

    def _cost_rate_for_material(mat_name, tx_dt):
        mat_key = _norm_text(mat_name)
        if not mat_key:
            return 0.0, False
        series = purchase_index.get(mat_key, [])
        if not series:
            fallback_rate = float(material_unit_cost_index.get(mat_key, 0) or 0)
            return (fallback_rate, fallback_rate > 0)
        for posted_dt, rate in reversed(series):
            if tx_dt and posted_dt and posted_dt.date() <= tx_dt:
                return float(rate or 0), True
        return float(series[-1][1] or 0), True

    def _bill_ref_variants(ref_value):
        ref = (ref_value or '').strip()
        if not ref:
            return set()
        variants = {ref}
        if ref.startswith('#') and len(ref) > 1:
            variants.add(ref[1:])
        elif ref.isdigit():
            variants.add(f"#{ref}")
        return {v.strip().lower() for v in variants if v}

    def _add_waive_to_map(target_map, bill_ref, amount):
        amt = float(amount or 0)
        if amt <= 0:
            return
        variants = _bill_ref_variants(bill_ref)
        if not variants:
            return
        for k in variants:
            target_map[k] = target_map.get(k, 0.0) + amt

    # Build waive-off(loss) index by bill reference.
    waive_off_by_bill = {}
    waive_query = WaiveOff.query.filter(
        WaiveOff.is_void == False,
        func.date(WaiveOff.date_posted) >= start_date,
        func.date(WaiveOff.date_posted) <= end_date
    )
    # DirectSale discounts are handled from DirectSale rows; avoid double counting.
    waive_query = waive_query.filter(
        ~func.lower(func.coalesce(WaiveOff.note, '')).like('[direct_sale_discount:%')
    )
    # Ignore orphan rows that reference a deleted payment.
    waive_query = waive_query.filter(
        or_(
            WaiveOff.payment_id.is_(None),
            exists().where(and_(Payment.id == WaiveOff.payment_id, Payment.tenant_id == WaiveOff.tenant_id))
        )
    )
    if resolved_client:
        waive_query = waive_query.filter(WaiveOff.client_name.ilike(f'%{resolved_client}%'))
    waive_rows = waive_query.all()
    represented_payment_ids = set()
    waive_events = []
    for w in waive_rows:
        if w.payment_id:
            represented_payment_ids.add(w.payment_id)
        amount = float(w.amount or 0)
        _add_waive_to_map(waive_off_by_bill, w.bill_no, amount)
        waive_events.append({
            'client_norm': _norm_text(w.client_name),
            'ref_variants': _bill_ref_variants(w.bill_no),
            'amount': amount
        })

    # Legacy fallback: Payment.discount rows not represented in waive_off table.
    legacy_discount_q = Payment.query.filter(Payment.is_void == False, Payment.discount > 0)
    legacy_discount_q = legacy_discount_q.filter(
        func.date(Payment.date_posted) >= start_date,
        func.date(Payment.date_posted) <= end_date
    )
    if resolved_client:
        legacy_discount_q = legacy_discount_q.filter(Payment.client_name.ilike(f'%{resolved_client}%'))
    for p in legacy_discount_q.all():
        if p.id in represented_payment_ids:
            continue
        bill_ref = p.manual_bill_no or p.auto_bill_no or f"PAY-{p.id}"
        amount = float(p.discount or 0)
        _add_waive_to_map(waive_off_by_bill, bill_ref, amount)
        waive_events.append({
            'client_norm': _norm_text(p.client_name),
            'ref_variants': _bill_ref_variants(bill_ref),
            'amount': amount
        })

    def _waive_for_bill(ref_value):
        total = 0.0
        for k in _bill_ref_variants(ref_value):
            total += float(waive_off_by_bill.get(k, 0.0) or 0.0)
        return total

    transactions = []

    booking_query = db.session.query(BookingItem, Booking).join(
        Booking, BookingItem.booking_id == Booking.id
    ).filter(
        Booking.is_void == False,
        func.date(Booking.date_posted) >= start_date,
        func.date(Booking.date_posted) <= end_date
    )
    if resolved_client:
        booking_query = booking_query.filter(Booking.client_name.ilike(f'%{resolved_client}%'))
    if material_query:
        booking_query = booking_query.filter(BookingItem.material_name.ilike(f'%{material_query}%'))

    booking_rows = booking_query.all()
    booking_gross_map = {}
    booking_waive_map = {}
    for item, booking in booking_rows:
        item_value = float(item.qty or 0) * float(item.price_at_time or 0)
        booking_gross_map[booking.id] = booking_gross_map.get(booking.id, 0) + item_value
        if booking.id not in booking_waive_map:
            booking_ref = booking.manual_bill_no or booking.auto_bill_no or f"BK-{booking.id}"
            booking_waive_map[booking.id] = _waive_for_bill(booking_ref)

    booking_gross_total = 0.0
    for item, booking in booking_rows:
        qty = float(item.qty or 0)
        sale_rate = float(item.price_at_time or 0)
        gross_revenue = qty * sale_rate
        booking_gross_total += gross_revenue
        booking_gross = float(booking_gross_map.get(booking.id, 0) or 0)
        booking_discount = float(booking.discount or 0)
        booking_waive = float(booking_waive_map.get(booking.id, 0) or 0)
        total_adjustment = booking_discount + booking_waive
        discount_share = (gross_revenue / booking_gross) * total_adjustment if booking_gross > 0 else 0
        net_revenue = max(0.0, gross_revenue - discount_share)

        tx_date = booking.date_posted.date() if booking.date_posted else None
        cost_rate, cogs_known = _cost_rate_for_material(item.material_name, tx_date)
        cogs = qty * cost_rate
        profit = net_revenue - cogs

        transactions.append({
            'date': booking.date_posted,
            'source': 'Booking',
            'reference': booking.manual_bill_no or booking.auto_bill_no or f'BK-{booking.id}',
            'client': booking.client_name,
            '_client_norm': _norm_text(booking.client_name),
            '_ref_variants': _bill_ref_variants(booking.manual_bill_no or booking.auto_bill_no or f'BK-{booking.id}'),
            'material': item.material_name,
            'qty': qty,
            'sale_rate': sale_rate,
            'cost_rate': cost_rate,
            'discount_loss': discount_share,
            'revenue': net_revenue,
            'cogs': cogs,
            'profit': profit,
            'is_loss': profit < 0,
            'cogs_known': cogs_known
        })

    direct_query = db.session.query(DirectSaleItem, DirectSale).join(
        DirectSale, DirectSaleItem.sale_id == DirectSale.id
    ).filter(
        DirectSale.is_void == False,
        func.date(DirectSale.date_posted) >= start_date,
        func.date(DirectSale.date_posted) <= end_date
    )
    if resolved_client:
        direct_query = direct_query.filter(DirectSale.client_name.ilike(f'%{resolved_client}%'))
    if material_query:
        direct_query = direct_query.filter(DirectSaleItem.product_name.ilike(f'%{material_query}%'))

    direct_rows = direct_query.all()
    sale_gross_map = {}
    sale_waive_map = {}
    for item, sale in direct_rows:
        sale_category = normalize_sale_category(getattr(sale, 'category', None))
        # Booking-delivery direct sale rows are fulfillment entries for existing bookings.
        # Profit is already recognized on booking lines, so exclude these from direct-sale P/L.
        if sale_category == 'Booking Delivery':
            continue
        if float(item.price_at_time or 0) <= 0:
            continue
        item_value = float(item.qty or 0) * float(item.price_at_time or 0)
        sale_gross_map[sale.id] = sale_gross_map.get(sale.id, 0) + item_value
        if sale.id not in sale_waive_map:
            sale_ref = sale.manual_bill_no or sale.auto_bill_no or f"DS-{sale.id}"
            if getattr(sale, 'invoice', None) and sale.invoice and sale.invoice.invoice_no:
                sale_ref = sale.invoice.invoice_no
            sale_waive_map[sale.id] = _waive_for_bill(sale_ref)

    direct_gross_total = 0.0
    for item, sale in direct_rows:
        sale_category = normalize_sale_category(getattr(sale, 'category', None))
        if sale_category == 'Booking Delivery':
            continue
        qty = float(item.qty or 0)
        sale_rate = float(item.price_at_time or 0)
        if sale_rate <= 0:
            continue
        gross_revenue = qty * sale_rate
        direct_gross_total += gross_revenue
        sale_gross = float(sale_gross_map.get(sale.id, 0) or 0)
        sale_discount = float(sale.discount or 0)
        sale_waive = float(sale_waive_map.get(sale.id, 0) or 0)
        total_adjustment = sale_discount + sale_waive
        discount_share = (gross_revenue / sale_gross) * total_adjustment if sale_gross > 0 else 0
        net_revenue = max(0.0, gross_revenue - discount_share)

        tx_date = sale.date_posted.date() if sale.date_posted else None
        cost_rate, cogs_known = _cost_rate_for_material(item.product_name, tx_date)
        cogs = qty * cost_rate
        profit = net_revenue - cogs

        transactions.append({
            'date': sale.date_posted,
            'source': 'Direct Sale',
            'reference': sale.manual_bill_no or sale.auto_bill_no or f'DS-{sale.id}',
            'client': sale.client_name,
            '_client_norm': _norm_text(sale.client_name),
            '_ref_variants': _bill_ref_variants(sale.manual_bill_no or sale.auto_bill_no or f'DS-{sale.id}'),
            'material': item.product_name,
            'qty': qty,
            'sale_rate': sale_rate,
            'cost_rate': cost_rate,
            'discount_loss': discount_share,
            'revenue': net_revenue,
            'cogs': cogs,
            'profit': profit,
            'is_loss': profit < 0,
            'cogs_known': cogs_known
        })

    # Delivery-rent variance:
    # - Positive difference (client rent charged > delivery rent cost) => company profit
    # - Negative difference (delivery rent cost > client rent charged) => company loss
    # This adjustment is operational and must not affect client ledger due.
    include_rent_variance = (not material_query) or _is_rent_material_name(material_query)
    if include_rent_variance:
        rent_loss_query = DirectSale.query.filter(
            DirectSale.is_void == False,
            func.date(DirectSale.date_posted) >= start_date,
            func.date(DirectSale.date_posted) <= end_date
        )
        if resolved_client:
            rent_loss_query = rent_loss_query.filter(DirectSale.client_name.ilike(f'%{resolved_client}%'))
        for sale in rent_loss_query.all():
            sale_items_payload = [
                {
                    'product_name': it.product_name,
                    'qty': it.qty,
                    'price_at_time': it.price_at_time
                }
                for it in (sale.items or [])
            ]
            fallback_rent_row = DeliveryRent.query.filter_by(sale_id=sale.id, is_void=False).order_by(DeliveryRent.id.desc()).first()
            fallback_delivery_cost = float(fallback_rent_row.amount or 0) if fallback_rent_row else 0.0
            effective_delivery_cost = float(getattr(sale, 'delivery_rent_cost', 0) or 0)
            if effective_delivery_cost <= 0:
                effective_delivery_cost = fallback_delivery_cost

            rent_rec = _rent_reconciliation_from_items(
                sale_items_payload,
                delivery_rent_cost=effective_delivery_cost,
                client_name=sale.client_name
            )
            rent_revenue = float(getattr(sale, 'rent_item_revenue', 0) or rent_rec['rent_item_revenue'])
            rent_cost = float(getattr(sale, 'delivery_rent_cost', 0) or rent_rec['delivery_rent_cost'])
            variance = rent_revenue - rent_cost
            if abs(variance) <= 0.0001:
                continue

            sale_ref = sale.manual_bill_no or sale.auto_bill_no or f"DS-{sale.id}"
            if getattr(sale, 'invoice', None) and sale.invoice and sale.invoice.invoice_no:
                sale_ref = sale.invoice.invoice_no

            transactions.append({
                'date': sale.date_posted,
                'source': ('Delivery Rent Variance (Company Profit)' if variance > 0 else 'Delivery Rent Variance (Company Loss)'),
                'reference': sale_ref,
                'client': sale.client_name,
                '_client_norm': _norm_text(sale.client_name),
                '_ref_variants': _bill_ref_variants(sale_ref),
                'material': 'Delivery Rent Difference',
                'qty': 0.0,
                'sale_rate': 0.0,
                'cost_rate': 0.0,
                'discount_loss': (abs(variance) if variance < 0 else 0.0),
                'revenue': 0.0,
                'cogs': 0.0,
                'profit': variance,
                'is_loss': variance < 0,
                'cogs_known': True
            })

    # Allocate waive-off events that are not directly bill-linked to any transaction row.
    matched_event_idx = set()
    for idx, ev in enumerate(waive_events):
        ev_refs = ev.get('ref_variants') or set()
        ev_client = ev.get('client_norm') or ''
        if not ev_refs:
            continue
        for tx in transactions:
            tx_refs = tx.get('_ref_variants') or set()
            tx_client = tx.get('_client_norm') or ''
            if ev_client and tx_client and ev_client != tx_client:
                continue
            if ev_refs & tx_refs:
                matched_event_idx.add(idx)
                break

    unallocated_by_client = {}
    for idx, ev in enumerate(waive_events):
        if idx in matched_event_idx:
            continue
        ckey = ev.get('client_norm') or ''
        if not ckey:
            continue
        unallocated_by_client[ckey] = unallocated_by_client.get(ckey, 0.0) + float(ev.get('amount') or 0.0)

    for ckey, amount in unallocated_by_client.items():
        if amount <= 0:
            continue
        candidates = [t for t in transactions if (t.get('_client_norm') == ckey and float(t.get('revenue') or 0) > 0)]
        if not candidates:
            # No sale row to allocate against: record standalone loss so it is visible in P/L.
            transactions.append({
                'date': datetime.combine(end_dt, datetime.min.time()),
                'source': 'Waive-Off',
                'reference': f'LOSS-{ckey[:10].upper() or "UNLINKED"}',
                'client': ckey or 'Unlinked',
                'material': '-',
                'qty': 0.0,
                'sale_rate': 0.0,
                'cost_rate': 0.0,
                'discount_loss': amount,
                'revenue': 0.0,
                'cogs': 0.0,
                'profit': -float(amount or 0),
                'is_loss': True,
                'cogs_known': True
            })
            continue
        total_rev = sum(float(t.get('revenue') or 0) for t in candidates)
        if total_rev <= 0:
            transactions.append({
                'date': datetime.combine(end_dt, datetime.min.time()),
                'source': 'Waive-Off',
                'reference': f'LOSS-{ckey[:10].upper() or "UNLINKED"}',
                'client': ckey or 'Unlinked',
                'material': '-',
                'qty': 0.0,
                'sale_rate': 0.0,
                'cost_rate': 0.0,
                'discount_loss': amount,
                'revenue': 0.0,
                'cogs': 0.0,
                'profit': -float(amount or 0),
                'is_loss': True,
                'cogs_known': True
            })
            continue
        allocated = 0.0
        for i, t in enumerate(candidates):
            if i == len(candidates) - 1:
                share = amount - allocated
            else:
                share = (float(t.get('revenue') or 0) / total_rev) * amount
                allocated += share
            t['revenue'] = max(0.0, float(t.get('revenue') or 0) - share)
            t['discount_loss'] = float(t.get('discount_loss') or 0) + float(share or 0)
            t['profit'] = float(t.get('revenue') or 0) - float(t.get('cogs') or 0)
            t['is_loss'] = bool(t.get('cogs_known')) and (float(t.get('profit') or 0) < 0)

    # Remove internal helper keys before rendering.
    for t in transactions:
        t.pop('_client_norm', None)
        t.pop('_ref_variants', None)

    transactions.sort(key=lambda x: x['date'] or datetime.min, reverse=True)

    metric_label_map = {
        'revenue': 'Revenue Rows',
        'discount_loss': 'Discount/Loss Rows',
        'cogs': 'Known COGS Rows',
        'net_profit': 'Net Profit/Loss Rows',
        'unknown_cost': 'Unknown Cost Rows',
    }
    metric_help_map = {
        'revenue': 'Shows rows where revenue > 0 from Booking and Direct Sale items.',
        'discount_loss': 'Shows all rows where discount/loss > 0 including bill discounts, waive-off losses, and delivery-rent variance loss.',
        'cogs': 'Shows rows with known material cost used in Estimated COGS.',
        'net_profit': 'Shows rows that contribute to net profit/loss where cost is known (profit = revenue - cogs).',
        'unknown_cost': 'Shows rows with unknown cost (N/A cost), excluded from net profit known-cost calculation.',
    }
    metric_filter_map = {
        'revenue': lambda t: float(t.get('revenue') or 0) > 0,
        'discount_loss': lambda t: float(t.get('discount_loss') or 0) > 0,
        'cogs': lambda t: bool(t.get('cogs_known')) and float(t.get('cogs') or 0) > 0,
        'net_profit': lambda t: bool(t.get('cogs_known')),
        'unknown_cost': lambda t: not bool(t.get('cogs_known')),
    }

    entries_transactions = transactions
    entry_metric_label = ''
    entry_metric_help = ''
    if entry_metric in metric_filter_map:
        entries_transactions = [t for t in transactions if metric_filter_map[entry_metric](t)]
        entry_metric_label = metric_label_map.get(entry_metric, '')
        entry_metric_help = metric_help_map.get(entry_metric, '')

    total_revenue = sum(float(t.get('revenue') or 0) for t in transactions)
    total_discount_loss = sum(float(t.get('discount_loss') or 0) for t in transactions)
    total_cogs = sum(float(t.get('cogs') or 0) for t in transactions if t.get('cogs_known'))
    known_cost_revenue = sum(float(t.get('revenue') or 0) for t in transactions if t.get('cogs_known'))
    unknown_cost_revenue = max(0.0, total_revenue - known_cost_revenue)
    total_profit = sum(float(t.get('profit') or 0) for t in transactions if t.get('cogs_known'))
    unknown_cost_rows = sum(1 for t in transactions if not t.get('cogs_known'))
    profit_rows = sum(1 for t in transactions if t.get('cogs_known') and float(t.get('profit') or 0) >= 0)
    loss_rows = sum(1 for t in transactions if t.get('cogs_known') and float(t.get('profit') or 0) < 0)
    margin_pct = (total_profit / known_cost_revenue * 100.0) if known_cost_revenue > 0 else 0.0
    markup_pct = (total_profit / total_cogs * 100.0) if total_cogs > 0 else 0.0

    # -------------------- Operational position (date-range level) --------------------
    # 1) Purchase side: GRN qty/value within selected date window.
    purchase_period_query = db.session.query(
        func.sum(GRNItem.qty),
        func.sum(GRNItem.qty * GRNItem.price_at_time)
    ).join(GRN, GRNItem.grn_id == GRN.id).filter(
        GRN.is_void == False,
        func.date(GRN.date_posted) >= start_date,
        func.date(GRN.date_posted) <= end_date
    )
    if material_query:
        purchase_period_query = purchase_period_query.filter(GRNItem.mat_name.ilike(f'%{material_query}%'))
    purchase_row = purchase_period_query.first() or (0, 0)
    purchase_qty = float(purchase_row[0] or 0)
    purchase_value = float(purchase_row[1] or 0)
    supplier_purchase_qty = purchase_qty
    supplier_purchase_amount = purchase_value

    purchase_material_breakdown = []
    purchase_material_query = db.session.query(
        GRNItem.mat_name,
        func.sum(GRNItem.qty),
        func.sum(GRNItem.qty * GRNItem.price_at_time)
    ).join(GRN, GRNItem.grn_id == GRN.id).filter(
        GRN.is_void == False,
        func.date(GRN.date_posted) >= start_date,
        func.date(GRN.date_posted) <= end_date
    )
    if material_query:
        purchase_material_query = purchase_material_query.filter(GRNItem.mat_name.ilike(f'%{material_query}%'))
    purchase_material_rows = purchase_material_query.group_by(GRNItem.mat_name).all()
    for mat_name, qty_sum, amt_sum in purchase_material_rows:
        purchase_material_breakdown.append({
            'material': (mat_name or '').strip() or '-',
            'qty': float(qty_sum or 0),
            'amount': float(amt_sum or 0),
        })
    purchase_material_breakdown.sort(key=lambda x: x.get('amount', 0), reverse=True)

    # Supplier credit/payable position in selected date window.
    supplier_credit_query = GRN.query.filter(
        GRN.is_void == False,
        func.date(GRN.date_posted) >= start_date,
        func.date(GRN.date_posted) <= end_date
    )
    if material_query:
        supplier_credit_query = supplier_credit_query.filter(
            GRN.items.any(GRNItem.mat_name.ilike(f'%{material_query}%'))
        )
    supplier_credit_total = 0.0
    for g in supplier_credit_query.all():
        supplier_credit_total += float(calculate_grn_total(g) or 0)

    supplier_paid_query = SupplierPayment.query.filter(
        SupplierPayment.is_void == False,
        func.date(SupplierPayment.date_posted) >= start_date,
        func.date(SupplierPayment.date_posted) <= end_date
    )
    supplier_paid_total = float(supplier_paid_query.with_entities(func.sum(SupplierPayment.amount)).scalar() or 0)
    supplier_net_payable = float(supplier_credit_total - supplier_paid_total)

    # 2) Delivery side (physical movement): OUT entries qty in the date window.
    delivery_qty_query = db.session.query(func.sum(Entry.qty)).filter(
        Entry.is_void == False,
        Entry.type == 'OUT',
        func.date(Entry.date) >= start_date,
        func.date(Entry.date) <= end_date
    )
    if resolved_client:
        delivery_qty_query = delivery_qty_query.filter(Entry.client.ilike(f'%{resolved_client}%'))
    if material_query:
        delivery_qty_query = delivery_qty_query.filter(Entry.material.ilike(f'%{material_query}%'))
    delivered_qty = float(delivery_qty_query.scalar() or 0)
    delivered_value = float(booking_gross_total + direct_gross_total)
    delivered_material_map = {}
    for t in transactions:
        mat = (t.get('material') or '').strip()
        if not mat or mat == '-':
            continue
        row = delivered_material_map.setdefault(mat, {'material': mat, 'qty': 0.0, 'amount': 0.0})
        row['qty'] += float(t.get('qty') or 0)
        row['amount'] += float(t.get('revenue') or 0)
    delivered_material_breakdown = sorted(delivered_material_map.values(), key=lambda x: x.get('amount', 0), reverse=True)

    # 3) Credit generated in selected period (booking + direct sale), prorated by filtered materials.
    credit_generated_booking = 0.0
    for item, booking in booking_rows:
        row_gross = float(item.qty or 0) * float(item.price_at_time or 0)
        bill_gross = float(booking_gross_map.get(booking.id, 0) or 0)
        if row_gross <= 0 or bill_gross <= 0:
            continue
        bill_credit = max(0.0, float(booking.amount or 0) - float(booking.discount or 0) - float(booking.paid_amount or 0))
        credit_generated_booking += (row_gross / bill_gross) * bill_credit

    credit_generated_sale = 0.0
    sale_paid_from_sales = 0.0
    for item, sale in direct_rows:
        sale_category = normalize_sale_category(getattr(sale, 'category', None))
        if sale_category == 'Booking Delivery':
            continue
        row_gross = float(item.qty or 0) * float(item.price_at_time or 0)
        if row_gross <= 0:
            continue
        sale_gross = float(sale_gross_map.get(sale.id, 0) or 0)
        if sale_gross <= 0:
            continue
        bill_credit = max(0.0, float(sale.amount or 0) - float(sale.discount or 0) - float(sale.paid_amount or 0))
        credit_generated_sale += (row_gross / sale_gross) * bill_credit
        sale_paid_from_sales += (row_gross / sale_gross) * float(sale.paid_amount or 0)

    credit_generated = float(credit_generated_booking + credit_generated_sale)

    # 4) Payments received in selected period.
    payment_received_only_q = Payment.query.filter(
        Payment.is_void == False,
        func.date(Payment.date_posted) >= start_date,
        func.date(Payment.date_posted) <= end_date
    )
    if resolved_client:
        payment_received_only_q = payment_received_only_q.filter(Payment.client_name.ilike(f'%{resolved_client}%'))
    payment_received_only = float(payment_received_only_q.with_entities(func.sum(Payment.amount)).scalar() or 0)

    booking_paid_collected = 0.0
    for item, booking in booking_rows:
        row_gross = float(item.qty or 0) * float(item.price_at_time or 0)
        bill_gross = float(booking_gross_map.get(booking.id, 0) or 0)
        if row_gross <= 0 or bill_gross <= 0:
            continue
        booking_paid_collected += (row_gross / bill_gross) * float(booking.paid_amount or 0)

    payment_received_total = float(payment_received_only + booking_paid_collected + sale_paid_from_sales)
    client_cash_received = payment_received_total
    client_credit_generated = credit_generated
    net_credit_movement = float(credit_generated - payment_received_total)
    # Client bill should represent generated bill value (paid-at-bill-time + credit),
    # not plus later payment receipts (which would double count).
    client_total_bill = float(client_credit_generated + booking_paid_collected + sale_paid_from_sales)
    client_total_paid = float(booking_paid_collected + sale_paid_from_sales + payment_received_only)
    client_total_pending = float(max(0.0, client_credit_generated - payment_received_only))
    entries_count = len(entries_transactions if view_mode == 'entries' else transactions)

    # -------------------- Grouped summaries for report analysis --------------------
    def _safe_pct(num, den):
        if not den:
            return 0.0
        return (float(num or 0) / float(den or 0)) * 100.0

    material_summary_map = {}
    for row in purchase_material_breakdown:
        mat = (row.get('material') or '').strip() or '-'
        rec = material_summary_map.setdefault(mat, {
            'material': mat,
            'received_qty': 0.0,
            'received_amount': 0.0,
            'sold_qty': 0.0,
            'sold_revenue': 0.0,
            'discount_loss': 0.0,
            'cogs_known': 0.0,
            'profit_known': 0.0,
            'unknown_cost_qty': 0.0,
            'unknown_cost_revenue': 0.0,
            'known_rows': 0,
            'unknown_rows': 0,
        })
        rec['received_qty'] += float(row.get('qty') or 0)
        rec['received_amount'] += float(row.get('amount') or 0)

    date_summary_map = {}
    received_date_rows = db.session.query(
        func.date(GRN.date_posted),
        func.sum(GRNItem.qty),
        func.sum(GRNItem.qty * GRNItem.price_at_time)
    ).join(GRN, GRNItem.grn_id == GRN.id).filter(
        GRN.is_void == False,
        func.date(GRN.date_posted) >= start_date,
        func.date(GRN.date_posted) <= end_date
    )
    if material_query:
        received_date_rows = received_date_rows.filter(GRNItem.mat_name.ilike(f'%{material_query}%'))
    received_date_rows = received_date_rows.group_by(func.date(GRN.date_posted)).all()
    for day_key, qty_sum, amt_sum in received_date_rows:
        dkey = str(day_key or '')
        row = date_summary_map.setdefault(dkey, {
            'date': dkey,
            'received_qty': 0.0,
            'received_amount': 0.0,
            'sold_qty': 0.0,
            'sold_revenue': 0.0,
            'discount_loss': 0.0,
            'cogs_known': 0.0,
            'profit_known': 0.0,
            'unknown_cost_revenue': 0.0,
            'known_rows': 0,
            'unknown_rows': 0,
        })
        row['received_qty'] += float(qty_sum or 0)
        row['received_amount'] += float(amt_sum or 0)

    client_summary_map = {}
    client_material_map = {}
    for t in transactions:
        mat = (t.get('material') or '').strip() or '-'
        client_name = (t.get('client') or '').strip() or '-'
        qty = float(t.get('qty') or 0)
        rev = float(t.get('revenue') or 0)
        disc = float(t.get('discount_loss') or 0)
        cogs = float(t.get('cogs') or 0)
        prof = float(t.get('profit') or 0)
        cogs_known_flag = bool(t.get('cogs_known'))
        day_val = t.get('date')
        day_key = day_val.strftime('%Y-%m-%d') if day_val else ''

        mrow = material_summary_map.setdefault(mat, {
            'material': mat,
            'received_qty': 0.0,
            'received_amount': 0.0,
            'sold_qty': 0.0,
            'sold_revenue': 0.0,
            'discount_loss': 0.0,
            'cogs_known': 0.0,
            'profit_known': 0.0,
            'unknown_cost_qty': 0.0,
            'unknown_cost_revenue': 0.0,
            'known_rows': 0,
            'unknown_rows': 0,
        })
        mrow['sold_qty'] += qty
        mrow['sold_revenue'] += rev
        mrow['discount_loss'] += disc
        if cogs_known_flag:
            mrow['cogs_known'] += cogs
            mrow['profit_known'] += prof
            mrow['known_rows'] += 1
        else:
            mrow['unknown_cost_qty'] += qty
            mrow['unknown_cost_revenue'] += rev
            mrow['unknown_rows'] += 1

        drow = date_summary_map.setdefault(day_key, {
            'date': day_key,
            'received_qty': 0.0,
            'received_amount': 0.0,
            'sold_qty': 0.0,
            'sold_revenue': 0.0,
            'discount_loss': 0.0,
            'cogs_known': 0.0,
            'profit_known': 0.0,
            'unknown_cost_revenue': 0.0,
            'known_rows': 0,
            'unknown_rows': 0,
        })
        drow['sold_qty'] += qty
        drow['sold_revenue'] += rev
        drow['discount_loss'] += disc
        if cogs_known_flag:
            drow['cogs_known'] += cogs
            drow['profit_known'] += prof
            drow['known_rows'] += 1
        else:
            drow['unknown_cost_revenue'] += rev
            drow['unknown_rows'] += 1

        crow = client_summary_map.setdefault(client_name, {
            'client': client_name,
            'sold_qty': 0.0,
            'sold_revenue': 0.0,
            'discount_loss': 0.0,
            'cogs_known': 0.0,
            'profit_known': 0.0,
            'unknown_cost_revenue': 0.0,
            'known_rows': 0,
            'unknown_rows': 0,
        })
        crow['sold_qty'] += qty
        crow['sold_revenue'] += rev
        crow['discount_loss'] += disc
        if cogs_known_flag:
            crow['cogs_known'] += cogs
            crow['profit_known'] += prof
            crow['known_rows'] += 1
        else:
            crow['unknown_cost_revenue'] += rev
            crow['unknown_rows'] += 1

        cm_key = (client_name, mat)
        cmrow = client_material_map.setdefault(cm_key, {
            'client': client_name,
            'material': mat,
            'sold_qty': 0.0,
            'sold_revenue': 0.0,
            'discount_loss': 0.0,
            'cogs_known': 0.0,
            'profit_known': 0.0,
            'unknown_cost_revenue': 0.0,
            'known_rows': 0,
            'unknown_rows': 0,
        })
        cmrow['sold_qty'] += qty
        cmrow['sold_revenue'] += rev
        cmrow['discount_loss'] += disc
        if cogs_known_flag:
            cmrow['cogs_known'] += cogs
            cmrow['profit_known'] += prof
            cmrow['known_rows'] += 1
        else:
            cmrow['unknown_cost_revenue'] += rev
            cmrow['unknown_rows'] += 1

    material_summary = sorted(material_summary_map.values(), key=lambda x: (x.get('sold_revenue', 0), x.get('received_amount', 0)), reverse=True)
    date_summary = sorted([v for v in date_summary_map.values() if v.get('date')], key=lambda x: x.get('date', ''), reverse=True)
    client_summary = sorted(client_summary_map.values(), key=lambda x: x.get('sold_revenue', 0), reverse=True)
    client_material_summary = sorted(client_material_map.values(), key=lambda x: (x.get('client', ''), -x.get('sold_revenue', 0)))

    for row in material_summary:
        known_rev = max(0.0, float(row.get('sold_revenue', 0) - row.get('unknown_cost_revenue', 0)))
        row['margin_pct_known'] = _safe_pct(row.get('profit_known', 0), known_rev)
        row['markup_pct_known'] = _safe_pct(row.get('profit_known', 0), row.get('cogs_known', 0))
    for row in date_summary:
        known_rev = max(0.0, float(row.get('sold_revenue', 0) - row.get('unknown_cost_revenue', 0)))
        row['margin_pct_known'] = _safe_pct(row.get('profit_known', 0), known_rev)
    for row in client_summary:
        known_rev = max(0.0, float(row.get('sold_revenue', 0) - row.get('unknown_cost_revenue', 0)))
        row['margin_pct_known'] = _safe_pct(row.get('profit_known', 0), known_rev)
    for row in client_material_summary:
        known_rev = max(0.0, float(row.get('sold_revenue', 0) - row.get('unknown_cost_revenue', 0)))
        row['margin_pct_known'] = _safe_pct(row.get('profit_known', 0), known_rev)

    missing_cost_materials = [
        {
            'material': r.get('material'),
            'unknown_rows': int(r.get('unknown_rows', 0) or 0),
            'unknown_qty': float(r.get('unknown_cost_qty', 0) or 0),
            'unknown_revenue': float(r.get('unknown_cost_revenue', 0) or 0),
        }
        for r in material_summary if float(r.get('unknown_cost_revenue', 0) or 0) > 0
    ]

    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()

    return render_template(
        template_name,
        transactions=(entries_transactions if view_mode == 'entries' else transactions),
        start_date=start_date,
        end_date=end_date,
        material=material_query,
        client=client_query,
        clients=clients,
        materials=materials,
        total_revenue=total_revenue,
        total_discount_loss=total_discount_loss,
        total_cogs=total_cogs,
        total_profit=total_profit,
        margin_pct=margin_pct,
        markup_pct=markup_pct,
        profit_rows=profit_rows,
        loss_rows=loss_rows,
        known_cost_revenue=known_cost_revenue,
        unknown_cost_revenue=unknown_cost_revenue,
        unknown_cost_rows=unknown_cost_rows,
        purchase_qty=purchase_qty,
        purchase_value=purchase_value,
        delivered_qty=delivered_qty,
        delivered_value=delivered_value,
        credit_generated=credit_generated,
        payment_received_total=payment_received_total,
        payment_received_only=payment_received_only,
        net_credit_movement=net_credit_movement,
        supplier_purchase_qty=supplier_purchase_qty,
        supplier_purchase_amount=supplier_purchase_amount,
        purchase_material_breakdown=purchase_material_breakdown,
        delivered_material_breakdown=delivered_material_breakdown,
        supplier_credit_total=supplier_credit_total,
        supplier_paid_total=supplier_paid_total,
        supplier_net_payable=supplier_net_payable,
        client_cash_received=client_cash_received,
        client_credit_generated=client_credit_generated,
        client_total_bill=client_total_bill,
        client_total_paid=client_total_paid,
        client_total_pending=client_total_pending,
        entries_count=entries_count,
        view_mode=view_mode,
        entry_metric=entry_metric,
        entry_metric_label=entry_metric_label,
        entry_metric_help=entry_metric_help
    )


# ==================== MAIN ROUTES ====================

@app.route('/')
@login_required
def index():
    if current_user.role == 'root':
        return redirect(url_for('tenants_dashboard'))
    today = pk_today().strftime('%B %d, %Y')
    today_date = pk_today()

    client_count = db.session.query(func.count(Client.id)).scalar() or 0

    # Re-query stats with is_void=False
    stats_query = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=0)).label('total_in'),
        func.sum(case((Entry.type == 'OUT', Entry.qty), else_=0)).label('total_out')
    ).filter(Entry.is_void == False).group_by(Entry.material).all()

    material_units = {
        (m.name or '').strip().lower(): (m.unit or 'Bags')
        for m in Material.query.with_entities(Material.name, Material.unit).all()
    }

    stats = sorted([{
        'name': row.material or "Unknown",
        'in': int(row.total_in or 0),
        'out': int(row.total_out or 0),
        'stock': int((row.total_in or 0) - (row.total_out or 0)),
        'unit': material_units.get(((row.material or '').strip().lower()), 'Bags')
    } for row in stats_query], key=lambda x: x['name'])

    total_stock = sum(s['stock'] for s in stats)

    # Daily Cash Calculation
    cash_payments = db.session.query(func.sum(Payment.amount)).filter(func.date(Payment.date_posted) == today_date, Payment.is_void == False).scalar() or 0
    cash_bookings = db.session.query(func.sum(Booking.paid_amount)).filter(func.date(Booking.date_posted) == today_date, Booking.is_void == False).scalar() or 0
    cash_sales = db.session.query(func.sum(DirectSale.paid_amount)).filter(func.date(DirectSale.date_posted) == today_date, DirectSale.is_void == False).scalar() or 0
    daily_cash = cash_payments + cash_bookings + cash_sales

    # Daily Credit Calculation
    credit_bookings = db.session.query(func.sum(Booking.amount - Booking.paid_amount)).filter(func.date(Booking.date_posted) == today_date, Booking.is_void == False).scalar() or 0
    credit_sales = db.session.query(func.sum(DirectSale.amount - DirectSale.paid_amount)).filter(func.date(DirectSale.date_posted) == today_date, DirectSale.is_void == False).scalar() or 0
    daily_credit = credit_bookings + credit_sales

    # Total Outstanding (Unpaid Bills) aligned with ledger net due
    open_pending = PendingBill.query.filter(
        PendingBill.is_paid == False,
        PendingBill.is_void == False
    ).all()
    effective_map = _compute_pending_effective_amount_map(open_pending)
    total_outstanding = sum(float(effective_map.get(pb.id, float(pb.amount or 0)) or 0) for pb in open_pending)

    # Daily Sales Breakdown
    sales_breakdown = {}

    # 1. Bookings
    booking_total = db.session.query(func.sum(Booking.amount)).filter(func.date(Booking.date_posted) == today_date, Booking.is_void == False).scalar() or 0
    if booking_total > 0:
        sales_breakdown['Bookings'] = booking_total

    # 2. Direct Sales
    ds_query = db.session.query(DirectSale.category, func.sum(DirectSale.amount))\
        .filter(func.date(DirectSale.date_posted) == today_date, DirectSale.is_void == False)\
        .group_by(DirectSale.category).all()

    for cat, amt in ds_query:
        if amt > 0:
            cat_name = normalize_sale_category(cat, default='Credit Customer')
            if cat_name == 'Credit Customer':
                cat_name = 'Credit Sales'
            elif cat_name == 'Cash':
                cat_name = 'Cash Sales'
            sales_breakdown[cat_name] = sales_breakdown.get(cat_name, 0) + amt

    sales_breakdown_list = [{'category': k, 'amount': v} for k, v in sales_breakdown.items()]
    sales_breakdown_list.sort(key=lambda x: x['amount'], reverse=True)

    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()

    return render_template('index.html',
                           today_date=today,
                           total_stock=int(total_stock),
                           client_count=client_count,
                           stats=stats,
                           daily_cash=daily_cash,
                           daily_credit=daily_credit,
                           total_outstanding=total_outstanding,
                           sales_breakdown=sales_breakdown_list,
                           clients=clients,
                           materials=materials)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = str(request.form.get('password') or '')
        remember = (request.form.get('remember_me') or '').lower() in ('1', 'true', 'on', 'yes')
        tenant_name = (request.form.get('tenant_name') or '').strip()
        user = None

        if username == os.environ.get('ROOT_USERNAME', 'root'):
            user = User.query.filter_by(username=username, tenant_id=None).first()
        else:
            if not tenant_name:
                flash('Tenant name required', 'danger')
                return render_template('login.html')
            tenant = Tenant.query.filter(func.lower(func.trim(Tenant.name)) == tenant_name.lower()).first()
            if not tenant:
                flash('Tenant not found', 'danger')
                return render_template('login.html')
            user = User.query.filter_by(username=username, tenant_id=tenant.id).first()
        if user and user.password_hash and check_password_hash(user.password_hash, password):
            if user.role != 'root' and user.status != 'active':
                flash('Account suspended', 'danger')
                return render_template('login.html')
            if user.role != 'root':
                if not user.tenant_id or not user.tenant or user.tenant.status != 'active':
                    flash('Tenant suspended', 'danger')
                    return render_template('login.html')
            login_user(user, remember=remember)
            session['tenant_id'] = user.tenant_id
            session['role'] = user.role
            next_url = request.args.get('next')
            return redirect(next_url or url_for('index'))
        flash('Invalid Credentials', 'danger')
    return render_template('login.html')


@app.route('/root/recovery', methods=['GET', 'POST'])
def root_recovery():
    root_username = os.environ.get('ROOT_USERNAME', 'root')
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        recovery_code = (request.form.get('recovery_code') or '').strip().upper()
        new_password = str(request.form.get('new_password') or '')
        confirm_password = str(request.form.get('confirm_password') or '')

        if username != root_username:
            flash('Recovery failed. Invalid credentials.', 'danger')
            return render_template('root_recovery.html', root_username=root_username)
        if not recovery_code or not new_password:
            flash('Recovery code and new password are required.', 'danger')
            return render_template('root_recovery.html', root_username=root_username)
        if len(new_password) < 8:
            flash('New password must be at least 8 characters.', 'danger')
            return render_template('root_recovery.html', root_username=root_username)
        if new_password != confirm_password:
            flash('Password confirmation does not match.', 'danger')
            return render_template('root_recovery.html', root_username=root_username)

        root_user = User.query.filter_by(username=root_username, tenant_id=None).first()
        if not root_user:
            flash('Root account not found.', 'danger')
            return render_template('root_recovery.html', root_username=root_username)

        hit = _consume_root_recovery_code(root_username, recovery_code)
        if not hit:
            flash('Invalid or already used recovery code.', 'danger')
            return render_template('root_recovery.html', root_username=root_username)

        root_user.password_hash = generate_password_hash(new_password)
        root_user.password_plain = None
        db.session.commit()
        flash('Root password reset successful. Please login with new password.', 'success')
        return redirect(url_for('login'))

    return render_template('root_recovery.html', root_username=root_username)


@app.route('/root/recovery_codes', methods=['GET', 'POST'])
@login_required
def root_recovery_codes():
    require_root()
    root_username = os.environ.get('ROOT_USERNAME', 'root')
    generated_codes = []

    if request.method == 'POST':
        action = (request.form.get('action') or '').strip().lower()
        if action == 'generate':
            note = (request.form.get('note') or '').strip()
            generated_codes = _create_root_recovery_codes(username=root_username, count=10, note=note)
            db.session.commit()
            audit_log(current_user, None, 'root.recovery_codes.generate', f'count=10 username={root_username}')
            flash('New offline recovery codes generated. Save them now; they will not be shown again.', 'success')
        elif action == 'revoke_unused':
            deleted = RootRecoveryCode.query.filter(
                RootRecoveryCode.username == root_username,
                RootRecoveryCode.used_at.is_(None)
            ).delete(synchronize_session=False)
            db.session.commit()
            audit_log(current_user, None, 'root.recovery_codes.revoke', f'deleted={deleted} username={root_username}')
            flash(f'Revoked {deleted} unused recovery codes.', 'warning')

    codes = RootRecoveryCode.query.filter(
        RootRecoveryCode.username == root_username
    ).order_by(RootRecoveryCode.created_at.desc()).all()
    unused_count = sum(1 for c in codes if c.used_at is None)
    used_count = sum(1 for c in codes if c.used_at is not None)

    return render_template(
        'root_recovery_codes.html',
        root_username=root_username,
        generated_codes=generated_codes,
        unused_count=unused_count,
        used_count=used_count,
        codes=codes
    )


@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.pop('tenant_id', None)
    session.pop('role', None)
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


# ==================== CLIENT ROUTES ====================

@app.route('/clients')
@login_required
def clients():
    search = request.args.get('search', '').strip()
    category = request.args.get('category', '').strip()
    category_normalized = category.lower()
    page_active = request.args.get('page_active', 1, type=int)
    page_inactive = request.args.get('page_inactive', 1, type=int)

    active_query = Client.query.filter(Client.is_active == True)
    if search:
        active_query = active_query.filter(
            db.or_(Client.name.ilike(f'%{search}%'), Client.code.ilike(f'%{search}%')))
    if category:
        active_query = active_query.filter(func.lower(func.trim(Client.category)) == category_normalized)
    active_pagination = active_query.order_by(Client.name.asc()).paginate(page=page_active, per_page=10)

    inactive_query = Client.query.filter(Client.is_active == False)
    if search:
        inactive_query = inactive_query.filter(
            db.or_(Client.name.ilike(f'%{search}%'), Client.code.ilike(f'%{search}%')))
    if category:
        inactive_query = inactive_query.filter(func.lower(func.trim(Client.category)) == category_normalized)
    inactive_pagination = inactive_query.order_by(Client.name.asc()).paginate(page=page_inactive, per_page=10)

    all_visible_clients = active_pagination.items + inactive_pagination.items
    for c in all_visible_clients:
        c.total_bills = db.session.query(func.count(PendingBill.id)).filter_by(client_code=c.code).scalar() or 0
        c.total_deliveries = db.session.query(func.sum(Entry.qty)).filter_by(client=c.name, type='OUT').scalar() or 0

    active_clients_list = Client.query.filter(Client.is_active == True).order_by(Client.name.asc()).all()
    categories = [
        row[0] for row in db.session.query(Client.category).distinct().filter(
            Client.category != None,
            func.trim(Client.category) != ''
        ).order_by(Client.category.asc()).all()
    ]
    for default_cat in ['General', 'Open Khata', 'Walking-Customer', 'Misc']:
        if default_cat not in categories:
            categories.append(default_cat)
    categories = sorted(categories, key=lambda x: str(x).lower())

    return render_template('clients.html',
                           active_pagination=active_pagination,
                           inactive_pagination=inactive_pagination,
                           search=search,
                           category=category,
                           active_clients=active_clients_list,
                           categories=categories)


@app.route('/add_client', methods=['POST'])
@login_required
def add_client():
    ensure_client_table_tenant_unique()
    name = request.form.get('name', '').strip()
    code = request.form.get('code', '').strip()
    if not name:
        flash('Client name is required', 'danger')
        return redirect(url_for('clients'))
    if not code:
        code = generate_client_code()
    if Client.query.filter_by(code=code).first():
        flash(f'Client code "{code}" already exists', 'danger')
        return redirect(url_for('clients'))
    category = request.form.get('category', 'General').strip() or 'General'
    opening_balance = _to_float_or_zero(request.form.get('opening_balance', 0))
    opening_balance_date = _resolve_opening_balance_date(request.form.get('opening_balance_date'))
    page_entries_raw = request.form.getlist('page_entry')
    page_entries_clean = [str(x).strip() for x in page_entries_raw if str(x).strip()]
    page_notes_value = ' | '.join(page_entries_clean) if page_entries_clean else (request.form.get('page_notes', '') or '').strip()

    new_c = Client(name=name,
                   code=code,
                   phone=request.form.get('phone', ''),
                   address=request.form.get('address', ''),
                   category=category,
                   book_no='',
                   financial_book_no='',
                   financial_page='',
                   cement_book_no='',
                   cement_page='',
                   steel_book_no='',
                   steel_page=page_notes_value,
                   location_url=(request.form.get('location_url', '') or '').strip(),
                   page_notes=page_notes_value,
                   opening_balance=opening_balance,
                   opening_balance_date=opening_balance_date)
    db.session.add(new_c)
    db.session.commit()
    flash('Client Registered', 'success')
    return redirect(url_for('clients'))


@app.route('/edit_client/<int:id>', methods=['POST'])
@login_required
def edit_client(id):
    c = db.session.get(Client, id)
    if c:
        old_code = c.code
        old_name = c.name
        new_code = request.form.get('code', '').strip()
        new_name = request.form.get('name', '').strip()

        if not new_code:
            flash('Client code is required', 'danger')
            return redirect(url_for('clients'))

        existing = Client.query.filter_by(code=new_code).first()
        if existing and existing.id != id:
            flash(f'Client code "{new_code}" already exists', 'danger')
            return redirect(url_for('clients'))

        if old_code != new_code or old_name != new_name:
            PendingBill.query.filter_by(tenant_id=current_user.tenant_id, client_code=old_code).update({
                'client_code': new_code,
                'client_name': new_name
            })
            Entry.query.filter_by(tenant_id=current_user.tenant_id, client_code=old_code).update({
                'client_code': new_code,
                'client': new_name
            })
            Entry.query.filter_by(tenant_id=current_user.tenant_id, client=old_name).update({'client': new_name})

            # Propagate name change to all related tables to prevent broken links
            Booking.query.filter_by(tenant_id=current_user.tenant_id, client_name=old_name).update({'client_name': new_name})
            DirectSale.query.filter_by(tenant_id=current_user.tenant_id, client_name=old_name).update({'client_name': new_name})
            Payment.query.filter_by(tenant_id=current_user.tenant_id, client_name=old_name).update({'client_name': new_name})
            WaiveOff.query.filter_by(tenant_id=current_user.tenant_id, client_name=old_name).update({
                'client_name': new_name,
                'client_code': new_code
            })
            Invoice.query.filter_by(tenant_id=current_user.tenant_id, client_name=old_name).update({'client_name': new_name})
            Invoice.query.filter_by(tenant_id=current_user.tenant_id, client_code=old_code).update({'client_code': new_code})

        page_entries_raw = request.form.getlist('page_entry')
        page_entries_clean = [str(x).strip() for x in page_entries_raw if str(x).strip()]
        page_notes_value = ' | '.join(page_entries_clean) if page_entries_clean else (request.form.get('page_notes', '') or '').strip()

        c.name = new_name
        c.code = new_code
        c.phone = request.form.get('phone', '')
        c.address = request.form.get('address', '')
        c.category = (request.form.get('category', 'General').strip() or 'General')
        c.book_no = ''
        c.financial_book_no = ''
        c.financial_page = ''
        c.cement_book_no = ''
        c.cement_page = ''
        c.steel_book_no = ''
        c.steel_page = page_notes_value
        c.location_url = (request.form.get('location_url', '') or '').strip()
        c.page_notes = page_notes_value
        c.opening_balance = _to_float_or_zero(request.form.get('opening_balance', c.opening_balance))
        c.opening_balance_date = _resolve_opening_balance_date(
            request.form.get('opening_balance_date'),
            fallback_dt=(c.opening_balance_date or c.created_at)
        )

        db.session.commit()
        flash('Client updated', 'success')
    return redirect(url_for('clients'))


@app.route('/client_opening_balance/<int:id>', methods=['POST'])
@login_required
def client_opening_balance(id):
    if not _user_can('can_manage_clients'):
        flash('Permission denied', 'danger')
        return redirect(url_for('client_ledger', id=id))
    c = db.session.get(Client, id)
    if not c:
        flash('Client not found', 'danger')
        return redirect(url_for('clients'))
    c.opening_balance = _to_float_or_zero(request.form.get('opening_balance', c.opening_balance))
    c.opening_balance_date = _resolve_opening_balance_date(
        request.form.get('opening_balance_date'),
        fallback_dt=(c.opening_balance_date or c.created_at)
    )
    db.session.commit()
    flash('Opening balance updated', 'success')
    return redirect(url_for('client_ledger', id=id))


@app.route('/delete_client/<int:id>', methods=['POST'])
@login_required
def delete_client(id):
    if not _user_can('can_manage_clients'):
        flash('Permission denied', 'danger')
        return redirect(url_for('clients'))
    c = db.session.get(Client, id)
    if c:
        c.is_active = False
        db.session.commit()
        flash('Client suspended', 'warning')
    return redirect(url_for('clients'))


@app.route('/clients/activate_all', methods=['POST'])
@login_required
def activate_all_clients():
    if not _user_can('can_manage_clients'):
        flash('Permission denied', 'danger')
        return redirect(url_for('clients'))
    count = Client.query.filter_by(is_active=False).update({'is_active': True}, synchronize_session=False)
    db.session.commit()
    flash(f'Activated {count} suspended clients.', 'success')
    return redirect(url_for('clients'))


@app.route('/transfer_client/<int:id>', methods=['POST'])
@login_required
def transfer_client(id):
    source_client = db.session.get(Client, id)
    target_client_id = request.form.get('target_client_id')
    if not source_client or not target_client_id:
        flash('Invalid transfer request', 'danger')
        return redirect(url_for('clients'))

    target_client = db.session.get(Client, int(target_client_id))
    if not target_client:
        flash('Target client not found', 'danger')
        return redirect(url_for('clients'))

    if target_client.id == source_client.id:
        flash('Cannot transfer to the same client', 'danger')
        return redirect(url_for('clients'))

    if not target_client.is_active:
        flash('Cannot transfer to an inactive client', 'danger')
        return redirect(url_for('clients'))

    entries_updated = Entry.query.filter_by(tenant_id=current_user.tenant_id, client_code=source_client.code).update({
        'client': target_client.name,
        'client_code': target_client.code
    })
    bills_updated = PendingBill.query.filter_by(tenant_id=current_user.tenant_id, client_code=source_client.code).update({
        'client_name': target_client.name,
        'client_code': target_client.code
    })
    waive_updated = WaiveOff.query.filter_by(tenant_id=current_user.tenant_id, client_code=source_client.code).update({
        'client_name': target_client.name,
        'client_code': target_client.code
    })

    source_client.is_active = False
    source_client.transferred_to_id = target_client.id
    db.session.commit()

    flash(f'Transferred {entries_updated} entries, {bills_updated} bills, and {waive_updated} waive-off rows.', 'success')
    return redirect(url_for('clients'))


@app.route('/reclaim_client/<int:id>', methods=['POST'])
@login_required
def reclaim_client(id):
    source_client = db.session.get(Client, id)
    if not source_client or source_client.is_active or not source_client.transferred_to_id:
        flash('Invalid reclaim request', 'danger')
        return redirect(url_for('clients', show_inactive=1))

    target_client = db.session.get(Client, source_client.transferred_to_id)
    if not target_client:
        flash('Target client not found', 'danger')
        return redirect(url_for('clients', show_inactive=1))

    source_client.is_active = True

    entries_reclaimed = Entry.query.filter_by(
        tenant_id=current_user.tenant_id,
        client_code=target_client.code, client=target_client.name).update({
            'client': source_client.name,
            'client_code': source_client.code
        })
    bills_reclaimed = PendingBill.query.filter_by(
        tenant_id=current_user.tenant_id,
        client_code=target_client.code, client_name=target_client.name).update({
            'client_name': source_client.name,
            'client_code': source_client.code
        })
    waive_reclaimed = WaiveOff.query.filter_by(
        tenant_id=current_user.tenant_id,
        client_code=target_client.code, client_name=target_client.name).update({
            'client_name': source_client.name,
            'client_code': source_client.code
        })

    source_client.transferred_to_id = None
    db.session.commit()

    flash(f'Reclaimed {entries_reclaimed} entries, {bills_reclaimed} bills, and {waive_reclaimed} waive-off rows.', 'success')
    return redirect(url_for('clients'))


# ==================== SUPPLIER ROUTES ====================

@app.route('/suppliers')
@login_required
def suppliers():
    suppliers_list = Supplier.query.order_by(Supplier.name.asc()).all()
    return render_template('suppliers.html', suppliers=suppliers_list)

@app.route('/add_supplier', methods=['POST'])
@login_required
def add_supplier():
    name = request.form.get('name', '').strip()
    if not name:
        flash('Supplier name is required', 'danger')
        return redirect(url_for('suppliers'))
    
    existing = Supplier.query.filter(func.lower(Supplier.name) == name.lower()).first()
    if existing:
        if request.args.get('ajax'):
            return jsonify({'success': True, 'id': existing.id, 'name': existing.name})
        flash('Supplier already exists', 'warning')
        return redirect(url_for('suppliers'))

    new_s = Supplier(
        name=name,
        phone=request.form.get('phone', ''),
        address=request.form.get('address', ''),
        opening_balance=_to_float_or_zero(request.form.get('opening_balance', 0)),
        opening_balance_date=_resolve_opening_balance_date(request.form.get('opening_balance_date')),
        is_active=True
    )
    db.session.add(new_s)
    db.session.commit()
    
    if request.args.get('ajax'):
        return jsonify({'success': True, 'id': new_s.id, 'name': new_s.name})
        
    flash('Supplier Added', 'success')
    return redirect(url_for('suppliers'))

@app.route('/edit_supplier/<int:id>', methods=['POST'])
@login_required
def edit_supplier(id):
    s = db.session.get(Supplier, id)
    if s:
        s.name = request.form.get('name', '').strip()
        s.phone = request.form.get('phone', '')
        s.address = request.form.get('address', '')
        s.opening_balance = _to_float_or_zero(request.form.get('opening_balance', s.opening_balance))
        s.opening_balance_date = _resolve_opening_balance_date(
            request.form.get('opening_balance_date'),
            fallback_dt=(s.opening_balance_date or s.created_at)
        )
        s.is_active = 'is_active' in request.form
        db.session.commit()
        flash('Supplier Updated', 'success')
    return redirect(url_for('suppliers'))


@app.route('/supplier_opening_balance/<int:id>', methods=['POST'])
@login_required
def supplier_opening_balance(id):
    if not _user_can('can_manage_suppliers'):
        flash('Permission denied', 'danger')
        return redirect(url_for('supplier_ledger', id=id))
    s = db.session.get(Supplier, id)
    if not s:
        flash('Supplier not found', 'danger')
        return redirect(url_for('suppliers'))
    s.opening_balance = _to_float_or_zero(request.form.get('opening_balance', s.opening_balance))
    s.opening_balance_date = _resolve_opening_balance_date(
        request.form.get('opening_balance_date'),
        fallback_dt=(s.opening_balance_date or s.created_at)
    )
    db.session.commit()
    flash('Opening balance updated', 'success')
    return redirect(url_for('supplier_ledger', id=id))

@app.route('/delete_supplier/<int:id>', methods=['POST'])
@login_required
def delete_supplier(id):
    if not _user_can('can_manage_suppliers'):
        flash('Permission denied', 'danger')
        return redirect(url_for('suppliers'))
    # Soft delete or hard delete logic here. For now, we rely on is_active toggle in edit.
    # Hard delete only if no GRNs attached, otherwise warn.
    s = db.session.get(Supplier, id)
    if s:
        if s.grns:
            flash('Cannot delete supplier with existing GRNs. Deactivate instead.', 'danger')
        else:
            db.session.delete(s)
            db.session.commit()
            flash('Supplier Deleted', 'warning')
    return redirect(url_for('suppliers'))


# ==================== MATERIAL ROUTES ====================

@app.route('/materials')
@login_required
def materials():
    page = request.args.get('page', 1, type=int)
    category_id = (request.args.get('category_id') or '').strip()
    unit_filter = (request.args.get('unit') or '').strip()
    q = Material.query
    if category_id:
        try:
            q = q.filter(Material.category_id == int(category_id))
        except ValueError:
            pass
    if unit_filter:
        q = q.filter(Material.unit == unit_filter)

    pagination = q.order_by(Material.code.asc()).paginate(page=page, per_page=10)
    # Fetch all materials for the merge modal dropdown
    all_materials = Material.query.order_by(Material.name.asc()).all()
    categories = MaterialCategory.query.order_by(MaterialCategory.name.asc()).all()
    units = [r[0] for r in db.session.query(Material.unit).distinct().filter(Material.unit != None, Material.unit != '').order_by(Material.unit).all()]

    return render_template('materials.html',
                           materials=pagination.items,
                           pagination=pagination,
                           all_materials=all_materials,
                           categories=categories,
                           category_filter=category_id,
                           unit_filter=unit_filter,
                           units=units)


@app.route('/api/material_next_code')
@login_required
def api_material_next_code():
    category_id = (request.args.get('category_id') or '').strip()
    material_name = (request.args.get('material_name') or '').strip()
    category = None
    if category_id:
        try:
            category = db.session.get(MaterialCategory, int(category_id))
        except Exception:
            category = None
    if not category or category.tenant_id != current_user.tenant_id:
        category = get_or_create_material_category(current_user.tenant_id, 'General')
    code = _next_material_code_for_category(category, material_name=material_name)
    return jsonify({
        'success': True,
        'code': code,
        'category_id': category.id if category else None,
        'category_name': category.name if category else 'General',
        'is_ft_product': bool((material_name or '').strip().upper().startswith('FT-'))
    })


@app.route('/api/client_next_code')
@login_required
def api_client_next_code():
    return jsonify({
        'success': True,
        'code': generate_client_code()
    })


@app.route('/delivery_persons')
@login_required
def delivery_persons_page():
    if not _user_can('can_manage_delivery_persons'):
        flash('Unauthorized', 'danger')
        return redirect(url_for('index'))
    rows = DeliveryPerson.query.order_by(DeliveryPerson.name.asc()).all()
    return render_template('delivery_persons.html', rows=rows)


@app.route('/delivery_persons/add', methods=['POST'])
@login_required
def add_delivery_person():
    if not _user_can('can_manage_delivery_persons'):
        flash('Unauthorized', 'danger')
        return redirect(url_for('index'))
    name = (request.form.get('name') or '').strip()
    phone = (request.form.get('phone') or '').strip()
    if not name:
        flash('Driver name is required', 'danger')
        return redirect(url_for('delivery_persons_page'))
    get_or_create_delivery_person(name, phone=phone)
    db.session.commit()
    flash('Delivery person saved', 'success')
    return redirect(url_for('delivery_persons_page'))


@app.route('/delivery_persons/toggle/<int:id>', methods=['POST'])
@login_required
def toggle_delivery_person(id):
    if not _user_can('can_manage_delivery_persons'):
        flash('Unauthorized', 'danger')
        return redirect(url_for('index'))
    row = db.session.get(DeliveryPerson, id)
    if row:
        row.is_active = not row.is_active
        db.session.commit()
        flash('Delivery person status updated', 'success')
    return redirect(url_for('delivery_persons_page'))


@app.route('/delivery_persons/edit/<int:id>', methods=['POST'])
@login_required
def edit_delivery_person(id):
    if not _user_can('can_manage_delivery_persons'):
        flash('Unauthorized', 'danger')
        return redirect(url_for('index'))

    row = db.session.get(DeliveryPerson, id)
    if not row:
        flash('Delivery person not found', 'danger')
        return redirect(url_for('delivery_persons_page'))

    new_name = (request.form.get('name') or '').strip()
    new_phone = (request.form.get('phone') or '').strip()
    if not new_name:
        flash('Driver name is required', 'danger')
        return redirect(url_for('delivery_persons_page'))

    existing = DeliveryPerson.query.filter(
        DeliveryPerson.id != row.id,
        func.lower(func.trim(DeliveryPerson.name)) == new_name.lower()
    ).first()
    if existing:
        flash('Driver name already exists', 'danger')
        return redirect(url_for('delivery_persons_page'))

    old_name = (row.name or '').strip()
    row.name = new_name
    row.phone = new_phone or None

    if old_name and old_name.lower() != new_name.lower():
        DeliveryRent.query.filter(
            func.lower(func.trim(DeliveryRent.delivery_person_name)) == old_name.lower()
        ).update({'delivery_person_name': new_name}, synchronize_session=False)

    db.session.commit()
    flash('Delivery person updated', 'success')
    return redirect(url_for('delivery_persons_page'))


@app.route('/delivery_rents')
@login_required
def delivery_rents_page():
    if not _user_can('can_view_delivery_rent'):
        flash('Unauthorized', 'danger')
        return redirect(url_for('index'))

    date_from = (request.args.get('date_from') or '').strip()
    date_to = (request.args.get('date_to') or '').strip()
    driver = (request.args.get('driver') or '').strip()

    q = DeliveryRent.query.filter_by(is_void=False)
    if date_from:
        q = q.filter(func.date(DeliveryRent.date_posted) >= date_from)
    if date_to:
        q = q.filter(func.date(DeliveryRent.date_posted) <= date_to)
    if driver:
        q = q.filter(func.lower(func.trim(DeliveryRent.delivery_person_name)) == driver.lower())

    rows = q.order_by(DeliveryRent.date_posted.desc()).all()
    total_rent = sum(float(r.amount or 0) for r in rows)

    totals_rows = db.session.query(
        DeliveryRent.delivery_person_name,
        func.sum(DeliveryRent.amount)
    ).filter(DeliveryRent.is_void == False)
    if date_from:
        totals_rows = totals_rows.filter(func.date(DeliveryRent.date_posted) >= date_from)
    if date_to:
        totals_rows = totals_rows.filter(func.date(DeliveryRent.date_posted) <= date_to)
    if driver:
        totals_rows = totals_rows.filter(func.lower(func.trim(DeliveryRent.delivery_person_name)) == driver.lower())
    totals_by_driver = totals_rows.group_by(DeliveryRent.delivery_person_name).order_by(
        func.sum(DeliveryRent.amount).desc()
    ).all()

    driver_names = sorted({
        (x.delivery_person_name or '').strip()
        for x in DeliveryRent.query.filter_by(is_void=False).all()
        if (x.delivery_person_name or '').strip()
    } | {
        (x.name or '').strip()
        for x in DeliveryPerson.query.filter_by(is_active=True).all()
        if (x.name or '').strip()
    })

    return render_template(
        'delivery_rents.html',
        rows=rows,
        total_rent=total_rent,
        totals_by_driver=totals_by_driver,
        driver_names=driver_names,
        date_from=date_from,
        date_to=date_to,
        driver_filter=driver
    )


@app.route('/delivery_rents/void/<int:id>', methods=['POST'])
@login_required
def void_delivery_rent(id):
    if not _user_can('can_manage_sales'):
        flash('Permission denied', 'danger')
        return redirect(url_for('delivery_rents_page'))
    row = db.session.get(DeliveryRent, id)
    if row:
        row.is_void = True
        db.session.commit()
        flash('Delivery rent entry voided.', 'success')
    return redirect(url_for('delivery_rents_page'))


@app.route('/merge_materials', methods=['POST'])
@login_required
def merge_materials():
    source_id = request.form.get('source_material_id')
    target_id = request.form.get('target_material_id')

    if not source_id or not target_id or source_id == target_id:
        flash('Invalid selection for merging', 'danger')
        return redirect(url_for('materials'))

    source_mat = db.session.get(Material, int(source_id))
    target_mat = db.session.get(Material, int(target_id))

    if not source_mat or not target_mat:
        flash('One or both materials not found', 'danger')
        return redirect(url_for('materials'))

    try:
        source_name = source_mat.name
        target_name = target_mat.name

        # 1. Update all Entry records
        Entry.query.filter_by(material=source_name).update({'material': target_name})

        # 2. Update BookingItem records
        BookingItem.query.filter_by(material_name=source_name).update({'material_name': target_name})

        # 3. Update DirectSaleItem records
        DirectSaleItem.query.filter_by(product_name=source_name).update({'product_name': target_name})

        # 4. Update GRNItem records
        GRNItem.query.filter_by(mat_name=source_name).update({'mat_name': target_name})

        # 5. Update DeliveryItem records
        DeliveryItem.query.filter_by(product=source_name).update({'product': target_name})

        # 6. Delete the source material
        db.session.delete(source_mat)

        db.session.commit()
        flash(f'Successfully merged "{source_name}" into "{target_name}". All records updated.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Merge failed: {str(e)}', 'danger')

    return redirect(url_for('materials'))


@app.route('/add_material', methods=['POST'])
@login_required
def add_material():
    ensure_material_table_tenant_unique()
    name = request.form.get('material_name', '').strip()
    code = request.form.get('material_code', '').strip()
    category_id = (request.form.get('category_id') or '').strip()
    unit = request.form.get('material_unit', '').strip() or 'Bags'
    if not name:
        flash('Material name is required', 'danger')
        return redirect(url_for('materials'))
    existing_name = Material.query.filter(
        func.lower(func.trim(Material.name)) == name.lower()
    ).first()
    if existing_name:
        flash(f'Material name "{name}" already exists', 'danger')
        return redirect(url_for('materials'))
    category = None
    if category_id:
        try:
            category = db.session.get(MaterialCategory, int(category_id))
        except Exception:
            category = None
    if not category or category.tenant_id != current_user.tenant_id:
        category = get_or_create_material_category(current_user.tenant_id, 'General')
    if not code:
        code = _next_material_code_for_category(category, material_name=name)
    if Material.query.filter_by(code=code).first():
        flash(f'Material code "{code}" already exists', 'danger')
        return redirect(url_for('materials'))
    new_mat = Material(name=name, code=code, category_id=category.id if category else None, unit=unit)
    db.session.add(new_mat)
    db.session.commit()
    if request.args.get('ajax'):
        return jsonify({'success': True, 'id': new_mat.id, 'name': new_mat.name, 'code': new_mat.code, 'price': new_mat.unit_price, 'unit': new_mat.unit})
    flash('Brand Added', 'success')
    return redirect(url_for('materials'))


@app.route('/edit_material/<int:id>', methods=['POST'])
@login_required
def edit_material(id):
    m = db.session.get(Material, id)
    if m:
        new_code = request.form.get('material_code', '').strip()
        new_name = request.form.get('material_name', '').strip()
        category_id = (request.form.get('category_id') or '').strip()
        new_unit = request.form.get('material_unit', '').strip() or 'Bags'
        if not new_code:
            flash('Material code is required', 'danger')
            return redirect(url_for('materials'))
        if not new_name:
            flash('Material name is required', 'danger')
            return redirect(url_for('materials'))
        existing_name = Material.query.filter(
            func.lower(func.trim(Material.name)) == new_name.lower()
        ).first()
        if existing_name and existing_name.id != id:
            flash(f'Material name "{new_name}" already exists', 'danger')
            return redirect(url_for('materials'))
        existing = Material.query.filter_by(code=new_code).first()
        if existing and existing.id != id:
            flash(f'Material code "{new_code}" already exists', 'danger')
            return redirect(url_for('materials'))
        old_name = m.name
        for e in Entry.query.filter_by(material=old_name).all():
            e.material = new_name
        m.name = new_name
        m.code = new_code
        m.unit = new_unit
        if category_id:
            try:
                cat = db.session.get(MaterialCategory, int(category_id))
            except Exception:
                cat = None
            if cat and cat.tenant_id == current_user.tenant_id:
                m.category_id = cat.id
        db.session.commit()
        flash('Brand Updated', 'info')
    return redirect(url_for('materials'))


@app.route('/bulk_update_material_unit', methods=['POST'])
@login_required
def bulk_update_material_unit():
    category_id = request.form.get('category_id')
    new_unit = request.form.get('new_unit', '').strip()
    
    if not new_unit:
        flash('Unit is required', 'danger')
        return redirect(url_for('materials'))

    query = Material.query
    if category_id:
        query = query.filter_by(category_id=int(category_id))
    
    count = query.update({Material.unit: new_unit}, synchronize_session=False)
    db.session.commit()
    flash(f'Updated unit to "{new_unit}" for {count} materials.', 'success')
    return redirect(url_for('materials'))


def _can_manage_categories():
    return current_user.is_authenticated and _user_can('can_manage_materials')


@app.route('/material_categories/add', methods=['POST'])
@login_required
def add_material_category():
    if not _can_manage_categories():
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))
    name = (request.form.get('category_name') or '').strip()
    if not name:
        flash('Category name is required', 'danger')
        return redirect(url_for('settings'))
    existing = MaterialCategory.query.filter_by(tenant_id=current_user.tenant_id).filter(
        func.lower(func.trim(MaterialCategory.name)) == name.lower()
    ).first()
    if existing:
        flash('Category already exists', 'danger')
        return redirect(url_for('settings'))
    db.session.add(MaterialCategory(tenant_id=current_user.tenant_id, name=name, is_active=True))
    db.session.commit()
    flash('Category added', 'success')
    return redirect(url_for('settings'))


@app.route('/material_categories/<int:id>/rename', methods=['POST'])
@login_required
def rename_material_category(id):
    if not _can_manage_categories():
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))
    name = (request.form.get('category_name') or '').strip()
    if not name:
        flash('Category name is required', 'danger')
        return redirect(url_for('settings'))
    cat = db.session.get(MaterialCategory, id)
    if not cat or cat.tenant_id != current_user.tenant_id:
        flash('Category not found', 'danger')
        return redirect(url_for('settings'))
    if cat.name.lower() == 'general' and name.lower() != 'general':
        flash('Default category cannot be renamed', 'danger')
        return redirect(url_for('settings'))
    existing = MaterialCategory.query.filter_by(tenant_id=current_user.tenant_id).filter(
        func.lower(func.trim(MaterialCategory.name)) == name.lower()
    ).first()
    if existing and existing.id != cat.id:
        flash('Category already exists', 'danger')
        return redirect(url_for('settings'))
    cat.name = name
    db.session.commit()
    flash('Category updated', 'success')
    return redirect(url_for('settings'))


@app.route('/material_categories/<int:id>/toggle', methods=['POST'])
@login_required
def toggle_material_category(id):
    if not _can_manage_categories():
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))
    cat = db.session.get(MaterialCategory, id)
    if not cat or cat.tenant_id != current_user.tenant_id:
        flash('Category not found', 'danger')
        return redirect(url_for('settings'))
    if cat.name.lower() == 'general' and cat.is_active:
        flash('Default category cannot be disabled', 'danger')
        return redirect(url_for('settings'))
    cat.is_active = not cat.is_active
    db.session.commit()
    flash('Category status updated', 'success')
    return redirect(url_for('settings'))


@app.route('/delete_material/<int:id>', methods=['POST'])
@login_required
def delete_material(id):
    if not _user_can('can_manage_materials'):
        flash('Permission denied', 'danger')
        return redirect(url_for('materials'))
    m = db.session.get(Material, id)
    if m:
        m.is_active = not bool(m.is_active)
        db.session.commit()
        flash('Material status updated', 'warning')
    return redirect(url_for('materials'))


@app.route('/materials/activate_all', methods=['POST'])
@login_required
def activate_all_materials():
    if not _user_can('can_manage_materials'):
        flash('Permission denied', 'danger')
        return redirect(url_for('materials'))
    count = Material.query.filter_by(is_active=False).update({'is_active': True}, synchronize_session=False)
    db.session.commit()
    flash(f'Activated {count} suspended materials.', 'success')
    return redirect(url_for('materials'))


# ==================== PENDING BILLS ROUTES ====================

def _build_notification_rows(category_filter='all', status_filter='all', risk_filter='all', q=''):
    q = (q or '').strip().lower()
    bills = PendingBill.query.filter(PendingBill.is_void == False).all()
    contact_count_rows = db.session.query(
        FollowUpContact.pending_bill_id,
        func.count(FollowUpContact.id)
    ).group_by(FollowUpContact.pending_bill_id).all()
    contact_count_by_bill = {bill_id: int(cnt or 0) for bill_id, cnt in contact_count_rows}
    rows = []
    for pb in bills:
        # Credit follow-up queue: only open credit balances.
        # Exclude paid, zero/negative, and cash-tagged rows.
        if pb.is_paid:
            continue
        if float(pb.amount or 0) <= 0:
            continue
        if pb.is_cash:
            continue

        category = _pending_bill_category(pb)
        age_days = _pending_bill_age_days(pb)
        contact_count = contact_count_by_bill.get(pb.id, 0)
        score, risk_level = _pending_bill_risk(pb, contact_count=contact_count)
        status = 'Paid' if pb.is_paid else 'Pending'
        row = {
            'bill': pb,
            'category': category,
            'status': status,
            'age_days': age_days,
            'risk_score': score,
            'risk_level': risk_level,
            'risk_level_key': _normalize_risk_label(risk_level),
            'amount': float(pb.amount or 0),
            'client_text': f"{pb.client_name or ''} {pb.client_code or ''}".strip(),
            'contact_count': contact_count
        }
        if category_filter != 'all':
            if category_filter == 'billed' and category != 'Billed':
                continue
            if category_filter == 'unbilled' and category != 'Unbilled':
                continue
            if category_filter == 'open_khata' and category != 'Open Khata':
                continue
            if category_filter == 'cash_unbilled' and category != 'Unbilled Cash':
                continue
            if category_filter == 'cash_paid' and category != 'Cash Paid':
                continue
        if status_filter != 'all' and status.lower() != status_filter.lower():
            continue
        if risk_filter != 'all' and _normalize_risk_label(risk_level) != _normalize_risk_label(risk_filter):
            continue
        if q:
            combined = f"{pb.client_name or ''} {pb.client_code or ''} {pb.bill_no or ''} {pb.reason or ''}".lower()
            if q not in combined:
                continue
        rows.append(row)

    rows.sort(key=lambda r: (r['risk_score'], r['age_days'], r['amount']), reverse=True)
    return rows


@app.route('/notifications')
@login_required
def notifications_page():
    category = request.args.get('category', 'all').strip().lower()
    status = request.args.get('status', 'all').strip().lower()
    risk = request.args.get('risk', 'all').strip().lower()
    q = request.args.get('q', '').strip()

    rows = _build_notification_rows(category_filter=category, status_filter=status, risk_filter=risk, q=q)
    reminders = FollowUpReminder.query.filter_by(is_done=False).order_by(FollowUpReminder.remind_at.asc()).all()
    contacts = FollowUpContact.query.order_by(FollowUpContact.contacted_at.desc(), FollowUpContact.id.desc()).all()
    reminder_by_bill = {}
    contact_count_by_bill = {}
    latest_contact_by_bill = {}
    for rem in reminders:
        if rem.pending_bill_id not in reminder_by_bill:
            reminder_by_bill[rem.pending_bill_id] = rem
    for c in contacts:
        bill_id = c.pending_bill_id
        contact_count_by_bill[bill_id] = contact_count_by_bill.get(bill_id, 0) + 1
        if bill_id not in latest_contact_by_bill:
            latest_contact_by_bill[bill_id] = c
    for row in rows:
        bill_id = row['bill'].id
        rem = reminder_by_bill.get(row['bill'].id)
        c = latest_contact_by_bill.get(bill_id)
        row['active_remind_at'] = rem.remind_at if rem else None
        row['active_note'] = rem.note if rem else ''
        row['active_reminder_id'] = rem.id if rem else None
        row['contact_count'] = max(row.get('contact_count', 0), contact_count_by_bill.get(bill_id, 0))
        row['last_contact_at'] = c.contacted_at if c else None
        row['last_contact_channel'] = c.channel if c else ''
        row['last_contact_response'] = c.response if c else ''
        row['last_contact_note'] = c.note if c else ''
    staff_emails = StaffEmail.query.order_by(StaffEmail.email.asc()).all()

    counts = {
        'total': len(rows),
        'very_high': sum(1 for r in rows if _normalize_risk_label(r['risk_level']) == 'very_high'),
        'high': sum(1 for r in rows if r['risk_level'] == 'High'),
        'medium': sum(1 for r in rows if r['risk_level'] == 'Medium'),
        'low': sum(1 for r in rows if r['risk_level'] == 'Low'),
        'pending': sum(1 for r in rows if r['status'] == 'Pending'),
    }

    return render_template(
        'notifications.html',
        rows=rows,
        reminders=reminders,
        staff_emails=staff_emails,
        counts=counts,
        filters={'category': category, 'status': status, 'risk': risk, 'q': q}
    )


@app.route('/notifications/upcoming')
@login_required
def notifications_upcoming():
    reminders = FollowUpReminder.query.filter_by(is_done=False).order_by(FollowUpReminder.remind_at.asc()).all()
    now = pk_now()
    return render_template('notifications_upcoming.html', reminders=reminders, now=now)


@app.route('/notifications/add_email', methods=['POST'])
@login_required
def notifications_add_email():
    email = (request.form.get('email') or '').strip().lower()
    if not email or '@' not in email:
        flash('Valid email required', 'danger')
        return redirect(url_for('notifications_page'))
    exists = StaffEmail.query.filter(func.lower(StaffEmail.email) == email).first()
    if not exists:
        db.session.add(StaffEmail(email=email, is_active=True))
        db.session.commit()
        flash('Staff email added', 'success')
    else:
        flash('Email already exists', 'warning')
    return redirect(url_for('notifications_page'))


@app.route('/notifications/toggle_email/<int:id>', methods=['POST'])
@login_required
def notifications_toggle_email(id):
    rec = db.session.get(StaffEmail, id)
    if rec:
        rec.is_active = not rec.is_active
        db.session.commit()
    return redirect(url_for('notifications_page'))


@app.route('/notifications/delete_email/<int:id>', methods=['POST'])
@login_required
def notifications_delete_email(id):
    rec = db.session.get(StaffEmail, id)
    if rec:
        db.session.delete(rec)
        db.session.commit()
    return redirect(url_for('notifications_page'))


@app.route('/notifications/set_reminder/<int:bill_id>', methods=['POST'])
@login_required
def notifications_set_reminder(bill_id):
    pb = db.session.get(PendingBill, bill_id)
    if not pb:
        flash('Pending bill not found', 'danger')
        return redirect(url_for('notifications_page'))
    remind_at_txt = (request.form.get('remind_at') or '').strip()
    note = (request.form.get('note') or '').strip()
    remind_at = _parse_dt_safe(remind_at_txt)
    if not remind_at:
        flash('Invalid reminder date/time', 'danger')
        return redirect(url_for('notifications_page'))

    # One active reminder per bill; overwrite old one.
    existing = FollowUpReminder.query.filter_by(pending_bill_id=pb.id, is_done=False).first()
    if existing:
        existing.remind_at = remind_at
        existing.note = note
        existing.alerted_at = None
        existing.acknowledged_at = None
    else:
        db.session.add(FollowUpReminder(pending_bill_id=pb.id, remind_at=remind_at, note=note))
    db.session.commit()
    flash('Reminder saved', 'success')
    return redirect(url_for('notifications_page'))


def _resolve_reminder_with_contact(rem, response_text, channel='Call', note='', contacted_at=None, created_by=''):
    if not rem:
        return False, 'Reminder not found'
    if not response_text:
        return False, 'Customer response is required'

    if channel not in ['Call', 'WhatsApp', 'SMS', 'Email', 'Visit', 'Other']:
        channel = 'Other'
    contact_time = contacted_at or pk_now()

    db.session.add(FollowUpContact(
        pending_bill_id=rem.pending_bill_id,
        reminder_id=rem.id,
        contacted_at=contact_time,
        channel=channel,
        response=response_text[:200],
        note=(note or 'Reminder marked done')[:500],
        created_by=created_by or ''
    ))
    rem.is_done = True
    rem.acknowledged_at = pk_now()
    db.session.commit()
    return True, 'Reminder closed and history saved'


@app.route('/notifications/log_contact/<int:bill_id>', methods=['POST'])
@login_required
def notifications_log_contact(bill_id):
    pb = db.session.get(PendingBill, bill_id)
    if not pb:
        flash('Pending bill not found', 'danger')
        return redirect(url_for('notifications_page'))

    contacted_at_txt = (request.form.get('contacted_at') or '').strip()
    contacted_at = _parse_dt_safe(contacted_at_txt) if contacted_at_txt else pk_now()
    if not contacted_at:
        flash('Invalid contact date/time', 'danger')
        return redirect(request.referrer or url_for('notifications_page'))

    channel = (request.form.get('channel') or 'Call').strip()
    if channel not in ['Call', 'WhatsApp', 'SMS', 'Email', 'Visit', 'Other']:
        channel = 'Other'

    response_text = (request.form.get('response') or '').strip()
    if not response_text:
        flash('Customer response is required to save history', 'danger')
        return redirect(request.referrer or url_for('notifications_page'))
    note = (request.form.get('note') or '').strip()

    db.session.add(FollowUpContact(
        pending_bill_id=pb.id,
        contacted_at=contacted_at,
        channel=channel,
        response=response_text[:200],
        note=note[:500],
        created_by=(current_user.username if current_user.is_authenticated else '')
    ))
    db.session.commit()
    flash('Contact history saved', 'success')
    return redirect(request.referrer or url_for('notifications_page'))


@app.route('/notifications/close_reminder/<int:id>', methods=['POST'])
@login_required
def notifications_close_reminder(id):
    rem = db.session.get(FollowUpReminder, id)
    response_text = (request.form.get('response') or '').strip()
    channel = (request.form.get('channel') or 'Call').strip()
    note = (request.form.get('note') or '').strip()
    contacted_at_txt = (request.form.get('contacted_at') or '').strip()
    contacted_at = _parse_dt_safe(contacted_at_txt) if contacted_at_txt else pk_now()
    if not contacted_at:
        contacted_at = pk_now()

    ok, msg = _resolve_reminder_with_contact(
        rem=rem,
        response_text=response_text,
        channel=channel,
        note=note,
        contacted_at=contacted_at,
        created_by=(current_user.username if current_user.is_authenticated else '')
    )
    flash(msg, 'success' if ok else 'danger')
    return redirect(request.referrer or url_for('notifications_page'))


@app.route('/notifications/set_severity/<int:bill_id>', methods=['POST'])
@login_required
def notifications_set_severity(bill_id):
    pb = db.session.get(PendingBill, bill_id)
    if not pb:
        flash('Pending bill not found', 'danger')
        return redirect(url_for('notifications_page'))

    level = _normalize_risk_label(request.form.get('severity'))
    valid = {'auto', 'low', 'medium', 'high', 'very_high'}
    if level not in valid:
        flash('Invalid severity selection', 'danger')
        return redirect(request.referrer or url_for('notifications_bill_detail', bill_id=bill_id))

    pb.risk_override = None if level == 'auto' else _risk_label_pretty(level)
    db.session.commit()
    flash('Severity updated', 'success')
    return redirect(request.referrer or url_for('notifications_bill_detail', bill_id=bill_id))


@app.route('/notifications/bill/<int:bill_id>')
@login_required
def notifications_bill_detail(bill_id):
    pb = db.session.get(PendingBill, bill_id)
    if not pb:
        flash('Pending bill not found', 'danger')
        return redirect(url_for('notifications_page'))

    age_days = _pending_bill_age_days(pb)
    category = _pending_bill_category(pb)
    # Load all open credit bills for this same client on detail screen.
    if (pb.client_code or '').strip():
        client_bills_query = PendingBill.query.filter(
            PendingBill.is_void == False,
            PendingBill.is_paid == False,
            PendingBill.is_cash == False,
            PendingBill.amount > 0,
            PendingBill.client_code == pb.client_code
        )
    else:
        client_name_norm = (pb.client_name or '').strip().lower()
        client_bills_query = PendingBill.query.filter(
            PendingBill.is_void == False,
            PendingBill.is_paid == False,
            PendingBill.is_cash == False,
            PendingBill.amount > 0,
            func.lower(func.trim(PendingBill.client_name)) == client_name_norm
        )
    client_open_bills = client_bills_query.order_by(PendingBill.id.desc()).all()
    client_bill_ids = [b.id for b in client_open_bills]
    client_total_due = sum(float(b.amount or 0) for b in client_open_bills)

    active_reminder = FollowUpReminder.query.filter_by(
        pending_bill_id=pb.id,
        is_done=False
    ).order_by(FollowUpReminder.remind_at.asc()).first()
    reminders = FollowUpReminder.query.filter(
        FollowUpReminder.pending_bill_id.in_(client_bill_ids)
    ).order_by(FollowUpReminder.created_at.desc(), FollowUpReminder.id.desc()).all() if client_bill_ids else []
    contact_logs = FollowUpContact.query.filter(
        FollowUpContact.pending_bill_id.in_(client_bill_ids)
    ).order_by(FollowUpContact.contacted_at.desc(), FollowUpContact.id.desc()).all() if client_bill_ids else []
    score, risk_level = _pending_bill_risk(pb, contact_count=len(contact_logs))

    reminder_ids = [r.id for r in reminders]
    reminder_contact_by_id = {}
    used_contact_ids = set()
    if reminder_ids:
        closure_contacts = FollowUpContact.query.filter(
            FollowUpContact.reminder_id.in_(reminder_ids)
        ).order_by(FollowUpContact.contacted_at.desc(), FollowUpContact.id.desc()).all()
        for c in closure_contacts:
            if c.reminder_id and c.reminder_id not in reminder_contact_by_id:
                reminder_contact_by_id[c.reminder_id] = c
                used_contact_ids.add(c.id)

    # Backfill matching for older rows created before reminder_id linkage existed.
    for r in reminders:
        if r.id in reminder_contact_by_id:
            continue
        if not r.is_done or not r.acknowledged_at:
            continue
        best = None
        best_diff = None
        for c in contact_logs:
            if c.id in used_contact_ids:
                continue
            if c.reminder_id:
                continue
            if not c.contacted_at:
                continue
            diff = abs((c.contacted_at - r.acknowledged_at).total_seconds())
            if diff <= 180 and (best_diff is None or diff < best_diff):
                best = c
                best_diff = diff
        if best:
            reminder_contact_by_id[r.id] = best
            used_contact_ids.add(best.id)

    additional_contacts = [c for c in contact_logs if c.id not in used_contact_ids]

    return render_template(
        'notifications_detail.html',
        bill=pb,
        client_open_bills=client_open_bills,
        client_total_due=client_total_due,
        score=score,
        risk_level=risk_level,
        age_days=age_days,
        category=category,
        active_reminder=active_reminder,
        reminders=reminders,
        contact_logs=contact_logs,
        reminder_contact_by_id=reminder_contact_by_id,
        additional_contacts=additional_contacts,
        severity_override=(_normalize_risk_label(pb.risk_override) if pb.risk_override else 'auto')
    )


@app.route('/api/notifications/contact_history/<int:bill_id>')
@login_required
def api_notifications_contact_history(bill_id):
    pb = db.session.get(PendingBill, bill_id)
    if not pb:
        return jsonify({'error': 'Pending bill not found'}), 404

    logs = FollowUpContact.query.filter_by(pending_bill_id=pb.id).order_by(
        FollowUpContact.contacted_at.desc(),
        FollowUpContact.id.desc()
    ).all()
    return jsonify([{
        'id': x.id,
        'contacted_at': x.contacted_at.strftime('%Y-%m-%d %H:%M') if x.contacted_at else '',
        'channel': x.channel or '',
        'response': x.response or '',
        'note': x.note or '',
        'created_by': x.created_by or ''
    } for x in logs])


@app.route('/notifications/ack_reminder/<int:id>', methods=['POST'])
@login_required
def notifications_ack_reminder(id):
    rem = db.session.get(FollowUpReminder, id)
    response_text = (request.form.get('response') or '').strip()
    channel = (request.form.get('channel') or 'Call').strip()
    note = (request.form.get('note') or '').strip()
    contacted_at_txt = (request.form.get('contacted_at') or '').strip()
    contacted_at = _parse_dt_safe(contacted_at_txt) if contacted_at_txt else pk_now()
    if not contacted_at:
        contacted_at = pk_now()

    ok, msg = _resolve_reminder_with_contact(
        rem=rem,
        response_text=response_text,
        channel=channel,
        note=note,
        contacted_at=contacted_at,
        created_by=(current_user.username if current_user.is_authenticated else '')
    )
    if not ok:
        return jsonify({'success': False, 'error': msg}), 400 if msg == 'Customer response is required' else 404
    return jsonify({'success': True})


@app.route('/api/notifications/due')
@login_required
def api_notifications_due():
    now = pk_now()
    due = FollowUpReminder.query.filter(
        FollowUpReminder.is_done == False,
        FollowUpReminder.remind_at <= now,
        FollowUpReminder.alerted_at == None
    ).order_by(FollowUpReminder.remind_at.asc()).all()
    payload = []
    for r in due:
        pb = r.pending_bill
        payload.append({
            'id': r.id,
            'client': pb.client_name if pb else '',
            'bill_no': pb.bill_no if pb else '',
            'amount': float(pb.amount or 0) if pb else 0,
            'note': r.note or '',
            'remind_at': r.remind_at.strftime('%Y-%m-%d %H:%M')
        })
        r.alerted_at = now
    if due:
        db.session.commit()
    return jsonify(payload)


@app.route('/notifications/send_daily_now', methods=['POST'])
@login_required
def notifications_send_daily_now():
    ok, msg = _send_daily_notifications_email()
    flash(msg, 'success' if ok else 'warning')
    return redirect(url_for('notifications_page'))


@app.route('/pending_bills')
@login_required
def pending_bills():
    page = request.args.get('page', 1, type=int)
    category = request.args.get('category', '').strip()
    filters = {
        'client_code': request.args.get('client_code', '').strip(),
        'bill_no': request.args.get('bill_no', '').strip(),
        'bill_from': request.args.get('bill_from', '').strip(),
        'bill_to': request.args.get('bill_to', '').strip(),
        'category': category,
        'bill_kind': request.args.get('bill_kind', '').strip().upper(),
        'is_cash': request.args.get('is_cash', '').strip(),
        'is_manual': request.args.get('is_manual', '').strip()
    }

    query = PendingBill.query

    if filters['client_code']: # Add is_void filter
        query = query.filter(PendingBill.client_code == filters['client_code'])
    if filters['bill_no']:
        bill_q = filters['bill_no']
        variants = _bill_no_variants(bill_q)
        ors = [PendingBill.bill_no.ilike(f"%{bill_q}%")]
        ors.extend([PendingBill.bill_no.ilike(v) for v in variants if v])
        query = query.filter(or_(*ors))
    if filters['bill_kind'] in ['SB', 'MB']:
        query = query.filter(PendingBill.bill_kind == filters['bill_kind'])
    if filters['is_cash'] != '':
        query = query.filter(PendingBill.is_cash == (filters['is_cash'] == '1'))
    if filters['is_manual'] != '':
        query = query.filter(PendingBill.is_manual == (filters['is_manual'] == '1'))

    query = query.filter(PendingBill.is_void == False)

    normalized_category = normalize_sale_category(category, default=category) if category else ''

    if category == 'Unbilled Cash' or normalized_category == 'Cash':
        query = query.filter(PendingBill.is_cash == True)
    elif category == 'Cash Paid':
        query = query.filter(
            PendingBill.is_paid == True,
            or_(
                PendingBill.client_code == OPEN_KHATA_CODE,
                func.upper(PendingBill.client_name) == OPEN_KHATA_NAME
            )
        )
    elif normalized_category == 'Open Khata':
        query = query.filter(or_(
            PendingBill.client_code == OPEN_KHATA_CODE,
            func.upper(PendingBill.client_name) == OPEN_KHATA_NAME
        ))
    elif normalized_category in ['Booking Delivery', 'Mixed Transaction', 'Credit Customer']:
        query = query.filter(
            func.lower(func.coalesce(PendingBill.reason, '')).like(
                f"direct sale ({normalized_category.lower()}):%"
            )
        )
    elif category:
        query = query.join(Client, PendingBill.client_code == Client.code).filter(Client.category == category)

    pagination = query.order_by(PendingBill.id.desc()).paginate(page=page, per_page=15)

    active_clients = Client.query.filter(Client.is_active == True).order_by(Client.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()

    return render_template('pending_bills.html',
                           bills=pagination.items,
                           pagination=pagination,
                           filters=filters,
                           clients=active_clients,
                           materials=materials)


@app.route('/add_pending_bill', methods=['POST'])
@login_required
def add_pending_bill():
    note = request.form.get('note', '').strip()
    client_code = request.form.get('client_code', '').strip()
    client_obj = get_client_by_input(client_code)
    photo_path = save_photo(request.files.get('photo'))

    if not client_obj:
        flash('Invalid Client Code.', 'danger')
        return redirect(url_for('pending_bills'))

    raw_bill_no = request.form.get('bill_no', '').strip()
    normalized_bill_no = normalize_manual_bill(raw_bill_no)
    if not normalized_bill_no:
        flash('Bill number is required', 'danger')
        return redirect(url_for('pending_bills'))

    conflict = find_bill_conflict(normalized_bill_no)
    if conflict:
        flash(f"Bill '{normalized_bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
        return redirect(url_for('pending_bills'))

    bill = PendingBill(client_code=client_code,
                       client_name=client_obj.name,
                       bill_no=normalized_bill_no,
                       bill_kind='MB',
                       is_manual=True,
                       nimbus_no=request.form.get('nimbus_no', '').strip(),
                       amount=float(request.form.get('amount') or 0),
                       reason=request.form.get('reason', '').strip(),
                       photo_url=request.form.get('photo_url', '').strip(),
                       photo_path=photo_path,
                       created_at=pk_now().strftime('%Y-%m-%d %H:%M'),
                       created_by=current_user.username,
                       note=note)
    db.session.add(bill)
    db.session.commit()
    flash('Pending bill added', 'success')
    return redirect(url_for('pending_bills'))


@app.route('/edit_pending_bill/<int:id>', methods=['POST'])
@login_required
def edit_pending_bill(id):
    bill = db.session.get(PendingBill, id)
    if bill:
        old_bill_no = bill.bill_no
        old_client_code = bill.client_code

        client_code = request.form.get('client_code', '').strip()
        client_obj = get_client_by_input(client_code)

        if not client_obj:
            flash('Invalid Client Code.', 'danger')
            return redirect(url_for('pending_bills'))

        bill.client_code = client_code
        bill.client_name = client_obj.name
        raw_bill_no = request.form.get('bill_no', '').strip()
        bill.bill_no = normalize_manual_bill(raw_bill_no) if raw_bill_no else ''
        bill.bill_kind = parse_bill_kind(bill.bill_no)
        bill.is_manual = (bill.bill_kind == 'MB')
        if bill.bill_no:
            conflict = find_bill_conflict(bill.bill_no)
            if conflict and not (conflict[0] == 'PendingBill' and conflict[1] == bill.id):
                flash(f"Bill '{bill.bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
                return redirect(url_for('pending_bills'))
        bill.nimbus_no = request.form.get('nimbus_no', '').strip()
        bill.amount = float(request.form.get('amount') or 0)
        bill.reason = request.form.get('reason', '').strip()
        bill.photo_url = request.form.get('photo_url', '').strip()
        bill.note = request.form.get('note', '').strip()
        
        new_photo = save_photo(request.files.get('photo'))
        if new_photo:
            bill.photo_path = new_photo

        update_data = {
            'bill_no': bill.bill_no,
            'client': bill.client_name,
            'client_code': bill.client_code
        }
        Entry.query.filter_by(bill_no=old_bill_no, client_code=old_client_code).update(update_data)

        db.session.commit()
        flash('Bill updated', 'success')
    return redirect(url_for('pending_bills'))


@app.route('/delete_pending_bill/<int:id>', methods=['POST'])
@login_required
def delete_pending_bill(id):
    bill = db.session.get(PendingBill, id)
    if bill:
        bill.is_void = True
        db.session.commit()
        flash('Bill voided', 'warning')
    return redirect(url_for('pending_bills'))


@app.route('/toggle_bill_paid/<int:id>', methods=['POST'])
@login_required
def toggle_bill_paid(id):
    bill = db.session.get(PendingBill, id)
    if bill:
        bill.is_paid = not bill.is_paid
        is_open_khata_bill = (
            bill.client_code == OPEN_KHATA_CODE or
            (bill.client_name or '').strip().upper() == OPEN_KHATA_NAME
        )
        if bill.is_paid and is_open_khata_bill:
            bill.is_cash = True
        elif (not bill.is_paid) and is_open_khata_bill:
            bill.is_cash = False
        db.session.commit()
        return jsonify({'success': True, 'is_paid': bill.is_paid})
    return jsonify({'success': False}), 404


# --- EXPORT ROUTES (Fixes Jinja Error) ---
@app.route('/export_pending_bills')
@login_required
def export_pending_bills():
    if current_user.role not in ['admin', 'root']:
        flash('Only tenant admin or root can run import/export operations.', 'danger')
        return redirect(url_for('index'))
    """Redirects to the generic export function for pending bills."""
    # This is a convenience route to fix a template error.
    # It redirects to the actual export endpoint in the import_export blueprint.
    args = request.args.to_dict()
    args['dataset'] = 'pending_bills'
    return redirect(url_for('import_export.export_data', **args))


@app.route('/export_unpaid_transactions')
@login_required
def export_unpaid_transactions():
    if current_user.role not in ['admin', 'root']:
        flash('Only tenant admin or root can run import/export operations.', 'danger')
        return redirect(url_for('index'))
    """Redirects to the generic export function for unpaid transactions."""
    args = request.args.to_dict()
    args['dataset'] = 'unpaid_transactions'
    return redirect(url_for('import_export.export_data', **args))


@app.route('/import_pending_bills', methods=['GET', 'POST'])
@login_required
def import_pending_bills():
    if current_user.role not in ['admin', 'root']:
        flash('Only tenant admin or root can run import/export operations.', 'danger')
        return redirect(url_for('index'))
    """Legacy pending-bills import endpoint (GET+POST compatibility)."""
    if request.method == 'GET':
        return redirect(url_for('import_export.import_export_page'))

    file = request.files.get('file')
    if not file:
        flash('No file selected for import.', 'danger')
        return redirect(url_for('pending_bills'))

    try:
        import pandas as pd
        from blueprints.import_export import backup_database, _process_pending_bills

        ok, msg = backup_database()
        if not ok:
            flash(f'Backup failed: {msg}', 'danger')
            return redirect(url_for('pending_bills'))

        if file.filename.lower().endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)

        df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
        df = df.fillna('')
        report = {'imported': 0, 'updated': 0, 'skipped': 0, 'errors': 0, 'error_details': [], 'discrepancies': []}
        _process_pending_bills(
            df=df,
            strategy='update',
            missing_client_strategy='create',
            report=report,
            allow_missing=False
        )
        db.session.commit()
        flash(
            f"Pending Bills import complete. Imported: {report['imported']}, Updated: {report['updated']}, "
            f"Skipped: {report['skipped']}, Errors: {report['errors']}",
            'success'
        )
    except Exception as e:
        db.session.rollback()
        flash(f'Import failed: {str(e)}', 'danger')

    return redirect(url_for('pending_bills'))


# --- SYSTEM REPORT & DIAGNOSTICS ---
@app.route('/system_report')
@login_required
def system_report():
    if current_user.role != 'admin':
        flash('Unauthorized', 'danger')
        return redirect(url_for('index'))

    report = {
        'sync_issues': [],
        'stock_issues': [],
        'unpaid_count': 0,
        'zero_amount_bills': 0
    }

    # 1. Check Dispatch vs Pending Bill Sync
    # Find dispatch entries that have a bill number but NO pending bill record
    entries = db.session.query(Entry).filter(
        Entry.type == 'OUT',
        Entry.is_void == False,
        Entry.bill_no != None,
        Entry.bill_no != ''
    ).all()

    for e in entries:
        if e.bill_no.upper() == 'CASH': continue # Skip cash entries

        pb = PendingBill.query.filter_by(bill_no=e.bill_no, client_code=e.client_code, is_void=False).first()
        if not pb:
            report['sync_issues'].append({
                'type': 'Missing Pending Bill',
                'desc': f"Entry #{e.id}: Bill {e.bill_no} for {e.client} ({e.client_code}) is missing from Pending Bills."
            })

    # 2. Check Financial Data
    report['unpaid_count'] = PendingBill.query.filter_by(is_paid=False, is_void=False).count()

    zero_bills = PendingBill.query.filter_by(is_paid=False, is_void=False, amount=0).all()
    for zb in zero_bills:
        report['sync_issues'].append({
            'type': 'Zero Amount Bill',
            'desc': f"Bill {zb.bill_no} for {zb.client_name} has 0.00 amount."
        })
        report['zero_amount_bills'] += 1

    # 3. Check Stock Consistency
    materials = Material.query.all()
    for m in materials:
        total_in = db.session.query(func.sum(Entry.qty)).filter_by(material=m.name, type='IN', is_void=False).scalar() or 0
        total_out = db.session.query(func.sum(Entry.qty)).filter_by(material=m.name, type='OUT', is_void=False).scalar() or 0
        calc_total = total_in - total_out

        if abs(calc_total - (m.total or 0)) > 0.1:
            report['stock_issues'].append({
                'material': m.name,
                'db_stock': m.total,
                'calc_stock': calc_total,
                'diff': m.total - calc_total
            })

    return render_template('system_report.html', report=report)

@app.route('/fix_system_issues')
@login_required
def fix_system_issues():
    """Auto-fix common sync issues"""
    if current_user.role != 'admin': return redirect(url_for('index'))

    # Logic to fix zero amount bills
    # (Implementation similar to fix_and_test.py but safe for web)
    # For now, we just redirect to report
    flash('Please use the console script fix_and_test.py for bulk fixes to ensure safety.', 'info')
    return redirect(url_for('system_report'))


def _payment_receipt_pending_bill_rows(payment):
    refs = _payment_receipt_refs(payment)
    if not refs:
        return []
    reason_filter = func.lower(func.coalesce(PendingBill.reason, '')).like('payment received%')
    bill_filter = or_(*[PendingBill.bill_no.ilike(r) for r in refs])
    client_obj = get_client_by_input(payment.client_name or '')
    if client_obj:
        client_filter = or_(
            PendingBill.client_code == client_obj.code,
            func.lower(func.coalesce(PendingBill.client_name, '')) == client_obj.name.lower(),
            func.coalesce(PendingBill.client_code, '') == ''
        )
    else:
        client_filter = func.lower(func.coalesce(PendingBill.client_name, '')) == (payment.client_name or '').strip().lower()
    return PendingBill.query.filter(reason_filter, bill_filter, client_filter).all()


def _run_reconciliation(apply_fixes=False):
    report = {
        'ran_at': pk_now().strftime('%Y-%m-%d %H:%M:%S'),
        'mode': 'fix' if apply_fixes else 'scan',
        'entries_scanned': 0,
        'broken_refs_count': 0,
        'broken_refs_sample': [],
        'direct_sale_mismatch_count': 0,
        'direct_sale_waive_mismatch_count': 0,
        'booking_mismatch_count': 0,
        'payment_mismatch_count': 0,
        'bill_normalized_count': 0,
        'bill_normalized_sample': [],
        'fixes_applied': 0
    }

    def _track_norm(entity, rid, field, old, new):
        if old == new:
            return
        report['bill_normalized_count'] += 1
        if len(report['bill_normalized_sample']) < 50:
            report['bill_normalized_sample'].append({
                'entity': entity,
                'id': rid,
                'field': field,
                'from': old,
                'to': new
            })
        if apply_fixes:
            report['fixes_applied'] += 1

    # 0) Bill normalization/backfill (legacy #123 / 123 / 123.0 -> SB-<NS>-n / MB NO.<x>)
    for bk in Booking.query.all():
        old_m = (bk.manual_bill_no or '').strip()
        old_a = (bk.auto_bill_no or '').strip()
        new_m = normalize_manual_bill(old_m) if old_m else ''
        new_a = normalize_auto_bill(old_a, namespace=AUTO_BILL_NAMESPACES['BOOKING']) if old_a else ''
        _track_norm('Booking', bk.id, 'manual_bill_no', old_m, new_m)
        _track_norm('Booking', bk.id, 'auto_bill_no', old_a, new_a)
        if apply_fixes:
            bk.manual_bill_no = new_m or None
            bk.auto_bill_no = new_a or None

    for py in Payment.query.all():
        old_m = (py.manual_bill_no or '').strip()
        old_a = (py.auto_bill_no or '').strip()
        new_m = normalize_manual_bill(old_m) if old_m else ''
        new_a = normalize_auto_bill(old_a, namespace=AUTO_BILL_NAMESPACES['PAYMENT']) if old_a else ''
        _track_norm('Payment', py.id, 'manual_bill_no', old_m, new_m)
        _track_norm('Payment', py.id, 'auto_bill_no', old_a, new_a)
        if apply_fixes:
            py.manual_bill_no = new_m or None
            py.auto_bill_no = new_a or None

    for sp in SupplierPayment.query.all():
        old_m = (sp.manual_bill_no or '').strip()
        old_a = (sp.auto_bill_no or '').strip()
        new_m = normalize_manual_bill(old_m) if old_m else ''
        new_a = normalize_auto_bill(old_a, namespace=AUTO_BILL_NAMESPACES['SUPPLIER_PAYMENT']) if old_a else ''
        _track_norm('SupplierPayment', sp.id, 'manual_bill_no', old_m, new_m)
        _track_norm('SupplierPayment', sp.id, 'auto_bill_no', old_a, new_a)
        if apply_fixes:
            sp.manual_bill_no = new_m or None
            sp.auto_bill_no = new_a or None

    for ds in DirectSale.query.all():
        old_m = (ds.manual_bill_no or '').strip()
        old_a = (ds.auto_bill_no or '').strip()
        new_m = normalize_manual_bill(old_m) if old_m else ''
        new_a = normalize_auto_bill(old_a, namespace=AUTO_BILL_NAMESPACES['DIRECT_SALE']) if old_a else ''
        _track_norm('DirectSale', ds.id, 'manual_bill_no', old_m, new_m)
        _track_norm('DirectSale', ds.id, 'auto_bill_no', old_a, new_a)
        if apply_fixes:
            ds.manual_bill_no = new_m or None
            ds.auto_bill_no = new_a or None

    for grn in GRN.query.all():
        old_m = (grn.manual_bill_no or '').strip()
        old_a = (grn.auto_bill_no or '').strip()
        new_m = normalize_manual_bill(old_m) if old_m else ''
        new_a = normalize_auto_bill(old_a, namespace=AUTO_BILL_NAMESPACES['GRN']) if old_a else ''
        _track_norm('GRN', grn.id, 'manual_bill_no', old_m, new_m)
        _track_norm('GRN', grn.id, 'auto_bill_no', old_a, new_a)
        if apply_fixes:
            grn.manual_bill_no = new_m or None
            grn.auto_bill_no = new_a or None

    for ent in Entry.query.all():
        old_a = (ent.auto_bill_no or '').strip()
        new_a = normalize_auto_bill(old_a, namespace=AUTO_BILL_NAMESPACES['ENTRY']) if old_a else ''
        _track_norm('Entry', ent.id, 'auto_bill_no', old_a, new_a)
        if apply_fixes:
            ent.auto_bill_no = new_a or None

    for inv in Invoice.query.all():
        old_no = (inv.invoice_no or '').strip()
        # Preserve INV-* style invoice numbers; normalize numeric/custom refs as MB.
        new_no = old_no
        if old_no and not old_no.upper().startswith('INV-'):
            new_no = normalize_manual_bill(old_no)
        _track_norm('Invoice', inv.id, 'invoice_no', old_no, new_no)
        if apply_fixes:
            inv.invoice_no = new_no or None

    for pb in PendingBill.query.all():
        old_no = (pb.bill_no or '').strip()
        old_kind = (pb.bill_kind or '').strip().upper()
        is_manual = bool(pb.is_manual)
        if old_no:
            if is_manual:
                new_no = normalize_manual_bill(old_no)
            else:
                new_no = normalize_auto_bill(old_no, namespace=AUTO_BILL_NS_DEFAULT)
                if not new_no:
                    new_no = normalize_manual_bill(old_no)
            new_kind = parse_bill_kind(new_no)
        else:
            new_no = ''
            new_kind = 'UNKNOWN'
        _track_norm('PendingBill', pb.id, 'bill_no', old_no, new_no)
        _track_norm('PendingBill', pb.id, 'bill_kind', old_kind, new_kind)
        if apply_fixes:
            pb.bill_no = new_no or None
            pb.bill_kind = new_kind

    # 1) Broken/ambiguous bill refs in transaction entries.
    entries = Entry.query.filter(Entry.is_void == False).all()
    for e in entries:
        ref = _entry_best_bill_ref(e)
        if not ref or ref.upper().startswith('UNBILLED'):
            continue
        report['entries_scanned'] += 1

        owner = find_bill_conflict(ref)
        if owner:
            continue

        resolved_variant = None
        for candidate in _bill_no_variants(ref):
            if candidate == ref:
                continue
            if find_bill_conflict(candidate):
                resolved_variant = candidate
                break

        if resolved_variant and apply_fixes:
            if (e.bill_no or '').strip() == ref:
                e.bill_no = resolved_variant
                report['fixes_applied'] += 1
                continue
            if (e.auto_bill_no or '').strip() == ref:
                e.auto_bill_no = resolved_variant
                report['fixes_applied'] += 1
                continue

        report['broken_refs_count'] += 1
        if len(report['broken_refs_sample']) < 25:
            report['broken_refs_sample'].append({
                'entry_id': e.id,
                'ref': ref,
                'client': e.client or '',
                'client_code': e.client_code or ''
            })

    # 2) Direct sale consistency (sale <-> entries/pending/rent void flags).
    for sale in DirectSale.query.all():
        refs = _direct_sale_bill_refs(sale)
        ds_entries = Entry.query.filter(Entry.bill_no.in_(refs), Entry.nimbus_no == 'Direct Sale').all()
        pb_rows = PendingBill.query.filter(PendingBill.bill_no.in_(refs)).all()
        rent_rows = DeliveryRent.query.filter_by(sale_id=sale.id).all()

        mismatch = False
        if sale.is_void and any(not x.is_void for x in ds_entries + pb_rows + rent_rows):
            mismatch = True
        if (not sale.is_void) and any(x.is_void for x in ds_entries + pb_rows + rent_rows):
            mismatch = True

        if mismatch:
            report['direct_sale_mismatch_count'] += 1
            if apply_fixes:
                if sale.is_void:
                    _void_direct_sale_entries_and_restore_stock(sale, refs=refs)
                    PendingBill.query.filter(PendingBill.bill_no.in_(refs)).update({'is_void': True}, synchronize_session=False)
                    DeliveryRent.query.filter_by(sale_id=sale.id).update({'is_void': True}, synchronize_session=False)
                else:
                    _unvoid_direct_sale_entries_and_apply_stock(sale, refs=refs)
                    PendingBill.query.filter(PendingBill.bill_no.in_(refs)).update({'is_void': False}, synchronize_session=False)
                    DeliveryRent.query.filter_by(sale_id=sale.id).update({'is_void': False}, synchronize_session=False)
                report['fixes_applied'] += 1

        sale_waive_rows = WaiveOff.query.filter(
            WaiveOff.payment_id.is_(None),
            WaiveOff.note == _direct_sale_waive_marker(sale.id)
        ).all()
        expected_waive = max(0.0, float(sale.discount or 0))
        waive_mismatch = False
        if expected_waive <= 0 and sale_waive_rows:
            waive_mismatch = True
        elif expected_waive > 0:
            if not sale_waive_rows:
                waive_mismatch = True
            elif any(abs(float((w.amount or 0) - expected_waive)) > 0.01 for w in sale_waive_rows):
                waive_mismatch = True
            elif any(bool(w.is_void) != bool(sale.is_void) for w in sale_waive_rows):
                waive_mismatch = True

        if waive_mismatch:
            report['direct_sale_waive_mismatch_count'] += 1
            if apply_fixes:
                _sync_direct_sale_waive_off(sale)
                report['fixes_applied'] += 1

    # 3) Booking consistency (booking <-> pending bill void flags).
    for bk in Booking.query.all():
        refs = _booking_bill_refs(bk)
        rows = PendingBill.query.filter(PendingBill.bill_no.in_(refs)).all()
        mismatch = any(pb.is_void != bool(bk.is_void) for pb in rows)
        if mismatch:
            report['booking_mismatch_count'] += 1
            if apply_fixes:
                PendingBill.query.filter(PendingBill.bill_no.in_(refs)).update({'is_void': bool(bk.is_void)}, synchronize_session=False)
                report['fixes_applied'] += 1

    # 4) Payment consistency (payment <-> payment-receipt pending bill void flags).
    for pay in Payment.query.all():
        rows = _payment_receipt_pending_bill_rows(pay)
        mismatch = any(pb.is_void != bool(pay.is_void) for pb in rows)
        if mismatch:
            report['payment_mismatch_count'] += 1
            if apply_fixes:
                _set_payment_receipt_pending_bill_void_state(pay, is_void=bool(pay.is_void))
                report['fixes_applied'] += 1

    return report


@app.route('/reconcile_data', methods=['POST'])
@login_required
def reconcile_data():
    if current_user.role != 'admin':
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))

    apply_fixes = str(request.form.get('apply_fixes', '')).strip().lower() in ['1', 'true', 'on', 'yes']
    try:
        report = _run_reconciliation(apply_fixes=apply_fixes)
        db.session.commit()
        if apply_fixes and report.get('bill_normalized_count', 0) > 0:
            try:
                reports_dir = os.path.join(basedir, 'instance', 'import_reports')
                os.makedirs(reports_dir, exist_ok=True)
                ts = pk_now().strftime('%Y%m%d_%H%M%S')
                path = os.path.join(reports_dir, f"bill_normalization_audit_{ts}.md")
                with open(path, 'w', encoding='utf-8') as fh:
                    fh.write("# Bill Normalization Audit Report\n\n")
                    fh.write(f"- Generated at: {report.get('ran_at')}\n")
                    fh.write(f"- Total normalized fields: {report.get('bill_normalized_count', 0)}\n\n")
                    fh.write("## Sample Changes\n\n")
                    for row in (report.get('bill_normalized_sample') or []):
                        fh.write(f"- {row.get('entity')}#{row.get('id')} `{row.get('field')}`: `{row.get('from')}` -> `{row.get('to')}`\n")
                report['bill_audit_report_path'] = path
            except Exception:
                pass
        session['recon_report'] = report
        flash(
            f"Reconciliation {report['mode']} complete. Broken refs: {report['broken_refs_count']}, "
            f"DS mismatches: {report['direct_sale_mismatch_count']}, "
            f"DS waive mismatches: {report.get('direct_sale_waive_mismatch_count', 0)}, "
            f"Booking mismatches: {report['booking_mismatch_count']}, "
            f"Payment mismatches: {report['payment_mismatch_count']}, "
            f"Bill normalized: {report.get('bill_normalized_count', 0)}, "
            f"Fixes: {report['fixes_applied']}",
            'success'
        )
    except Exception as e:
        db.session.rollback()
        flash(f'Reconciliation failed: {str(e)}', 'danger')
    return redirect(url_for('settings'))

# ==================== API ROUTES ====================

@app.route('/api/clients/search')
@login_required
def api_clients_search():
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify([])
    clients = Client.query.filter(
        db.or_(Client.name.ilike(f'%{q}%'), Client.code.ilike(f'%{q}%'))).limit(10).all()
    return jsonify([{'name': c.name, 'code': c.code, 'category': c.category} for c in clients])


@app.route('/api/check_bill/<path:bill_no>')
@login_required
def check_bill_api(bill_no):
    entry = Entry.query.filter_by(bill_no=bill_no).first()
    if entry:
        return jsonify({
            'exists': True,
            'url': url_for('tracking', search=bill_no),
            'material': entry.material,
            'qty': int(entry.qty)
        })
    return jsonify({'exists': False})


@app.route('/uploads/<path:filename>')
@login_required
def uploaded_file(filename):
    upload_dir = os.path.join(basedir, 'static', 'uploads')
    return send_from_directory(upload_dir, filename)


@app.route('/mixed_transactions')
@login_required
def mixed_transactions():
    return redirect(url_for('tracking', category='Mixed Transaction'))


def _ams_cleanup_export_cache():
    now_ts = time.time()
    expired = [k for k, v in AMS_ASSISTANT_EXPORT_CACHE.items() if float(v.get('expires_at', 0) or 0) <= now_ts]
    for k in expired:
        AMS_ASSISTANT_EXPORT_CACHE.pop(k, None)


def _ams_cleanup_context_cache():
    now_ts = time.time()
    expired = [k for k, v in AMS_ASSISTANT_CONTEXT_CACHE.items() if float(v.get('expires_at', 0) or 0) <= now_ts]
    for k in expired:
        AMS_ASSISTANT_CONTEXT_CACHE.pop(k, None)


def _ams_get_context_for_user():
    _ams_cleanup_context_cache()
    return AMS_ASSISTANT_CONTEXT_CACHE.get(current_user.id) or {}


def _ams_set_context_for_user(ctx):
    base = dict(ctx or {})
    base['expires_at'] = time.time() + (2 * 60 * 60)  # 2 hours rolling context
    AMS_ASSISTANT_CONTEXT_CACHE[current_user.id] = base


def _ams_resolve_followup(user_query, intent, client_obj, material_name, start_date, end_date):
    q = (user_query or '').lower()
    ctx = _ams_get_context_for_user()
    followup_markers = ['only', 'just', 'same', 'entries', 'that', 'those', 'this']
    is_followup = any(m in q for m in followup_markers)

    # If follow-up omitted client/date/intent, inherit from previous context.
    if is_followup:
        if not client_obj and ctx.get('client_code'):
            client_obj = Client.query.filter(func.lower(Client.code) == str(ctx.get('client_code')).lower()).first()
        if not material_name and ctx.get('material_name'):
            material_name = ctx.get('material_name')
        if intent == 'unknown' and ctx.get('intent'):
            intent = ctx.get('intent')

        if (re.search(r'\b(only|entries|just)\b', q) and
            (ctx.get('intent') in ['client_ledger', 'client_overview', 'client_remaining']) and
            intent in ['unknown', 'client_overview']):
            intent = 'client_ledger'

        # Natural conversational follow-up support:
        # "his materials", "her materials", "materials", etc.
        if client_obj and (
            re.search(r'\b(his|her|their)\b', q) or
            'materials' in q or
            'material' in q
        ):
            if intent in ['unknown', 'client_overview', 'client_ledger']:
                intent = 'client_remaining'

        # If user added a material in follow-up, prefer material remaining view.
        if client_obj and material_name and intent in ['unknown', 'client_overview']:
            intent = 'client_remaining'

        if ctx.get('start_date') and ctx.get('end_date'):
            try:
                if not re.search(r'\d{4}-\d{2}-\d{2}|yesterday|today|this month|last month', q):
                    start_date = datetime.strptime(str(ctx.get('start_date')), '%Y-%m-%d').date()
                    end_date = datetime.strptime(str(ctx.get('end_date')), '%Y-%m-%d').date()
            except Exception:
                pass

    return intent, client_obj, material_name, start_date, end_date


def _ams_parse_date_range(text):
    q = (text or '').lower()
    today = pk_today()
    matches = re.findall(r'\b(\d{4}-\d{2}-\d{2})\b', q)

    def _to_date(val):
        try:
            return datetime.strptime(val, '%Y-%m-%d').date()
        except Exception:
            return None

    if len(matches) >= 2:
        d1 = _to_date(matches[0])
        d2 = _to_date(matches[1])
        if d1 and d2:
            return (min(d1, d2), max(d1, d2))
    if len(matches) == 1:
        d = _to_date(matches[0])
        if d:
            return (d, d)
    if 'yesterday' in q:
        d = today - timedelta(days=1)
        return (d, d)
    if 'today' in q:
        return (today, today)
    if 'last month' in q:
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        return (first_prev, last_prev)
    if 'this month' in q:
        return (today.replace(day=1), today)
    return (today - timedelta(days=30), today)


def _ams_best_material_match(text):
    q = (text or '').strip().lower()
    if not q:
        return None
    names = [x[0] for x in Material.query.with_entities(Material.name).all() if x[0]]
    direct = [n for n in names if n.lower() in q]
    if direct:
        return sorted(direct, key=lambda x: len(x), reverse=True)[0]
    m = re.search(r'(?:material|for|of|about)\s+([a-z0-9\- ]{3,80})', q)
    if m:
        guess = m.group(1).strip()
        row = Material.query.filter(Material.name.ilike(f'%{guess}%')).order_by(func.length(Material.name).asc()).first()
        if row:
            return row.name
    # Token fuzzy fallback (handles typos like "stee" and short aliases like "dg")
    tokens = [t for t in re.findall(r'[a-z0-9\-]+', q) if len(t) >= 2]
    best_name = None
    best_score = 0
    for n in names:
        nl = n.lower()
        score = 0
        for t in tokens:
            if t in nl:
                score += len(t)
            elif nl.startswith(t):
                score += max(1, len(t) - 1)
            elif t.startswith(nl[:max(2, min(4, len(nl)))]):
                score += 1
        if score > best_score:
            best_name = n
            best_score = score
    if best_name and best_score >= 3:
        return best_name
    return None


def _ams_best_client_match(text):
    q = (text or '').strip().lower()
    if not q:
        return None
    code_match = re.search(r'\b(fbmcl-\d+|fbm-\d+|tmpc-\d+)\b', q, flags=re.IGNORECASE)
    if code_match:
        client = Client.query.filter(func.lower(Client.code) == code_match.group(1).lower()).first()
        if client:
            return client
    rows = Client.query.with_entities(Client.name, Client.code).all()
    names = [r[0] for r in rows if r[0]]
    direct = [n for n in names if n.lower() in q]
    if direct:
        best = sorted(direct, key=lambda x: len(x), reverse=True)[0]
        return Client.query.filter_by(name=best).first()

    # Token-based loose matching (e.g., "tahir remaining", "rehman cement")
    stop = {
        'remaining', 'total', 'material', 'reserved', 'cement', 'steel', 'loss', 'losses',
        'ledger', 'client', 'from', 'to', 'between', 'yesterday', 'today', 'report', 'show',
        'for', 'of', 'and', 'in', 'by', 'how', 'much', 'give', 'me'
    }
    tokens = [t for t in re.findall(r'[a-z0-9]+', q) if len(t) >= 3 and t not in stop]
    if not tokens:
        return None
    best_name = None
    best_score = 0
    for n in names:
        nl = (n or '').lower()
        score = 0
        for t in tokens:
            if t in nl:
                score += len(t)
        if score > best_score:
            best_score = score
            best_name = n
    if best_name and best_score >= 3:
        return Client.query.filter_by(name=best_name).first()
    return None


def _ams_detect_intent(text):
    q = (text or '').lower()
    if re.search(r'\b(his|her|their)\s+materials?\b', q):
        return 'client_remaining'
    if re.search(r'\bmaterials?\b', q):
        return 'client_remaining'
    if 'remaining' in q or 'balance' in q:
        return 'client_remaining'
    if 'loss' in q:
        return 'losses'
    if 'client ledger' in q or ('ledger' in q and ('client' in q or _ams_best_client_match(q))):
        return 'client_ledger'
    if 'material ledger' in q:
        return 'material_ledger'
    if 'grn' in q or 'goods receipt' in q:
        return 'grn'
    if 'overview' in q or 'summary' in q:
        return 'client_overview'
    if 'received' in q or 'inward' in q:
        return 'material_received'
    if 'deliver' in q or 'dispatched' in q or 'how much' in q:
        return 'material_delivered'
    return 'unknown'


def _ams_material_keyword(text):
    q = (text or '').lower()
    for k in ['cement', 'steel', 'rent', 'sand', 'crush']:
        if k in q:
            return k
    return None


def _ams_query_losses(start_date, end_date, material_name=None):
    bq = Booking.query.filter(
        Booking.is_void == False,
        func.date(Booking.date_posted) >= start_date.isoformat(),
        func.date(Booking.date_posted) <= end_date.isoformat()
    )
    sq = DirectSale.query.filter(
        DirectSale.is_void == False,
        func.date(DirectSale.date_posted) >= start_date.isoformat(),
        func.date(DirectSale.date_posted) <= end_date.isoformat()
    )
    pq = Payment.query.filter(
        Payment.is_void == False,
        func.date(Payment.date_posted) >= start_date.isoformat(),
        func.date(Payment.date_posted) <= end_date.isoformat()
    )
    wq = WaiveOff.query.filter(
        WaiveOff.is_void == False,
        func.date(WaiveOff.date_posted) >= start_date.isoformat(),
        func.date(WaiveOff.date_posted) <= end_date.isoformat()
    )

    rows = []
    booking_discount = float(bq.with_entities(func.sum(Booking.discount)).scalar() or 0)
    sale_discount = float(sq.with_entities(func.sum(DirectSale.discount)).scalar() or 0)
    payment_discount = float(pq.with_entities(func.sum(Payment.discount)).scalar() or 0)
    waive_loss = float(wq.with_entities(func.sum(WaiveOff.amount)).scalar() or 0)
    rent_variance_loss = float(sq.with_entities(func.sum(DirectSale.rent_variance_loss)).scalar() or 0)

    rows.append({'component': 'Booking Discount', 'amount': round(booking_discount, 2)})
    rows.append({'component': 'Direct Sale Discount', 'amount': round(sale_discount, 2)})
    rows.append({'component': 'Payment Discount', 'amount': round(payment_discount, 2)})
    rows.append({'component': 'Waive-Off', 'amount': round(waive_loss, 2)})
    rows.append({'component': 'Delivery Rent Variance Loss', 'amount': round(rent_variance_loss, 2)})

    if material_name:
        est_loss = 0.0
        b_items = db.session.query(BookingItem, Booking).join(
            Booking, BookingItem.booking_id == Booking.id
        ).filter(
            Booking.is_void == False,
            func.date(Booking.date_posted) >= start_date.isoformat(),
            func.date(Booking.date_posted) <= end_date.isoformat(),
            BookingItem.material_name.ilike(f'%{material_name}%')
        ).all()
        for item, booking in b_items:
            qty = float(item.qty or 0)
            sale_rate = float(item.price_at_time or 0)
            c_rate, known = _cost_rate_for_material(item.material_name, booking.date_posted.date() if booking.date_posted else None)
            if not known:
                continue
            p = (qty * sale_rate) - (qty * c_rate)
            if p < 0:
                est_loss += abs(p)
        ds_items = db.session.query(DirectSaleItem, DirectSale).join(
            DirectSale, DirectSaleItem.sale_id == DirectSale.id
        ).filter(
            DirectSale.is_void == False,
            func.date(DirectSale.date_posted) >= start_date.isoformat(),
            func.date(DirectSale.date_posted) <= end_date.isoformat(),
            DirectSaleItem.product_name.ilike(f'%{material_name}%')
        ).all()
        for item, sale in ds_items:
            if normalize_sale_category(getattr(sale, 'category', None)) == 'Booking Delivery':
                continue
            qty = float(item.qty or 0)
            sale_rate = float(item.price_at_time or 0)
            c_rate, known = _cost_rate_for_material(item.product_name, sale.date_posted.date() if sale.date_posted else None)
            if not known:
                continue
            p = (qty * sale_rate) - (qty * c_rate)
            if p < 0:
                est_loss += abs(p)
        rows.append({'component': f'Estimated Material Loss ({material_name})', 'amount': round(est_loss, 2)})

    total = round(sum(float(r.get('amount') or 0) for r in rows), 2)
    return {
        'title': 'Loss Summary',
        'summary': f"Total loss from {start_date} to {end_date}: Rs. {total:,.2f}",
        'rows': rows
    }


def _ams_query_material_flow(start_date, end_date, material_name=None, flow_type='OUT'):
    q = Entry.query.filter(
        Entry.is_void == False,
        Entry.type == flow_type,
        Entry.date >= start_date.isoformat(),
        Entry.date <= end_date.isoformat()
    )
    if material_name:
        q = q.filter(Entry.material.ilike(f'%{material_name}%'))
    rows = db.session.query(
        Entry.material,
        func.sum(Entry.qty).label('qty')
    ).filter(
        Entry.id.in_(q.with_entities(Entry.id))
    ).group_by(Entry.material).order_by(func.sum(Entry.qty).desc()).all()
    out = [{'material': r.material, 'qty': round(float(r.qty or 0), 2)} for r in rows if r.material]
    total = round(sum(x['qty'] for x in out), 2)
    action = 'Delivered' if flow_type == 'OUT' else 'Received'
    return {
        'title': f'{action} Material Summary',
        'summary': f"{action} qty from {start_date} to {end_date}: {total:,.2f}",
        'rows': out
    }


def _ams_query_grn(start_date, end_date, material_name=None):
    q = db.session.query(
        GRN.date_posted,
        GRN.supplier,
        GRN.manual_bill_no,
        GRN.auto_bill_no,
        GRNItem.mat_name,
        GRNItem.qty,
        GRNItem.price_at_time
    ).join(GRNItem, GRNItem.grn_id == GRN.id).filter(
        GRN.is_void == False,
        func.date(GRN.date_posted) >= start_date.isoformat(),
        func.date(GRN.date_posted) <= end_date.isoformat()
    )
    if material_name:
        q = q.filter(GRNItem.mat_name.ilike(f'%{material_name}%'))
    rows = []
    total_value = 0.0
    for r in q.order_by(GRN.date_posted.desc()).all():
        line_total = float(r.qty or 0) * float(r.price_at_time or 0)
        total_value += line_total
        rows.append({
            'date': r.date_posted.strftime('%Y-%m-%d') if r.date_posted else '',
            'supplier': r.supplier or '',
            'bill_no': r.manual_bill_no or r.auto_bill_no or '',
            'material': r.mat_name or '',
            'qty': round(float(r.qty or 0), 2),
            'rate': round(float(r.price_at_time or 0), 2),
            'line_total': round(line_total, 2),
        })
    return {
        'title': 'GRN Summary',
        'summary': f"GRN value from {start_date} to {end_date}: Rs. {total_value:,.2f}",
        'rows': rows
    }


def _ams_query_client_ledger(client_obj, start_date, end_date, material_name=None):
    summary = _compute_client_financial_summary(client_obj)
    financial_history, _, _, _, _, _ = _build_client_ledger_rows(client_obj)
    rows = []
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    for row in financial_history:
        dt = row.get('date')
        dt_val = dt if isinstance(dt, datetime) else _parse_dt_safe(dt)
        if dt_val and (dt_val < start_dt or dt_val > end_dt):
            continue
        if material_name:
            desc = str(row.get('description') or '').lower()
            bill_no = str(row.get('bill_no') or '').lower()
            if material_name.lower() not in desc and material_name.lower() not in bill_no:
                continue
        rows.append({
            'date': row.get('date_display') or '',
            'description': row.get('description') or '',
            'bill_no': row.get('bill_no') or '',
            'debit': round(float(row.get('debit') or 0), 2),
            'credit': round(float(row.get('credit') or 0), 2),
            'balance': round(float(row.get('balance') or 0), 2),
        })
    return {
        'title': f'Client Ledger: {client_obj.name}',
        'summary': f"Balance: Rs. {float(summary.get('balance', 0) or 0):,.2f} | Debit: Rs. {float(summary.get('total_debit', 0) or 0):,.2f} | Credit: Rs. {float(summary.get('total_credit', 0) or 0):,.2f}",
        'rows': rows
    }


def _ams_query_material_ledger(start_date, end_date, material_name=None):
    q = Entry.query.filter(
        Entry.is_void == False,
        Entry.date >= start_date.isoformat(),
        Entry.date <= end_date.isoformat()
    )
    if material_name:
        q = q.filter(Entry.material.ilike(f'%{material_name}%'))
    rows = []
    total_in = 0.0
    total_out = 0.0
    for e in q.order_by(Entry.date.desc(), Entry.time.desc()).limit(500).all():
        qty = float(e.qty or 0)
        if (e.type or '').upper() == 'IN':
            total_in += qty
        elif (e.type or '').upper() == 'OUT':
            total_out += qty
        rows.append({
            'date': e.date or '',
            'time': e.time or '',
            'type': e.type or '',
            'material': e.material or '',
            'client': e.client or '',
            'qty': round(qty, 2),
            'bill_no': e.bill_no or e.auto_bill_no or '',
        })
    return {
        'title': 'Material Ledger',
        'summary': f"IN: {total_in:,.2f} | OUT: {total_out:,.2f} | NET: {(total_in-total_out):,.2f}",
        'rows': rows
    }


def _ams_query_client_remaining(client_obj, material_name=None, material_keyword=None):
    def _key(v):
        txt = (v or '').strip().lower()
        return re.sub(r'[^a-z0-9]+', '', txt)

    bookings = Booking.query.filter_by(client_name=client_obj.name, is_void=False).all()
    booking_ids = [b.id for b in bookings]
    booked_totals = {}
    labels = {}
    if booking_ids:
        for item in BookingItem.query.filter(BookingItem.booking_id.in_(booking_ids)).all():
            k = _key(item.material_name)
            if not k:
                continue
            booked_totals[k] = booked_totals.get(k, 0.0) + float(item.qty or 0)
            labels.setdefault(k, (item.material_name or '').strip())

    entries = Entry.query.filter(
        (Entry.client_code == client_obj.code) | (Entry.client == client_obj.name),
        Entry.type == 'OUT',
        Entry.is_void == False,
        not_(and_(Entry.nimbus_no == 'Direct Sale', Entry.client_category != 'Booking Delivery'))
    ).all()
    delivered_totals = {}
    for e in entries:
        k = _key(e.booked_material or e.material)
        if not k:
            continue
        delivered_totals[k] = delivered_totals.get(k, 0.0) + float(e.qty or 0)

    rows = []
    for k, booked in booked_totals.items():
        delivered = float(delivered_totals.get(k, 0.0))
        balance = float(booked - delivered)
        mat_label = labels.get(k, k)
        if material_name and material_name.lower() not in mat_label.lower():
            continue
        if material_keyword and material_keyword.lower() not in mat_label.lower():
            continue
        rows.append({
            'material': mat_label,
            'booked': round(booked, 2),
            'delivered': round(delivered, 2),
            'remaining': round(balance, 2),
        })
    rows.sort(key=lambda x: x.get('remaining', 0), reverse=True)
    total_remaining = round(sum(float(r.get('remaining') or 0) for r in rows), 2)
    filter_label = material_name or material_keyword or 'all materials'
    return {
        'title': f'Remaining Material: {client_obj.name}',
        'summary': f"Total remaining ({filter_label}): {total_remaining:,.2f}",
        'rows': rows
    }


def _ams_query_client_overview(client_obj):
    # Financial outstanding
    pending_total = float(db.session.query(func.sum(PendingBill.amount)).filter(
        PendingBill.client_code == client_obj.code,
        PendingBill.is_void == False,
        PendingBill.is_paid == False
    ).scalar() or 0)

    # Bookings and sales totals
    booking_total = float(db.session.query(func.sum(Booking.amount)).filter(
        func.lower(func.trim(Booking.client_name)) == (client_obj.name or '').strip().lower(),
        Booking.is_void == False
    ).scalar() or 0)
    booking_paid = float(db.session.query(func.sum(Booking.paid_amount)).filter(
        func.lower(func.trim(Booking.client_name)) == (client_obj.name or '').strip().lower(),
        Booking.is_void == False
    ).scalar() or 0)

    sale_total = float(db.session.query(func.sum(DirectSale.amount)).filter(
        func.lower(func.trim(DirectSale.client_name)) == (client_obj.name or '').strip().lower(),
        DirectSale.is_void == False
    ).scalar() or 0)
    sale_paid = float(db.session.query(func.sum(DirectSale.paid_amount)).filter(
        func.lower(func.trim(DirectSale.client_name)) == (client_obj.name or '').strip().lower(),
        DirectSale.is_void == False
    ).scalar() or 0)

    payments_total = float(db.session.query(func.sum(Payment.amount)).filter(
        func.lower(func.trim(Payment.client_name)) == (client_obj.name or '').strip().lower(),
        Payment.is_void == False
    ).scalar() or 0)

    # Reserved material remaining
    rem = _ams_query_client_remaining(client_obj)
    rem_rows = rem.get('rows') or []
    reserved_remaining_total = float(sum(float(r.get('remaining') or 0) for r in rem_rows))

    rows = [
        {'metric': 'Pending Outstanding Amount', 'value': round(pending_total, 2), 'unit': 'PKR'},
        {'metric': 'Booking Total', 'value': round(booking_total, 2), 'unit': 'PKR'},
        {'metric': 'Booking Paid', 'value': round(booking_paid, 2), 'unit': 'PKR'},
        {'metric': 'Booking Remaining', 'value': round(max(0.0, booking_total - booking_paid), 2), 'unit': 'PKR'},
        {'metric': 'Sales Total', 'value': round(sale_total, 2), 'unit': 'PKR'},
        {'metric': 'Sales Paid', 'value': round(sale_paid, 2), 'unit': 'PKR'},
        {'metric': 'Sales Remaining', 'value': round(max(0.0, sale_total - sale_paid), 2), 'unit': 'PKR'},
        {'metric': 'Payments Received', 'value': round(payments_total, 2), 'unit': 'PKR'},
        {'metric': 'Reserved Material Remaining', 'value': round(reserved_remaining_total, 2), 'unit': 'Qty'},
    ]
    # Add top material balances for quick answers.
    for r in rem_rows[:10]:
        rows.append({
            'metric': f"Remaining: {r.get('material')}",
            'value': round(float(r.get('remaining') or 0), 2),
            'unit': 'Qty'
        })

    return {
        'title': f'Client Overview: {client_obj.name}',
        'summary': (
            f"Outstanding: Rs. {pending_total:,.2f} | "
            f"Reserved Remaining Qty: {reserved_remaining_total:,.2f}"
        ),
        'rows': rows
    }


def _ams_get_configured_api_key():
    settings_obj = Settings.query.first()
    if settings_obj and (settings_obj.ams_openai_api_key or '').strip():
        return (settings_obj.ams_openai_api_key or '').strip()
    return (os.environ.get('OPENAI_API_KEY', '') or '').strip()


def _ams_call_openai(api_key, user_query, summary_text, sample_rows):
    if not api_key:
        return ''
    model = (os.environ.get('AMS_ASSISTANT_MODEL') or 'gpt-4o-mini').strip()
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are an accounting assistant for ERP users. "
                    "Treat person-like names in user query as client names when context is accounts/ledger/materials. "
                    "This assistant is strictly read-only: never suggest edits/deletes/voids/updates. "
                    "Respond in concise plain text. Do not fabricate numbers. Use provided data only."
                )
            },
            {
                "role": "user",
                "content": f"Question: {user_query}\nSummary: {summary_text}\nSample rows: {json.dumps(sample_rows[:10], ensure_ascii=True)}"
            }
        ]
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode('utf-8'),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            obj = json.loads(resp.read().decode('utf-8'))
        return (((obj.get('choices') or [{}])[0].get('message') or {}).get('content') or '').strip()
    except Exception:
        return ''


@app.route('/ams_assistant')
@login_required
def ams_assistant_page():
    return render_template('ams_assistant.html')


@app.route('/api/ams_assistant/chat', methods=['POST'])
@login_required
def ams_assistant_chat_api():
    try:
        payload = request.get_json(silent=True) or {}
        user_query = str(payload.get('message') or '').strip()
        if not user_query:
            return jsonify({'ok': False, 'error': 'Query is required.'}), 400

        # Safety guard: AMS Assistant is report/read-only and must never modify app data.
        q_low = user_query.lower()
        write_patterns = [
            r'\b(edit|update|change|modify|delete|remove|void|restore|create|add|insert|post|adjust|merge|rename|wipe)\b',
            r'\b(mark\s+paid|set\s+paid|clear\s+bill|close\s+bill)\b'
        ]
        if any(re.search(p, q_low) for p in write_patterns):
            return jsonify({
                'ok': True,
                'intent': 'read_only_guard',
                'title': 'Read-Only Assistant',
                'answer': 'AMS Assistant is read-only. It can only view/analyze data and export reports, not change records.',
                'summary': '',
                'rows': [],
                'row_count': 0,
                'excel_url': ''
            })

        start_date, end_date = _ams_parse_date_range(user_query)
        material_name = _ams_best_material_match(user_query)
        material_keyword = _ams_material_keyword(user_query)
        client_obj = _ams_best_client_match(user_query)
        intent = _ams_detect_intent(user_query)
        intent, client_obj, material_name, start_date, end_date = _ams_resolve_followup(
            user_query, intent, client_obj, material_name, start_date, end_date
        )
        wants_excel = any(x in user_query.lower() for x in ['excel', 'xlsx', 'sheet', 'download'])

        if intent == 'losses':
            result = _ams_query_losses(start_date, end_date, material_name=material_name)
        elif intent == 'material_received':
            result = _ams_query_material_flow(start_date, end_date, material_name=material_name, flow_type='IN')
        elif intent == 'material_delivered':
            result = _ams_query_material_flow(start_date, end_date, material_name=material_name, flow_type='OUT')
        elif intent == 'grn':
            result = _ams_query_grn(start_date, end_date, material_name=material_name)
        elif intent == 'client_ledger':
            if not client_obj:
                return jsonify({'ok': True, 'answer': 'Please include the client name or code for client ledger queries.'})
            result = _ams_query_client_ledger(client_obj, start_date, end_date, material_name=material_name)
        elif intent == 'client_overview':
            if not client_obj:
                return jsonify({'ok': True, 'answer': 'Please include the client name or code for client overview query.'})
            result = _ams_query_client_overview(client_obj)
        elif intent == 'material_ledger':
            result = _ams_query_material_ledger(start_date, end_date, material_name=material_name)
        elif intent == 'client_remaining':
            if not client_obj:
                return jsonify({'ok': True, 'answer': 'Please include the client name or code for remaining balance query.'})
            result = _ams_query_client_remaining(client_obj, material_name=material_name, material_keyword=material_keyword)
        else:
            # Smart fallback: if a client is recognized, return a full client overview.
            if client_obj:
                result = _ams_query_client_overview(client_obj)
                intent = 'client_overview'
            else:
                return jsonify({
                    'ok': True,
                    'answer': 'I can help with: losses, delivered/received quantity, GRN summary, client ledger, material ledger, client remaining material, and client overview. Include date range and optional material/client.'
                })

        rows = result.get('rows') or []
        summary_text = result.get('summary') or ''
        answer = summary_text
        api_key = _ams_get_configured_api_key()
        ai_answer = _ams_call_openai(api_key, user_query, summary_text, rows)
        if ai_answer:
            answer = ai_answer
        elif rows:
            answer = f"{summary_text}\nRows found: {len(rows)}"

        export_url = ''
        if wants_excel and rows:
            import pandas as pd
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='openpyxl') as writer:
                pd.DataFrame(rows).to_excel(writer, index=False, sheet_name='Result')
            out.seek(0)
            _ams_cleanup_export_cache()
            token = secrets.token_urlsafe(24)
            AMS_ASSISTANT_EXPORT_CACHE[token] = {
                'user_id': current_user.id,
                'tenant_id': getattr(current_user, 'tenant_id', None),
                'filename': f"ams_assistant_{pk_now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                'content': out.getvalue(),
                'mimetype': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'expires_at': time.time() + 1800,
            }
            export_url = url_for('ams_assistant_export_api', token=token)

        _ams_set_context_for_user({
            'intent': intent,
            'client_code': (client_obj.code if client_obj else ''),
            'client_name': (client_obj.name if client_obj else ''),
            'material_name': material_name or '',
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
        })

        return jsonify({
            'ok': True,
            'intent': intent,
            'title': result.get('title') or 'Result',
            'date_range': {'start': start_date.isoformat(), 'end': end_date.isoformat()},
            'answer': answer,
            'summary': summary_text,
            'rows': rows[:200],
            'row_count': len(rows),
            'excel_url': export_url,
        })
        
    except Exception as e:
        app.logger.exception("AMS assistant chat failed")
        return jsonify({'ok': False, 'error': f'Assistant failed: {str(e)}'}), 500


@app.route('/api/ams_assistant/export/<string:token>')
@login_required
def ams_assistant_export_api(token):
    _ams_cleanup_export_cache()
    rec = AMS_ASSISTANT_EXPORT_CACHE.get(token)
    if not rec:
        flash('Export link expired. Please run the assistant query again.', 'warning')
        return redirect(url_for('ams_assistant_page'))
    if rec.get('user_id') != current_user.id:
        flash('Unauthorized export link.', 'danger')
        return redirect(url_for('ams_assistant_page'))
    return send_file(
        io.BytesIO(rec.get('content') or b''),
        as_attachment=True,
        download_name=rec.get('filename') or 'ams_assistant.xlsx',
        mimetype=rec.get('mimetype') or 'application/octet-stream'
    )


@app.route('/export_clients')
@login_required
def export_clients():
    if current_user.role not in ['admin', 'root']:
        flash('Only tenant admin or root can run import/export operations.', 'danger')
        return redirect(url_for('index'))
    import pandas as pd

    clients = Client.query.order_by(Client.name.asc()).all()
    pending_rows = db.session.query(
        PendingBill.client_code,
        func.sum(PendingBill.amount)
    ).filter(
        PendingBill.is_void == False,
        PendingBill.is_paid == False
    ).group_by(PendingBill.client_code).all()
    pending_map = {code: float(total or 0) for code, total in pending_rows if code}

    data = []
    for c in clients:
        data.append({
            'client_name': c.name,
            'client_code': c.code,
            'phone': c.phone or '',
            'address': c.address or '',
            'location_url': c.location_url or '',
            'category': c.category or '',
            'status': 'ACTIVE' if c.is_active else 'INACTIVE',
            'financial_book_no': c.financial_book_no or '',
            'financial_page_no': c.financial_page or '',
            'cement_book_no': c.cement_book_no or '',
            'cement_page_no': c.cement_page or '',
            'steel_book_no': c.steel_book_no or '',
            'steel_page_no': c.steel_page or '',
            'other_book_no': c.book_no or '',
            'notes': c.page_notes or '',
            'pending_amount': float(pending_map.get(c.code, 0.0))
        })

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        pd.DataFrame(data).to_excel(writer, index=False, sheet_name='Clients')
    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"clients_full_{pk_today().strftime('%Y%m%d')}.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


# ==================== TENANT ROUTES ====================

@app.route('/tenants')
@login_required
def tenants_dashboard():
    require_root()
    q = (request.args.get('q') or '').strip()
    status = (request.args.get('status') or '').strip().lower()

    tenants_query = Tenant.query.filter(Tenant.name != DEFAULT_TENANT_NAME)
    if q:
        tenants_query = tenants_query.filter(func.lower(func.trim(Tenant.name)).like(f"%{q.lower()}%"))
    if status in ('active', 'suspended'):
        tenants_query = tenants_query.filter(Tenant.status == status)

    tenants = tenants_query.order_by(Tenant.created_at.desc()).all()
    tenant_ids = [t.id for t in tenants]
    tenant_users_map = {}
    if tenant_ids:
        tenant_users = User.query.filter(User.tenant_id.in_(tenant_ids)).order_by(User.username.asc()).all()
        for u in tenant_users:
            tenant_users_map.setdefault(u.tenant_id, []).append(u)
    total = Tenant.query.filter(Tenant.name != DEFAULT_TENANT_NAME).count()
    active = Tenant.query.filter(Tenant.name != DEFAULT_TENANT_NAME, Tenant.status == 'active').count()
    suspended = Tenant.query.filter(Tenant.name != DEFAULT_TENANT_NAME, Tenant.status == 'suspended').count()
    expiring_soon = Tenant.query.filter(
        Tenant.name != DEFAULT_TENANT_NAME,
        Tenant.expiry_date.isnot(None),
        Tenant.expiry_date <= (pk_today() + timedelta(days=30))
    ).count()
    root_username = os.environ.get('ROOT_USERNAME', 'root')
    root_recovery_unused = RootRecoveryCode.query.filter(
        RootRecoveryCode.username == root_username,
        RootRecoveryCode.used_at.is_(None)
    ).count()

    return render_template(
        'tenants.html',
        tenants=tenants,
        total=total,
        active=active,
        suspended=suspended,
        expiring_soon=expiring_soon,
        root_recovery_unused=root_recovery_unused,
        tenant_users_map=tenant_users_map,
        q=q,
        status_filter=status,
        test_tenant_name=TEST_TENANT_NAME
    )


@app.route('/tenants/create', methods=['POST'])
@login_required
def tenants_create():
    require_root()
    ensure_user_table_tenant_unique()
    name = (request.form.get('name') or '').strip()
    status = (request.form.get('status') or 'active').strip()
    subscription_plan = (request.form.get('subscription_plan') or '').strip()
    expiry_date_raw = (request.form.get('expiry_date') or '').strip()
    expiry_date = None
    if expiry_date_raw:
        try:
            expiry_date = datetime.strptime(expiry_date_raw, '%Y-%m-%d').date()
        except ValueError:
            flash('Invalid expiry date. Use YYYY-MM-DD format.', 'danger')
            return redirect(url_for('tenants_dashboard'))
    if not name:
        flash('Tenant name required', 'danger')
        return redirect(url_for('tenants_dashboard'))

    existing = Tenant.query.filter_by(name=name).first()
    if existing:
        flash('Tenant already exists', 'warning')
        return redirect(url_for('tenants_dashboard'))

    tenant = Tenant(
        name=name,
        status=status,
        subscription_plan=subscription_plan,
        expiry_date=expiry_date
    )
    db.session.add(tenant)
    db.session.flush()

    # Create a default tenant admin account
    default_username = os.environ.get('DEFAULT_TENANT_ADMIN_USERNAME', 'admin')
    default_password = os.environ.get('DEFAULT_TENANT_ADMIN_PASSWORD', 'Admin@12345')
    existing_admin = User.query.filter_by(username=default_username, tenant_id=tenant.id).first()
    if not existing_admin:
        db.session.add(User(
            username=default_username,
            password_hash=generate_password_hash(default_password),
            password_plain=default_password,
            role='admin',
            status='active',
            tenant_id=tenant.id
        ))
    db.session.commit()
    audit_log(current_user, tenant.id, 'tenant.create', f'name={name}')
    flash(f'Tenant created. Default admin login: {default_username} / {default_password}', 'success')
    return redirect(url_for('tenants_dashboard'))


@app.route('/tenants/<tenant_id>/reset_admin', methods=['POST'])
@login_required
def tenants_reset_admin(tenant_id):
    require_root()
    tenant = db.session.get(Tenant, tenant_id)
    if not tenant:
        flash('Tenant not found', 'danger')
        return redirect(url_for('tenants_dashboard'))

    default_username = os.environ.get('DEFAULT_TENANT_ADMIN_USERNAME', 'admin')
    default_password = os.environ.get('DEFAULT_TENANT_ADMIN_PASSWORD', 'Admin@12345')

    admin_user = User.query.filter_by(username=default_username, tenant_id=tenant.id).first()
    if not admin_user:
        admin_user = User(
            username=default_username,
            password_hash=generate_password_hash(default_password),
            password_plain=default_password,
            role='admin',
            status='active',
            tenant_id=tenant.id
        )
        db.session.add(admin_user)
    else:
        admin_user.password_hash = generate_password_hash(default_password)
        admin_user.password_plain = default_password
        admin_user.status = 'active'

    db.session.commit()
    audit_log(current_user, tenant.id, 'tenant.reset_admin', f'username={default_username}')
    flash(f'Reset admin for {tenant.name}. Login: {default_username} / {default_password}', 'success')
    return redirect(url_for('tenants_dashboard'))


@app.route('/tenants/<tenant_id>/reset_missing_passwords', methods=['POST'])
@login_required
def tenants_reset_missing_passwords(tenant_id):
    require_root()
    tenant = db.session.get(Tenant, tenant_id)
    if not tenant:
        flash('Tenant not found', 'danger')
        return redirect(url_for('tenants_dashboard'))

    default_user_password = os.environ.get('DEFAULT_TENANT_USER_PASSWORD', 'User@12345')
    default_admin_password = os.environ.get('DEFAULT_TENANT_ADMIN_PASSWORD', 'Admin@12345')

    users = User.query.filter_by(tenant_id=tenant.id).all()
    updated = 0
    for u in users:
        if (u.password_plain or '').strip():
            continue
        new_pw = default_admin_password if (u.role or '').strip().lower() == 'admin' else default_user_password
        u.password_hash = generate_password_hash(new_pw)
        u.password_plain = new_pw
        updated += 1

    db.session.commit()
    audit_log(current_user, tenant.id, 'tenant.reset_missing_passwords', f'updated={updated}')
    if updated:
        flash(f'Filled missing passwords for {updated} user(s) in {tenant.name}.', 'success')
    else:
        flash('All tenant users already have stored passwords.', 'info')
    return redirect(url_for('tenants_dashboard'))


@app.route('/tenants/<tenant_id>/status', methods=['POST'])
@login_required
def tenants_update_status(tenant_id):
    require_root()
    status = (request.form.get('status') or '').strip()
    tenant = db.session.get(Tenant, tenant_id)
    if not tenant:
        flash('Tenant not found', 'danger')
        return redirect(url_for('tenants_dashboard'))
    if status not in ('active', 'suspended'):
        flash('Invalid status', 'danger')
        return redirect(url_for('tenants_dashboard'))
    tenant.status = status
    db.session.commit()
    audit_log(current_user, tenant.id, 'tenant.status', f'status={status}')
    flash('Tenant status updated', 'success')
    return redirect(url_for('tenants_dashboard'))


@app.route('/tenants/<tenant_id>/update', methods=['POST'])
@login_required
def tenants_update(tenant_id):
    require_root()
    tenant = db.session.get(Tenant, tenant_id)
    if not tenant:
        flash('Tenant not found', 'danger')
        return redirect(url_for('tenants_dashboard'))

    name = (request.form.get('name') or '').strip()
    status = (request.form.get('status') or '').strip().lower()
    subscription_plan = (request.form.get('subscription_plan') or '').strip()
    expiry_date_raw = (request.form.get('expiry_date') or '').strip()

    if not name:
        flash('Tenant name required', 'danger')
        return redirect(url_for('tenants_dashboard'))
    if status not in ('active', 'suspended'):
        flash('Invalid status', 'danger')
        return redirect(url_for('tenants_dashboard'))

    duplicate = Tenant.query.filter(
        Tenant.id != tenant.id,
        func.lower(func.trim(Tenant.name)) == name.lower()
    ).first()
    if duplicate:
        flash('Another tenant already uses this name.', 'danger')
        return redirect(url_for('tenants_dashboard'))

    expiry_date = None
    if expiry_date_raw:
        try:
            expiry_date = datetime.strptime(expiry_date_raw, '%Y-%m-%d').date()
        except ValueError:
            flash('Invalid expiry date. Use YYYY-MM-DD format.', 'danger')
            return redirect(url_for('tenants_dashboard'))

    tenant.name = name
    tenant.status = status
    tenant.subscription_plan = subscription_plan
    tenant.expiry_date = expiry_date
    db.session.commit()
    audit_log(current_user, tenant.id, 'tenant.update', f'name={name}, status={status}, expiry={expiry_date or ""}')
    flash('Tenant details updated', 'success')
    return redirect(url_for('tenants_dashboard'))


@app.route('/tenants/<tenant_id>/backup_history')
@login_required
def tenants_backup_history(tenant_id):
    require_root()
    tenant = db.session.get(Tenant, tenant_id)
    if not tenant:
        flash('Tenant not found', 'danger')
        return redirect(url_for('tenants_dashboard'))
    rows = TenantWipeBackupHistory.query.filter_by(tenant_id=tenant.id).order_by(TenantWipeBackupHistory.created_at.desc()).all()
    return render_template('tenant_backup_history.html', tenant=tenant, rows=rows)


@app.route('/tenants/backup_history/download/<int:history_id>')
@login_required
def tenants_backup_history_download(history_id):
    require_root()
    row = db.session.get(TenantWipeBackupHistory, history_id)
    if not row:
        flash('Backup history record not found.', 'danger')
        return redirect(url_for('tenants_dashboard'))
    fpath = (row.backup_path or '').strip()
    if not fpath or not os.path.exists(fpath):
        flash('Backup file no longer exists on disk.', 'danger')
        return redirect(url_for('tenants_backup_history', tenant_id=row.tenant_id))
    return send_file(
        fpath,
        as_attachment=True,
        download_name=row.backup_filename or os.path.basename(fpath),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.route('/tenants/backup_history/restore/<int:history_id>', methods=['POST'])
@login_required
def tenants_backup_history_restore(history_id):
    require_root()
    row = db.session.get(TenantWipeBackupHistory, history_id)
    if not row:
        flash('Backup history record not found.', 'danger')
        return redirect(url_for('tenants_dashboard'))

    tenant_id = row.tenant_id
    tenant_name = row.tenant_name
    backup_filename = row.backup_filename
    fpath = (row.backup_path or '').strip()
    if not fpath or not os.path.exists(fpath):
        flash('Backup file no longer exists on disk.', 'danger')
        return redirect(url_for('tenants_backup_history', tenant_id=tenant_id))

    try:
        from blueprints.import_export import _run_full_raw_import_bytes

        with open(fpath, 'rb') as f:
            file_bytes = f.read()

        scope_ctx = {
            'scope': 'tenant',
            'target_tenant_id': tenant_id,
            'target_tenant_name': tenant_name,
            'role': 'root',
        }
        _run_full_raw_import_bytes(
            file_bytes=file_bytes,
            scope_ctx=scope_ctx,
            mode='replace_tenant_data',
            source_file_name=backup_filename or os.path.basename(fpath)
        )
        flash(f"Backup restored to tenant '{tenant_name}' successfully.", 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Backup restore failed: {e}', 'danger')

    return redirect(url_for('tenants_backup_history', tenant_id=tenant_id))


@app.route('/root/backup-settings')
@login_required
def root_backup_settings():
    require_root()
    row = _get_or_create_root_backup_settings()
    history_rows = RootBackupEmailHistory.query.order_by(RootBackupEmailHistory.created_at.desc()).limit(200).all()
    return render_template('root_backup_settings.html', row=row, history_rows=history_rows)


@app.route('/root/backup-settings/save', methods=['POST'])
@login_required
def root_backup_settings_save():
    require_root()
    row = _get_or_create_root_backup_settings()

    row.enabled = ('enabled' in request.form)
    row.frequency = 'hourly'
    row.recipient_emails = (request.form.get('recipient_emails') or '').strip()
    row.include_full_raw_xlsx = ('include_full_raw_xlsx' in request.form)
    row.include_sqlite_db = ('include_sqlite_db' in request.form)
    row.subject_prefix = (request.form.get('subject_prefix') or 'PWARE Root Backup').strip() or 'PWARE Root Backup'
    try:
        keep_count = int(request.form.get('keep_history_count') or row.keep_history_count or 200)
    except Exception:
        keep_count = 200
    row.keep_history_count = max(10, min(5000, keep_count))

    if not row.include_full_raw_xlsx and not row.include_sqlite_db:
        flash('Select at least one backup payload (XLSX or DB).', 'danger')
        return redirect(url_for('root_backup_settings'))

    db.session.commit()
    _cleanup_root_backup_history(row.keep_history_count)
    flash('Root backup settings updated.', 'success')
    return redirect(url_for('root_backup_settings'))


@app.route('/root/backup-settings/send-now', methods=['POST'])
@login_required
def root_backup_settings_send_now():
    require_root()
    ok, msg = _send_hourly_all_tenants_backup_email(trigger_type='manual-send-now', force_send=True)
    flash(msg, 'success' if ok else 'danger')
    return redirect(url_for('root_backup_settings'))


@app.route('/root/backup-settings/history/download/<int:history_id>')
@login_required
def root_backup_settings_history_download(history_id):
    require_root()
    row = db.session.get(RootBackupEmailHistory, history_id)
    if not row:
        flash('History record not found.', 'danger')
        return redirect(url_for('root_backup_settings'))
    fpath = (row.backup_path or '').strip()
    if not fpath or not os.path.exists(fpath):
        flash('Backup ZIP not found on disk.', 'danger')
        return redirect(url_for('root_backup_settings'))
    return send_file(
        fpath,
        as_attachment=True,
        download_name=row.attachment_name or os.path.basename(fpath),
        mimetype='application/zip'
    )


@app.route('/root/backup-settings/history/clear', methods=['POST'])
@login_required
def root_backup_settings_history_clear():
    require_root()
    rows = RootBackupEmailHistory.query.all()
    removed = 0
    for r in rows:
        fpath = (r.backup_path or '').strip()
        if fpath and os.path.exists(fpath):
            try:
                os.remove(fpath)
            except Exception:
                pass
        db.session.delete(r)
        removed += 1
    db.session.commit()
    flash(f'Cleared root backup history ({removed} record(s)).', 'success')
    return redirect(url_for('root_backup_settings'))


@app.route('/tenants/<tenant_id>/delete', methods=['POST'])
@login_required
def tenants_delete(tenant_id):
    require_root()
    tenant = db.session.get(Tenant, tenant_id)
    if not tenant:
        flash('Tenant not found', 'danger')
        return redirect(url_for('tenants_dashboard'))
    if not can_hard_delete_tenant(tenant):
        flash('Hard delete allowed only for test tenant', 'danger')
        return redirect(url_for('tenants_dashboard'))
    hard_delete_tenant(tenant.id)
    audit_log(current_user, tenant_id, 'tenant.delete', f'name={tenant.name}')
    flash('Test tenant deleted permanently', 'success')
    return redirect(url_for('tenants_dashboard'))


# ==================== SETTINGS ROUTES ====================

EDITABLE_USER_PERMISSION_FIELDS = [
    'can_view_dashboard',
    'can_manage_grn',
    'can_view_stock',
    'can_view_daily',
    'can_view_history',
    'can_manage_bookings',
    'can_manage_payments',
    'can_manage_sales',
    'can_view_delivery_rent',
    'can_view_client_ledger',
    'can_view_supplier_ledger',
    'can_view_decision_ledger',
    'can_manage_pending_bills',
    'can_view_reports',
    'can_manage_notifications',
    'can_import_export',
    'can_manage_clients',
    'can_manage_suppliers',
    'can_manage_materials',
    'can_manage_delivery_persons',
    'can_access_settings',
]


def _permissions_from_request_form():
    return {field: (field in request.form) for field in EDITABLE_USER_PERMISSION_FIELDS}


@app.route('/settings')
@login_required
def settings():
    if current_user.role == 'root':
        return redirect(url_for('tenants_dashboard'))
    if current_user.role != 'admin' and not _user_can('can_access_settings'):
        flash('Unauthorized: Admin access required.', 'danger')
        return redirect(url_for('index'))
    settings_obj = Settings.query.first()
    if not settings_obj:
        settings_obj = Settings()
    categories = MaterialCategory.query.order_by(MaterialCategory.name.asc()).all()
    recon_report = session.pop('recon_report', None)
    return render_template('settings.html', users=User.query.all(), settings=settings_obj, categories=categories, recon_report=recon_report)


@app.route('/add_user', methods=['POST'])
@login_required
def add_user():
    if current_user.role != 'admin':
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))
    un = request.form.get('username', '').strip()
    raw_pw = str(request.form.get('password') or '').strip()
    if not raw_pw:
        raw_pw = os.environ.get('DEFAULT_TENANT_USER_PASSWORD', 'User@12345')
    pw = generate_password_hash(raw_pw)
    rl = request.form.get('role', 'user')

    if not un:
        flash('Username is required', 'danger')
        return redirect(url_for('settings'))
    if User.query.filter_by(username=un, tenant_id=current_user.tenant_id).first():
        flash('Username already exists in this tenant', 'danger')
    else:
        permission_values = _permissions_from_request_form()
        restrict_backdated_grn_edit = ('restrict_backdated_edit' in request.form)
        new_u = User(username=un,
                     password_hash=pw,
                     password_plain=raw_pw,
                     role=rl,
                     restrict_backdated_edit=restrict_backdated_grn_edit,
                     can_manage_directory=(
                         permission_values.get('can_manage_clients', False)
                         or permission_values.get('can_manage_suppliers', False)
                         or permission_values.get('can_manage_materials', False)
                         or permission_values.get('can_manage_delivery_persons', False)
                     ),
                     **permission_values)
        db.session.add(new_u)
        db.session.commit()
        flash(f'User created. Login: Tenant Name + {un} / {raw_pw}', 'success')
    return redirect(url_for('settings'))


@app.route('/edit_user_permissions/<int:id>', methods=['POST'])
@login_required
def edit_user_permissions(id):
    if current_user.role != 'admin':
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))
    u = db.session.get(User, id)
    if u and u.role != 'root' and u.username != 'admin':
        u.role = request.form.get('role', 'user')
        raw_pw = str(request.form.get('password') or '').strip()
        if raw_pw:
            u.password_hash = generate_password_hash(raw_pw)
            u.password_plain = raw_pw
        permission_values = _permissions_from_request_form()
        for field, value in permission_values.items():
            setattr(u, field, value)
        u.can_manage_directory = (
            permission_values.get('can_manage_clients', False)
            or permission_values.get('can_manage_suppliers', False)
            or permission_values.get('can_manage_materials', False)
            or permission_values.get('can_manage_delivery_persons', False)
        )
        u.restrict_backdated_edit = ('restrict_backdated_edit' in request.form)
        db.session.commit()
        flash('Permissions Updated', 'success')
    return redirect(url_for('settings'))


@app.route('/delete_user/<int:id>', methods=['POST'])
@login_required
def delete_user(id):
    if current_user.role != 'admin':
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))
    u = db.session.get(User, id)
    if u and u.role != 'root' and u.username != 'admin':
        if u.id == current_user.id:
            flash('You cannot deactivate your own account.', 'danger')
            return redirect(url_for('settings'))
        # Orphan-safe strategy: never hard-delete users. Keep historical references intact.
        u.status = 'inactive'
        if u.role != 'admin':
            u.role = 'user'
        db.session.commit()
        flash('User deactivated (kept for historical records).', 'warning')
    return redirect(url_for('settings'))


@app.route('/change_password', methods=['POST'])
@login_required
def change_password():
    raw_pw = str(request.form.get('password') or '').strip()
    if not raw_pw:
        flash('Password is required', 'danger')
        return redirect(url_for('settings'))
    current_user.password_hash = generate_password_hash(raw_pw)
    current_user.password_plain = raw_pw
    db.session.commit()
    flash('Password Updated', 'success')
    return redirect(url_for('settings'))


@app.route('/void_audit')
@login_required
def void_audit_page():
    if current_user.role == 'root':
        return redirect(url_for('tenants_dashboard'))
    if current_user.role != 'admin' and not _user_can('can_access_settings'):
        flash('Unauthorized: Admin access required.', 'danger')
        return redirect(url_for('index'))

    section = (request.args.get('section') or 'all').strip().lower()
    q = (request.args.get('q') or '').strip().lower()
    rows = []

    def _entry_voided_by_edit(entry_obj):
        if not entry_obj or not entry_obj.is_void:
            return False
        if (entry_obj.nimbus_no or '').strip().lower() != 'direct sale':
            return False
        bill_ref = (entry_obj.bill_no or '').strip() or (entry_obj.auto_bill_no or '').strip()
        if not bill_ref:
            return False
        newer_active = Entry.query.filter(
            Entry.id != entry_obj.id,
            Entry.is_void == False,
            Entry.nimbus_no == entry_obj.nimbus_no,
            Entry.type == entry_obj.type,
            Entry.material == entry_obj.material,
            Entry.client == entry_obj.client,
            Entry.qty == entry_obj.qty,
            or_(Entry.bill_no == bill_ref, Entry.auto_bill_no == bill_ref)
        ).order_by(Entry.id.desc()).first()
        return bool(newer_active and newer_active.id > entry_obj.id)

    def _pending_voided_by_edit(pb_obj):
        if not pb_obj or not pb_obj.is_void:
            return False
        bill_ref = (pb_obj.bill_no or '').strip()
        if not bill_ref:
            return False
        if not (pb_obj.reason or '').strip().lower().startswith('direct sale'):
            return False
        newer_active = PendingBill.query.filter(
            PendingBill.id != pb_obj.id,
            PendingBill.is_void == False,
            PendingBill.bill_no == bill_ref,
            PendingBill.client_name == pb_obj.client_name
        ).order_by(PendingBill.id.desc()).first()
        return bool(newer_active and newer_active.id > pb_obj.id)

    def _push_row(entity, obj_id, title, details, when_dt, status_label):
        rows.append({
            'entity': entity,
            'id': obj_id,
            'title': title,
            'details': details,
            'when': when_dt,
            'status': status_label,
            'is_void': status_label.startswith('Voided'),
            'is_suspended': status_label.startswith('Suspended'),
        })

    voided_enabled = section in ('all', 'voided', 'transactions')
    suspended_enabled = section in ('all', 'suspended', 'directory')

    if voided_enabled:
        for e in Entry.query.filter_by(is_void=True).all():
            dt = _parse_dt_safe(f"{(e.date or '').strip()} {(e.time or '').strip()}".strip()) or _parse_dt_safe(e.date)
            _push_row(
                'Entry', e.id,
                f"Entry #{e.id} ({e.type or '-'})",
                f"Client: {e.client or '-'} | Material: {e.material or '-'} | Bill: {e.bill_no or e.auto_bill_no or '-'}",
                dt,
                ('Voided by Edit' if _entry_voided_by_edit(e) else 'Voided Transaction')
            )
        for b in Booking.query.filter_by(is_void=True).all():
            _push_row(
                'Booking', b.id,
                f"Booking #{b.id}",
                f"Client: {b.client_name or '-'} | Bill: {b.manual_bill_no or b.auto_bill_no or '-'} | Amount: {float(b.amount or 0):.2f}",
                _parse_dt_safe(b.date_posted),
                'Voided Bill'
            )
        for p in Payment.query.filter_by(is_void=True).all():
            _push_row(
                'Payment', p.id,
                f"Payment #{p.id}",
                f"Client: {p.client_name or '-'} | Bill: {p.manual_bill_no or p.auto_bill_no or '-'} | Amount: {float(p.amount or 0):.2f}",
                _parse_dt_safe(p.date_posted),
                'Voided Bill'
            )
        for s in DirectSale.query.filter_by(is_void=True).all():
            _push_row(
                'DirectSale', s.id,
                f"Direct Sale #{s.id}",
                f"Client: {s.client_name or '-'} | Bill: {s.manual_bill_no or s.auto_bill_no or '-'} | Amount: {float(s.amount or 0):.2f}",
                _parse_dt_safe(s.date_posted),
                'Voided Bill'
            )
        for pb in PendingBill.query.filter_by(is_void=True).all():
            _push_row(
                'PendingBill', pb.id,
                f"Pending Bill #{pb.id}",
                f"Client: {pb.client_name or '-'} | Bill: {pb.bill_no or '-'} | Amount: {float(pb.amount or 0):.2f}",
                _parse_dt_safe(pb.created_at),
                ('Voided by Edit' if _pending_voided_by_edit(pb) else 'Voided Bill')
            )
        for dr in DeliveryRent.query.filter_by(is_void=True).all():
            _push_row(
                'DeliveryRent', dr.id,
                f"Delivery Rent #{dr.id}",
                f"Driver: {dr.delivery_person_name or '-'} | Bill: {dr.bill_no or '-'} | Amount: {float(dr.amount or 0):.2f}",
                _parse_dt_safe(dr.date_posted),
                'Voided Transaction'
            )
        for sp in SupplierPayment.query.filter_by(is_void=True).all():
            supplier_name = sp.supplier.name if sp.supplier else f"Supplier #{sp.supplier_id}"
            _push_row(
                'SupplierPayment', sp.id,
                f"Supplier Payment #{sp.id}",
                f"Supplier: {supplier_name} | Amount: {float(sp.amount or 0):.2f} | Method: {sp.method or '-'}",
                _parse_dt_safe(sp.date_posted),
                'Voided Transaction'
            )

    if suspended_enabled:
        for c in Client.query.filter_by(is_active=False).all():
            _push_row(
                'Client', c.id,
                f"Client #{c.id}",
                f"{c.name or '-'} ({c.code or '-'})",
                _parse_dt_safe(c.created_at),
                'Suspended Master'
            )
        for m in Material.query.filter_by(is_active=False).all():
            _push_row(
                'Material', m.id,
                f"Material #{m.id}",
                f"{m.name or '-'} ({m.code or '-'}) | Unit: {m.unit or '-'}",
                _parse_dt_safe(m.created_at),
                'Suspended Master'
            )
        for d in DeliveryPerson.query.filter_by(is_active=False).all():
            _push_row(
                'DeliveryPerson', d.id,
                f"Delivery Person #{d.id}",
                d.name or '-',
                _parse_dt_safe(d.created_at),
                'Suspended Master'
            )

    if q:
        rows = [
            r for r in rows
            if q in f"{r['entity']} {r['title']} {r['details']} {r['status']}".lower()
        ]

    rows.sort(key=lambda x: x.get('when') or datetime.min, reverse=True)

    total = len(rows)
    page = max(1, request.args.get('page', 1, type=int))
    per_page = 50
    pages = max(1, (total + per_page - 1) // per_page)
    if page > pages:
        page = pages
    start = (page - 1) * per_page
    page_rows = rows[start:start + per_page]

    counts = {
        'total': total,
        'voided': sum(1 for r in rows if r['is_void']),
        'suspended': sum(1 for r in rows if r['is_suspended']),
    }

    return render_template(
        'void_audit.html',
        rows=page_rows,
        section=section,
        q=request.args.get('q', ''),
        page=page,
        pages=pages,
        total=total,
        counts=counts
    )


@app.route('/void_audit/restore/<string:entity>/<int:record_id>', methods=['POST'])
@login_required
def restore_audit_record(entity, record_id):
    if current_user.role == 'root':
        return redirect(url_for('tenants_dashboard'))
    if current_user.role != 'admin' and not _user_can('can_access_settings'):
        flash('Unauthorized: Admin access required.', 'danger')
        return redirect(url_for('index'))

    key = (entity or '').strip()
    changed = False
    found = True

    if key == 'Entry':
        obj = db.session.get(Entry, record_id)
        found = bool(obj)
        changed = _set_entry_void_state(obj, False) if obj else False
    elif key == 'Booking':
        obj = db.session.get(Booking, record_id)
        found = bool(obj)
        changed = _set_booking_void_state(obj, False) if obj else False
    elif key == 'Payment':
        obj = db.session.get(Payment, record_id)
        found = bool(obj)
        changed = _set_payment_void_state(obj, False) if obj else False
    elif key == 'DirectSale':
        obj = db.session.get(DirectSale, record_id)
        found = bool(obj)
        changed = _set_direct_sale_void_state(obj, False) if obj else False
    elif key == 'PendingBill':
        obj = db.session.get(PendingBill, record_id)
        found = bool(obj)
        if obj and obj.is_void:
            obj.is_void = False
            changed = True
    elif key == 'DeliveryRent':
        obj = db.session.get(DeliveryRent, record_id)
        found = bool(obj)
        if obj and obj.is_void:
            obj.is_void = False
            changed = True
    elif key == 'SupplierPayment':
        obj = db.session.get(SupplierPayment, record_id)
        found = bool(obj)
        if obj and obj.is_void:
            obj.is_void = False
            changed = True
    elif key == 'Client':
        obj = db.session.get(Client, record_id)
        found = bool(obj)
        if obj and not obj.is_active:
            obj.is_active = True
            changed = True
    elif key == 'Material':
        obj = db.session.get(Material, record_id)
        found = bool(obj)
        if obj and not obj.is_active:
            obj.is_active = True
            changed = True
    elif key == 'DeliveryPerson':
        obj = db.session.get(DeliveryPerson, record_id)
        found = bool(obj)
        if obj and not obj.is_active:
            obj.is_active = True
            changed = True
    else:
        found = False

    if not found:
        flash('Record not found', 'danger')
    elif changed:
        db.session.commit()
        flash('Record restored successfully', 'success')
    else:
        flash('Record is already active/restored', 'info')

    next_url = (request.form.get('next') or '').strip()
    return redirect(next_url or url_for('void_audit_page'))


@app.route('/update_settings', methods=['POST'])
@login_required
def update_settings():
    if current_user.role != 'admin':
        flash('Unauthorized', 'danger')
        return redirect(url_for('settings'))

    settings_obj = Settings.query.first()
    if not settings_obj:
        settings_obj = Settings()
        db.session.add(settings_obj)

    settings_obj.company_name = request.form.get('company_name', settings_obj.company_name or 'FAZAL BUILDING MATERIALS')
    settings_obj.company_address = request.form.get('company_address', settings_obj.company_address or 'JALAL PUR SOBTIAN')
    settings_obj.company_phone = request.form.get('company_phone', settings_obj.company_phone or '+92302-0000993 +92331-0000993')
    settings_obj.currency = request.form.get('currency', settings_obj.currency or 'PKR')
    settings_obj.allow_global_negative_stock = 'allow_global_negative_stock' in request.form
    settings_obj.smtp_host = request.form.get('smtp_host', settings_obj.smtp_host or '').strip()
    settings_obj.smtp_port = int(request.form.get('smtp_port', settings_obj.smtp_port or 587) or 587)
    settings_obj.smtp_user = request.form.get('smtp_user', settings_obj.smtp_user or '').strip()
    smtp_pass = request.form.get('smtp_pass', '').strip().replace(' ', '')
    if smtp_pass:
        settings_obj.smtp_pass = smtp_pass
    elif 'clear_smtp_pass' in request.form:
        settings_obj.smtp_pass = None
    settings_obj.smtp_from = request.form.get('smtp_from', settings_obj.smtp_from or '').strip()
    settings_obj.smtp_use_tls = 'smtp_use_tls' in request.form
    ams_key = request.form.get('ams_openai_api_key', '').strip()
    if ams_key:
        settings_obj.ams_openai_api_key = ams_key
    elif 'clear_ams_openai_api_key' in request.form:
        settings_obj.ams_openai_api_key = None
    settings_obj.notify_daily_time = request.form.get('notify_daily_time', settings_obj.notify_daily_time or '08:00').strip() or '08:00'

    db.session.commit()
    flash('Settings updated successfully', 'success')
    return redirect(url_for('settings'))


def _create_pre_wipe_tenant_backup(tenant):
    """Create tenant-scoped snapshot before destructive wipe and return (filename, path)."""
    from blueprints.import_export import _build_full_raw_export_bytes

    scope_ctx = {
        'scope': 'tenant',
        'target_tenant_id': tenant.id,
        'target_tenant_name': tenant.name,
        'role': 'root',
    }
    content = _build_full_raw_export_bytes(scope_ctx=scope_ctx)
    stamp = pk_now().strftime('%Y%m%d_%H%M%S')
    safe_tenant = re.sub(r'[^A-Za-z0-9_.-]+', '_', (tenant.name or 'tenant')).strip('._') or 'tenant'
    filename = f"pre_wipe_{safe_tenant}_{tenant.id}_{stamp}.xlsx"
    backup_dir = os.path.join(basedir, 'instance', 'root_tenant_wipe_backups', tenant.id)
    os.makedirs(backup_dir, exist_ok=True)
    backup_path = os.path.join(backup_dir, filename)
    with open(backup_path, 'wb') as f:
        f.write(content or b'')
    return filename, backup_path


def _enforce_tenant_wipe_backup_retention(tenant_id, keep=3):
    rows = TenantWipeBackupHistory.query.filter_by(tenant_id=tenant_id).order_by(TenantWipeBackupHistory.created_at.desc()).all()
    if len(rows) <= keep:
        return
    old_rows = rows[keep:]
    for r in old_rows:
        fpath = (r.backup_path or '').strip()
        if fpath and os.path.exists(fpath):
            try:
                os.remove(fpath)
            except Exception:
                pass
        db.session.delete(r)
    db.session.commit()


@app.route('/delete_selected_data', methods=['POST'])
@login_required
def delete_selected_data():
    if current_user.role not in ['admin', 'root']:
        flash('Only tenant admin or root can erase tenant data from settings.', 'danger')
        return redirect(url_for('index'))

    hard_delete_override = request.form.get('hard_delete_override') == '1'
    required_confirm = "DELETE ALL DATA" if hard_delete_override else "DELETE SELECTED"
    if request.form.get('confirm_text') != required_confirm:
        if hard_delete_override:
            flash('Incorrect confirmation text. Type DELETE ALL DATA for hard cleanup.', 'danger')
        else:
            flash('Incorrect confirmation text', 'danger')
        return redirect(url_for('settings'))

    targets = request.form.getlist('delete_targets')
    if not targets:
        flash('No datasets selected for deletion', 'warning')
        return redirect(url_for('settings'))
    tenant_id = current_user.tenant_id
    tenant = db.session.get(Tenant, tenant_id) if tenant_id else None
    if not tenant:
        flash('Tenant context missing. Cannot run wipe.', 'danger')
        return redirect(url_for('settings'))

    history_row = None
    backup_filename = None
    backup_path = None
    if _WIPE_BACKUP_ENABLED:
        try:
            backup_filename, backup_path = _create_pre_wipe_tenant_backup(tenant)
            history_row = TenantWipeBackupHistory(
                tenant_id=tenant.id,
                tenant_name=tenant.name,
                performed_by=getattr(current_user, 'username', None),
                performed_by_role=getattr(current_user, 'role', None),
                targets=', '.join(sorted(set(targets))),
                backup_filename=backup_filename,
                backup_path=backup_path,
                wipe_status='pending',
                note='Snapshot captured before wipe.'
            )
            db.session.add(history_row)
            db.session.commit()
            _enforce_tenant_wipe_backup_retention(tenant.id, keep=3)
        except Exception as e:
            db.session.rollback()
            # Auto-heal for older DBs where new history table was not created yet.
            try:
                db.create_all()
                history_row = TenantWipeBackupHistory(
                    tenant_id=tenant.id,
                    tenant_name=tenant.name,
                    performed_by=getattr(current_user, 'username', None),
                    performed_by_role=getattr(current_user, 'role', None),
                    targets=', '.join(sorted(set(targets))),
                    backup_filename=backup_filename or 'snapshot_missing.xlsx',
                    backup_path=backup_path or '',
                    wipe_status='pending',
                    note='Snapshot captured before wipe (post-autoheal).'
                )
                db.session.add(history_row)
                db.session.commit()
                _enforce_tenant_wipe_backup_retention(tenant.id, keep=3)
            except Exception as e2:
                db.session.rollback()
                flash(f'Pre-wipe backup failed. Wipe blocked. Error: {e2}', 'danger')
                return redirect(url_for('settings'))

    def _tq(model):
        return model.query.filter(model.tenant_id == tenant_id)

    forbidden_targets = {
        'clients',
        'materials',
        'pending_bills',
        'dispatching',
        'receiving',
        'direct_sales',
        'payments',
        'bookings',
        'invoices',
    }
    blocked = sorted(set(targets).intersection(forbidden_targets))
    if blocked and not hard_delete_override:
        flash(f'Hard delete blocked for: {", ".join(blocked)}. Use suspend/void workflows instead.', 'danger')
        return redirect(url_for('settings'))
    if blocked and hard_delete_override:
        flash(f'Hard cleanup override enabled for: {", ".join(blocked)}', 'warning')

    try:
        deleted_info = []
        # Keep core config intact: Users/Roles and Settings.
        # Wipe transactional/master data only.

        full_set = {
            'clients', 'suppliers', 'supplier_payments', 'pending_bills',
            'dispatching', 'receiving', 'grn', 'materials', 'material_categories',
            'direct_sales', 'delivery_rents', 'delivery_persons',
            'invoices', 'payments', 'bookings', 'notifications'
        }
        is_full_wipe = full_set.issubset(set(targets))

        if is_full_wipe:
            # Full transactional reset (everything except users/settings/staff email).
            _tq(FollowUpContact).delete()
            _tq(FollowUpReminder).delete()
            _tq(StaffEmail).delete()
            _tq(PendingBill).delete()
            _tq(Entry).delete()
            _tq(DeliveryItem).delete()
            _tq(Delivery).delete()
            _tq(GRNItem).delete()
            _tq(GRN).delete()
            _tq(DirectSaleItem).delete()
            _tq(DirectSale).delete()
            _tq(WaiveOff).delete()
            _tq(Payment).delete()
            _tq(SupplierPayment).delete()
            _tq(BookingItem).delete()
            _tq(Booking).delete()
            _tq(Invoice).delete()
            _tq(ReconBasket).delete()
            _tq(Supplier).delete()
            _tq(DeliveryRent).delete()
            _tq(DeliveryPerson).delete()
            _tq(Material).delete()
            _tq(MaterialCategory).delete()
            _tq(Client).delete()
            _tq(BillCounter).delete()
            db.session.add(BillCounter(tenant_id=tenant_id, namespace=AUTO_BILL_NS_DEFAULT, count=1000))
            deleted_info.append('Full Wipe (All Transactions)')
            if history_row:
                history_row.wipe_status = 'completed'
                history_row.note = f'Completed full wipe. Targets: {", ".join(sorted(set(targets)))}'
            db.session.commit()
            flash(f'Data Wiped: {", ".join(deleted_info)}', 'danger')
            return redirect(url_for('settings'))

        if 'pending_bills' in targets:
            # Bulk delete does not trigger ORM cascades, so clear dependent follow-up tables first.
            _tq(FollowUpContact).delete()
            _tq(FollowUpReminder).delete()
            _tq(PendingBill).delete()
            deleted_info.append('Pending Bills + Follow-ups')

        if 'notifications' in targets:
            _tq(FollowUpContact).delete()
            _tq(FollowUpReminder).delete()
            _tq(StaffEmail).delete()
            deleted_info.append('Notification Data (Follow-ups + Staff Emails)')

        if 'dispatching' in targets:
            _tq(Entry).filter_by(type='OUT').delete()
            _tq(DeliveryItem).delete()
            _tq(Delivery).delete()
            deleted_info.append('Dispatching Entries')

        if 'receiving' in targets:
            _tq(Entry).filter_by(type='IN').delete()
            deleted_info.append('Receiving Entries')

        if 'grn' in targets:
            _tq(GRNItem).delete()
            _tq(GRN).delete()
            deleted_info.append('GRN Records')

        if 'supplier_payments' in targets:
            _tq(SupplierPayment).delete()
            deleted_info.append('Supplier Payments')

        if 'suppliers' in targets:
            _tq(GRN).update({'supplier_id': None}, synchronize_session=False)
            _tq(Supplier).delete()
            deleted_info.append('Suppliers')

        if 'direct_sales' in targets:
            linked_invoice_ids = [
                row[0] for row in _tq(DirectSale).with_entities(DirectSale.invoice_id)
                .filter(DirectSale.invoice_id.isnot(None)).distinct().all()
            ]
            _tq(DeliveryRent).delete()
            _tq(DirectSaleItem).delete()
            _tq(DirectSale).delete()
            _tq(Entry).filter(Entry.nimbus_no == 'Direct Sale').delete(synchronize_session=False)
            _tq(PendingBill).filter(
                func.lower(func.coalesce(PendingBill.reason, '')).like('direct sale%')
            ).delete(synchronize_session=False)
            if linked_invoice_ids:
                _tq(Invoice).filter(Invoice.id.in_(linked_invoice_ids)).delete(synchronize_session=False)
            deleted_info.append('Direct Sales')

        if 'payments' in targets:
            _tq(WaiveOff).delete()
            _tq(Payment).delete()
            _tq(PendingBill).filter(
                func.lower(func.coalesce(PendingBill.reason, '')).like('payment received%')
            ).delete(synchronize_session=False)
            deleted_info.append('Payments')

        if 'delivery_rents' in targets:
            _tq(DeliveryRent).delete()
            deleted_info.append('Delivery Rents')

        if 'delivery_persons' in targets:
            _tq(DeliveryPerson).delete()
            deleted_info.append('Delivery Persons')

        if 'bookings' in targets:
            _tq(BookingItem).delete()
            _tq(Booking).delete()
            _tq(PendingBill).filter(
                func.lower(func.coalesce(PendingBill.reason, '')).like('booking:%')
            ).delete(synchronize_session=False)
            deleted_info.append('Bookings')

        if 'invoices' in targets:
            _tq(Invoice).delete()
            deleted_info.append('Invoices')

        if 'materials' in targets:
            _tq(Material).delete()
            deleted_info.append('Materials')

        if 'material_categories' in targets:
            _tq(Material).update({'category_id': None}, synchronize_session=False)
            _tq(MaterialCategory).delete()
            deleted_info.append('Material Categories')

        if 'clients' in targets:
            _tq(Client).delete()
            _tq(ReconBasket).delete()
            deleted_info.append('Clients + Reconciliation Basket')

        # Always clean orphan invoices to avoid hidden "bill already exists" residue.
        orphan_invoice_count = _tq(Invoice).filter(
            ~exists().where(and_(DirectSale.invoice_id == Invoice.id, DirectSale.tenant_id == tenant_id)),
            ~exists().where(and_(Entry.invoice_id == Invoice.id, Entry.tenant_id == tenant_id))
        ).delete(synchronize_session=False)
        if orphan_invoice_count:
            deleted_info.append(f'Orphan Invoices ({orphan_invoice_count})')

        if history_row:
            history_row.wipe_status = 'completed'
            history_row.note = f'Completed selective wipe. Targets: {", ".join(sorted(set(targets)))}'
        db.session.commit()
        flash(f'Data Wiped: {", ".join(deleted_info)}', 'danger')
    except Exception as e:
        db.session.rollback()
        if history_row:
            try:
                history_row.wipe_status = 'failed'
                history_row.note = f'Wipe failed after snapshot: {e}'
                db.session.commit()
            except Exception:
                db.session.rollback()
        flash(f'Wipe failed: {str(e)}', 'danger')

    return redirect(url_for('settings'))


@app.route('/delete_all_data', methods=['POST'])
@login_required
def delete_all_data():
    return redirect(url_for('settings'))


@app.route('/generate_dummy_data')
@login_required
def generate_dummy_data():
    flash('This legacy test-data feature has been permanently removed.', 'warning')
    return redirect(url_for('settings'))


# ==================== GRN ROUTES ====================

def calculate_grn_total(grn):
    item_total = sum((item.qty or 0) * (item.price_at_time or 0) for item in grn.items)
    expenses = (grn.loading_cost or 0) + (grn.freight_cost or 0) + (grn.other_expense or 0)
    tax = (grn.tax_amount or 0)
    discount = (grn.discount or 0)
    adjustment = (grn.adjustment_amount or 0)
    return item_total + expenses + tax - discount + adjustment


def _grn_bill_ref(grn):
    return (getattr(grn, 'manual_bill_no', None) or getattr(grn, 'auto_bill_no', None) or f"GRN-{getattr(grn, 'id', '')}").strip()


def _grn_auto_payment_note(grn):
    return f"[AUTO_GRN_PAY:{grn.id}] Auto-payment for GRN #{_grn_bill_ref(grn)}"


def _find_grn_auto_supplier_payment(grn):
    if not grn:
        return None
    marker = f"[auto_grn_pay:{grn.id}]"
    q = SupplierPayment.query.filter_by(is_void=False)
    if getattr(grn, 'supplier_id', None):
        q = q.filter(SupplierPayment.supplier_id == grn.supplier_id)
    marker_row = q.filter(
        func.lower(func.coalesce(SupplierPayment.note, '')).like(f"{marker}%")
    ).order_by(SupplierPayment.id.desc()).first()
    if marker_row:
        return marker_row

    # Legacy fallback for rows created before marker support.
    bill_ref = _grn_bill_ref(grn).lower()
    if not bill_ref:
        return None
    legacy_row = q.filter(
        func.lower(func.coalesce(SupplierPayment.note, '')).like(f"auto-payment for grn #{bill_ref}%")
    ).order_by(SupplierPayment.id.desc()).first()
    return legacy_row


def _sync_grn_auto_supplier_payment(grn, old_supplier_id=None):
    if not grn:
        return
    # If supplier changed, void old supplier's auto row (if any).
    if old_supplier_id and old_supplier_id != getattr(grn, 'supplier_id', None):
        old_marker = f"[auto_grn_pay:{grn.id}]"
        old_row = SupplierPayment.query.filter(
            SupplierPayment.is_void == False,
            SupplierPayment.supplier_id == old_supplier_id,
            func.lower(func.coalesce(SupplierPayment.note, '')).like(f"{old_marker}%")
        ).order_by(SupplierPayment.id.desc()).first()
        if old_row:
            old_row.is_void = True

    row = _find_grn_auto_supplier_payment(grn)
    paid = max(0.0, float(getattr(grn, 'paid_amount', 0) or 0))

    if not getattr(grn, 'supplier_id', None) or paid <= 0:
        if row:
            row.is_void = True
        return

    if not row:
        row = SupplierPayment(
            supplier_id=grn.supplier_id,
            is_void=False
        )
        db.session.add(row)
    row.is_void = False
    row.supplier_id = grn.supplier_id
    row.amount = paid
    row.method = (grn.payment_type or row.method or 'Cash')
    row.date_posted = grn.date_posted or pk_now()
    row.note = _grn_auto_payment_note(grn)
    row.bank_name = grn.bank_name or ''
    row.account_name = grn.account_name or ''
    row.account_no = grn.account_no or ''


def _is_grn_backdate_restricted_user():
    if not current_user.is_authenticated:
        return False
    if current_user.role in ('admin', 'root'):
        return False
    return bool(getattr(current_user, 'restrict_backdated_edit', False))


def _enforce_grn_backdate_policy(grn_dt, action_label, redirect_endpoint='grn', **redirect_kwargs):
    if not _is_grn_backdate_restricted_user():
        return None
    if not grn_dt:
        return None
    grn_date = grn_dt.date() if isinstance(grn_dt, datetime) else grn_dt
    if grn_date < pk_today():
        flash(f'{action_label} blocked: back-dated GRN edits are restricted for your account.', 'danger')
        return redirect(url_for(redirect_endpoint, **redirect_kwargs))
    return None

@app.route('/grn', methods=['GET', 'POST'])
@login_required
def grn():
    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'add':
            supplier_input = request.form.get('supplier', '').strip()
            supplier_id = request.form.get('supplier_id')
            
            supplier_obj = None
            if supplier_id:
                supplier_obj = db.session.get(Supplier, int(supplier_id))
            elif supplier_input:
                supplier_obj = get_supplier_by_input(supplier_input)
                if not supplier_obj:
                    # Auto-create supplier if not found
                    supplier_obj = Supplier(name=supplier_input, is_active=True)
                    db.session.add(supplier_obj)
                    db.session.flush()
            
            supplier_name = supplier_obj.name if supplier_obj else supplier_input

            manual_bill_raw = request.form.get('manual_bill_no', '').strip()
            manual_bill = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
            auto_bill = get_next_bill_no(AUTO_BILL_NAMESPACES['GRN'])
            note = request.form.get('note', '').strip()
            photo = save_photo(request.files.get('photo'))
            photo_url = request.form.get('photo_url', '').strip()
            loading_cost = float(request.form.get('loading_cost', 0) or 0)
            freight_cost = float(request.form.get('freight_cost', 0) or 0)
            other_expense = float(request.form.get('other_expense', 0) or 0)
            adjustment_amount = float(request.form.get('adjustment_amount', 0) or 0)
            discount = float(request.form.get('discount', 0) or 0)
            paid_amount = float(request.form.get('paid_amount', 0) or 0)
            payment_type = request.form.get('payment_type', '').strip()
            bank_name = request.form.get('bank_name', '').strip()
            account_name = request.form.get('account_name', '').strip()
            account_no = request.form.get('account_no', '').strip()
            tax_percent = float(request.form.get('tax_percent', 0) or 0)
            tax_amount = float(request.form.get('tax_amount', 0) or 0)
            tax_type = request.form.get('tax_type', '').strip()
            supplier_invoice_no = request.form.get('supplier_invoice_no', '').strip()
            due_date_str = request.form.get('due_date')
            bill_date_str = request.form.get('bill_date')

            date_str = request.form.get('date')
            if date_str:
                try:
                    date_posted = datetime.strptime(date_str, '%Y-%m-%d')
                    if date_posted.date() == pk_today():
                        date_posted = pk_now()
                except ValueError:
                    date_posted = pk_now()
            else:
                date_posted = pk_now()

            restricted = _enforce_grn_backdate_policy(date_posted, 'Add GRN')
            if restricted:
                return restricted
            
            due_date = None
            if due_date_str:
                try: due_date = datetime.strptime(due_date_str, '%Y-%m-%d').date()
                except: pass
                
            bill_date = None
            if bill_date_str:
                try: bill_date = datetime.strptime(bill_date_str, '%Y-%m-%d').date()
                except: pass

            if manual_bill:
                conflict = find_bill_conflict(manual_bill)
                if conflict:
                    flash(f"Manual bill '{manual_bill}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
                    return redirect(url_for('grn'))

            new_grn = GRN(
                supplier=supplier_name, 
                supplier_id=supplier_obj.id if supplier_obj else None,
                manual_bill_no=manual_bill,
                auto_bill_no=auto_bill, 
                photo_path=photo, 
                photo_url=photo_url, 
                note=note,
                loading_cost=loading_cost,
                freight_cost=freight_cost,
                other_expense=other_expense,
                adjustment_amount=adjustment_amount,
                discount=discount,
                paid_amount=paid_amount,
                payment_type=payment_type,
                bank_name=bank_name,
                account_name=account_name,
                account_no=account_no,
                tax_percent=tax_percent,
                tax_amount=tax_amount,
                tax_type=tax_type,
                supplier_invoice_no=supplier_invoice_no,
                due_date=due_date,
                bill_date=bill_date,
                date_posted=date_posted
            )
            db.session.add(new_grn)
            db.session.flush()

            mat_names = request.form.getlist('mat_name[]')
            qtys = request.form.getlist('qty[]')
            prices = request.form.getlist('price[]')

            for name, qty, price in zip(mat_names, qtys, prices):
                if name and qty:
                    qty_val = float(qty)
                    price_val = float(price) if price else 0
                    item = GRNItem(grn_id=new_grn.id, mat_name=name, qty=qty_val, price_at_time=price_val)
                    db.session.add(item)

                    mat = Material.query.filter_by(name=name).first()
                    if mat:
                        mat.total += qty_val

                    entry = Entry(
                        date=date_posted.strftime('%Y-%m-%d'),
                        time=date_posted.strftime('%H:%M:%S'),
                        type='IN',
                        material=name,
                        client=supplier_name,
                        qty=qty_val,
                        bill_no=manual_bill or '',
                        auto_bill_no=auto_bill,
                        created_by=current_user.username,
                        note=note
                    )
                    db.session.add(entry)

            _sync_grn_auto_supplier_payment(new_grn)

            db.session.commit()
            flash('GRN added successfully!', 'success')

        elif action == 'delete':
            grn_id = request.form.get('id')
            grn_obj = db.session.get(GRN, grn_id)
            if grn_obj:
                restricted = _enforce_grn_backdate_policy(grn_obj.date_posted, 'Void GRN')
                if restricted:
                    return restricted
                for item in grn_obj.items:
                    mat = Material.query.filter_by(name=item.mat_name).first()
                    if mat:
                        mat.total -= item.qty
                auto_pay = _find_grn_auto_supplier_payment(grn_obj)
                if auto_pay:
                    auto_pay.is_void = True
                db.session.delete(grn_obj)
                db.session.commit()
                flash('GRN deleted successfully!', 'success')

        return redirect(url_for('grn'))

    search = request.args.get('search', '').strip()
    sort_by = request.args.get('sort', 'date')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    query = GRN.query
    if search:
        query = query.filter(or_(
            GRN.supplier.ilike(f'%{search}%'),
            GRN.manual_bill_no.ilike(f'%{search}%'),
            GRN.auto_bill_no.ilike(f'%{search}%')
        ))
    if start_date:
        query = query.filter(func.date(GRN.date_posted) >= start_date)
    if end_date:
        query = query.filter(func.date(GRN.date_posted) <= end_date)
    
    if sort_by == 'supplier':
        grns = query.order_by(GRN.supplier.asc()).all()
    else:
        grns = query.order_by(GRN.date_posted.desc()).all()

    materials = Material.query.order_by(Material.name.asc()).all()
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    suppliers_list = Supplier.query.filter_by(is_active=True).order_by(Supplier.name.asc()).all()

    settings = Settings.query.first()
    next_auto = peek_next_bill_no(AUTO_BILL_NAMESPACES['GRN'])

    return render_template('grn_wizard.html', grns=grns, materials=materials, settings=settings, next_auto=next_auto, clients=clients, suppliers=suppliers_list, today_date=pk_today().strftime('%Y-%m-%d'), search=search, sort=sort_by, start_date=start_date, end_date=end_date)


@app.route('/edit_grn/<int:id>', methods=['GET', 'POST'])
@login_required
def edit_grn(id):
    grn_obj = GRN.query.get_or_404(id)
    restricted = _enforce_grn_backdate_policy(grn_obj.date_posted, 'Edit GRN')
    if restricted:
        return restricted
    
    if request.method == 'POST':
        old_supplier_id = grn_obj.supplier_id
        # 1. Reverse Stock for existing items
        for item in grn_obj.items:
            mat = Material.query.filter_by(name=item.mat_name).first()
            if mat:
                mat.total = (mat.total or 0) - (item.qty or 0)
        
        # 2. Delete old items and entries
        GRNItem.query.filter_by(grn_id=grn_obj.id).delete()
        Entry.query.filter(Entry.auto_bill_no == grn_obj.auto_bill_no, Entry.type == 'IN').delete()
        
        # 3. Update GRN fields
        supplier_input = request.form.get('supplier', '').strip()
        supplier_id_input = (request.form.get('supplier_id') or '').strip()
        supplier_obj = None
        if supplier_id_input.isdigit():
            supplier_obj = db.session.get(Supplier, int(supplier_id_input))
        if not supplier_obj and supplier_input:
            supplier_obj = get_supplier_by_input(supplier_input)
        if not supplier_obj and supplier_input:
            supplier_obj = Supplier(name=supplier_input, is_active=True)
            db.session.add(supplier_obj)
            db.session.flush()

        grn_obj.supplier = supplier_obj.name if supplier_obj else supplier_input
        grn_obj.supplier_id = supplier_obj.id if supplier_obj else None
        manual_bill_raw = request.form.get('manual_bill_no', '').strip()
        grn_obj.manual_bill_no = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
        grn_obj.note = request.form.get('note', '').strip()
        grn_obj.photo_url = request.form.get('photo_url', '').strip()
        
        new_photo = save_photo(request.files.get('photo'))
        if new_photo:
            grn_obj.photo_path = new_photo
            
        grn_obj.loading_cost = float(request.form.get('loading_cost', 0) or 0)
        grn_obj.freight_cost = float(request.form.get('freight_cost', 0) or 0)
        grn_obj.other_expense = float(request.form.get('other_expense', 0) or 0)
        grn_obj.adjustment_amount = float(request.form.get('adjustment_amount', 0) or 0)
        grn_obj.discount = float(request.form.get('discount', 0) or 0)
        grn_obj.paid_amount = float(request.form.get('paid_amount', 0) or 0)
        grn_obj.payment_type = request.form.get('payment_type', '').strip()
        grn_obj.bank_name = request.form.get('bank_name', '').strip()
        grn_obj.account_name = request.form.get('account_name', '').strip()
        grn_obj.account_no = request.form.get('account_no', '').strip()
        grn_obj.tax_percent = float(request.form.get('tax_percent', 0) or 0)
        grn_obj.tax_amount = float(request.form.get('tax_amount', 0) or 0)
        grn_obj.tax_type = request.form.get('tax_type', '').strip()
        grn_obj.supplier_invoice_no = request.form.get('supplier_invoice_no', '').strip()
        
        if grn_obj.manual_bill_no:
            conflict = find_bill_conflict(grn_obj.manual_bill_no)
            if conflict and not (conflict[0] == 'GRN' and conflict[1] == grn_obj.id):
                flash(f"Manual bill '{grn_obj.manual_bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
                return redirect(url_for('edit_grn', id=grn_obj.id))

        date_str = request.form.get('date')
        if date_str:
            try:
                date_posted = datetime.strptime(date_str, '%Y-%m-%d')
                restricted = _enforce_grn_backdate_policy(date_posted, 'Edit GRN')
                if restricted:
                    return restricted
                if date_posted.date() == pk_today():
                    grn_obj.date_posted = pk_now()
                else:
                    grn_obj.date_posted = date_posted
            except ValueError:
                pass

        # 4. Add new items
        mat_names = request.form.getlist('mat_name[]')
        qtys = request.form.getlist('qty[]')
        prices = request.form.getlist('price[]')
        
        for name, qty, price in zip(mat_names, qtys, prices):
            if name and qty:
                qty_val = float(qty)
                price_val = float(price) if price else 0
                item = GRNItem(grn_id=grn_obj.id, mat_name=name, qty=qty_val, price_at_time=price_val)
                db.session.add(item)
                
                mat = Material.query.filter_by(name=name).first()
                if mat:
                    mat.total = (mat.total or 0) + qty_val
                
                entry = Entry(
                    date=grn_obj.date_posted.strftime('%Y-%m-%d'),
                    time=grn_obj.date_posted.strftime('%H:%M:%S'),
                    type='IN',
                    material=name,
                    client=grn_obj.supplier,
                    qty=qty_val,
                    bill_no=grn_obj.manual_bill_no or '',
                    auto_bill_no=grn_obj.auto_bill_no,
                    created_by=current_user.username,
                    note=grn_obj.note
                )
                db.session.add(entry)

        _sync_grn_auto_supplier_payment(grn_obj, old_supplier_id=old_supplier_id)
        
        db.session.commit()
        flash('GRN updated successfully', 'success')
        return redirect(url_for('grn'))

    grns = GRN.query.order_by(GRN.date_posted.desc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    suppliers_list = Supplier.query.filter_by(is_active=True).order_by(Supplier.name.asc()).all()
    settings = Settings.query.first()
    
    return render_template('grn_wizard.html', grns=grns, materials=materials, settings=settings, clients=clients, suppliers=suppliers_list, today_date=pk_today().strftime('%Y-%m-%d'), edit_grn=grn_obj, search='', sort='date', start_date=None, end_date=None)

@app.route('/export_grn')
@login_required
def export_grn():
    if current_user.role not in ['admin', 'root']:
        flash('Only tenant admin or root can run import/export operations.', 'danger')
        return redirect(url_for('index'))
    search = request.args.get('search', '').strip()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    query = GRN.query
    if search:
        query = query.filter(or_(
            GRN.supplier.ilike(f'%{search}%'),
            GRN.manual_bill_no.ilike(f'%{search}%'),
            GRN.auto_bill_no.ilike(f'%{search}%')
        ))
    if start_date:
        query = query.filter(func.date(GRN.date_posted) >= start_date)
    if end_date:
        query = query.filter(func.date(GRN.date_posted) <= end_date)
    
    grns = query.order_by(GRN.date_posted.desc()).all()
    
    data = []
    for g in grns:
        total = calculate_grn_total(g)
        data.append({
            'Date': g.date_posted.strftime('%Y-%m-%d'),
            'GRN #': g.manual_bill_no or g.auto_bill_no,
            'Supplier': g.supplier,
            'Items Count': len(g.items),
            'Total Qty': sum(i.qty for i in g.items),
            'Discount': g.discount,
            'Tax': g.tax_amount,
            'Expenses': (g.loading_cost or 0) + (g.freight_cost or 0) + (g.other_expense or 0),
            'Net Amount': total,
            'Note': g.note
        })
    
    import pandas as pd
    df = pd.DataFrame(data)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='GRN List')
    output.seek(0)
    
    return send_file(output, as_attachment=True, download_name=f"GRN_Export_{pk_today()}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/api/supplier_balance/<int:id>')
@login_required
def api_supplier_balance(id):
    supplier = db.session.get(Supplier, id)
    if not supplier:
        return jsonify({'balance': 0})

    ledger_rows, balance, total_bill, total_paid = _build_supplier_ledger_rows(supplier)
    return jsonify({
        'balance': float(balance or 0),
        'opening_balance': float(_to_float_or_zero(getattr(supplier, 'opening_balance', 0))),
        'total_bill': float(total_bill or 0),
        'total_paid': float(total_paid or 0),
        'rows': len(ledger_rows or [])
    })

@app.route('/supplier_ledger/<int:id>')
@login_required
def supplier_ledger(id):
    supplier = Supplier.query.get_or_404(id)
    ledger, balance, total_bill, total_paid = _build_supplier_ledger_rows(supplier)
    page = request.args.get('page', 1, type=int) or 1
    per_page = 10
    total_entries = len(ledger)
    total_pages = max(1, (total_entries + per_page - 1) // per_page)
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages
    start = (page - 1) * per_page
    end = start + per_page
    page_rows = ledger[start:end]
    return render_template(
        'supplier_ledger.html',
        supplier=supplier,
        ledger=page_rows,
        ledger_total=total_entries,
        final_balance=balance,
        total_bill=total_bill,
        total_paid=total_paid,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        today_date=pk_today().strftime('%Y-%m-%d'),
        current_pk_datetime=pk_now().strftime('%Y-%m-%dT%H:%M')
    )


def _build_supplier_ledger_rows(supplier):
    # Fetch GRNs (Credits)
    grns = GRN.query.filter(
        or_(GRN.supplier_id == supplier.id, GRN.supplier == supplier.name),
        GRN.is_void == False
    ).all()

    # Fetch Payments (Debits)
    payments = SupplierPayment.query.filter_by(supplier_id=supplier.id, is_void=False).all()

    ledger = []
    opening_balance = _to_float_or_zero(getattr(supplier, 'opening_balance', 0))
    if opening_balance != 0:
        opening_dt = (
            getattr(supplier, 'opening_balance_date', None)
            or getattr(supplier, 'created_at', None)
            or datetime.min
        )
        ledger.append({
            'date': opening_dt,
            'type': 'OPENING',
            'ref': 'OPENING',
            'bill_no': '',
            'description': 'Opening Balance',
            'credit': opening_balance if opening_balance > 0 else 0,
            'debit': abs(opening_balance) if opening_balance < 0 else 0,
            'id': 0
        })
    for g in grns:
        total = calculate_grn_total(g)
        item_lines = []
        for gi in (g.items or []):
            qty_val = float(gi.qty or 0)
            rate_val = float(gi.price_at_time or 0)
            item_lines.append({
                'name': gi.mat_name or '',
                'qty': qty_val,
                'rate': rate_val,
                'amount': qty_val * rate_val
            })
        ledger.append({
            'date': g.date_posted,
            'type': 'GRN',
            'ref': g.manual_bill_no or g.auto_bill_no,
            'bill_no': g.manual_bill_no or g.auto_bill_no,
            'description': f"Goods Receipt ({len(g.items)} items)",
            'credit': total, # Payable to supplier
            'debit': 0,
            'id': g.id,
            'item_lines': item_lines,
            'note': (g.note or '').strip()
        })

    for p in payments:
        ledger.append({
            'date': p.date_posted,
            'type': 'Payment',
            'ref': f"PAY-{p.id}",
            'bill_no': f"PAY-{p.id}",
            'description': f"Payment ({p.method})",
            'credit': 0,
            'debit': p.amount, # Paid to supplier
            'id': p.id,
            'payment_obj': p,
            'item_lines': [],
            'note': (p.note or '').strip()
        })

    # Sort oldest -> newest by exact timestamp.
    def _supplier_row_sort_key(row):
        dt = row.get('date')
        if isinstance(dt, date) and not isinstance(dt, datetime):
            dt = datetime.combine(dt, datetime.min.time())
        dt_key = dt if isinstance(dt, datetime) else datetime.min
        row_type = row.get('type')
        if row_type == 'OPENING':
            type_key = 0
        elif row_type == 'GRN':
            type_key = 1
        else:
            type_key = 2
        id_key = int(row.get('id') or 0)
        return (dt_key, type_key, id_key)

    ledger.sort(key=_supplier_row_sort_key)

    # Calculate running balance
    balance = 0
    for row in ledger:
        balance += (row['credit'] - row['debit'])
        row['balance'] = balance

    total_bill = sum(float(x.get('credit') or 0) for x in ledger)
    total_paid = sum(float(x.get('debit') or 0) for x in ledger)
    return ledger, balance, total_bill, total_paid


@app.route('/download_supplier_ledger/<int:id>')
@login_required
def download_supplier_ledger(id):
    supplier = Supplier.query.get_or_404(id)
    ledger, final_balance, total_bill, total_paid = _build_supplier_ledger_rows(supplier)
    action = (request.args.get('action') or 'download').lower()
    disposition = 'inline' if action == 'print' else 'attachment'
    rendered = render_template(
        'supplier_ledger_print.html',
        supplier=supplier,
        ledger=ledger,
        final_balance=final_balance,
        total_bill=total_bill,
        total_paid=total_paid,
        generated_at=pk_now(),
        auto_print=(action == 'print')
    )
    # Prefer WeasyPrint for download output when available.
    if action != 'print':
        pdf_response = _try_render_weasy_pdf(
            rendered,
            f'SupplierLedger-{supplier.id}.pdf',
            disposition=disposition
        )
        if pdf_response:
            return pdf_response

    response = make_response(rendered)
    response.headers['Content-Disposition'] = f'{disposition}; filename=SupplierLedger-{supplier.id}.html'
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    return response


@app.route('/download_supplier_payment/<int:payment_id>')
@login_required
def download_supplier_payment(payment_id):
    payment = SupplierPayment.query.get_or_404(payment_id)
    supplier = db.session.get(Supplier, payment.supplier_id)
    supplier_name = supplier.name if supplier else 'Supplier'

    bill_view = SimpleNamespace(
        manual_bill_no=f"PAY-{payment.id}",
        auto_bill_no='',
        invoice_no='',
        date_posted=payment.date_posted,
        client_name=supplier_name,
        supplier=supplier_name,
        amount=payment.amount or 0,
        paid_amount=0,
        method=payment.method or '',
        bank_name=payment.bank_name or '',
        account_name=payment.account_name or '',
        account_no=payment.account_no or '',
        note=payment.note or ''
    )

    action = (request.args.get('action') or 'download').lower()
    disposition = 'inline' if action == 'print' else 'attachment'

    rendered = render_template(
        'view_bill.html',
        bill=bill_view,
        type='Payment',
        items=[],
        client=None,
        client_balance=0,
        previous_balance=0,
        recent_deliveries=[],
        material_ledger_recent=[],
        material_stock_summary=[],
        auto_print=(action == 'print')
    )
    if action == 'download':
        pdf_response = _try_render_weasy_pdf(
            rendered,
            f'SupplierPayment-{payment.id}.pdf',
            disposition=disposition
        )
        if pdf_response:
            return pdf_response

    response = make_response(rendered)
    response.headers['Content-Disposition'] = f'{disposition}; filename=SupplierPayment-{payment.id}.html'
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    return response

@app.route('/add_supplier_payment', methods=['POST'])
@login_required
def add_supplier_payment():
    supplier_id = request.form.get('supplier_id')
    if not supplier_id:
        flash('Supplier is required', 'danger')
        return_to = (request.form.get('return_to') or '').strip().lower()
        if return_to == 'payments':
            return redirect(url_for('payments_page', party='supplier'))
        return redirect(url_for('suppliers'))
    amount = float(request.form.get('amount', 0) or 0)
    method = request.form.get('method', 'Cash')
    note = request.form.get('note', '').strip()
    date_str = request.form.get('date')
    bank_name = request.form.get('bank_name', '').strip()
    account_name = request.form.get('account_name', '').strip()
    account_no = request.form.get('account_no', '').strip()
    manual_bill_raw = request.form.get('manual_bill_no', '').strip()
    manual_bill_no = normalize_manual_bill(manual_bill_raw) if manual_bill_raw else ''
    date_posted = resolve_posted_datetime(date_str)
    auto_bill_no = get_next_bill_no(AUTO_BILL_NAMESPACES['SUPPLIER_PAYMENT'])

    if manual_bill_no:
        conflict = find_bill_conflict(manual_bill_no)
        if conflict:
            flash(f"Manual bill '{manual_bill_no}' already exists in {conflict[0]} #{conflict[1]}.", 'danger')
            return_to = (request.form.get('return_to') or '').strip().lower()
            if return_to == 'payments':
                return redirect(url_for('payments_page', party='supplier'))
            return redirect(url_for('suppliers'))
        
    payment = SupplierPayment(
        supplier_id=int(supplier_id), 
        amount=amount, 
        method=method, 
        note=note, 
        date_posted=date_posted,
        bank_name=bank_name,
        account_name=account_name,
        account_no=account_no,
        manual_bill_no=manual_bill_no,
        auto_bill_no=auto_bill_no
    )
    db.session.add(payment)
    db.session.commit()
    flash('Supplier payment recorded', 'success')
    return_to = (request.form.get('return_to') or '').strip().lower()
    if return_to == 'payments':
        return redirect(url_for('payments_page', party='supplier'))
    return redirect(url_for('supplier_ledger', id=supplier_id))

@app.route('/edit_supplier_payment/<int:id>', methods=['POST'])
@login_required
def edit_supplier_payment(id):
    payment = SupplierPayment.query.get_or_404(id)
    payment.amount = float(request.form.get('amount', 0) or 0)
    payment.method = request.form.get('method', 'Cash')
    payment.note = request.form.get('note', '').strip()
    date_str = request.form.get('date')
    if date_str:
        payment.date_posted = resolve_posted_datetime(date_str, fallback_dt=payment.date_posted or pk_now())
        
    payment.bank_name = request.form.get('bank_name', '').strip()
    payment.account_name = request.form.get('account_name', '').strip()
    payment.account_no = request.form.get('account_no', '').strip()
    
    db.session.commit()
    flash('Payment updated', 'success')
    return_to = (request.form.get('return_to') or '').strip().lower()
    if return_to == 'payments':
        return redirect(url_for('payments_page', party='supplier', show='all'))
    return redirect(url_for('supplier_ledger', id=payment.supplier_id))

@app.route('/delete_supplier_payment/<int:id>', methods=['POST'])
@login_required
def delete_supplier_payment(id):
    payment = SupplierPayment.query.get_or_404(id)
    payment.is_void = True
    db.session.commit()
    flash('Supplier payment voided', 'success')
    return_to = (request.form.get('return_to') or '').strip().lower()
    if return_to == 'payments':
        return redirect(url_for('payments_page', party='supplier', show='all'))
    return redirect(url_for('supplier_ledger', id=payment.supplier_id))


@app.route('/restore_supplier_payment/<int:id>', methods=['POST'])
@login_required
def restore_supplier_payment(id):
    payment = SupplierPayment.query.get_or_404(id)
    payment.is_void = False
    db.session.commit()
    flash('Supplier payment restored', 'success')
    return_to = (request.form.get('return_to') or '').strip().lower()
    if return_to == 'payments':
        return redirect(url_for('payments_page', party='supplier', show='all'))
    return redirect(url_for('supplier_ledger', id=payment.supplier_id))

# ==================== REDIRECTS FOR LEGACY ROUTES ====================

@app.route('/stock_summary')
@login_required
def stock_summary_redirect():
    return redirect(url_for('inventory.stock_summary'))

@app.route('/daily_transactions')
@login_required
def daily_transactions_redirect():
    return redirect(url_for('inventory.daily_transactions', **request.args))

# ==================== BLUEPRINTS ====================

try:
    from blueprints.inventory import inventory_bp
    app.register_blueprint(inventory_bp, url_prefix='/inventory')
except ImportError as e:
    print(f"Error loading inventory blueprint: {e}")

from blueprints.import_export import import_export_bp
app.register_blueprint(import_export_bp, url_prefix='/import_export')

try:
    from blueprints.data_lab import bp as data_lab_bp
    app.register_blueprint(data_lab_bp, url_prefix='/data_lab')
except ImportError as e:
    print(f"Error loading data_lab blueprint: {e}")


# ==================== MAIN ====================

if __name__ == '__main__':
    with app.app_context():
        _bootstrap_database()

    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
