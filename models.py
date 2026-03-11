import uuid
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from sqlalchemy import event, inspect, UniqueConstraint, func
from sqlalchemy.orm import with_loader_criteria

db = SQLAlchemy()
PK_TZ = ZoneInfo('Asia/Karachi')


def pk_model_now():
    """Default timestamp for all transactional records in Pakistan Standard Time."""
    return datetime.now(PK_TZ).replace(tzinfo=None)


def _get_request_g():
    try:
        from flask import g
        return g
    except Exception:
        return None


def get_current_tenant_id():
    g = _get_request_g()
    if not g:
        return None
    return getattr(g, 'tenant_id', None)


def get_enforce_tenant():
    g = _get_request_g()
    if not g:
        return False
    return bool(getattr(g, 'enforce_tenant', False))


def is_root_context():
    g = _get_request_g()
    if not g:
        return False
    return bool(getattr(g, 'is_root', False))


AUTO_BILL_NS_DEFAULT_MODEL = 'GEN'


def _normalize_namespace_model(namespace):
    ns = (namespace or AUTO_BILL_NS_DEFAULT_MODEL).strip().upper()
    if not ns:
        ns = AUTO_BILL_NS_DEFAULT_MODEL
    if not re.fullmatch(r'[A-Z][A-Z0-9]{1,7}', ns):
        ns = AUTO_BILL_NS_DEFAULT_MODEL
    return ns


def _extract_sb_parts_model(value):
    raw = (value or '').strip()
    if not raw:
        return (None, None)
    txt = raw.upper()
    if txt.startswith('MB NO.'):
        return (None, None)

    m = re.match(r'^SB\s*-\s*([A-Z][A-Z0-9]{1,7})\s*-\s*(\d+)$', txt)
    if m:
        return (_normalize_namespace_model(m.group(1)), int(m.group(2)))

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


def _normalize_auto_bill_model(value, namespace=AUTO_BILL_NS_DEFAULT_MODEL):
    ns_default = _normalize_namespace_model(namespace)
    parsed_ns, seq = _extract_sb_parts_model(value)
    if seq is None:
        return None
    ns = parsed_ns or ns_default
    return f"SB-{ns}-{int(seq)}"


def _normalize_manual_bill_model(value):
    raw = (value or '').strip()
    if not raw:
        return None
    upper = raw.upper()
    if upper.startswith('MB NO.') or upper.startswith('SB NO.'):
        body = raw.split('.', 1)[1].strip() if '.' in raw else ''
    else:
        body = raw
    if body.startswith('#'):
        body = body[1:].strip()
    if re.fullmatch(r'\d+\.0+', body or ''):
        body = body.split('.', 1)[0]
    if not body:
        return None
    if re.fullmatch(r'\d+', body):
        body = str(int(body))
    return f"MB NO.{body}"


def _parse_bill_kind_model(value):
    txt = (value or '').strip().upper()
    if txt.startswith('SB NO.') or txt.startswith('SB-'):
        return 'SB'
    if txt.startswith('MB NO.'):
        return 'MB'
    _, seq = _extract_sb_parts_model(value)
    if seq is not None:
        return 'SB'
    return 'UNKNOWN'


class TenantScopedMixin:
    tenant_id = db.Column(db.String(36), db.ForeignKey('tenant.id'), index=True, nullable=True)


class Tenant(db.Model):
    __tablename__ = 'tenant'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = db.Column(db.String(120), unique=True, nullable=False)
    status = db.Column(db.String(20), default='active')
    subscription_plan = db.Column(db.String(50))
    expiry_date = db.Column(db.Date)
    db_uri = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=pk_model_now)
    updated_at = db.Column(db.DateTime, default=pk_model_now, onupdate=pk_model_now)


class Role(db.Model):
    __tablename__ = 'role'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True, nullable=False)
    scope = db.Column(db.String(20), default='tenant')


class Permission(db.Model):
    __tablename__ = 'permission'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)
    description = db.Column(db.String(200))


class RolePermission(db.Model):
    __tablename__ = 'role_permission'
    id = db.Column(db.Integer, primary_key=True)
    role_id = db.Column(db.Integer, db.ForeignKey('role.id'), nullable=False)
    permission_id = db.Column(db.Integer, db.ForeignKey('permission.id'), nullable=False)


class UserRole(db.Model):
    __tablename__ = 'user_role'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    role_id = db.Column(db.Integer, db.ForeignKey('role.id'), nullable=False)
    tenant_id = db.Column(db.String(36), db.ForeignKey('tenant.id'), nullable=True)


class TenantFeature(db.Model):
    __tablename__ = 'tenant_feature'
    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.String(36), db.ForeignKey('tenant.id'), nullable=False, index=True)
    feature_name = db.Column(db.String(80), nullable=False)
    enabled = db.Column(db.Boolean, default=True)


class AuditLog(TenantScopedMixin, db.Model):
    __tablename__ = 'audit_log'
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer)
    action = db.Column(db.String(200), nullable=False)
    details = db.Column(db.String(1000))
    timestamp = db.Column(db.DateTime, default=pk_model_now, index=True)


class User(UserMixin, TenantScopedMixin, db.Model):
    __table_args__ = (
        UniqueConstraint('tenant_id', 'username', name='uq_user_tenant_username'),
    )
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    password_hash = db.Column(db.String(200))
    password_plain = db.Column(db.String(200))
    role = db.Column(db.String(20), default='user')
    status = db.Column(db.String(20), default='active')
    can_view_stock = db.Column(db.Boolean, default=True)
    can_view_daily = db.Column(db.Boolean, default=True)
    can_view_history = db.Column(db.Boolean, default=True)
    can_import_export = db.Column(db.Boolean, default=False)
    can_manage_directory = db.Column(db.Boolean, default=False)
    can_view_dashboard = db.Column(db.Boolean, default=True)
    can_manage_grn = db.Column(db.Boolean, default=True)
    can_manage_bookings = db.Column(db.Boolean, default=True)
    can_manage_payments = db.Column(db.Boolean, default=True)
    can_manage_sales = db.Column(db.Boolean, default=True)
    can_view_delivery_rent = db.Column(db.Boolean, default=True)
    can_manage_pending_bills = db.Column(db.Boolean, default=True)
    can_view_reports = db.Column(db.Boolean, default=True)
    can_manage_notifications = db.Column(db.Boolean, default=True)
    can_view_client_ledger = db.Column(db.Boolean, default=True)
    can_view_supplier_ledger = db.Column(db.Boolean, default=True)
    can_view_decision_ledger = db.Column(db.Boolean, default=True)
    can_manage_clients = db.Column(db.Boolean, default=False)
    can_manage_suppliers = db.Column(db.Boolean, default=False)
    can_manage_materials = db.Column(db.Boolean, default=False)
    can_manage_delivery_persons = db.Column(db.Boolean, default=False)
    can_access_settings = db.Column(db.Boolean, default=False)
    restrict_backdated_edit = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=pk_model_now)

    tenant = db.relationship('Tenant', backref=db.backref('users', lazy=True))


class Client(TenantScopedMixin, db.Model):
    __table_args__ = (
        UniqueConstraint('tenant_id', 'code', name='uq_client_tenant_code'),
    )
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20))
    address = db.Column(db.String(200))
    category = db.Column(db.String(50), default='General')
    opening_balance = db.Column(db.Float, default=0)
    opening_balance_date = db.Column(db.DateTime, default=pk_model_now, index=True)
    is_active = db.Column(db.Boolean, default=True)
    transferred_to_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)
    require_manual_invoice = db.Column(db.Boolean, default=False)
    book_no = db.Column(db.String(50))
    financial_page = db.Column(db.String(50))
    cement_page = db.Column(db.String(50))
    steel_page = db.Column(db.String(50))
    financial_book_no = db.Column(db.String(50))
    cement_book_no = db.Column(db.String(50))
    steel_book_no = db.Column(db.String(50))
    location_url = db.Column(db.String(500))
    page_notes = db.Column(db.String(300))
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


class Supplier(TenantScopedMixin, db.Model):
    __table_args__ = (
        UniqueConstraint('tenant_id', 'name', name='uq_supplier_tenant_name'),
    )
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20))
    address = db.Column(db.String(200))
    opening_balance = db.Column(db.Float, default=0)
    opening_balance_date = db.Column(db.DateTime, default=pk_model_now, index=True)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


class SupplierPayment(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    supplier_id = db.Column(db.Integer, db.ForeignKey('supplier.id'), nullable=False)
    amount = db.Column(db.Float, default=0)
    method = db.Column(db.String(50))
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    note = db.Column(db.String(500))
    is_void = db.Column(db.Boolean, default=False)
    bank_name = db.Column(db.String(100))
    account_name = db.Column(db.String(100))
    account_no = db.Column(db.String(50))
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    
    supplier = db.relationship('Supplier', backref='payments')


class Material(TenantScopedMixin, db.Model):
    __table_args__ = (
        UniqueConstraint('tenant_id', 'code', name='uq_material_tenant_code'),
    )
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50), nullable=False)
    name = db.Column(db.String(100), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey('material_category.id'), index=True, nullable=True)
    unit_price = db.Column(db.Float, default=0)
    total = db.Column(db.Float, default=0)
    unit = db.Column(db.String(20), default='Bags')
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)

    category = db.relationship('MaterialCategory', backref=db.backref('materials', lazy=True))


class MaterialCategory(TenantScopedMixin, db.Model):
    __tablename__ = 'material_category'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


def get_or_create_material_category(tenant_id, name='General'):
    target = (name or 'General').strip()
    if not target:
        target = 'General'
    q = MaterialCategory.query.filter_by(tenant_id=tenant_id).filter(
        func.lower(func.trim(MaterialCategory.name)) == target.lower()
    )
    cat = q.first()
    if cat:
        if not cat.is_active:
            cat.is_active = True
        return cat
    cat = MaterialCategory(tenant_id=tenant_id, name=target, is_active=True)
    db.session.add(cat)
    db.session.flush()
    return cat


class Entry(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String(20))
    time = db.Column(db.String(20))
    type = db.Column(db.String(10))
    material = db.Column(db.String(100))
    client = db.Column(db.String(100))
    client_code = db.Column(db.String(50))
    client_category = db.Column(db.String(50))
    qty = db.Column(db.Float, default=0)
    bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    nimbus_no = db.Column(db.String(50))
    invoice_id = db.Column(db.Integer, db.ForeignKey('invoice.id'), nullable=True)
    created_by = db.Column(db.String(80))
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)
    is_void = db.Column(db.Boolean, default=False)
    transaction_category = db.Column(db.String(50))
    driver_name = db.Column(db.String(100))
    note = db.Column(db.String(500))
    booked_material = db.Column(db.String(100))
    is_alternate = db.Column(db.Boolean, default=False)


class PendingBill(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_code = db.Column(db.String(50))
    client_name = db.Column(db.String(100))
    bill_no = db.Column(db.String(50))
    bill_kind = db.Column(db.String(10), default='UNKNOWN', index=True)
    nimbus_no = db.Column(db.String(50))
    amount = db.Column(db.Float, default=0)
    reason = db.Column(db.String(200))
    photo_url = db.Column(db.String(200))
    photo_path = db.Column(db.String(200))
    is_paid = db.Column(db.Boolean, default=False)
    is_cash = db.Column(db.Boolean, default=False)
    is_manual = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.String(50))
    created_by = db.Column(db.String(80))
    is_void = db.Column(db.Boolean, default=False)
    note = db.Column(db.String(500))
    risk_override = db.Column(db.String(20))


class Booking(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    amount = db.Column(db.Float, default=0)
    paid_amount = db.Column(db.Float, default=0)
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    photo_url = db.Column(db.String(500))
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    items = db.relationship('BookingItem', backref='booking', lazy=True, cascade='all, delete-orphan')
    is_void = db.Column(db.Boolean, default=False)
    note = db.Column(db.String(500))
    discount = db.Column(db.Float, default=0)
    discount_reason = db.Column(db.String(200))


class BookingItem(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    booking_id = db.Column(db.Integer, db.ForeignKey('booking.id'), nullable=False)
    material_name = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)
    price_at_time = db.Column(db.Float, default=0)


class Payment(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    amount = db.Column(db.Float, default=0)
    method = db.Column(db.String(50))
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    photo_url = db.Column(db.String(500))
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    is_void = db.Column(db.Boolean, default=False)
    note = db.Column(db.String(500))
    discount = db.Column(db.Float, default=0)
    discount_reason = db.Column(db.String(200))
    bank_name = db.Column(db.String(100))
    account_name = db.Column(db.String(100))
    account_no = db.Column(db.String(50))


class WaiveOff(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    payment_id = db.Column(db.Integer, db.ForeignKey('payment.id'), nullable=True, index=True)
    client_code = db.Column(db.String(50), index=True)
    client_name = db.Column(db.String(100), index=True)
    bill_no = db.Column(db.String(50), index=True)
    amount = db.Column(db.Float, default=0)
    reason = db.Column(db.String(300))
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    created_by = db.Column(db.String(80))
    note = db.Column(db.String(500))
    is_void = db.Column(db.Boolean, default=False, index=True)

    payment = db.relationship('Payment', backref=db.backref('waive_off_rows', lazy=True))


class Invoice(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_code = db.Column(db.String(50))
    client_name = db.Column(db.String(100))
    invoice_no = db.Column(db.String(50))
    is_manual = db.Column(db.Boolean, default=False)
    date = db.Column(db.Date)
    total_amount = db.Column(db.Float, default=0)
    balance = db.Column(db.Float, default=0)
    status = db.Column(db.String(20), default='OPEN')
    is_cash = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.String(50))
    created_by = db.Column(db.String(80))
    is_void = db.Column(db.Boolean, default=False)
    note = db.Column(db.String(500))

    entries = db.relationship('Entry', backref='invoice', lazy=True)
    direct_sales = db.relationship('DirectSale', backref='invoice', lazy=True)


class BillCounter(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    namespace = db.Column(db.String(12), default='GEN', index=True, nullable=False)
    count = db.Column(db.Integer, default=1000)


class DirectSale(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    category = db.Column(db.String(50))
    amount = db.Column(db.Float, default=0)
    paid_amount = db.Column(db.Float, default=0)
    discount = db.Column(db.Float, default=0)
    discount_reason = db.Column(db.String(200))
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    photo_url = db.Column(db.String(500))
    invoice_id = db.Column(db.Integer, db.ForeignKey('invoice.id'), nullable=True)
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    items = db.relationship('DirectSaleItem', backref='direct_sale', lazy=True, cascade='all, delete-orphan')
    is_void = db.Column(db.Boolean, default=False)
    note = db.Column(db.String(500))
    driver_name = db.Column(db.String(100))
    rent_item_revenue = db.Column(db.Float, default=0)
    delivery_rent_cost = db.Column(db.Float, default=0)
    rent_variance_loss = db.Column(db.Float, default=0)


class DeliveryPerson(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    phone = db.Column(db.String(30))
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


class DeliveryRent(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sale_id = db.Column(db.Integer, db.ForeignKey('direct_sale.id'), nullable=True, index=True)
    delivery_person_name = db.Column(db.String(100), nullable=False, index=True)
    bill_no = db.Column(db.String(50), index=True)
    amount = db.Column(db.Float, default=0)
    note = db.Column(db.String(500))
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    created_by = db.Column(db.String(80))
    is_void = db.Column(db.Boolean, default=False, index=True)

    sale = db.relationship('DirectSale', backref=db.backref('delivery_rents', lazy=True))


class DirectSaleItem(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sale_id = db.Column(db.Integer, db.ForeignKey('direct_sale.id'), nullable=False)
    product_name = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)
    price_at_time = db.Column(db.Float, default=0)


class GRN(TenantScopedMixin, db.Model):
    """Goods Receipt Note - for stock receiving"""
    id = db.Column(db.Integer, primary_key=True)
    supplier_id = db.Column(db.Integer, db.ForeignKey('supplier.id'), nullable=True)
    supplier = db.Column(db.String(100))
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    photo_url = db.Column(db.String(500))
    loading_cost = db.Column(db.Float, default=0)
    freight_cost = db.Column(db.Float, default=0)
    other_expense = db.Column(db.Float, default=0)
    adjustment_amount = db.Column(db.Float, default=0)
    discount = db.Column(db.Float, default=0)
    paid_amount = db.Column(db.Float, default=0)
    payment_type = db.Column(db.String(50))
    tax_percent = db.Column(db.Float, default=0)
    tax_amount = db.Column(db.Float, default=0)
    tax_type = db.Column(db.String(50))
    bank_name = db.Column(db.String(100))
    account_name = db.Column(db.String(100))
    account_no = db.Column(db.String(50))
    supplier_invoice_no = db.Column(db.String(50))
    due_date = db.Column(db.Date)
    bill_date = db.Column(db.Date)
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    items = db.relationship('GRNItem', backref='grn', lazy=True, cascade='all, delete-orphan')
    supplier_rel = db.relationship('Supplier', backref='grns')
    is_void = db.Column(db.Boolean, default=False)
    note = db.Column(db.String(500))


class GRNItem(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    grn_id = db.Column(db.Integer, db.ForeignKey('grn.id'), nullable=False)
    mat_name = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)
    price_at_time = db.Column(db.Float, default=0)


class Delivery(TenantScopedMixin, db.Model):
    """Delivery records for dispatching"""
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    date_posted = db.Column(db.DateTime, default=pk_model_now, index=True)
    items = db.relationship('DeliveryItem', backref='delivery', lazy=True, cascade='all, delete-orphan')


class DeliveryItem(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    delivery_id = db.Column(db.Integer, db.ForeignKey('delivery.id'), nullable=False)
    product = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)


class Settings(TenantScopedMixin, db.Model):
    """Application settings"""
    id = db.Column(db.Integer, primary_key=True)
    currency = db.Column(db.String(10), default='PKR')
    company_name = db.Column(db.String(100), default='FAZAL BUILDING MATERIALS')
    company_address = db.Column(db.String(200), default='JALAL PUR SOBTIAN')
    company_phone = db.Column(db.String(50), default='+92302-0000993 +92331-0000993')
    company_email = db.Column(db.String(100))
    tax_rate = db.Column(db.Float, default=0)
    invoice_prefix = db.Column(db.String(10), default='INV-')
    bill_prefix = db.Column(db.String(10), default='#')
    ui_theme = db.Column(db.String(20), default='dark_navy')
    allow_global_negative_stock = db.Column(db.Boolean, default=False, nullable=False)
    smtp_host = db.Column(db.String(200))
    smtp_port = db.Column(db.Integer, default=587)
    smtp_user = db.Column(db.String(200))
    smtp_pass = db.Column(db.String(200))
    smtp_from = db.Column(db.String(200))
    smtp_use_tls = db.Column(db.Boolean, default=True)
    ams_openai_api_key = db.Column(db.String(300))
    notify_daily_time = db.Column(db.String(10), default='08:00')
    google_client_id = db.Column(db.String(500))
    google_client_secret = db.Column(db.String(500))
    google_refresh_token = db.Column(db.String(1000))
    google_access_token = db.Column(db.String(1000))
    google_token_expiry = db.Column(db.String(50))
    google_sender_email = db.Column(db.String(200))


class TenantWipeBackupHistory(db.Model):
    __tablename__ = 'tenant_wipe_backup_history'
    id = db.Column(db.Integer, primary_key=True)
    tenant_id = db.Column(db.String(36), db.ForeignKey('tenant.id'), index=True, nullable=False)
    tenant_name = db.Column(db.String(120), nullable=False)
    performed_by = db.Column(db.String(80), nullable=True)
    performed_by_role = db.Column(db.String(20), nullable=True)
    targets = db.Column(db.String(1000), nullable=True)
    backup_filename = db.Column(db.String(255), nullable=False)
    backup_path = db.Column(db.String(1000), nullable=False)
    wipe_status = db.Column(db.String(20), default='pending', index=True)  # pending|completed|failed
    note = db.Column(db.String(500), nullable=True)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


class RootBackupSettings(db.Model):
    __tablename__ = 'root_backup_settings'
    id = db.Column(db.Integer, primary_key=True)
    enabled = db.Column(db.Boolean, default=False, nullable=False)
    frequency = db.Column(db.String(20), default='hourly', nullable=False)
    recipient_emails = db.Column(db.String(1000), nullable=True)
    include_full_raw_xlsx = db.Column(db.Boolean, default=True, nullable=False)
    include_sqlite_db = db.Column(db.Boolean, default=True, nullable=False)
    subject_prefix = db.Column(db.String(120), default='PWARE Root Backup', nullable=False)
    keep_history_count = db.Column(db.Integer, default=200, nullable=False)
    last_sent_at = db.Column(db.DateTime, nullable=True)
    last_status = db.Column(db.String(20), nullable=True)
    last_message = db.Column(db.String(500), nullable=True)
    updated_at = db.Column(db.DateTime, default=pk_model_now, onupdate=pk_model_now, index=True)


class RootBackupEmailHistory(db.Model):
    __tablename__ = 'root_backup_email_history'
    id = db.Column(db.Integer, primary_key=True)
    trigger_type = db.Column(db.String(30), default='auto', nullable=False, index=True)
    status = db.Column(db.String(20), default='failed', nullable=False, index=True)
    recipient_emails = db.Column(db.String(1000), nullable=True)
    subject = db.Column(db.String(300), nullable=True)
    attachment_name = db.Column(db.String(255), nullable=True)
    attachment_size_kb = db.Column(db.Integer, nullable=True)
    backup_path = db.Column(db.String(1000), nullable=True)
    message = db.Column(db.String(1000), nullable=True)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


class StaffEmail(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(200), unique=True, nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


class FollowUpReminder(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pending_bill_id = db.Column(db.Integer, db.ForeignKey('pending_bill.id'), nullable=False)
    remind_at = db.Column(db.DateTime, nullable=False)
    note = db.Column(db.String(500))
    is_done = db.Column(db.Boolean, default=False)
    alerted_at = db.Column(db.DateTime)
    acknowledged_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)

    pending_bill = db.relationship('PendingBill', backref=db.backref('reminders', lazy=True, cascade='all, delete-orphan'))


class FollowUpContact(TenantScopedMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pending_bill_id = db.Column(db.Integer, db.ForeignKey('pending_bill.id'), nullable=False)
    reminder_id = db.Column(db.Integer, db.ForeignKey('follow_up_reminder.id'))
    contacted_at = db.Column(db.DateTime, default=pk_model_now, nullable=False)
    channel = db.Column(db.String(30), default='Call')
    response = db.Column(db.String(200))
    note = db.Column(db.String(500))
    created_by = db.Column(db.String(80))
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)

    pending_bill = db.relationship('PendingBill', backref=db.backref('contact_logs', lazy=True, cascade='all, delete-orphan'))
    reminder = db.relationship('FollowUpReminder', backref=db.backref('closure_contact_logs', lazy=True))


class ReconBasket(TenantScopedMixin, db.Model):
    """Reconciliation Basket for Data Lab"""
    id = db.Column(db.Integer, primary_key=True)
    bill_no = db.Column(db.String(50))
    inv_date = db.Column(db.Date)
    inv_client = db.Column(db.String(100))
    fin_client = db.Column(db.String(100))
    inv_material = db.Column(db.String(100))
    inv_qty = db.Column(db.Float, default=0)
    status = db.Column(db.String(20))
    match_score = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)


class SchemaVersion(db.Model):
    """Tracks database schema version for upgrades."""
    id = db.Column(db.Integer, primary_key=True)
    version = db.Column(db.Integer, default=1)
    applied_at = db.Column(db.DateTime, default=pk_model_now)


class RootRecoveryCode(db.Model):
    __tablename__ = 'root_recovery_code'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False, index=True, default='root')
    code_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=pk_model_now, index=True)
    used_at = db.Column(db.DateTime, nullable=True, index=True)
    generated_by = db.Column(db.String(80))
    note = db.Column(db.String(300))


@event.listens_for(db.session, 'do_orm_execute')
def _add_tenant_criteria(execute_state):
    if not execute_state.is_select:
        return
    if not get_enforce_tenant():
        return
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return
    execute_state.statement = execute_state.statement.options(
        with_loader_criteria(TenantScopedMixin, lambda cls: cls.tenant_id == tenant_id, include_aliases=True)
    )


@event.listens_for(db.session, 'before_flush')
def _set_tenant_id(session, flush_context, instances):
    tenant_id = get_current_tenant_id()
    enforce = get_enforce_tenant()
    if enforce and tenant_id:
        for obj in session.new:
            if isinstance(obj, TenantScopedMixin) and getattr(obj, 'tenant_id', None) is None:
                obj.tenant_id = tenant_id

    # Normalize bill identities before writing any changed/new objects.
    for obj in list(session.new) + list(session.dirty):
        if isinstance(obj, Booking):
            obj.auto_bill_no = _normalize_auto_bill_model(getattr(obj, 'auto_bill_no', None), namespace='BK')
            obj.manual_bill_no = _normalize_manual_bill_model(getattr(obj, 'manual_bill_no', None))
        elif isinstance(obj, Payment):
            obj.auto_bill_no = _normalize_auto_bill_model(getattr(obj, 'auto_bill_no', None), namespace='CP')
            obj.manual_bill_no = _normalize_manual_bill_model(getattr(obj, 'manual_bill_no', None))
        elif isinstance(obj, SupplierPayment):
            obj.auto_bill_no = _normalize_auto_bill_model(getattr(obj, 'auto_bill_no', None), namespace='SP')
            obj.manual_bill_no = _normalize_manual_bill_model(getattr(obj, 'manual_bill_no', None))
        elif isinstance(obj, DirectSale):
            obj.auto_bill_no = _normalize_auto_bill_model(getattr(obj, 'auto_bill_no', None), namespace='SL')
            obj.manual_bill_no = _normalize_manual_bill_model(getattr(obj, 'manual_bill_no', None))
        elif isinstance(obj, GRN):
            obj.auto_bill_no = _normalize_auto_bill_model(getattr(obj, 'auto_bill_no', None), namespace='GRN')
            obj.manual_bill_no = _normalize_manual_bill_model(getattr(obj, 'manual_bill_no', None))
        elif isinstance(obj, Entry):
            obj.auto_bill_no = _normalize_auto_bill_model(getattr(obj, 'auto_bill_no', None), namespace='EN')
        elif isinstance(obj, PendingBill):
            bill_no = (getattr(obj, 'bill_no', None) or '').strip()
            if bill_no:
                if bool(getattr(obj, 'is_manual', False)):
                    obj.bill_no = _normalize_manual_bill_model(bill_no)
                else:
                    obj.bill_no = _normalize_auto_bill_model(bill_no, namespace='GEN') or _normalize_manual_bill_model(bill_no)
            obj.bill_kind = _parse_bill_kind_model(getattr(obj, 'bill_no', None))
        elif isinstance(obj, Invoice):
            inv_no = (getattr(obj, 'invoice_no', None) or '').strip()
            if inv_no and (not inv_no.upper().startswith('INV-')):
                if bool(getattr(obj, 'is_manual', False)):
                    obj.invoice_no = _normalize_manual_bill_model(inv_no)
                else:
                    obj.invoice_no = _normalize_auto_bill_model(inv_no, namespace='EN') or _normalize_manual_bill_model(inv_no)
        elif isinstance(obj, BillCounter):
            obj.namespace = _normalize_namespace_model(getattr(obj, 'namespace', None))

    for obj in session.dirty:
        if isinstance(obj, TenantScopedMixin):
            hist = inspect(obj).attrs.tenant_id.history
            if hist.has_changes() and not is_root_context():
                raise ValueError('tenant_id modification is not allowed')

