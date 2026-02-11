from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from datetime import datetime

db = SQLAlchemy()


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(200))
    role = db.Column(db.String(20), default='user')  # 'admin' or 'user'
    can_view_stock = db.Column(db.Boolean, default=True)
    can_view_daily = db.Column(db.Boolean, default=True)
    can_view_history = db.Column(db.Boolean, default=True)
    can_import_export = db.Column(db.Boolean, default=False)
    can_manage_directory = db.Column(db.Boolean, default=False)
    restrict_backdated_edit = db.Column(db.Boolean, default=False)


class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    phone = db.Column(db.String(20))
    address = db.Column(db.String(200))
    category = db.Column(db.String(50), default='General')
    is_active = db.Column(db.Boolean, default=True)
    transferred_to_id = db.Column(db.Integer, db.ForeignKey('client.id'), nullable=True)
    require_manual_invoice = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.now)


class Material(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(50), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    unit_price = db.Column(db.Float, default=0)
    total = db.Column(db.Float, default=0)
    created_at = db.Column(db.DateTime, default=datetime.now)


class Entry(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String(20))
    time = db.Column(db.String(20))
    type = db.Column(db.String(10))  # 'IN' or 'OUT'
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
    created_at = db.Column(db.DateTime, default=datetime.now)
    is_void = db.Column(db.Boolean, default=False)


class PendingBill(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_code = db.Column(db.String(50))
    client_name = db.Column(db.String(100))
    bill_no = db.Column(db.String(50))
    nimbus_no = db.Column(db.String(50))
    amount = db.Column(db.Float, default=0)
    reason = db.Column(db.String(200))
    photo_url = db.Column(db.String(200))
    is_paid = db.Column(db.Boolean, default=False)
    is_cash = db.Column(db.Boolean, default=False)
    is_manual = db.Column(db.Boolean, default=False)  # True if has manual bill no
    created_at = db.Column(db.String(50))
    created_by = db.Column(db.String(80))
    is_void = db.Column(db.Boolean, default=False)


class Booking(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    amount = db.Column(db.Float, default=0)
    paid_amount = db.Column(db.Float, default=0)
    manual_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    date_posted = db.Column(db.DateTime, default=datetime.now)
    items = db.relationship('BookingItem', backref='booking', lazy=True, cascade='all, delete-orphan')
    is_void = db.Column(db.Boolean, default=False)


class BookingItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    booking_id = db.Column(db.Integer, db.ForeignKey('booking.id'), nullable=False)
    material_name = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)
    price_at_time = db.Column(db.Float, default=0)


class Payment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    amount = db.Column(db.Float, default=0)
    method = db.Column(db.String(50))  # 'Cash', 'Bank Transfer', 'Cheque', etc.
    manual_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    date_posted = db.Column(db.DateTime, default=datetime.now)
    is_void = db.Column(db.Boolean, default=False)


class Invoice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_code = db.Column(db.String(50))
    client_name = db.Column(db.String(100))
    invoice_no = db.Column(db.String(50))
    is_manual = db.Column(db.Boolean, default=False)
    date = db.Column(db.Date)
    total_amount = db.Column(db.Float, default=0)
    balance = db.Column(db.Float, default=0)
    status = db.Column(db.String(20), default='OPEN')  # 'OPEN', 'PARTIAL', 'PAID'
    is_cash = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.String(50))
    created_by = db.Column(db.String(80))
    is_void = db.Column(db.Boolean, default=False)
    
    # Relationships
    entries = db.relationship('Entry', backref='invoice', lazy=True)
    direct_sales = db.relationship('DirectSale', backref='invoice', lazy=True)


class BillCounter(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    count = db.Column(db.Integer, default=1000)


class DirectSale(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    category = db.Column(db.String(50))
    amount = db.Column(db.Float, default=0)
    paid_amount = db.Column(db.Float, default=0)
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    invoice_id = db.Column(db.Integer, db.ForeignKey('invoice.id'), nullable=True)
    date_posted = db.Column(db.DateTime, default=datetime.now)
    items = db.relationship('DirectSaleItem', backref='direct_sale', lazy=True, cascade='all, delete-orphan')
    is_void = db.Column(db.Boolean, default=False)


class DirectSaleItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sale_id = db.Column(db.Integer, db.ForeignKey('direct_sale.id'), nullable=False)
    product_name = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)
    price_at_time = db.Column(db.Float, default=0)


class GRN(db.Model):
    """Goods Receipt Note - for stock receiving"""
    id = db.Column(db.Integer, primary_key=True)
    supplier = db.Column(db.String(100))
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    date_posted = db.Column(db.DateTime, default=datetime.now)
    items = db.relationship('GRNItem', backref='grn', lazy=True, cascade='all, delete-orphan')
    is_void = db.Column(db.Boolean, default=False)


class GRNItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    grn_id = db.Column(db.Integer, db.ForeignKey('grn.id'), nullable=False)
    mat_name = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)
    price_at_time = db.Column(db.Float, default=0)


class Delivery(db.Model):
    """Delivery records for dispatching"""
    id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(100))
    manual_bill_no = db.Column(db.String(50))
    auto_bill_no = db.Column(db.String(50))
    photo_path = db.Column(db.String(200))
    date_posted = db.Column(db.DateTime, default=datetime.now)
    items = db.relationship('DeliveryItem', backref='delivery', lazy=True, cascade='all, delete-orphan')


class DeliveryItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    delivery_id = db.Column(db.Integer, db.ForeignKey('delivery.id'), nullable=False)
    product = db.Column(db.String(100))
    qty = db.Column(db.Float, default=0)


class Settings(db.Model):
    """Application settings"""
    id = db.Column(db.Integer, primary_key=True)
    currency = db.Column(db.String(10), default='PKR')
    company_name = db.Column(db.String(100), default='Ahmed Cement')
    company_address = db.Column(db.String(200))
    company_phone = db.Column(db.String(20))
    company_email = db.Column(db.String(100))
    tax_rate = db.Column(db.Float, default=0)
    invoice_prefix = db.Column(db.String(10), default='INV-')
    bill_prefix = db.Column(db.String(10), default='#')
    allow_global_negative_stock = db.Column(db.Boolean, default=False, nullable=False)