from __future__ import annotations

import random
import shutil
from datetime import timedelta
from pathlib import Path
import sys
import os

from werkzeug.security import generate_password_hash
from flask import g

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import app, db, pk_now, _direct_sale_default_bill_ref, _rent_reconciliation_from_items, _sync_delivery_rent_for_sale, _sync_payment_waive_off, _sync_direct_sale_waive_off
from models import User, Tenant, Client, Supplier, SupplierPayment, MaterialCategory, Material, Entry, PendingBill, Booking, BookingItem, Payment, WaiveOff, Invoice, DirectSale, DirectSaleItem, DeliveryPerson, DeliveryRent, GRN, GRNItem, Delivery, DeliveryItem, ReconBasket, get_or_create_material_category

OPEN_KHATA_CODE = 'OPEN-KHATA'
OPEN_KHATA_NAME = 'OPEN KHATA'


def db_path() -> Path:
    return Path(app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', ''))


def backup_db() -> Path:
    p = db_path()
    bdir = p.parent / 'backups'
    bdir.mkdir(parents=True, exist_ok=True)
    out = bdir / f"prod_seed_backup_{pk_now().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copy2(p, out)
    return out


def ensure_admin() -> User:
    target_tenant_name = (os.environ.get('TARGET_TENANT_NAME') or '').strip()
    tenant = (Tenant.query.filter_by(name=target_tenant_name).first() if target_tenant_name else None)
    if not tenant:
        tenant = Tenant.query.filter_by(name='Default Branch').first() or Tenant.query.first()
    admin = User.query.filter_by(username='admin', tenant_id=tenant.id).first()
    if not tenant:
        raise RuntimeError('No tenant found')
    if not admin:
        admin = User(username='admin', password_hash=generate_password_hash('admin123'), password_plain='admin123', role='admin', status='active', tenant_id=tenant.id)
        db.session.add(admin)
    else:
        admin.password_hash = generate_password_hash('admin123')
        admin.password_plain = 'admin123'
        admin.role = 'admin'
        admin.status = 'active'
        admin.tenant_id = tenant.id
    db.session.commit()
    return admin


def wipe_tenant(tid: str) -> None:
    for m in [ReconBasket, DeliveryItem, Delivery, DeliveryRent, WaiveOff, SupplierPayment, DirectSaleItem, BookingItem, GRNItem, Entry, PendingBill, Invoice, DirectSale, Booking, Payment, GRN, Supplier, DeliveryPerson, Material, MaterialCategory]:
        db.session.query(m).filter_by(tenant_id=tid).delete(synchronize_session=False)
    db.session.query(Client).filter(Client.tenant_id == tid, Client.code != OPEN_KHATA_CODE).delete(synchronize_session=False)
    # Cleanup legacy/orphan seeded rows from previous non-tenant-enforced runs.
    db.session.query(DeliveryRent).filter(DeliveryRent.tenant_id.is_(None)).delete(synchronize_session=False)
    db.session.query(WaiveOff).filter(WaiveOff.tenant_id.is_(None)).delete(synchronize_session=False)
    ok = Client.query.filter_by(tenant_id=tid, code=OPEN_KHATA_CODE).first()
    if ok:
        ok.name = OPEN_KHATA_NAME
        ok.is_active = True
    db.session.commit()


def seed_master(tid: str):
    random.seed(55666988)
    cats = {n: get_or_create_material_category(tid, n) for n in ['Cement', 'Steel', 'Bricks', 'Sand', 'Services']}
    db.session.flush()

    mats = []
    for i, n in enumerate(['DG KHAN', 'BESTWAY', 'FAUJI', 'MAPLE LEAF', 'LUCKY', 'ASKARI', 'PAKCEM', 'KOHAT', 'POWER CEMENT'], 1):
        mats.append(Material(tenant_id=tid, code=f'CEM-{i:03d}', name=n, unit_price=round(random.uniform(1250, 1490), 2), total=0, category_id=cats['Cement'].id))
    for i in range(1, 8):
        mats.append(Material(tenant_id=tid, code=f'STL-{i:03d}', name=f'STEEL-{i}', unit_price=round(random.uniform(220000, 285000), 2), total=0, category_id=cats['Steel'].id))
    for i in range(1, 6):
        mats.append(Material(tenant_id=tid, code=f'BRK-{i:03d}', name=f'BRICKS-{i}', unit_price=round(random.uniform(15, 28), 2), total=0, category_id=cats['Bricks'].id))
    for i in range(1, 5):
        mats.append(Material(tenant_id=tid, code=f'SND-{i:03d}', name=f'SAND-{i}', unit_price=round(random.uniform(90, 180), 2), total=0, category_id=cats['Sand'].id))
    mats.append(Material(tenant_id=tid, code='SRV-001', name='Loader Rent', unit_price=500, total=0, category_id=cats['Services'].id))
    db.session.add_all(mats)

    clients = [Client(tenant_id=tid, code=f'FBM-{i:05d}', name=f'Client {i:03d}', phone=f"03{random.randint(100000000, 499999999)}", address=f'Block {random.randint(1,25)}', category='General', is_active=True) for i in range(1, 81)]
    clients.append(Client(tenant_id=tid, code='FBM-09999', name='Adnan Doian', phone='03001234567', address='Doian', category='General', is_active=True))
    db.session.add_all(clients)
    if not Client.query.filter_by(tenant_id=tid, code=OPEN_KHATA_CODE).first():
        db.session.add(Client(tenant_id=tid, code=OPEN_KHATA_CODE, name=OPEN_KHATA_NAME, category='Open Khata', is_active=True))

    db.session.add_all([Supplier(tenant_id=tid, name=f'Supplier {i:02d}', phone=f"0311{random.randint(1000000,9999999)}", is_active=True) for i in range(1, 21)])
    # DeliveryPerson.name is globally unique in current schema; avoid cross-tenant duplicate insert.
    db.session.commit()


def add_pending(tid: str, client: Client | None, cname: str, bill: str, amount: float, reason: str, is_cash: bool = False):
    if amount <= 0:
        return
    db.session.add(PendingBill(tenant_id=tid, client_code=(client.code if client else OPEN_KHATA_CODE if cname == OPEN_KHATA_NAME else None), client_name=cname, bill_no=bill, amount=round(amount, 2), reason=reason, is_paid=False, is_cash=is_cash, is_manual=bill.upper().startswith('MB NO.'), created_at=pk_now().strftime('%Y-%m-%d %H:%M'), created_by='production-seed', is_void=False))


def seed_grn(tid: str):
    mats = Material.query.filter(Material.tenant_id == tid, Material.name != 'Loader Rent').all()
    sups = Supplier.query.filter_by(tenant_id=tid).all()
    now = pk_now()
    for i in range(1, 101):
        sup = random.choice(sups)
        posted = now - timedelta(days=random.randint(1, 120), hours=random.randint(0, 23))
        grn = GRN(tenant_id=tid, supplier_id=sup.id, supplier=sup.name, manual_bill_no=f'MB NO.GRN{i:05d}', auto_bill_no=f'SB-GRN-{9000+i}', discount=round(random.choice([0, 0, random.uniform(100, 1800)]), 2), freight_cost=round(random.uniform(0, 900), 2), loading_cost=round(random.uniform(0, 700), 2), other_expense=round(random.uniform(0, 500), 2), date_posted=posted, note='production seed grn', is_void=False)
        db.session.add(grn)
        db.session.flush()
        for _ in range(random.randint(2, 5)):
            mat = random.choice(mats)
            qty = round(random.uniform(80, 600), 2)
            rate = round(float(mat.unit_price or 1) * random.uniform(0.88, 1.12), 2)
            db.session.add(GRNItem(tenant_id=tid, grn_id=grn.id, mat_name=mat.name, qty=qty, price_at_time=rate))
            mat.total = round(float(mat.total or 0) + qty, 2)
            db.session.add(Entry(tenant_id=tid, date=posted.strftime('%Y-%m-%d'), time=posted.strftime('%H:%M:%S'), type='IN', material=mat.name, client=sup.name, qty=qty, bill_no=grn.manual_bill_no, nimbus_no='GRN', created_by='production-seed', transaction_category='Billed', is_void=False))
    db.session.commit()


def seed_business(tid: str):
    now = pk_now()
    clients = Client.query.filter(Client.tenant_id == tid, Client.code != OPEN_KHATA_CODE).all()
    open_khata = Client.query.filter_by(tenant_id=tid, code=OPEN_KHATA_CODE).first()
    mats = Material.query.filter(Material.tenant_id == tid, Material.name != 'Loader Rent').all()
    rent_mat = Material.query.filter_by(tenant_id=tid, name='Loader Rent').first()
    dps = DeliveryPerson.query.filter_by(tenant_id=tid, is_active=True).all()
    if not dps:
        dps = [type('dp', (), {'name': n}) for n in ['Shoaib Loader', 'Usman Driver', 'Rizwan Rider', 'Shahzaib']]

    for i in range(1, 71):
        c = random.choice(clients)
        posted = now - timedelta(days=random.randint(1, 90), hours=random.randint(0, 20))
        b = Booking(tenant_id=tid, client_name=c.name, manual_bill_no=f'MB NO.BK{i:05d}', auto_bill_no=f'SB-BK-{3000+i}', date_posted=posted, note='production seed booking', discount=0.0, discount_reason='', is_void=False)
        db.session.add(b)
        db.session.flush()
        gross = 0.0
        for _ in range(random.randint(1, 3)):
            m = random.choice(mats)
            qty = round(random.uniform(15, 120), 2)
            rate = round(float(m.unit_price or 0) * random.uniform(0.92, 1.15), 2)
            gross += qty * rate
            db.session.add(BookingItem(tenant_id=tid, booking_id=b.id, material_name=m.name, qty=qty, price_at_time=rate))
        discount = round(gross * random.choice([0, 0, 0.03, 0.05]), 2)
        paid = round(max(0.0, gross - discount) * random.choice([0, 0.25, 0.6]), 2)
        due = max(0.0, gross - discount - paid)
        b.amount = round(gross, 2)
        b.discount = discount
        b.discount_reason = 'Booking adjustment' if discount > 0 else ''
        b.paid_amount = paid
        add_pending(tid, c, c.name, b.manual_bill_no, due, 'Booking due', False)

    for i in range(1, 81):
        c = random.choice(clients)
        posted = now - timedelta(days=random.randint(0, 80), hours=random.randint(0, 18))
        d = round(random.choice([0, 0, 0, random.uniform(200, 2500)]), 2)
        p = Payment(tenant_id=tid, client_name=c.name, amount=round(random.uniform(3000, 45000), 2), method=random.choice(['Cash', 'Bank Transfer', 'Cheque']), manual_bill_no=f'MB NO.PAY{i:05d}', auto_bill_no=f'SB-CP-{6000+i}', date_posted=posted, discount=d, discount_reason='Payment discount write-off' if d > 0 else '', note='production seed payment', is_void=False)
        db.session.add(p)
        db.session.flush()
        _sync_payment_waive_off(p)

    sale_cats = (['Booking Delivery'] * 15) + (['Mixed Transaction'] * 35) + (['Credit Customer'] * 25) + (['Cash'] * 20) + (['Open Khata'] * 10)
    random.shuffle(sale_cats)
    for i, cat in enumerate(sale_cats, 1):
        posted = now - timedelta(days=random.randint(0, 75), hours=random.randint(0, 23))
        c = open_khata if cat == 'Open Khata' else random.choice(clients)
        cname = OPEN_KHATA_NAME if cat == 'Open Khata' else c.name
        ccode = OPEN_KHATA_CODE if cat == 'Open Khata' else c.code
        driver = random.choice(dps).name
        items = []
        if cat in ['Booking Delivery', 'Mixed Transaction']:
            m = random.choice(mats)
            items.append({'name': m.name, 'qty': round(random.uniform(8, 35), 2), 'rate': 0.0, 'entry_cat': 'Booking Delivery'})
        if cat in ['Mixed Transaction', 'Credit Customer', 'Cash', 'Open Khata']:
            for _ in range(random.randint(1, 2)):
                m = random.choice(mats)
                items.append({'name': m.name, 'qty': round(random.uniform(4, 22), 2), 'rate': round(float(m.unit_price or 0) * random.uniform(0.9, 1.2), 2), 'entry_cat': 'Credit Customer' if cat != 'Cash' else 'Cash'})
        if cat != 'Booking Delivery' and random.random() < 0.45:
            items.append({'name': rent_mat.name, 'qty': float(random.choice([1, 1, 2, 3])), 'rate': float(random.choice([400, 500, 800, 1200])), 'entry_cat': 'Credit Customer' if cat != 'Cash' else 'Cash'})

        gross = sum(x['qty'] * x['rate'] for x in items if x['rate'] > 0)
        if cat == 'Booking Delivery':
            gross = 0.0
        disc = round(gross * random.choice([0, 0, 0.02, 0.06]), 2)
        if cat == 'Cash':
            paid = round(max(0.0, gross - disc), 2)
        elif cat == 'Booking Delivery':
            paid = 0.0
        elif cat == 'Mixed Transaction':
            paid = round(max(0.0, gross - disc) * random.choice([0.2, 0.5, 0.75]), 2)
        else:
            paid = round(max(0.0, gross - disc) * random.choice([0, 0.3]), 2)

        rent_rev = sum(x['qty'] * x['rate'] for x in items if 'rent' in x['name'].lower() and x['rate'] > 0)
        delivery_cost = (max(0.0, rent_rev - random.uniform(100, 400)) if random.random() < 0.33 else (rent_rev if random.random() < 0.66 else rent_rev + random.uniform(100, 900)))

        s = DirectSale(tenant_id=tid, client_name=cname, category=cat, amount=round(gross, 2), paid_amount=round(paid, 2), discount=round(disc, 2), discount_reason='Sale discount write-off' if disc > 0 else '', manual_bill_no=f'MB NO.SL{i:05d}', auto_bill_no=f'SB-SL-{1000+i}', date_posted=posted, note='production seed sale', driver_name=driver, is_void=False)
        db.session.add(s)
        db.session.flush()

        for x in items:
            db.session.add(DirectSaleItem(tenant_id=tid, sale_id=s.id, product_name=x['name'], qty=x['qty'], price_at_time=x['rate']))
            db.session.add(Entry(tenant_id=tid, date=posted.strftime('%Y-%m-%d'), time=posted.strftime('%H:%M:%S'), type='OUT', material=x['name'], client=cname, client_code=ccode, qty=x['qty'], bill_no=_direct_sale_default_bill_ref(s), nimbus_no='Direct Sale', created_by='production-seed', client_category=x['entry_cat'], transaction_category='Unbilled' if cat == 'Cash' else 'Billed', driver_name=driver, is_void=False))
            mobj = Material.query.filter_by(tenant_id=tid, name=x['name']).first()
            if mobj:
                mobj.total = round(float(mobj.total or 0) - float(x['qty']), 2)

        rec = _rent_reconciliation_from_items([{'product_name': x['name'], 'qty': x['qty'], 'price_at_time': x['rate']} for x in items], delivery_rent_cost=delivery_cost)
        s.rent_item_revenue = round(float(rec['rent_item_revenue']), 2)
        s.delivery_rent_cost = round(float(rec['delivery_rent_cost']), 2)
        s.rent_variance_loss = round(float(rec['rent_variance_loss']), 2)
        _sync_delivery_rent_for_sale(s, include_in_bill=(delivery_cost > 0), rent_amount=delivery_cost, rent_note='seeded rent cost')
        _sync_direct_sale_waive_off(s)

        due = max(0.0, float(s.amount or 0) - float(s.discount or 0) - float(s.paid_amount or 0))
        add_pending(tid, c if cat != 'Open Khata' else None, cname, _direct_sale_default_bill_ref(s), due, f'Direct sale ({cat.lower()}) due', is_cash=(cat == 'Cash'))

        if random.random() < 0.45 and cat in ['Mixed Transaction', 'Credit Customer', 'Cash']:
            status = 'PAID' if due <= 0 else ('PARTIAL' if float(s.paid_amount or 0) > 0 else 'OPEN')
            inv = Invoice(tenant_id=tid, client_code=ccode, client_name=cname, invoice_no=f"INV-{posted.strftime('%Y%m%d')}-{s.id}", is_manual=False, date=posted.date(), total_amount=float(s.amount or 0), balance=due, status=status, is_cash=(cat == 'Cash'), created_at=posted.strftime('%Y-%m-%d %H:%M'), created_by='production-seed', note='linked seeded invoice', is_void=False)
            db.session.add(inv)
            db.session.flush()
            s.invoice_id = inv.id

    adnan = Client.query.filter_by(tenant_id=tid, name='Adnan Doian').first()
    posted = pk_now().replace(year=2026, month=3, day=5, hour=17, minute=41, second=0, microsecond=0)
    s = DirectSale(tenant_id=tid, client_name='Adnan Doian', category='Mixed Transaction', amount=13500.0, paid_amount=10000.0, discount=0.0, discount_reason='', manual_bill_no='MB NO.55666988', auto_bill_no='SB-SL-1000', date_posted=posted, note='xdg', driver_name='Shoaib Loader', is_void=False)
    db.session.add(s)
    db.session.flush()
    rows = [('DG KHAN', 20.0, 0.0, 'Booking Delivery'), ('KOHAT', 10.0, 1350.0, 'Credit Customer'), ('Loader Rent', 1000.0, 0.0, 'Booking Delivery'), ('Loader Rent', 500.0, 0.0, 'Booking Delivery')]
    for name, qty, rate, ecat in rows:
        db.session.add(DirectSaleItem(tenant_id=tid, sale_id=s.id, product_name=name, qty=qty, price_at_time=rate))
        db.session.add(Entry(tenant_id=tid, date=posted.strftime('%Y-%m-%d'), time=posted.strftime('%H:%M:%S'), type='OUT', material=name, client='Adnan Doian', client_code=(adnan.code if adnan else 'FBM-09999'), qty=qty, bill_no='MB NO.55666988', nimbus_no='Direct Sale', created_by='production-seed', client_category=ecat, transaction_category='Billed', driver_name='Shoaib Loader', is_void=False))
        m = Material.query.filter_by(tenant_id=tid, name=name).first()
        if m:
            m.total = round(float(m.total or 0) - qty, 2)
    s.rent_item_revenue = 0.0
    s.delivery_rent_cost = 1500.0
    s.rent_variance_loss = 1500.0
    _sync_delivery_rent_for_sale(s, include_in_bill=True, rent_amount=1500.0, rent_note='Rent note')
    _sync_direct_sale_waive_off(s)
    add_pending(tid, adnan, 'Adnan Doian', 'MB NO.55666988', 3500.0, 'Direct sale (mixed transaction) due', False)

    for i in range(1, 8):
        d = Delivery(tenant_id=tid, client_name=random.choice(clients).name, manual_bill_no=f'MB NO.DLV{i:05d}', auto_bill_no=f'SB-EN-{8000+i}', date_posted=now - timedelta(days=random.randint(1, 40)))
        db.session.add(d)
        db.session.flush()
        for _ in range(random.randint(1, 2)):
            m = random.choice(mats)
            db.session.add(DeliveryItem(tenant_id=tid, delivery_id=d.id, product=m.name, qty=round(random.uniform(2, 10), 2)))

    db.session.commit()


def audit(tid: str):
    errs = []
    if GRN.query.filter_by(tenant_id=tid).count() < 100:
        errs.append('GRN count below 100')

    for s in DirectSale.query.filter_by(tenant_id=tid, is_void=False).all():
        bill = _direct_sale_default_bill_ref(s)
        calc = sum(float(i.qty or 0) * float(i.price_at_time or 0) for i in s.items if float(i.price_at_time or 0) > 0)
        if abs(float(s.amount or 0) - calc) > 0.05:
            errs.append(f'sale amount mismatch #{s.id}')
        due = max(0.0, float(s.amount or 0) - float(s.discount or 0) - float(s.paid_amount or 0))
        pb = db.session.query(db.func.sum(PendingBill.amount)).filter_by(tenant_id=tid, bill_no=bill, is_void=False).scalar() or 0
        if abs(float(pb) - due) > 0.05:
            errs.append(f'pending mismatch #{s.id}')
        rows = WaiveOff.query.filter(WaiveOff.tenant_id == tid, WaiveOff.payment_id.is_(None), WaiveOff.note == f"[direct_sale_discount:{s.id}]", WaiveOff.is_void == False).all()
        if float(s.discount or 0) > 0:
            if not rows:
                errs.append(f'missing sale waive #{s.id}')
            elif abs(sum(float(r.amount or 0) for r in rows) - float(s.discount or 0)) > 0.05:
                errs.append(f'sale waive mismatch #{s.id}')
        elif rows:
            errs.append(f'unexpected sale waive #{s.id}')
        rec = _rent_reconciliation_from_items([{'product_name': i.product_name, 'qty': i.qty, 'price_at_time': i.price_at_time} for i in s.items], delivery_rent_cost=s.delivery_rent_cost or 0)
        if abs(float(s.rent_variance_loss or 0) - float(rec['rent_variance_loss'] or 0)) > 0.05:
            errs.append(f'rent variance mismatch #{s.id}')
        rr = DeliveryRent.query.filter_by(tenant_id=tid, sale_id=s.id, is_void=False).order_by(DeliveryRent.id.desc()).first()
        if float(s.delivery_rent_cost or 0) > 0 and (not rr or abs(float(rr.amount or 0) - float(s.delivery_rent_cost or 0)) > 0.05):
            errs.append(f'delivery rent row mismatch #{s.id}')
        if float(s.delivery_rent_cost or 0) <= 0 and rr:
            errs.append(f'unexpected delivery rent row #{s.id}')
        if s.invoice_id:
            inv = db.session.get(Invoice, s.invoice_id)
            if not inv:
                errs.append(f'missing invoice #{s.id}')
            else:
                inv_due = max(0.0, float(s.amount or 0) - float(s.discount or 0) - float(s.paid_amount or 0))
                if abs(float(inv.total_amount or 0) - float(s.amount or 0)) > 0.05 or abs(float(inv.balance or 0) - inv_due) > 0.05:
                    errs.append(f'invoice mismatch #{s.id}')

    for p in Payment.query.filter_by(tenant_id=tid, is_void=False).all():
        rows = WaiveOff.query.filter_by(tenant_id=tid, payment_id=p.id, is_void=False).all()
        total = sum(float(r.amount or 0) for r in rows)
        if float(p.discount or 0) > 0 and abs(total - float(p.discount or 0)) > 0.05:
            errs.append(f'payment waive mismatch #{p.id}')
        if float(p.discount or 0) <= 0 and rows:
            errs.append(f'unexpected payment waive #{p.id}')

    for m in Material.query.filter_by(tenant_id=tid).all():
        in_qty = db.session.query(db.func.sum(Entry.qty)).filter_by(tenant_id=tid, material=m.name, type='IN', is_void=False).scalar() or 0
        out_qty = db.session.query(db.func.sum(Entry.qty)).filter_by(tenant_id=tid, material=m.name, type='OUT', is_void=False).scalar() or 0
        expected = float(in_qty) - float(out_qty)
        if abs(float(m.total or 0) - expected) > 0.2:
            errs.append(f'stock mismatch {m.name}')

    reg = DirectSale.query.filter_by(tenant_id=tid, manual_bill_no='MB NO.55666988').first()
    if not reg:
        errs.append('missing regression bill MB NO.55666988')

    return errs, {
        'grn_count': GRN.query.filter_by(tenant_id=tid).count(),
        'booking_count': Booking.query.filter_by(tenant_id=tid).count(),
        'payment_count': Payment.query.filter_by(tenant_id=tid).count(),
        'sale_count': DirectSale.query.filter_by(tenant_id=tid).count(),
        'invoice_count': Invoice.query.filter_by(tenant_id=tid).count(),
        'delivery_rent_count': DeliveryRent.query.filter_by(tenant_id=tid, is_void=False).count(),
        'waive_off_count': WaiveOff.query.filter_by(tenant_id=tid, is_void=False).count(),
        'pending_count': PendingBill.query.filter_by(tenant_id=tid, is_void=False).count(),
    }


def smoke() -> list[tuple[str, int]]:
    fails = []
    with app.test_client() as c:
        c.post('/login', data={'username': 'admin', 'password': 'admin123'}, follow_redirects=True)
        for ep in ['/', '/grn', '/bookings', '/payments', '/direct_sales', '/delivery_rents', '/pending_bills', '/tracking', '/decision_ledger', '/profit_reports', '/financial_details?type=cash', '/inventory/stock_summary', '/inventory/daily_transactions']:
            r = c.get(ep, follow_redirects=True)
            if r.status_code >= 500:
                fails.append((ep, r.status_code))
    return fails


def write_report(path: Path, backup: Path, stats: dict, errs: list[str], smoke_fails: list[tuple[str, int]]):
    lines = [
        '# Production Seed + Strict Audit Report',
        '',
        f"- Generated at (PKT): {pk_now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Backup DB: {backup}",
        '',
        '## Seed/Audit Counts',
    ]
    for k, v in stats.items():
        lines.append(f'- {k}: {v}')
    lines += ['', '## Route Smoke Failures']
    if smoke_fails:
        lines += [f'- {c} -> {ep}' for ep, c in smoke_fails]
    else:
        lines.append('- none')
    lines += ['', '## Invariant Failures']
    if errs:
        lines += [f'- {e}' for e in errs]
    else:
        lines.append('- none')
    lines += ['', '## Strict Policy Result', '- PASS' if (not errs and not smoke_fails) else '- FAIL']
    path.write_text('\n'.join(lines), encoding='utf-8')


def main():
    report = ROOT / 'FULLREPORT_PRODUCTION_SEED_AUDIT.md'
    with app.app_context():
        backup = backup_db()
        admin = ensure_admin()
        tid = admin.tenant_id
        g.tenant_id = tid
        g.enforce_tenant = True
        g.is_root = False
        wipe_tenant(tid)
        seed_master(tid)
        seed_grn(tid)
        seed_business(tid)
        errs, stats = audit(tid)
        smoke_fails = smoke()
        write_report(report, backup, stats, errs, smoke_fails)
        print(f'Backup: {backup}')
        print(f'Report: {report}')
        print(f"Errors: {len(errs)}")
        print(f"Smoke failures: {len(smoke_fails)}")
        if errs:
            print('Top errors:')
            for e in errs[:20]:
                print(f' - {e}')


if __name__ == '__main__':
    main()
