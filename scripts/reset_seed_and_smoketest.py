from __future__ import annotations

import random
import shutil
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

from werkzeug.security import generate_password_hash

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import app
from models import (
    db,
    User,
    Client,
    Material,
    Entry,
    PendingBill,
    Booking,
    BookingItem,
    Payment,
    Invoice,
    BillCounter,
    DirectSale,
    DirectSaleItem,
    GRN,
    GRNItem,
    Delivery,
    DeliveryItem,
    Settings,
    ReconBasket,
)


OPEN_KHATA_CODE = "OPEN-KHATA"
OPEN_KHATA_NAME = "OPEN KHATA"


def backup_db() -> Path:
    db_uri = app.config["SQLALCHEMY_DATABASE_URI"]
    db_path = Path(db_uri.replace("sqlite:///", ""))
    backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = backup_dir / f"ahmed_cement_backup_before_full_reset_{ts}.db"
    shutil.copy2(db_path, out)
    return out


def ensure_admin() -> None:
    admin = User.query.filter_by(username="admin").first()
    if not admin:
        db.session.add(
            User(
                username="admin",
                password_hash=generate_password_hash("admin123"),
                role="admin",
            )
        )
    else:
        admin.password_hash = generate_password_hash("admin123")
        admin.role = "admin"
    db.session.commit()


def wipe_business_data() -> None:
    # Child/detail first
    ReconBasket.query.delete()
    DirectSaleItem.query.delete()
    BookingItem.query.delete()
    GRNItem.query.delete()
    DeliveryItem.query.delete()
    Entry.query.delete()
    PendingBill.query.delete()

    # Parent transactions
    DirectSale.query.delete()
    Booking.query.delete()
    Payment.query.delete()
    Invoice.query.delete()
    GRN.query.delete()
    Delivery.query.delete()

    # Master business data
    Client.query.delete()
    Material.query.delete()

    # Keep settings/counters but normalize
    bc = BillCounter.query.first()
    if not bc:
        bc = BillCounter(count=1000)
        db.session.add(bc)
    else:
        bc.count = 1000

    settings = Settings.query.first()
    if not settings:
        settings = Settings()
        db.session.add(settings)

    db.session.commit()


def seed_dummy_data() -> dict:
    random.seed(42)

    # Materials
    mats = []
    for i, name in enumerate(
        ["DG", "BESTWAY", "FAUJI", "PIONEER", "PAKCEM", "LUCKY", "MAPLELEAF", "ASKARI"],
        start=1,
    ):
        m = Material(code=f"MAT-{i:04d}", name=name, unit_price=random.randint(1180, 1420), total=0)
        db.session.add(m)
        mats.append(m)

    # Clients
    clients = []
    for i in range(1, 51):
        c = Client(
            code=f"FBM-{i:05d}",
            name=f"Client {i:03d}",
            phone=f"0300{random.randint(1000000, 9999999)}",
            address=f"Area {random.randint(1, 20)}",
            category="General" if i % 4 else "Credit Customer",
            is_active=True,
        )
        db.session.add(c)
        clients.append(c)

    open_khata_client = Client(code=OPEN_KHATA_CODE, name=OPEN_KHATA_NAME, category="Open Khata", is_active=True)
    db.session.add(open_khata_client)

    db.session.commit()

    # GRN / IN entries
    for g in range(1, 11):
        grn = GRN(
            supplier=f"Supplier {g:02d}",
            manual_bill_no=f"GRN-{1000+g}",
            auto_bill_no=f"#{2000+g}",
            date_posted=datetime.now() - timedelta(days=random.randint(5, 30)),
            note="Dummy GRN",
        )
        db.session.add(grn)
        db.session.flush()
        for _ in range(random.randint(2, 4)):
            m = random.choice(mats)
            qty = random.randint(100, 300)
            price = m.unit_price
            db.session.add(GRNItem(grn_id=grn.id, mat_name=m.name, qty=qty, price_at_time=price))
            m.total = (m.total or 0) + qty
            db.session.add(
                Entry(
                    date=(datetime.now() - timedelta(days=random.randint(5, 30))).strftime("%Y-%m-%d"),
                    time="10:00:00",
                    type="IN",
                    material=m.name,
                    client=grn.supplier,
                    qty=qty,
                    bill_no=grn.manual_bill_no,
                    auto_bill_no=grn.auto_bill_no,
                    created_by="admin",
                    note="Dummy GRN IN",
                )
            )

    # Bookings + booking dispatch + pending
    for b in range(1, 31):
        c = random.choice(clients)
        bill_no = f"BK-{3000+b}"
        amount = 0
        bk = Booking(
            client_name=c.name,
            amount=0,
            paid_amount=0,
            manual_bill_no=bill_no,
            date_posted=datetime.now() - timedelta(days=random.randint(1, 25)),
            note="Dummy booking",
        )
        db.session.add(bk)
        db.session.flush()
        for _ in range(random.randint(1, 3)):
            m = random.choice(mats)
            qty = random.randint(20, 80)
            rate = m.unit_price
            amount += qty * rate
            db.session.add(BookingItem(booking_id=bk.id, material_name=m.name, qty=qty, price_at_time=rate))
            # Partial delivery
            delivered = int(qty * random.uniform(0.2, 0.9))
            if delivered > 0:
                m.total = (m.total or 0) - delivered
                db.session.add(
                    Entry(
                        date=(datetime.now() - timedelta(days=random.randint(0, 20))).strftime("%Y-%m-%d"),
                        time="13:00:00",
                        type="OUT",
                        material=m.name,
                        client=c.name,
                        client_code=c.code,
                        client_category="Booking Delivery",
                        qty=delivered,
                        bill_no=bill_no,
                        nimbus_no="Dummy Booking Dispatch",
                        transaction_category="BILLED",
                        created_by="admin",
                    )
                )
        paid = random.choice([0, amount * 0.3, amount * 0.6])
        bk.amount = amount
        bk.paid_amount = paid
        if amount - paid > 0:
            db.session.add(
                PendingBill(
                    client_code=c.code,
                    client_name=c.name,
                    bill_no=bill_no,
                    amount=amount - paid,
                    reason="Dummy Booking Balance",
                    created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                    created_by="admin",
                    is_manual=True,
                )
            )

    # Payments
    for p in range(1, 26):
        c = random.choice(clients)
        amt = random.randint(5000, 50000)
        db.session.add(
            Payment(
                client_name=c.name,
                amount=amt,
                method=random.choice(["Cash", "Bank Transfer", "Cheque"]),
                manual_bill_no=f"PAY-{4000+p}",
                date_posted=datetime.now() - timedelta(days=random.randint(0, 20)),
                note="Dummy payment",
            )
        )

    # Direct sales and dispatch
    sale_cats = ["Cash", "Credit Customer", "Open Khata", "Mixed Transaction"]
    for s in range(1, 36):
        cat = random.choice(sale_cats)
        if cat == "Open Khata":
            c = open_khata_client
        else:
            c = random.choice(clients)
        bill_no = f"DS-{5000+s}"
        ds = DirectSale(
            client_name=c.name,
            category=cat,
            amount=0,
            paid_amount=0,
            manual_bill_no=bill_no,
            auto_bill_no=None,
            date_posted=datetime.now() - timedelta(days=random.randint(0, 20)),
            note="Dummy direct sale",
        )
        db.session.add(ds)
        db.session.flush()
        total_amt = 0
        total_paid = 0
        for _ in range(random.randint(1, 2)):
            m = random.choice(mats)
            qty = random.randint(10, 50)
            price = m.unit_price
            m.total = (m.total or 0) - qty
            db.session.add(DirectSaleItem(sale_id=ds.id, product_name=m.name, qty=qty, price_at_time=price))
            db.session.add(
                Entry(
                    date=(datetime.now() - timedelta(days=random.randint(0, 20))).strftime("%Y-%m-%d"),
                    time="15:00:00",
                    type="OUT",
                    material=m.name,
                    client=c.name,
                    client_code=c.code,
                    client_category=cat,
                    qty=qty,
                    bill_no=bill_no,
                    nimbus_no="Dummy Direct Sale",
                    transaction_category="BILLED" if cat != "Cash" else "UNBILLED",
                    created_by="admin",
                )
            )
            total_amt += qty * price
        if cat == "Cash":
            total_paid = total_amt
        elif cat == "Mixed Transaction":
            total_paid = total_amt * 0.5
        else:
            total_paid = 0
        ds.amount = total_amt
        ds.paid_amount = total_paid
        if total_amt - total_paid > 0:
            db.session.add(
                PendingBill(
                    client_code=c.code,
                    client_name=c.name,
                    bill_no=bill_no,
                    amount=total_amt - total_paid,
                    reason=f"Dummy {cat} sale",
                    created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                    created_by="admin",
                    is_manual=True,
                    is_cash=(cat == "Cash"),
                )
            )

    # Extra open khata pending entries
    for i in range(1, 11):
        db.session.add(
            PendingBill(
                client_code=OPEN_KHATA_CODE,
                client_name=OPEN_KHATA_NAME,
                bill_no=f"OK-{6000+i}",
                nimbus_no=f"N-{7000+i}",
                amount=random.randint(3000, 25000),
                reason="Dummy Open Khata",
                created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                created_by="admin",
                is_manual=True,
            )
        )

    # Deliveries (legacy table coverage)
    for i in range(1, 6):
        c = random.choice(clients)
        d = Delivery(
            client_name=c.name,
            manual_bill_no=f"DLV-{9000+i}",
            auto_bill_no=f"#{9500+i}",
            date_posted=datetime.now() - timedelta(days=random.randint(1, 20)),
        )
        db.session.add(d)
        db.session.flush()
        for _ in range(random.randint(1, 2)):
            m = random.choice(mats)
            q = random.randint(5, 20)
            db.session.add(DeliveryItem(delivery_id=d.id, product=m.name, qty=q))

    # Recon basket samples
    for i in range(1, 6):
        db.session.add(
            ReconBasket(
                bill_no=f"RB-{9900+i}",
                inv_date=date.today() - timedelta(days=i),
                inv_client=f"INV CLIENT {i}",
                fin_client=f"FIN CLIENT {i}",
                inv_material=random.choice(mats).name,
                inv_qty=random.randint(5, 25),
                status=random.choice(["GREEN", "YELLOW", "RED", "BLUE"]),
                match_score=random.randint(50, 100),
            )
        )

    # Simple invoices
    for i in range(1, 11):
        c = random.choice(clients)
        total = random.randint(10000, 90000)
        paid = random.choice([0, int(total * 0.4), total])
        bal = total - paid
        status = "PAID" if bal == 0 else ("PARTIAL" if paid > 0 else "OPEN")
        db.session.add(
            Invoice(
                client_code=c.code,
                client_name=c.name,
                invoice_no=f"INV-{8000+i}",
                is_manual=True,
                date=date.today() - timedelta(days=random.randint(1, 20)),
                total_amount=total,
                balance=bal,
                status=status,
                created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                created_by="admin",
            )
        )

    db.session.commit()

    return {
        "clients": Client.query.count(),
        "materials": Material.query.count(),
        "entries": Entry.query.count(),
        "bookings": Booking.query.count(),
        "booking_items": BookingItem.query.count(),
        "payments": Payment.query.count(),
        "direct_sales": DirectSale.query.count(),
        "direct_sale_items": DirectSaleItem.query.count(),
        "pending_bills": PendingBill.query.count(),
        "grn": GRN.query.count(),
        "grn_items": GRNItem.query.count(),
        "deliveries": Delivery.query.count(),
        "delivery_items": DeliveryItem.query.count(),
        "recon_basket": ReconBasket.query.count(),
        "invoices": Invoice.query.count(),
    }


def smoke_test() -> list[tuple[str, int]]:
    failures: list[tuple[str, int]] = []
    with app.test_client() as c:
        c.post("/login", data={"username": "admin", "password": "admin123"}, follow_redirects=True)

        endpoints = [
            "/",
            "/clients",
            "/materials",
            "/bookings",
            "/payments",
            "/direct_sales",
            "/pending_bills",
            "/tracking",
            "/decision_ledger",
            "/unpaid_transactions",
            "/financial_details?type=cash",
            "/inventory/stock_summary",
            "/inventory/daily_transactions",
            "/import_export/",
            "/grn",
            "/settings",
        ]

        # Dynamic ledger pages
        first_clients = Client.query.order_by(Client.id.asc()).limit(10).all()
        endpoints += [f"/ledger/{x.id}" for x in first_clients]
        first_mats = Material.query.order_by(Material.id.asc()).limit(5).all()
        endpoints += [f"/material_ledger/{m.id}" for m in first_mats]

        for ep in endpoints:
            resp = c.get(ep, follow_redirects=True)
            if resp.status_code >= 500:
                failures.append((ep, resp.status_code))
    return failures


def main() -> None:
    with app.app_context():
        print(f"Backup: {backup_db()}")
        ensure_admin()
        wipe_business_data()
        stats = seed_dummy_data()
        print("Seeded data:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

        failures = smoke_test()
        print(f"Smoke test failures: {len(failures)}")
        for ep, code in failures:
            print(f"  {code} -> {ep}")


if __name__ == "__main__":
    main()
