import os
import pandas as pd
from datetime import datetime

from main import app, db, OPEN_KHATA_CODE, OPEN_KHATA_NAME, generate_material_code
from models import Client, Material, Entry, PendingBill, Booking, BookingItem, Payment, Invoice, DirectSale, DirectSaleItem, GRN, GRNItem, Delivery, DeliveryItem, ReconBasket

DATA_PATH = r"c:\Users\AHMED\Downloads\DATA CLEANING.xlsx"


def _norm(val):
    if val is None:
        return ""
    return str(val).strip()


def _parse_date(val):
    if not val:
        return None
    try:
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return None


def _reset_data():
    # Keep users and settings; wipe transactional tables
    for model in [
        Entry, PendingBill, BookingItem, Booking, Payment, Invoice,
        DirectSaleItem, DirectSale, GRNItem, GRN, DeliveryItem, Delivery,
        ReconBasket, Material, Client
    ]:
        db.session.query(model).delete()
    db.session.commit()


def _load_sheets():
    xl = pd.ExcelFile(DATA_PATH)
    idx = pd.read_excel(xl, "INDEX").fillna("")
    cement = pd.read_excel(xl, "CEMENT").fillna("")
    bills = pd.read_excel(xl, "BILLS").fillna("")
    imp = pd.read_excel(xl, "IMPORT").fillna("")

    idx.columns = [c.strip().upper() for c in idx.columns]
    cement.columns = [c.strip().upper() for c in cement.columns]
    bills.columns = [c.strip().upper() for c in bills.columns]
    imp.columns = [c.strip().upper() for c in imp.columns]
    return idx, cement, bills, imp


def _build_client_map(idx, bills, imp):
    name_by_code = {}

    for _, r in idx.iterrows():
        code = _norm(r.get("CLIENT_CODE"))
        name = _norm(r.get("NAME"))
        if code and name:
            name_by_code[code] = name

    for _, r in bills.iterrows():
        code = _norm(r.get("CODE"))
        name = _norm(r.get("NAME"))
        if code and name and code not in name_by_code:
            name_by_code[code] = name

    for _, r in imp.iterrows():
        code = _norm(r.get("CLIENT_CODE"))
        name = _norm(r.get("CLIENT_NAME"))
        if code and name and code not in name_by_code:
            name_by_code[code] = name

    return name_by_code


def _import_clients(idx, bills, imp):
    name_by_code = _build_client_map(idx, bills, imp)

    # Collect all codes
    codes = set()
    codes.update([_norm(x) for x in idx.get("CLIENT_CODE", []) if _norm(x)])
    codes.update([_norm(x) for x in bills.get("CODE", []) if _norm(x)])
    codes.update([_norm(x) for x in imp.get("CLIENT_CODE", []) if _norm(x)])

    for code in sorted(codes):
        if code.upper() == "OPEN KHATA":
            code = OPEN_KHATA_CODE
            name = OPEN_KHATA_NAME
        else:
            name = name_by_code.get(code, f"UNKNOWN {code}")

        if Client.query.filter_by(code=code).first():
            continue

        row = idx[idx["CLIENT_CODE"] == code]
        phone = _norm(row["PHONE"].iloc[0]) if not row.empty else ""
        address = _norm(row["ADDRESS"].iloc[0]) if not row.empty else ""
        status = _norm(row["STATUS"].iloc[0]).upper() if not row.empty else "ACTIVE"
        is_active = status in ["ACTIVE", "TRUE", "1", ""]

        db.session.add(Client(
            code=code,
            name=name,
            phone=phone,
            address=address,
            is_active=is_active
        ))
    db.session.commit()


def _import_materials(cement, imp):
    mats = set([_norm(x) for x in cement.get("ITEMS", []) if _norm(x)])
    mats.update([_norm(x) for x in imp.get("CEMENT_BRAND", []) if _norm(x)])

    for m in sorted(mats):
        if Material.query.filter_by(name=m).first():
            continue
        db.session.add(Material(
            name=m,
            code=generate_material_code(),
            unit_price=0,
            total=0
        ))
    db.session.commit()


def _import_entries(imp):
    for _, r in imp.iterrows():
        code = _norm(r.get("CLIENT_CODE"))
        if code.upper() == "OPEN KHATA":
            code = OPEN_KHATA_CODE
        name = _norm(r.get("CLIENT_NAME"))
        category = _norm(r.get("CLIENT_CATEGORY"))
        bill_no = _norm(r.get("BILL_NO"))
        if bill_no.upper() == "UNBILLED":
            bill_no = ""

        mat = _norm(r.get("CEMENT_BRAND"))
        qty = float(r.get("QTY") or 0)
        date_str = _parse_date(r.get("BILL_DATE")) or datetime.now().strftime("%Y-%m-%d")
        nimbus = _norm(r.get("NIMBUS"))

        client = Client.query.filter_by(code=code).first()
        if not client and name:
            client = Client.query.filter_by(name=name).first()

        final_code = client.code if client else code
        final_name = client.name if client else name

        db.session.add(Entry(
            date=date_str,
            time="12:00:00",
            type="OUT",
            material=mat or None,
            client=final_name,
            client_code=final_code,
            client_category=category or None,
            qty=qty,
            bill_no=bill_no or None,
            nimbus_no=nimbus or None,
            created_by="import"
        ))
    db.session.commit()


def _import_pending_bills(bills):
    for _, r in bills.iterrows():
        code = _norm(r.get("CODE"))
        if code.upper() == "OPEN KHATA":
            code = OPEN_KHATA_CODE
        bill_no = _norm(r.get("BILL NO"))
        name = _norm(r.get("NAME"))
        reason = _norm(r.get("REASON")) or _norm(r.get("NOTES")) or "Imported"
        nimbus = _norm(r.get("NIMBUS"))

        if not bill_no or not code:
            continue

        client = Client.query.filter_by(code=code).first()
        client_name = client.name if client else name

        if PendingBill.query.filter_by(bill_no=bill_no).first():
            continue

        db.session.add(PendingBill(
            client_code=code,
            client_name=client_name,
            bill_no=bill_no,
            amount=0,
            reason=reason,
            nimbus_no=nimbus,
            created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
            created_by="import",
            is_manual=True,
            note="Imported (amount missing)"
        ))
    db.session.commit()


def run():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(DATA_PATH)

    with app.app_context():
        idx, cement, bills, imp = _load_sheets()
        _reset_data()
        _import_clients(idx, bills, imp)
        _import_materials(cement, imp)
        _import_entries(imp)
        _import_pending_bills(bills)


if __name__ == "__main__":
    run()
