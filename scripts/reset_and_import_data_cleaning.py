from __future__ import annotations

import shutil
import re
from pathlib import Path
from datetime import datetime
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import app
from models import (
    db,
    Client,
    Material,
    Entry,
    PendingBill,
    Booking,
    BookingItem,
    Payment,
    DirectSale,
    DirectSaleItem,
    GRN,
    GRNItem,
    Delivery,
    DeliveryItem,
    Invoice,
    ReconBasket,
)


OPEN_KHATA_CODE = "OPEN-KHATA"
OPEN_KHATA_NAME = "OPEN KHATA"
FBM_RE = re.compile(r"^FBM-\d{5,6}$", re.IGNORECASE)
IMPORT_BILLS_SHEET = False


def _clean_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    s = str(value).strip()
    if s.lower() == "nan":
        return ""
    return s


def _norm_num_like(value: object) -> str:
    s = _clean_str(value)
    if not s:
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


def _norm_date(value: object) -> str:
    s = _clean_str(value)
    if not s:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def backup_db(db_path: Path) -> Path:
    backup_dir = db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"ahmed_cement_backup_before_reset_{ts}.db"
    shutil.copy2(db_path, backup_path)
    return backup_path


def reset_business_data() -> None:
    # Child/detail tables first
    ReconBasket.query.delete()
    DirectSaleItem.query.delete()
    BookingItem.query.delete()
    DeliveryItem.query.delete()
    GRNItem.query.delete()
    Entry.query.delete()
    PendingBill.query.delete()

    # Parent tables
    Booking.query.delete()
    Payment.query.delete()
    DirectSale.query.delete()
    Invoice.query.delete()
    Delivery.query.delete()
    GRN.query.delete()

    # Master data rebuilt from file
    Client.query.delete()
    Material.query.delete()

    db.session.commit()


def run_import(xlsx_path: Path) -> dict:
    idx = pd.read_excel(xlsx_path, sheet_name="INDEX").fillna("")
    imp = pd.read_excel(xlsx_path, sheet_name="IMPORT").fillna("")
    bills = pd.DataFrame()
    if IMPORT_BILLS_SHEET:
        bills = pd.read_excel(xlsx_path, sheet_name="BILLS").fillna("")

    idx.columns = [str(c).strip().upper() for c in idx.columns]
    imp.columns = [str(c).strip().upper() for c in imp.columns]
    if not bills.empty:
        bills.columns = [str(c).strip().upper() for c in bills.columns]

    # Verified clients: active + FBM code
    verified_rows = []
    for _, r in idx.iterrows():
        code = _clean_str(r.get("CLIENT_CODE")).upper()
        status = _clean_str(r.get("STATUS")).upper()
        if status == "ACTIVE" and FBM_RE.match(code):
            verified_rows.append(
                {
                    "code": code,
                    "name": _clean_str(r.get("NAME")) or code,
                    "phone": _clean_str(r.get("PHONE")),
                    "address": _clean_str(r.get("ADDRESS")),
                }
            )

    verified_by_code = {r["code"]: r for r in verified_rows}

    # Create verified clients
    for r in verified_rows:
        db.session.add(
            Client(
                code=r["code"],
                name=r["name"],
                phone=r["phone"],
                address=r["address"],
                category="General",
                is_active=True,
            )
        )

    # Add unified Open Khata client
    db.session.add(
        Client(
            code=OPEN_KHATA_CODE,
            name=OPEN_KHATA_NAME,
            category="Open Khata",
            is_active=True,
        )
    )
    db.session.flush()

    # Materials from import sheet
    mat_names = sorted(
        {
            _clean_str(v)
            for v in imp.get("CEMENT_BRAND", pd.Series(dtype=str)).tolist()
            if _clean_str(v)
        }
    )
    for i, name in enumerate(mat_names, start=1):
        code = f"MAT-{i:04d}"
        db.session.add(Material(code=code, name=name, total=0))
    db.session.flush()

    # Import dispatch entries
    imported_entries = 0
    open_khata_entries = 0
    for _, r in imp.iterrows():
        mat = _clean_str(r.get("CEMENT_BRAND"))
        if not mat:
            continue
        qty_s = _clean_str(r.get("QTY"))
        try:
            qty = float(qty_s or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue

        raw_code = _clean_str(r.get("CLIENT_CODE")).upper()
        raw_name = _clean_str(r.get("CLIENT_NAME"))
        bill_no = _norm_num_like(r.get("BILL_NO"))
        nimbus_no = _norm_num_like(r.get("NIMBUS"))
        tx_cat = _clean_str(r.get("TRANSACTION_CATEGORY")).upper()
        bill_date = _norm_date(r.get("BILL_DATE"))
        note = _clean_str(r.get("NOTES"))

        if raw_code in verified_by_code:
            client_code = raw_code
            client_name = verified_by_code[raw_code]["name"] or raw_name or raw_code
            client_category = "General"
        else:
            client_code = OPEN_KHATA_CODE
            client_name = raw_name or OPEN_KHATA_NAME
            client_category = "Open Khata"
            open_khata_entries += 1

        # Keep material stock consistent with OUT entries
        mat_obj = Material.query.filter_by(name=mat).first()
        if mat_obj:
            mat_obj.total = (mat_obj.total or 0) - qty

        db.session.add(
            Entry(
                date=bill_date,
                time="00:00:00",
                type="OUT",
                material=mat,
                client=client_name,
                client_code=client_code,
                client_category=client_category,
                qty=qty,
                bill_no=bill_no or None,
                nimbus_no=nimbus_no or None,
                transaction_category=tx_cat or None,
                note=note or None,
                created_by="data-cleaning-import",
            )
        )
        imported_entries += 1

    # Optional pending bills from BILLS sheet (amount unknown => 0)
    imported_bills = 0
    if not bills.empty:
        for _, r in bills.iterrows():
            bill_no = _norm_num_like(r.get("BILL NO"))
            if not bill_no:
                continue
            raw_code = _clean_str(r.get("CODE")).upper()
            raw_name = _clean_str(r.get("NAME"))
            notes = _clean_str(r.get("NOTES"))
            reason = _clean_str(r.get("REASON"))
            nimbus = _norm_num_like(r.get("NIMBUS"))

            if raw_code in verified_by_code:
                client_code = raw_code
                client_name = verified_by_code[raw_code]["name"] or raw_name or raw_code
            else:
                client_code = OPEN_KHATA_CODE
                client_name = raw_name or OPEN_KHATA_NAME

            exists = PendingBill.query.filter_by(bill_no=bill_no, client_code=client_code).first()
            if exists:
                continue

            db.session.add(
                PendingBill(
                    client_code=client_code,
                    client_name=client_name,
                    bill_no=bill_no,
                    nimbus_no=nimbus or None,
                    amount=0,
                    reason=reason or "Imported from BILLS sheet",
                    note=notes or None,
                    is_paid=False,
                    is_manual=True,
                    created_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                    created_by="data-cleaning-import",
                )
            )
            imported_bills += 1

    db.session.commit()

    return {
        "verified_clients": len(verified_rows),
        "open_khata_client": 1,
        "materials": len(mat_names),
        "entries": imported_entries,
        "open_khata_entries": open_khata_entries,
        "pending_bills": imported_bills,
    }


def main() -> None:
    xlsx_path = Path(r"C:\Users\AHMED\Downloads\DATA CLEANING.xlsx")
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Missing file: {xlsx_path}")

    with app.app_context():
        db_uri = app.config["SQLALCHEMY_DATABASE_URI"]
        db_path = Path(db_uri.replace("sqlite:///", ""))
        backup_path = backup_db(db_path)
        print(f"Backup: {backup_path}")

        reset_business_data()
        result = run_import(xlsx_path)
        print("Import complete:")
        for k, v in result.items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
