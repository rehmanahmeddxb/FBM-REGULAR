import sqlite3
import json
from datetime import datetime, timedelta
import random

# Connect to database
conn = sqlite3.connect('/home/claude/rv5/instance/ahmed_cement.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("=" * 80)
print("DATABASE ANALYSIS REPORT")
print("=" * 80)

# 1. Check all tables
print("\n1. DATABASE TABLES:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cursor.fetchall()]
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"   - {table}: {count} records")

# 2. Check Entry records with dispatch (OUT) and bill_no
print("\n2. DISPATCH ENTRIES ANALYSIS:")
cursor.execute("""
    SELECT id, date, client, client_code, material, qty, bill_no, auto_bill_no, nimbus_no, invoice_id
    FROM entry 
    WHERE type = 'OUT' AND is_void = 0
    ORDER BY date DESC
    LIMIT 20
""")
entries = cursor.fetchall()
print(f"   Total dispatch entries found: {len(entries)}")
print("\n   Sample dispatch entries:")
for entry in entries[:5]:
    print(f"   ID: {entry['id']} | Date: {entry['date']} | Client: {entry['client']} ({entry['client_code']})")
    print(f"      Material: {entry['material']} | Qty: {entry['qty']}")
    print(f"      Bill No: {entry['bill_no']} | Auto Bill: {entry['auto_bill_no']} | Nimbus: {entry['nimbus_no']} | Invoice ID: {entry['invoice_id']}")
    print()

# 3. Check if bill_no matches with client in PendingBill
print("\n3. PENDING BILLS ANALYSIS:")
cursor.execute("""
    SELECT id, client_code, client_name, bill_no, nimbus_no, amount, is_paid, is_cash, created_at
    FROM pending_bill
    WHERE is_void = 0
    ORDER BY created_at DESC
    LIMIT 20
""")
pending_bills = cursor.fetchall()
print(f"   Total pending bills: {len(pending_bills)}")
print("\n   Sample pending bills:")
for pb in pending_bills[:5]:
    print(f"   ID: {pb['id']} | Client: {pb['client_name']} ({pb['client_code']})")
    print(f"      Bill No: {pb['bill_no']} | Nimbus: {pb['nimbus_no']} | Amount: {pb['amount']}")
    print(f"      Is Paid: {pb['is_paid']} | Is Cash: {pb['is_cash']} | Created: {pb['created_at']}")
    print()

# 4. Cross-check: Find dispatch entries and their corresponding pending bills
print("\n4. CROSS-CHECKING DISPATCH ENTRIES WITH PENDING BILLS:")
cursor.execute("""
    SELECT DISTINCT e.bill_no, e.auto_bill_no, e.nimbus_no, e.client, e.client_code
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0
    AND (e.bill_no IS NOT NULL OR e.auto_bill_no IS NOT NULL OR e.nimbus_no IS NOT NULL)
    LIMIT 10
""")
dispatch_bills = cursor.fetchall()

for db_entry in dispatch_bills:
    bill_identifiers = [db_entry['bill_no'], db_entry['auto_bill_no'], db_entry['nimbus_no']]
    bill_identifiers = [b for b in bill_identifiers if b]
    
    if bill_identifiers:
        print(f"\n   Dispatch Entry - Client: {db_entry['client']} ({db_entry['client_code']})")
        print(f"      Bills: {', '.join(bill_identifiers)}")
        
        # Check if any of these bills exist in PendingBill
        for bill in bill_identifiers:
            cursor.execute("""
                SELECT * FROM pending_bill 
                WHERE (bill_no = ? OR nimbus_no = ?) 
                AND client_code = ?
                AND is_void = 0
            """, (bill, bill, db_entry['client_code']))
            matching_pb = cursor.fetchone()
            
            if matching_pb:
                print(f"      ✓ Found in PendingBill: {bill} (Paid: {matching_pb['is_paid']})")
            else:
                print(f"      ✗ NOT found in PendingBill: {bill}")

# 5. Check unpaid bills that should appear in unpaid section
print("\n\n5. UNPAID BILLS THAT SHOULD APPEAR IN UNPAID SECTION:")
cursor.execute("""
    SELECT id, client_code, client_name, bill_no, nimbus_no, amount, is_paid, created_at
    FROM pending_bill
    WHERE is_void = 0 AND is_paid = 0
    ORDER BY created_at DESC
    LIMIT 10
""")
unpaid_bills = cursor.fetchall()
print(f"   Total unpaid bills (is_paid=0): {len(unpaid_bills)}")
for ub in unpaid_bills:
    print(f"   ID: {ub['id']} | {ub['client_name']} | Bill: {ub['bill_no'] or ub['nimbus_no']} | Amount: {ub['amount']}")

# 6. Check invoices
print("\n\n6. INVOICES ANALYSIS:")
cursor.execute("""
    SELECT id, client_code, client_name, invoice_no, is_manual, date, total_amount, balance, status
    FROM invoice
    WHERE is_void = 0
    ORDER BY date DESC
    LIMIT 10
""")
invoices = cursor.fetchall()
print(f"   Total invoices: {len(invoices)}")
for inv in invoices[:5]:
    print(f"   ID: {inv['id']} | {inv['client_name']} | Invoice: {inv['invoice_no']}")
    print(f"      Total: {inv['total_amount']} | Balance: {inv['balance']} | Status: {inv['status']}")

# 7. Check clients
print("\n\n7. CLIENTS ANALYSIS:")
cursor.execute("SELECT id, code, name, phone, category, is_active FROM client LIMIT 10")
clients = cursor.fetchall()
print(f"   Total clients: {len(clients)}")
for client in clients[:5]:
    print(f"   ID: {client['id']} | Code: {client['code']} | Name: {client['name']} | Category: {client['category']}")

# 8. Check materials
print("\n\n8. MATERIALS ANALYSIS:")
cursor.execute("SELECT id, code, name, unit_price, total FROM material")
materials = cursor.fetchall()
print(f"   Total materials: {len(materials)}")
for mat in materials:
    print(f"   ID: {mat['id']} | Code: {mat['code']} | Name: {mat['name']} | Price: {mat['unit_price']} | Stock: {mat['total']}")

print("\n" + "=" * 80)
print("ISSUES IDENTIFIED:")
print("=" * 80)

# Issue 1: Dispatch entries without bills
cursor.execute("""
    SELECT COUNT(*) FROM entry 
    WHERE type = 'OUT' AND is_void = 0 
    AND bill_no IS NULL AND auto_bill_no IS NULL AND nimbus_no IS NULL
""")
no_bill_count = cursor.fetchone()[0]
if no_bill_count > 0:
    print(f"\n⚠️  ISSUE 1: {no_bill_count} dispatch entries have NO bill numbers")

# Issue 2: Bills in dispatch but not in PendingBill
cursor.execute("""
    SELECT e.id, e.bill_no, e.auto_bill_no, e.nimbus_no, e.client, e.client_code
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0
    AND (e.bill_no IS NOT NULL OR e.auto_bill_no IS NOT NULL)
""")
all_dispatch = cursor.fetchall()
missing_in_pending = []
for dispatch in all_dispatch:
    bill = dispatch['bill_no'] or dispatch['auto_bill_no']
    if bill:
        cursor.execute("""
            SELECT id FROM pending_bill 
            WHERE (bill_no = ? OR nimbus_no = ?) 
            AND client_code = ?
            AND is_void = 0
        """, (bill, bill, dispatch['client_code']))
        if not cursor.fetchone():
            missing_in_pending.append(dispatch)

if missing_in_pending:
    print(f"\n⚠️  ISSUE 2: {len(missing_in_pending)} dispatch entries have bills but are NOT in PendingBill table")
    for m in missing_in_pending[:5]:
        print(f"   - Entry ID {m['id']}: Bill {m['bill_no'] or m['auto_bill_no']} for client {m['client']}")

# Issue 3: Unpaid bills not showing correctly
print(f"\n⚠️  ISSUE 3: Check if unpaid bills (is_paid=0) are showing in the unpaid section")
print(f"   - Total unpaid bills in database: {len(unpaid_bills)}")
print(f"   - These should all appear in the 'Unpaid Transactions' view")

conn.close()
print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
