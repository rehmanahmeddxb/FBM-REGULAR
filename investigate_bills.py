import sqlite3
from datetime import datetime

# Connect to database
conn = sqlite3.connect('/home/claude/rv5/instance/ahmed_cement.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("=" * 80)
print("INVESTIGATING BILL MATCHING ISSUE")
print("=" * 80)

# Check dispatch entries that have bills but no matching PendingBill
cursor.execute("""
    SELECT e.id, e.date, e.bill_no, e.auto_bill_no, e.client, e.client_code
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0
    AND (e.bill_no IS NOT NULL AND e.bill_no != '' AND e.bill_no != 'CASH')
    LIMIT 20
""")
sample_entries = cursor.fetchall()

print("\nSample dispatch entries:")
for entry in sample_entries[:5]:
    print(f"\nEntry {entry['id']}:")
    print(f"  Client: {entry['client']} | Code: {entry['client_code']}")
    print(f"  Bill No: {entry['bill_no']} | Auto Bill: {entry['auto_bill_no']}")
    
    # Check if PendingBill exists with exact match
    cursor.execute("""
        SELECT id, bill_no, client_code FROM pending_bill
        WHERE bill_no = ? AND client_code = ? AND is_void = 0
    """, (entry['bill_no'], entry['client_code']))
    pb = cursor.fetchone()
    
    if pb:
        print(f"  ✓ Found PendingBill: ID {pb['id']}")
    else:
        print(f"  ✗ NO PendingBill found")
        
        # Check if client_code is NULL
        if not entry['client_code']:
            print(f"    ISSUE: Entry has NULL client_code!")

# Check how many entries have NULL client_code
cursor.execute("""
    SELECT COUNT(*) FROM entry
    WHERE type = 'OUT' AND is_void = 0
    AND (bill_no IS NOT NULL AND bill_no != '')
    AND client_code IS NULL
""")
null_code_count = cursor.fetchone()[0]

print(f"\n\n⚠️  CRITICAL ISSUE: {null_code_count} dispatch entries with bills but NULL client_code!")

# Fix entries with NULL client_code by matching client names
if null_code_count > 0:
    print("\nFixing entries with NULL client_code...")
    
    cursor.execute("""
        SELECT e.id, e.client
        FROM entry e
        WHERE e.type = 'OUT' AND e.is_void = 0
        AND (e.bill_no IS NOT NULL AND e.bill_no != '')
        AND e.client_code IS NULL
    """)
    entries_to_fix = cursor.fetchall()
    
    fixed = 0
    for entry in entries_to_fix:
        # Try to find client by name
        cursor.execute("""
            SELECT code FROM client WHERE name = ? OR code = ?
        """, (entry['client'], entry['client']))
        client = cursor.fetchone()
        
        if client:
            cursor.execute("""
                UPDATE entry SET client_code = ? WHERE id = ?
            """, (client['code'], entry['id']))
            fixed += 1
    
    conn.commit()
    print(f"✓ Fixed {fixed} entries with NULL client_code")

# Now re-create missing PendingBills with correct matching
print("\n\nRe-checking and creating missing PendingBills...")

cursor.execute("""
    SELECT e.id, e.date, e.bill_no, e.auto_bill_no, e.nimbus_no, 
           e.client, e.client_code, e.material, e.qty, e.created_by
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0
    AND (e.bill_no IS NOT NULL AND e.bill_no != '' AND e.bill_no != 'CASH')
    AND e.client_code IS NOT NULL
""")
all_entries = cursor.fetchall()

created = 0
for entry in all_entries:
    # Check if PendingBill already exists
    cursor.execute("""
        SELECT id FROM pending_bill
        WHERE bill_no = ? AND client_code = ? AND is_void = 0
    """, (entry['bill_no'], entry['client_code']))
    
    if not cursor.fetchone():
        # Get material price
        cursor.execute("SELECT unit_price FROM material WHERE name = ?", (entry['material'],))
        mat_result = cursor.fetchone()
        unit_price = mat_result['unit_price'] if mat_result else 1400
        
        amount = entry['qty'] * unit_price
        
        # Create PendingBill
        cursor.execute("""
            INSERT INTO pending_bill 
            (client_code, client_name, bill_no, nimbus_no, amount, reason, is_paid, is_cash, is_manual, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry['client_code'],
            entry['client'],
            entry['bill_no'],
            entry['nimbus_no'],
            amount,
            f"Dispatch: {entry['qty']} bags of {entry['material']}",
            0,  # is_paid = False
            0,
            1 if entry['bill_no'] else 0,
            entry['date'] or datetime.now().strftime('%Y-%m-%d %H:%M'),
            entry['created_by'] or 'system'
        ))
        created += 1

conn.commit()
print(f"✓ Created {created} additional PendingBill records")

# Final verification
print("\n" + "=" * 80)
print("FINAL VERIFICATION")
print("=" * 80)

cursor.execute("""
    SELECT COUNT(*) FROM entry 
    WHERE type = 'OUT' AND is_void = 0 
    AND (bill_no IS NOT NULL AND bill_no != '' AND bill_no != 'CASH')
    AND client_code IS NOT NULL
""")
dispatch_with_bills = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(DISTINCT pb.id) FROM pending_bill pb
    INNER JOIN entry e ON (
        pb.bill_no = e.bill_no
        AND pb.client_code = e.client_code
    )
    WHERE e.type = 'OUT' AND e.is_void = 0 AND pb.is_void = 0
    AND e.bill_no != 'CASH' AND e.bill_no != ''
""")
matching_pending_bills = cursor.fetchone()[0]

print(f"\n✓ Dispatch entries with valid bills: {dispatch_with_bills}")
print(f"✓ Matching PendingBills: {matching_pending_bills}")
print(f"✓ Match ratio: {matching_pending_bills/dispatch_with_bills*100:.1f}%")

# Count unpaid bills
cursor.execute("""
    SELECT COUNT(*) FROM pending_bill 
    WHERE is_void = 0 AND is_paid = 0 AND amount > 0
""")
unpaid_count = cursor.fetchone()[0]

print(f"\n✓ Total unpaid bills: {unpaid_count}")

conn.close()
print("\n" + "=" * 80)
print("INVESTIGATION AND FIX COMPLETE!")
print("=" * 80)
