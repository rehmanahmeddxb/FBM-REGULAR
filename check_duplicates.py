import sqlite3

conn = sqlite3.connect('/home/claude/rv5/instance/ahmed_cement.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("=" * 80)
print("CHECKING FOR DUPLICATE BILLS")
print("=" * 80)

# Find duplicate pending bills (same bill_no and client_code)
cursor.execute("""
    SELECT bill_no, client_code, COUNT(*) as count
    FROM pending_bill
    WHERE is_void = 0 AND bill_no != '' AND bill_no IS NOT NULL
    GROUP BY bill_no, client_code
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 20
""")
duplicates = cursor.fetchall()

print(f"\nFound {len(duplicates)} bill numbers with duplicates")
if duplicates:
    print("\nTop duplicates:")
    for dup in duplicates[:10]:
        print(f"  Bill {dup['bill_no']} for client {dup['client_code']}: {dup['count']} records")

# Consolidate duplicates: keep the first, delete others, sum amounts
print("\n\nConsolidating duplicate pending bills...")

total_consolidated = 0
for dup in duplicates:
    cursor.execute("""
        SELECT id, amount, is_paid FROM pending_bill
        WHERE bill_no = ? AND client_code = ? AND is_void = 0
        ORDER BY id ASC
    """, (dup['bill_no'], dup['client_code']))
    
    records = cursor.fetchall()
    if len(records) <= 1:
        continue
    
    # Keep first record, sum all amounts, delete others
    keep_id = records[0]['id']
    total_amount = sum(r['amount'] for r in records)
    any_paid = any(r['is_paid'] for r in records)
    
    # Update the first record
    cursor.execute("""
        UPDATE pending_bill 
        SET amount = ?, is_paid = ?
        WHERE id = ?
    """, (total_amount, 1 if any_paid else 0, keep_id))
    
    # Mark others as void instead of deleting (to preserve audit trail)
    for record in records[1:]:
        cursor.execute("""
            UPDATE pending_bill SET is_void = 1 WHERE id = ?
        """, (record['id'],))
        total_consolidated += 1

conn.commit()
print(f"✓ Consolidated {total_consolidated} duplicate records")

# Now check matching again
print("\n" + "=" * 80)
print("RECHECKING BILL MATCHING")
print("=" * 80)

cursor.execute("""
    SELECT COUNT(DISTINCT e.bill_no) as unique_bills
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0 
    AND e.bill_no IS NOT NULL AND e.bill_no != '' AND e.bill_no != 'CASH'
    AND e.client_code IS NOT NULL
""")
unique_entry_bills = cursor.fetchone()['unique_bills']

cursor.execute("""
    SELECT COUNT(DISTINCT bill_no) as unique_bills
    FROM pending_bill
    WHERE is_void = 0 AND bill_no IS NOT NULL AND bill_no != ''
""")
unique_pending_bills = cursor.fetchone()['unique_bills']

print(f"\n✓ Unique bills in dispatch entries: {unique_entry_bills}")
print(f"✓ Unique bills in pending_bill table: {unique_pending_bills}")

# Count how many dispatch bills have a matching pending bill
cursor.execute("""
    SELECT COUNT(DISTINCT e.bill_no)
    FROM entry e
    INNER JOIN pending_bill pb ON (
        e.bill_no = pb.bill_no 
        AND e.client_code = pb.client_code
    )
    WHERE e.type = 'OUT' AND e.is_void = 0 AND pb.is_void = 0
    AND e.bill_no != 'CASH' AND e.bill_no != ''
    AND e.client_code IS NOT NULL
""")
matched_bills = cursor.fetchone()[0]

print(f"✓ Bills that match between Entry and PendingBill: {matched_bills}")
print(f"✓ Match ratio: {matched_bills/unique_entry_bills*100:.1f}%")

# Sample some unmatched entries to see why they don't match
print("\n\nSample unmatched entries:")
cursor.execute("""
    SELECT e.id, e.bill_no, e.client, e.client_code, e.material, e.qty
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0 
    AND e.bill_no IS NOT NULL AND e.bill_no != '' AND e.bill_no != 'CASH'
    AND e.client_code IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM pending_bill pb
        WHERE pb.bill_no = e.bill_no 
        AND pb.client_code = e.client_code
        AND pb.is_void = 0
    )
    LIMIT 5
""")
unmatched = cursor.fetchall()

for um in unmatched:
    print(f"\nEntry {um['id']}: Bill {um['bill_no']}, Client: {um['client']} ({um['client_code']})")
    print(f"  Material: {um['material']}, Qty: {um['qty']}")
    
    # Check if there's a pending bill with this bill number but different client
    cursor.execute("""
        SELECT id, client_code, client_name FROM pending_bill
        WHERE bill_no = ? AND is_void = 0
    """, (um['bill_no'],))
    other_pb = cursor.fetchone()
    
    if other_pb:
        print(f"  ⚠️  PendingBill exists but with different client: {other_pb['client_name']} ({other_pb['client_code']})")
    else:
        print(f"  ✗ No PendingBill exists at all for this bill number")

# Count unpaid vs paid
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN is_paid = 0 THEN 1 ELSE 0 END) as unpaid,
        SUM(CASE WHEN is_paid = 1 THEN 1 ELSE 0 END) as paid
    FROM pending_bill
    WHERE is_void = 0 AND amount > 0
""")
status = cursor.fetchone()

print("\n\n" + "=" * 80)
print("PENDING BILLS STATUS SUMMARY")
print("=" * 80)
print(f"\nTotal bills (amount > 0): {status['total']}")
print(f"  Unpaid: {status['unpaid']}")
print(f"  Paid: {status['paid']}")

conn.close()
print("\n" + "=" * 80)
print("ANALYSIS COMPLETE!")
print("=" * 80)
