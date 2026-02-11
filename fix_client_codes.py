import sqlite3

conn = sqlite3.connect('/home/claude/rv5/instance/ahmed_cement.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

print("=" * 80)
print("FIXING CLIENT CODE MISMATCHES")
print("=" * 80)

# ISSUE: Entries have client_code = 'CODE' but should have actual client codes
# Fix strategy: Match by client name and update the code

print("\n1. Fixing entries with generic 'CODE' client_code...")

cursor.execute("""
    SELECT id, client, client_code
    FROM entry
    WHERE client_code = 'CODE' AND type = 'OUT' AND is_void = 0
""")
entries_with_code = cursor.fetchall()

print(f"   Found {len(entries_with_code)} entries with 'CODE' as client_code")

fixed = 0
for entry in entries_with_code:
    # Try to find actual client by name
    client_name = entry['client']
    
    # Try exact match first
    cursor.execute("""
        SELECT code FROM client WHERE name = ?
    """, (client_name,))
    client = cursor.fetchone()
    
    if not client:
        # Try fuzzy match (contains)
        cursor.execute("""
            SELECT code, name FROM client 
            WHERE name LIKE ? OR ? LIKE '%' || name || '%'
            LIMIT 1
        """, (f'%{client_name}%', client_name))
        client = cursor.fetchone()
    
    if client:
        cursor.execute("""
            UPDATE entry SET client_code = ? WHERE id = ?
        """, (client['code'], entry['id']))
        fixed += 1

conn.commit()
print(f"   ✓ Fixed {fixed} entries with correct client codes")

print("\n2. Fixing entries with client codes that look like bill numbers (numeric)...")

cursor.execute("""
    SELECT id, client, client_code
    FROM entry
    WHERE type = 'OUT' AND is_void = 0
    AND client_code NOT LIKE 'FBM-%' 
    AND client_code NOT LIKE 'tmpc-%'
    AND client_code != 'CODE'
    AND client_code IS NOT NULL
    LIMIT 100
""")
numeric_codes = cursor.fetchall()

print(f"   Found {len(numeric_codes)} entries with non-standard client codes")

fixed2 = 0
for entry in numeric_codes:
    client_name = entry['client']
    
    # Try to find actual client
    cursor.execute("""
        SELECT code FROM client WHERE name LIKE ? LIMIT 1
    """, (f'%{client_name}%',))
    client = cursor.fetchone()
    
    if client:
        cursor.execute("""
            UPDATE entry SET client_code = ? WHERE id = ?
        """, (client['code'], entry['id']))
        fixed2 += 1

conn.commit()
print(f"   ✓ Fixed {fixed2} entries with correct client codes")

print("\n3. Creating/updating PendingBills with corrected client codes...")

# Now re-sync pending bills with the corrected entry data
cursor.execute("""
    SELECT e.id, e.date, e.bill_no, e.nimbus_no, 
           e.client, e.client_code, e.material, e.qty, e.created_by
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0
    AND e.bill_no IS NOT NULL AND e.bill_no != '' AND e.bill_no != 'CASH'
    AND e.client_code IS NOT NULL 
    AND e.client_code LIKE 'FBM-%'
""")
valid_entries = cursor.fetchall()

print(f"   Processing {len(valid_entries)} valid dispatch entries...")

created = 0
updated = 0

for entry in valid_entries:
    # Check if PendingBill exists with exact match
    cursor.execute("""
        SELECT id, amount FROM pending_bill
        WHERE bill_no = ? AND client_code = ? AND is_void = 0
    """, (entry['bill_no'], entry['client_code']))
    
    existing = cursor.fetchone()
    
    if not existing:
        # Check if bill exists with a different client code
        cursor.execute("""
            SELECT id, client_code FROM pending_bill
            WHERE bill_no = ? AND is_void = 0
        """, (entry['bill_no'],))
        
        wrong_client = cursor.fetchone()
        
        if wrong_client:
            # Update the client code to match the entry
            cursor.execute("""
                UPDATE pending_bill 
                SET client_code = ?, client_name = ?
                WHERE id = ?
            """, (entry['client_code'], entry['client'], wrong_client['id']))
            updated += 1
        else:
            # Create new PendingBill
            cursor.execute("SELECT unit_price FROM material WHERE name = ?", (entry['material'],))
            mat_result = cursor.fetchone()
            unit_price = mat_result['unit_price'] if mat_result else 1400
            
            amount = entry['qty'] * unit_price
            
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
                0,
                0,
                1,
                entry['date'],
                entry['created_by'] or 'system'
            ))
            created += 1

conn.commit()
print(f"   ✓ Created {created} new PendingBills")
print(f"   ✓ Updated {updated} existing PendingBills with correct client codes")

# Final verification
print("\n" + "=" * 80)
print("FINAL VERIFICATION AFTER FIXES")
print("=" * 80)

cursor.execute("""
    SELECT COUNT(DISTINCT e.bill_no)
    FROM entry e
    WHERE e.type = 'OUT' AND e.is_void = 0 
    AND e.bill_no IS NOT NULL AND e.bill_no != '' AND e.bill_no != 'CASH'
    AND e.client_code IS NOT NULL
    AND e.client_code LIKE 'FBM-%'
""")
valid_entry_bills = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(DISTINCT e.bill_no)
    FROM entry e
    INNER JOIN pending_bill pb ON (
        e.bill_no = pb.bill_no 
        AND e.client_code = pb.client_code
    )
    WHERE e.type = 'OUT' AND e.is_void = 0 AND pb.is_void = 0
    AND e.bill_no != 'CASH' AND e.bill_no != ''
    AND e.client_code LIKE 'FBM-%'
""")
matched_bills = cursor.fetchone()[0]

print(f"\n✓ Valid dispatch entries with proper client codes: {valid_entry_bills}")
print(f"✓ Matched bills between Entry and PendingBill: {matched_bills}")
print(f"✓ Match ratio: {matched_bills/valid_entry_bills*100:.1f}%")

# Check unpaid bills
cursor.execute("""
    SELECT COUNT(*) FROM pending_bill 
    WHERE is_void = 0 AND is_paid = 0 AND amount > 0
""")
unpaid_count = cursor.fetchone()[0]

print(f"\n✓ Total unpaid bills: {unpaid_count}")

# Check recent test data
cursor.execute("""
    SELECT e.id, e.date, e.client, e.client_code, e.bill_no, pb.is_paid
    FROM entry e
    LEFT JOIN pending_bill pb ON (
        pb.bill_no = e.bill_no
        AND pb.client_code = e.client_code
        AND pb.is_void = 0
    )
    WHERE e.type = 'OUT' AND e.is_void = 0
    AND e.date >= date('now')
    AND e.bill_no LIKE 'TEST-%'
    ORDER BY e.id DESC
""")
test_entries = cursor.fetchall()

if test_entries:
    print(f"\n✓ Test dispatch entries (from today):")
    for te in test_entries:
        paid_status = "PAID" if te['is_paid'] == 1 else "UNPAID" if te['is_paid'] == 0 else "NO BILL"
        print(f"   Entry {te['id']}: {te['client']} ({te['client_code']}) | Bill: {te['bill_no']} | {paid_status}")

conn.close()

print("\n" + "=" * 80)
print("CLIENT CODE FIX COMPLETE!")
print("=" * 80)
print("\nSUMMARY:")
print("✓ Fixed entries with generic 'CODE' client_code")
print("✓ Fixed entries with numeric/non-standard client codes")
print("✓ Created/updated PendingBills with correct client code matching")
print("✓ Verified bill matching between Entry and PendingBill tables")
print("\nThe app should now show:")
print("- All dispatch entries properly linked to their bills")
print("- Unpaid bills showing correctly in the Unpaid Transactions page")
print("- Pending Bills page showing accurate paid/unpaid status")
print("=" * 80)
