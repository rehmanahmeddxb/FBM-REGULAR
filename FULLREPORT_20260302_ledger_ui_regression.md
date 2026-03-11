# Full Regression Report (2026-03-02)

## Scope Checked
- App bootstrap / route registration
- Supplier ledger page actions and CRUD visibility
- Supplier ledger print layout compactness
- Recent theme + compact-row changes impact on button visibility
- Key templates for edit/void button presence (bookings, sales, payments, GRN, tracking, pending bills, supplier/client ledgers)

## Commands / Validation
- `python -c "from app import create_app; create_app(); print('app boot OK')"` passed.
- Template scan for `bi-pencil`, `bi-trash`, `btn-outline-danger`, `btn-outline-warning` across `templates/` confirms expected action controls are present.
- Supplier ledger actions specifically re-verified after fix.

## Findings
1. **Resolved:** Supplier ledger `Actions` column controls were clipped by generic `overflow: hidden` compact-table CSS.
   - Root cause: compact style was applied to all cells including action buttons.
   - Fix: limit clipping to non-action columns and force visible overflow for action column.
   - CRUD restored on supplier ledger rows:
     - GRN: Download, Print, Edit, Void
     - Payment: Download, Print, Edit, Void

2. **Verified:** Major operational templates still include edit/void controls after recent compact/theme updates:
   - `bookings.html`, `direct_sales.html`, `payments.html`, `grn_wizard.html`, `tracking.html`, `pending_bills.html`, `supplier_ledger.html`.

## Preventive Safeguard (Important)
When applying UI compacting/theming updates in future, **do not apply global clipping (`overflow: hidden`, `text-overflow: ellipsis`, `white-space: nowrap`) to action/button columns**.

Mandatory pattern for tables with CRUD actions:
- Apply compact clipping only to non-action columns.
- Keep action cells explicitly visible:
  - `.actions-cell { overflow: visible !important; white-space: nowrap !important; }`
  - `.actions-head { overflow: visible !important; }`
- Re-test all action buttons after each UI/style change:
  - `Edit`, `Void/Delete`, `Print`, `Download`, modal triggers.

## Added Process Note
Before finalizing any UI update:
1. Run bootstrap check.
2. Scan templates for action buttons.
3. Manually verify at least:
   - Supplier ledger
   - Client ledger
   - Sales
   - Payments
   - GRN
4. Confirm no legacy function/button disappeared.

## Current Status
- No blocking regression found after fixes.
- Supplier ledger actions and CRUD are restored and visible.
