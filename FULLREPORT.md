# FULLREPORT

Date: 2026-03-08

## 1) Due Sale client blocked when client had booking balance
- Problem:
  - `Due Sale (Credit Customer)` was hiding clients that had booking balance.
  - Backend also rejected due-sale save for such clients.
- Impact:
  - User had to force `Booked + Due` even when actual transaction was due-only.
- Fix Applied:
  - Removed backend restriction that blocked due sale for booked clients.
  - Updated frontend client filter so `Due Sale` does not hide booked clients.
- Status: Fixed

## 2) Add Booking form missing date selector
- Problem:
  - Add Booking backend expected `date`, but Add Booking modal had no date input.
- Impact:
  - Could not explicitly choose booking date/time while creating booking.
- Fix Applied:
  - Added `BOOKING DATE & TIME` (`datetime-local`) input in Add Booking modal.
  - Added default current datetime initialization.
- Status: Fixed

## 3) Client Ledger PDF Financial Description missing item + qty
- Problem:
  - Financial Transaction Ledger description only showed generic labels (`Booking`, `Direct Sale`).
- Impact:
  - Item-level context was missing in financial section of PDF.
- Fix Applied:
  - Extended ledger row builder to include line items in description for booking/sale.
- Status: Fixed

## 4) Client Ledger PDF Financial Description missing rate (price)
- Problem:
  - Description showed item/qty but not rate.
- Impact:
  - Could not audit amount composition from financial section quickly.
- Fix Applied:
  - Included `qty x rate` format in booking/sale description.
- Status: Fixed

## 5) Discount shown merged in bill row instead of separate row
- Problem:
  - Discount was reducing main debit in same bill row.
- Impact:
  - Discount visibility and audit trail were unclear.
- Fix Applied:
  - Main bill row now keeps full debit.
  - Added separate discount row immediately below bill:
    - `Booking Discount (Loss)`
    - `Direct Sale Discount (Loss)`
- Status: Fixed

## 6) Alternate (ALT) mapping lost after editing sale
- Problem:
  - Edit flow could drop `booked_material` linkage (e.g., `DG > ALT > KOHAT`).
- Impact:
  - Alternate mapping disappeared and stock/source semantics changed.
- Fix Applied:
  - Edit backend now preserves prior alternate mapping when saving.
  - Also keeps stock deduction based on original booked material when ALT is active.
- Status: Fixed

## 7) Edit Sale UI lacked Alternate Material option
- Problem:
  - Add Sale supported ALT input, Edit Sale rows did not.
- Impact:
  - Edit flow was not parity with add flow and could miss critical fields.
- Fix Applied:
  - Added `Alternate Material (Optional)` to edit rows.
  - Wired edit row combobox behavior for ALT fields.
  - Backend edit now reads `alternate_material[]` and validates it.
- Status: Fixed

## 8) Edit allowed partial/invalid client/material text (e.g., `ko`)
- Problem:
  - Partial mistyped values could be saved in edit flow.
- Impact:
  - Data quality issue; invalid client/material references could enter transactions.
- Fix Applied:
  - Backend edit now enforces:
    - Registered client required for booking/mixed/credit sale types.
    - Material must exist in master list.
    - Alternate material must exist in master list.
  - Frontend pre-submit validation added to block partial/invalid client/material selections.
- Status: Fixed

## 9) Voided Vault showing rows user did not manually void
- Problem Reported:
  - User saw many voided rows and suspected bug.
- Investigation Findings:
  - Vault lists all rows where `is_void=True`.
  - Direct sale edit/resync flow auto-voids old child rows (entries/pending bills) then creates replacement active rows.
  - This is why old rows appear in vault even without explicit manual void click.
- Fix Applied:
  - No code change in this session (investigation only).
- Status: Investigated (UX clarity issue remains)

## Suggested follow-up (not yet implemented)
- Add an origin flag for void actions (`manual` vs `system-resync`) and show this in Voided Vault.
- Optionally hide/compact system-resync voids by default in vault filters.
