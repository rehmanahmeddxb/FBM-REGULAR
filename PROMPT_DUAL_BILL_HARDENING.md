# Production Prompt: Dual Bill Hardening

Harden the billing system so Auto and Manual bill identities never mix across modules, imports, and views.

## Objectives
1. Auto bill references must be namespace-safe and collision-safe.
2. Manual bill references must remain user-controlled but normalized and unique.
3. Bill lookup/view/download must never open the wrong party/document.
4. Pending, ledgers, and totals must stay synchronized after every create/edit/void/import.

## Required Formats
1. Auto bill: `SB-<NS>-<N>` where:
   - `BK` = Booking
   - `CP` = Client Payment
   - `SP` = Supplier Payment
   - `GRN` = Goods Receipt Note
   - `SL` = Direct Sale
   - `EN` = Entry/Dispatch internal auto refs
2. Manual bill: `MB NO.<VALUE>`

## Implementation Rules
1. Add canonical helpers:
   - `normalize_auto_bill(value, namespace)`
   - `normalize_manual_bill(value)`
   - `parse_bill_kind(value)`
   - `parse_bill_namespace(value)`
2. Enforce normalization in model/session hooks before write.
3. Use per-namespace `BillCounter` rows and sync each counter against DB max before issuing next bill.
4. Generate auto bills with explicit namespace in all modules.
5. Apply conflict checks before commit and return clear errors.
6. Bill resolver (`view_bill`, `download_invoice`) must use source hints first and show collision resolution when ambiguous.
7. Pending bills must store normalized `bill_no` + `bill_kind` and support kind filtering/search.
8. Keep unbilled cash flow auto-only (no manual bill requirement there).

## Migration/Import Safety
1. Convert legacy refs (`#1000`, `1000`, `1000.0`, `SB NO.1000`) to canonical format during reconciliation/import.
2. Assign namespace by source module during conversion.
3. Emit audit report of changed fields (`from -> to`).
4. After import, re-sync namespace counters before next bill generation.

## Quality Gates
1. No route or modal regression.
2. Same text must never open wrong party/type.
3. SB and MB must remain visually and logically separated everywhere.
4. Pending SB/MB filters and totals must remain accurate.
5. Run compile + route/module smoke tests after patch.
