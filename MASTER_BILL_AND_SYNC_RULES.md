# Master Bill and Data Sync Rules

## Purpose
Prevent bill mixing, wrong document opening, and balance mismatch across all modules.

## Canonical Bill Rules
1. Auto bill must always be namespaced and normalized as `SB-<NS>-<N>`.
2. Manual bill must always be normalized as `MB NO.<VALUE>`.
3. No save is allowed with empty/unnormalized bill identity when bill is required.
4. Bill kind must always be derived:
   - `SB` for `SB-...` (and legacy `SB NO...`)
   - `MB` for `MB NO...`
5. Unbilled cash flow remains auto-only by business rule.

## Namespace Map (Auto Bills)
- Booking: `SB-BK-<N>`
- Client Payment: `SB-CP-<N>`
- Supplier Payment: `SB-SP-<N>`
- GRN: `SB-GRN-<N>`
- Direct Sale: `SB-SL-<N>`
- Entry/Dispatch internal auto refs: `SB-EN-<N>`

## Counter and Uniqueness Rules
1. Maintain independent counters per namespace.
2. Before issuing next auto bill:
   - scan existing rows of same namespace,
   - align counter to `max(existing)+1`.
3. Auto bill uniqueness is enforced on full normalized reference (namespace-safe).
4. Manual bill uniqueness is enforced before commit using conflict checks.
5. After imports/backfills, always re-sync counters before next auto generation.

## Resolver Safety Rules
1. `view_bill`/`download_invoice` must trust explicit `src` + `src_id` first.
2. If no source hint and multiple candidates remain, show collision resolution UI.
3. Never auto-open a different party/type just because text partially matches.

## Pending and Ledger Consistency
1. Pending rows must carry normalized `bill_no` and `bill_kind`.
2. Pending filters/search must support both raw and normalized values.
3. Opening balance, bookings, payments, sales, GRN, and pending totals must be recalculated from the same canonical refs.
4. Any edit/void/restore must update all dependent views/rows (entries, pending, ledgers) in same transaction.

## Import and Migration Rules
1. Legacy refs (`#1000`, `1000`, `1000.0`, old `SB NO.x`) must be normalized during reconciliation/import.
2. Namespace assignment for legacy auto refs must follow source module.
3. Keep migration audit trail (`from -> to`) for all changed bill fields.
4. Never drop old rows silently; normalize and preserve linkage.

## Change Control Rule (Mandatory)
When changing bill logic in one module, update all related modules in the same patch:
- create/edit routes
- conflict detection
- view/download resolver
- pending sync
- ledger aggregation
- import/backfill
- tests

No partial rollout is allowed.
