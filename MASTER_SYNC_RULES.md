# Master Sync Rules (Financial Consistency)

## Production Prompt (Standard)
Ensure every financial value is computed from one canonical source and remains consistent across all related modules, APIs, modals, ledgers, exports, and printable views.  
Any change to balances, opening balances, totals, pending amounts, bill references, or transaction classification must be propagated to every dependent path in the app before merge.  
No partial fixes are allowed: if one screen changes and another linked screen does not, treat it as a bug.  
Preserve existing routes, modal behavior, and workflows; apply backward-compatible changes only.  
Add focused verification queries/tests for each touched flow and confirm there is no data drift.

## Non-Negotiable Rules
1. One formula per metric.
- `supplier balance` must always include: opening balance + GRN totals - supplier payments.
- `client balance` must always include: opening balance + (debits - credits - discounts/waive-offs).
- Do not duplicate formulas in separate places when a shared helper exists.

2. Opening balance must be included everywhere relevant.
- UI badges, ledgers, APIs, PDF/print exports, and detail pages must match.
- Date-cutoff views must include opening balance only when opening date is within cutoff.

3. Source-hinted bill resolution for collision safety.
- When opening/downloading bills from a typed list (e.g., GRN ledger), pass source hints (`src`, `src_id`) to avoid wrong document type resolution.

4. No hidden divergence.
- If a value is shown in one page and used in another action, both must read from the same backend logic.
- Avoid JS-only local math for authoritative balances; fetch canonical API data.

5. Safe change policy.
- No breaking route changes.
- Keep existing request/response keys stable; additive response keys are preferred.
- Changes must be minimal and reversible.

## Mandatory Audit Checklist Before Finalizing
1. Identify every read/write path for the changed metric.
2. Confirm formulas match across:
- create/edit flows
- list pages
- ledger pages
- APIs
- print/download views
- imports/exports (if applicable)
3. Check reference normalization (`#123`, `123`, `123.0`) and collision handling.
4. Validate with data snapshots:
- no orphan links
- no cross-entity bill misrouting
- totals recomputation matches stored/derived outputs
5. Run compile/smoke checks and record what was/was not verified.

## Current Canonical Notes
- Supplier payable in GRN and payments must use `/api/supplier_balance/<id>` that is aligned with supplier ledger logic.
- `_client_balance_as_of` is the canonical path for historical client balance snapshots and must include opening balance effects.
- Auto bill canonical format: `SB NO.<n>`.
- Manual bill canonical format: `MB NO.<value>`.
- `Unbilled` cash-sale flow must remain auto-only; do not force manual bill entry there.
