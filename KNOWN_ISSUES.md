# Known Issues Memory

## 2026-03-06

- id: DS-DISCOUNT-SYNC-001
  status: mitigated
  evidence: `main.py` direct sale discount existed on `direct_sale.discount` but no dedicated `waive_off` sync path; client ledger separate waive-off rows were therefore inconsistent for direct sales.
  recommended_fix: Keep direct-sale discount synchronized into `waive_off` using a stable source marker and same bill reference.
  verification_command: `rg -n "_sync_direct_sale_waive_off|_direct_sale_waive_marker|WaiveOff.note == _direct_sale_waive_marker" main.py`

- id: DS-RENT-PERSIST-002
  status: mitigated
  evidence: `main.py` delivery rent row was deleted/not created unless `rent_in_bill` checkbox was set, causing intermittent "rent not saved" behavior from add/edit sale forms.
  recommended_fix: Persist `DeliveryRent` whenever `delivery_rent > 0`; keep row synchronized by `sale_id`.
  verification_command: `rg -n "_sync_delivery_rent_for_sale|include = float\\(rent_amount" main.py`

- id: DS-DISCOUNT-VALIDATION-003
  status: mitigated
  evidence: add/edit direct sale accepted invalid discount states (negative or larger than total), which could hide/neutralize ledger impact and feel like discount not saved.
  recommended_fix: enforce discount bounds in both add and edit routes.
  verification_command: `rg -n "Discount cannot be negative|Discount cannot exceed total amount" main.py`

- id: DS-RECOVERY-COVERAGE-004
  status: mitigated
  evidence: reconciliation checked direct-sale entry/pending/rent void mismatches but not direct-sale waive-off mismatch.
  recommended_fix: include direct-sale waive-off mismatch scan/fix in reconciliation report.
  verification_command: `rg -n "direct_sale_waive_mismatch_count|DS waive mismatches" main.py`
