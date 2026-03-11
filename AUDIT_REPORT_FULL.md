# Full Application Audit Report

Date: 2026-02-26
Repository: `e:\WORKINGS\current working\pware`
Auditor: Codex (automated + static deep audit)

## Scope Covered
- Backend routes, business logic, permissions, and data mutation paths
- Database model definitions and tenant isolation behavior
- UI templates for visibility, navigation, loading/progress, and destructive actions
- Reporting/export/printing code paths
- Operational quality checks (syntax/tests/tooling)

## Audit Method
- Static analysis via `rg` across `main.py`, `models.py`, `templates/`, `blueprints/`
- Runtime introspection of Flask route map for mutating endpoints and permission-map coverage
- Sanity checks:
  - `python -m compileall -q .` -> PASS
  - `python test_modules.py` -> FAIL (console encoding)
  - `python -m pytest -q` -> FAIL (`pytest` not installed)

## Executive Summary
The app is functionally advanced and near-complete for core workflows (sales, billing, ledgers, imports, printing). However, several **high/critical** risks remain in security and data integrity, especially around:
1. plaintext credential handling,
2. tenant-safety in bulk updates/deletes,
3. GET-based state-changing routes,
4. missing CSRF protections.

These should be remediated before production hardening.

---

## Findings (Prioritized)

### 1) CRITICAL: Plaintext passwords are stored and exposed in UI
- Evidence:
  - `models.py:110` -> `password_plain = db.Column(db.String(200))`
  - `main.py:7408, 7436, 7444, 7558, 7585, 7629` writes plaintext into DB
  - `templates/settings.html:161-162` displays plaintext password
- Risk:
  - Full account compromise on DB leak or shoulder-surfing/admin misuse.
- Recommendation:
  - Remove `password_plain` field usage completely.
  - Never store recoverable passwords.
  - Use reset tokens/forced reset flow instead.

### 2) CRITICAL: Tenant isolation does not protect UPDATE/DELETE bulk mutations
- Evidence:
  - `models.py:531-541` tenant criteria is applied only when `execute_state.is_select`.
  - Bulk updates/deletes in app logic often filter by business fields only (example: `main.py:5766+` transfer/reclaim updates by client code/name).
- Risk:
  - In multi-tenant deployments, cross-tenant data corruption is possible where codes/names collide.
- Recommendation:
  - Enforce tenant criteria for mutating queries as well.
  - Add explicit `tenant_id == current_user.tenant_id` filter for every bulk update/delete.
  - Add DB constraints + integration tests for cross-tenant isolation.

### 3) HIGH: State-changing operations exposed via GET routes
- Evidence:
  - `main.py:3532` `/delete_bill/<...>` (GET)
  - `main.py:4930` `/delete_entry/<id>` (GET)
  - `main.py:5750` `/delete_client/<id>` (GET)
  - `main.py:5887` `/delete_supplier/<id>` (GET)
  - `main.py:6336` `/delete_material/<id>` (GET)
  - `main.py:6962` `/delete_pending_bill/<id>` (GET)
  - `main.py:7601` `/delete_user/<id>` (GET)
- Risk:
  - CSRF/prefetch/crawler-triggered unintended destructive actions.
- Recommendation:
  - Convert all mutating endpoints to POST/DELETE.
  - Replace anchor links with forms + CSRF token.

### 4) HIGH: No CSRF protection on forms
- Evidence:
  - Extensive POST forms in templates, no CSRF token framework usage detected (`Flask-WTF`/CSRF middleware absent).
- Risk:
  - Cross-site request forgery on admin/session-bearing users.
- Recommendation:
  - Introduce CSRF protection globally.
  - Add token verification for all mutating endpoints.

### 5) HIGH: Tenant-scoped models use global unique constraints (cross-tenant conflicts)
- Evidence:
  - `models.py:362` `DeliveryPerson.name` is `unique=True`
  - `models.py:477` `StaffEmail.email` is `unique=True`
- Risk:
  - Same value in another tenant can fail unexpectedly.
- Recommendation:
  - Replace with composite unique (`tenant_id`, field).

### 6) MEDIUM: Hard wipe success is flashed as error style
- Evidence:
  - `main.py:7977, 8083` -> `flash('Data Wiped ...', 'danger')`
- Impact:
  - Operational confusion; success appears as failure.
- Recommendation:
  - Use `success` or `warning` for successful wipes, reserve `danger` for failures.

### 7) MEDIUM: HTML structure defect in tracking page (duplicate container)
- Evidence:
  - `templates/tracking.html:57` and `templates/tracking.html:59` duplicate opening card `<div>`.
- Impact:
  - Layout instability and unpredictable DOM behavior on some screens.
- Recommendation:
  - Remove duplicate wrapper and revalidate template structure.

### 8) MEDIUM: PDF download can silently degrade to HTML fallback
- Evidence:
  - `main.py:3512` sets HTML fallback response if WeasyPrint unavailable.
- Impact:
  - Users expecting PDF may receive HTML attachment unexpectedly.
- Recommendation:
  - Show explicit warning when PDF engine unavailable.
  - Optionally block PDF action with user-facing error instead of silent fallback.

### 9) MEDIUM: Access-control architecture drift risk
- Evidence:
  - Central permission map at `main.py:199` plus many inline checks.
  - Dynamic scan found mutating endpoints not in central map (e.g. `update_settings`, `delete_selected_data`, tenant admin endpoints).
- Impact:
  - Easy to introduce inconsistent authorization behavior over time.
- Recommendation:
  - Standardize on one enforcement strategy (central map + explicit exceptions).
  - Add automated test that fails when mutating endpoints lack policy declaration.

### 10) LOW: Inconsistent datetime typing increases complexity and parse bugs
- Evidence:
  - `models.py:266` (`PendingBill.created_at` string)
  - `models.py:326` (`Invoice.created_at` string)
- Impact:
  - Extra parsing code, sorting/reporting edge-case bugs.
- Recommendation:
  - Migrate to `DateTime` columns with backfill migration.

### 11) LOW: Operational tooling gap for regression safety
- Evidence:
  - `python -m pytest -q` failed because `pytest` missing.
  - `test_modules.py` fails on Windows cp1252 due box-drawing chars (`test_modules.py:132`).
- Impact:
  - Low confidence in safe refactors and release readiness.
- Recommendation:
  - Add/enable test dependencies.
  - Make scripts encoding-safe for Windows terminals.

---

## Reporting / Printing / Export Audit Notes
- Core invoice viewing and download flow is robust and recently improved for bill normalization.
- Progress-modal UX now broadly covers download/export actions, but should be regression-tested after any new custom JS action path.
- `import_export_new.html` is primary and aligned with backend routes; legacy `import_export.html` contains references to endpoints that do not appear implemented (`/import_data_ajax`, `/import_status`, `/import_export/import_excel_all`, `/export_data_filter`) and should be deprecated or removed to avoid confusion.

---

## Recommended Remediation Plan
1. Security first (Critical/High):
   - Remove plaintext password storage/display.
   - Enforce CSRF.
   - Convert all mutating GET routes to POST.
2. Multi-tenant integrity:
   - Apply tenant filters to all bulk update/delete operations.
   - Fix tenant-unsafe unique constraints.
3. UX/operational cleanup:
   - Fix tracking template duplicate wrapper.
   - Correct wipe success flash category.
   - Make PDF fallback explicit.
4. Quality gates:
   - Add route-permission audit test and baseline pytest suite.

---

## Residual Risk After This Audit
This report is a deep static + route/workflow audit, not a full dynamic pen-test. Residual risk remains in runtime-specific behavior (browser/device combinations, production proxy settings, and data-dependent edge cases) until integrated tests and staging validation are completed.
