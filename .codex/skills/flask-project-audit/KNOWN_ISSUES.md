# KNOWN ISSUES - Flask Project Audit Memory

Last updated: 2026-02-28 (PKT)
Repository: `e:\WORKINGS\current working\pware`

## Status Legend
- `open`: confirmed issue not fixed
- `mitigated`: partial fix exists, follow-up needed
- `verified`: fixed and validated

## Issues

### FPA-001 - Plaintext password storage
- Status: `open`
- Severity: Critical
- Evidence:
  - `models.py` (`User.password_plain`)
  - `tenancy.py` seeds plain credentials (`ROOT_PASSWORD`, `DEFAULT_ADMIN_PASSWORD` into `password_plain`)
- Risk: credential disclosure on DB leak/admin misuse.
- Recommended fix:
  - remove `password_plain` column usage entirely
  - migrate existing values to null
  - keep only hashed passwords + reset flow
- Verify:
  - `rg -n "password_plain" models.py main.py tenancy.py`

### FPA-002 - Weak default credentials in code paths
- Status: `open`
- Severity: High
- Evidence:
  - `tenancy.py`: fallback defaults like `ChangeMeNow!2026`, `admin123`
- Risk: predictable credentials in misconfigured environments.
- Recommended fix:
  - fail startup unless secure env vars are set for production
  - remove weak defaults
- Verify:
  - `rg -n "ROOT_PASSWORD|DEFAULT_ADMIN_PASSWORD" tenancy.py`

### FPA-003 - CSRF protection missing
- Status: `open`
- Severity: High
- Evidence:
  - `main.py` has `_protect_against_csrf()` no-op
  - many POST forms without CSRF middleware/token enforcement
- Risk: cross-site request forgery on authenticated sessions.
- Recommended fix:
  - integrate Flask-WTF/CSRFProtect globally
  - enforce CSRF token on mutating routes
- Verify:
  - inspect app init for `CSRFProtect(app)` and token checks

### FPA-004 - Route/controller monolith complexity
- Status: `open`
- Severity: Medium
- Evidence:
  - `main.py` contains most app logic/routes (~9k+ lines)
- Risk: regression-prone changes, difficult testing/review.
- Recommended fix:
  - split by domain modules (sales, payments, ledgers, admin)
  - add service layer for shared financial logic
- Verify:
  - reduced route/function count per file and unit tests per module

### FPA-005 - Dependency drift between `requirements.txt` and `pyproject.toml`
- Status: `open`
- Severity: Medium
- Evidence:
  - differing version pins and duplicate deps (`flask-login` repeated, major version differences)
- Risk: inconsistent environments and deployment surprises.
- Recommended fix:
  - standardize on one dependency source of truth
  - align versions and lock strategy
- Verify:
  - compare `requirements.txt`, `pyproject.toml`, `uv.lock`

### FPA-006 - Dev runner uses shell patterns unsafe/non-portable
- Status: `open`
- Severity: Medium
- Evidence:
  - `run.py` uses `subprocess.run(..., shell=True)` and `os.system("kill -9 ...")`
- Risk: portability and command-injection risk if input ever becomes dynamic.
- Recommended fix:
  - avoid shell=True
  - use direct process APIs per OS
- Verify:
  - `rg -n "shell=True|os\.system\(" run.py`

### FPA-007 - Documentation drift across legacy reports
- Status: `mitigated`
- Severity: Low
- Evidence:
  - multiple historical reports (`AUDIT_REPORT_FULL.md`, `PLAN-B.md`, `FULLREPORT_*.md`) with outdated statements
- Risk: operators following stale procedures.
- Recommended fix:
  - keep one canonical `CURRENT_AUDIT.md` and archive others
- Verify:
  - doc index points to one active operational report

### FPA-008 - Missing templates referenced by active routes
- Status: `open`
- Severity: High
- Evidence (render_template references with no file present):
  - `admin_dashboard.html`
  - `admin_modules.html`
  - `system_report.html`
  - `template_404.html`
  - `template_dashboard.html`
  - `template_form.html`
  - `template_import.html`
  - `template_view.html`
- Risk: runtime 500 errors on route access.
- Recommended fix:
  - either add missing templates or remove/disable related routes/blueprints
  - add CI check that every `render_template()` target exists
- Verify:
  - run template-reference checker script and confirm `missing_templates = 0`

### FPA-009 - Runtime route surface includes scaffold blueprint in production
- Status: `open`
- Severity: Medium
- Evidence:
  - `test_modules.py` shows active `template` blueprint endpoints under `/module_name/*`
- Risk: accidental exposure of placeholder routes and maintenance confusion.
- Recommended fix:
  - disable `module_template.py` registration in production
  - keep it as development-only scaffold
- Verify:
  - route list no longer includes `template.*` endpoints in production build

## Next Audit Pass Priority
1. Security baseline (FPA-001/002/003)
2. Tenancy write-safety regression tests
3. Main controller decomposition
4. Dependency normalization
