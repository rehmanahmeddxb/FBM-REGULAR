---
name: flask-project-audit
description: Audit Flask repositories end-to-end for architecture consistency, security, performance, routing/import integrity, configuration hygiene, and recurring coding mistakes. Use when asked to review or harden a Flask app, validate routes/models/configs, generate actionable findings, or maintain a persistent known-issues memory for this project.
---

# Flask Project Audit

## Architecture Summary (This Repository)
- App entry: `main.py` (monolithic route/controller layer), `app.py` (factory shim), `run.py` (dev watchdog script).
- Data layer: SQLAlchemy models in `models.py` with tenant-scoped mixin + global loader criteria.
- Tenancy/auth: `tenancy.py` (`root`, tenant `admin`, `user`) + Flask-Login.
- Blueprints: `inventory`, `import_export`, `data_lab`, `admin`.
- UI: Jinja templates in `templates/` (Bootstrap-based).
- Config/deps: `requirements.txt`, `pyproject.toml`, `.replit`, `uv.lock`.
- Current scale snapshot: ~17 Python files, ~50 templates, ~10 markdown files, ~169 route decorators.

## Audit Workflow (Use Every Time)
1. Inventory structure.
- Run: `rg --files`
- Confirm entrypoints, blueprints, models, config files.

2. Validate imports/routes wiring.
- Run: `rg -n "@app\.route|Blueprint\(|register_blueprint|login_required" main.py blueprints`
- Check for dead routes, duplicate endpoints, missing templates, broken redirects.

3. Verify data model integrity.
- Inspect `models.py` for:
  - plaintext secrets
  - missing constraints/indexes
  - tenant-unsafe uniqueness
  - datetime consistency

4. Verify tenancy isolation.
- Inspect `models.py` + `tenancy.py` + bulk update/delete code in `main.py` and blueprints.
- Ensure write operations are tenant-filtered.

5. Security audit.
- Check for:
  - CSRF gaps
  - dangerous GET mutators
  - weak defaults
  - unsafe subprocess/shell use
  - insecure file handling

6. Performance audit.
- Identify N+1 query patterns, repeated queries in loops, no pagination on large lists, costly imports/exports.

7. Config/dependency audit.
- Compare `requirements.txt` vs `pyproject.toml` for drift.
- Flag version conflicts and duplicate deps.

8. Reporting output.
- Return findings ordered by severity: Critical > High > Medium > Low.
- Include exact file/line references and concrete fix direction.

9. Persist project memory.
- Update `KNOWN_ISSUES.md` with:
  - issue id
  - status (`open`, `mitigated`, `verified`)
  - evidence
  - recommended fix
  - verification command

## Recurring Problem Patterns to Detect
- Mutating routes using GET.
- Missing CSRF protection on forms.
- Plaintext password storage/display.
- Hardcoded default admin/root credentials.
- Tenant isolation bypass in bulk writes.
- Inconsistent role/permission checks between endpoint map and inline guards.
- Datetime timezone inconsistencies.
- Dependency version drift across config files.
- Legacy docs/scripts that no longer match current architecture.

## Refactoring Suggestions Template
When issues are found, suggest these refactor tracks:
- Security hardening track: CSRF, password model cleanup, auth/role normalization.
- Tenancy integrity track: explicit tenant filters for all writes + tests.
- Route hygiene track: convert GET mutators to POST/DELETE; unify permission gating.
- Architecture track: split `main.py` into service + route modules.
- Reliability track: add tests for route permissions, tenant boundaries, and financial calculations.

## Debugging Checklist
- `python -m compileall -q .`
- `python test_modules.py`
- Validate route map and endpoint permissions.
- Smoke-check critical flows:
  - login/logout
  - booking/payment/sale create+edit
  - GRN + supplier payment
  - import/export per tenant
  - profit/ledger pages
- Verify no cross-tenant data leak by creating same client/material codes in two tenants.

## Output Contract
Always provide:
1. Executive summary (3-6 lines)
2. Findings list by severity with file references
3. Open questions/assumptions
4. Suggested patch sequence (small, safe steps)
5. What was verified vs not verified

## Known Issues File Policy
- Treat `KNOWN_ISSUES.md` as authoritative project memory.
- Do not delete old issues; mark status changes.
- Add date and evidence links/paths for every update.
