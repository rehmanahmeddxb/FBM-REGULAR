# PWARE Xerox Specification (Backend + Frontend + Database)

This file is a reconstruction-grade spec to rebuild the same app behavior in any other environment.

Scope includes:
- backend architecture
- route/module structure
- database schema with field types/sizes/defaults
- frontend templates/forms with field options and constraints
- environment, bootstrap, deployment, and parity tests

## 1. Product Identity

- App Name: `AMS SYSTEM FOR EASE` (PWARE project)
- Framework: Flask monolith with blueprints
- Primary file: `main.py`
- Compatibility app factory: `app.py:create_app()`
- Database engine: SQLite via SQLAlchemy
- Tenant model: shared DB with `tenant_id` scoping
- Time zone standard: `Asia/Karachi`

## 2. Runtime and Dependencies

## 2.1 Python
- Required: `>=3.11` (from `pyproject.toml`)

## 2.2 Dependencies (observed)
- `Flask`
- `Flask-SQLAlchemy`
- `Flask-Login`
- `SQLAlchemy`
- `pandas`
- `openpyxl`
- `reportlab`
- optional: `flask-weasyprint`
- optional runtime: `gunicorn`
- optional local watcher: `watchdog`

## 2.3 Environment Variables

Required in production:

```env
SECRET_KEY=<random_64+_chars>
MAX_UPLOAD_MB=256
ALLOW_OPEN_CORS=0

APP_INSTANCE_DIR=/opt/pware/instance
APP_DB_PATH=/opt/pware/instance/ahmed_cement.db

ROOT_USERNAME=root
ROOT_PASSWORD=<strong-password>
DEFAULT_TENANT_NAME=Default Branch
DEFAULT_ADMIN_USERNAME=admin
DEFAULT_ADMIN_PASSWORD=<strong-password>
DEFAULT_TENANT_ADMIN_USERNAME=admin
DEFAULT_TENANT_ADMIN_PASSWORD=<strong-password>
DEFAULT_TENANT_USER_PASSWORD=<strong-password>
TEST_TENANT_NAME=TEST-TENANT

SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASS=
SMTP_USE_TLS=1
SMTP_FROM=
NOTIFY_DAILY_TIME=08:00
```

## 3. Repository Structure

```text
pware/
  main.py                # main monolith app + route handlers + bootstrapping
  app.py                 # create_app() compatibility loader
  models.py              # SQLAlchemy models + tenant event hooks
  tenancy.py             # tenant bootstrap + root/admin seeding + tenant guards
  blueprints/
    inventory.py         # stock_summary, daily_transactions, inventory_log
    import_export.py     # import/export + app upgrade + db restore
    data_lab.py          # reconciliation basket + corrections
    admin.py             # module diagnostics/admin helpers
  templates/             # Jinja2 frontend pages and forms
  static/                # css/js/theme assets
  utils/module_loader.py # blueprint auto-loader
  requirements.txt
  pyproject.toml
```

## 4. Backend Architecture

## 4.1 App bootstrap flow

1. Initialize Flask app in `main.py`
2. Configure DB URI from `APP_DB_PATH` fallback
3. `db.init_app(app)`
4. `init_tenancy(app)` registers per-request tenant context
5. `_bootstrap_database()` runs:
   - `db.create_all()`
   - `_ensure_user_password_column()`
   - `_ensure_model_columns()`
   - `_ensure_material_categories()`
   - `_ensure_discount_columns()`
   - `_ensure_waive_off_table()`
   - `_backfill_legacy_payment_discounts_to_waive_off()`
   - `_ensure_user_permission_defaults()`
   - `bootstrap_tenancy()`
6. Permission guard `@app.before_request` enforces endpoint access by permission map.
7. Notification worker starts via request hook.

## 4.2 Security/auth characteristics

- Auth: Flask-Login (`User` model)
- Root/admin bypass for most permission checks
- Tenant context in Flask `g`:
  - `g.user`
  - `g.is_root`
  - `g.tenant_id`
  - `g.enforce_tenant`
- DB-level tenant criteria automatically applied on SELECT via SQLAlchemy events.
- Tenant mutation protection in `before_flush`.
- Note: CSRF check currently disabled in `main.py` (`_protect_against_csrf` returns `None`).

## 4.3 Permission model (high-level)

Permission fields include:
- `can_view_dashboard`
- `can_manage_grn`
- `can_view_stock`
- `can_view_daily`
- `can_view_history`
- `can_manage_bookings`
- `can_manage_payments`
- `can_manage_sales`
- `can_view_delivery_rent`
- `can_view_client_ledger`
- `can_view_supplier_ledger`
- `can_view_decision_ledger`
- `can_manage_pending_bills`
- `can_view_reports`
- `can_manage_notifications`
- `can_import_export`
- `can_manage_clients`
- `can_manage_suppliers`
- `can_manage_materials`
- `can_manage_delivery_persons`
- `can_access_settings`

Route-to-permission mapping exists in `ENDPOINT_PERMISSION_MAP` in `main.py`.

## 4.4 Business constants/options

- Open khata canonical values:
  - `OPEN_KHATA_CODE = OPEN-KHATA`
  - `OPEN_KHATA_NAME = OPEN KHATA`
- Sale category choices:
  - `Booking Delivery`
  - `Mixed Transaction`
  - `Credit Customer`
  - `Open Khata`
  - `Cash`

## 5. Backend Route Structure (Grouped)

This is the implementation grouping to preserve.

## 5.1 Core transactions
- Bookings: `/bookings`, `/add_booking`, `/edit_bill/Booking/<id>`
- Payments: `/payments`, `/add_payment`, `/edit_bill/Payment/<id>`
- Direct sales: `/direct_sales`, `/add_direct_sale`, `/edit_bill/DirectSale/<id>`
- Void/restore: `/void_transaction/<type>/<id>`, `/unvoid_transaction/<type>/<id>`

## 5.2 Ledger/bills
- Bill view/download: `/view_bill/<bill_no>`, `/download_invoice/<bill_no>`
- Client ledger: `/ledger`, `/ledger/<client_id>`, `/client_ledger/<id>`, `/download_client_ledger/<id>`
- Supplier ledger: `/supplier_ledger/<id>`, `/download_supplier_ledger/<id>`, `/download_supplier_payment/<id>`
- Decision ledger: `/decision_ledger`

## 5.3 Inventory/materials
- Dispatching: `/dispatching`, `/add_record`, `/edit_entry/<id>`, `/delete_entry/<id>`
- Tracking/history: `/tracking`
- GRN: `/grn`, `/edit_grn/<id>`, `/export_grn`
- Materials: `/materials`, `/add_material`, `/edit_material/<id>`, `/merge_materials`, category routes
- Inventory blueprint routes: `/inventory/stock_summary`, `/inventory/daily_transactions`, `/inventory/inventory_log`

## 5.4 Clients/suppliers
- Clients: `/clients`, `/add_client`, `/edit_client/<id>`, `/delete_client/<id>`, transfer/reclaim
- Client opening balance update: `/client_opening_balance/<id>`
- Suppliers: `/suppliers`, `/add_supplier`, `/edit_supplier/<id>`, `/delete_supplier/<id>`
- Supplier opening balance update: `/supplier_opening_balance/<id>`
- Supplier payments: add/edit/delete/restore routes

## 5.5 Reports and admin
- Pending bills: `/pending_bills` + add/edit/delete/toggle/import/export
- Financial/profit/unpaid reports
- Notifications module routes
- Settings/users/void-audit routes
- Root tenancy routes: `/tenants`, create/reset/status/delete
- Import/export blueprint routes: `/import_export/...`
- Data lab blueprint routes: `/data_lab/...`

## 6. Database Xerox (Full Schema Dictionary)

Type notation:
- `str(N)` means SQLAlchemy `String(N)`
- `float` = `Float`
- `int` = `Integer`
- `bool` = `Boolean`
- `date` = `Date`
- `datetime` = `DateTime`

All tenant-scoped models include `tenant_id: str(36), FK tenant.id, index, nullable`.

## 6.1 Tenant and RBAC tables

### `tenant`
- `id`: str(36), PK
- `name`: str(120), unique, required
- `status`: str(20), default `active`
- `subscription_plan`: str(50)
- `expiry_date`: date
- `db_uri`: str(500)
- `created_at`: datetime
- `updated_at`: datetime

### `role`
- `id`: int, PK
- `name`: str(50), unique, required
- `scope`: str(20), default `tenant`

### `permission`
- `id`: int, PK
- `name`: str(80), unique, required
- `description`: str(200)

### `role_permission`
- `id`: int, PK
- `role_id`: int, FK role.id, required
- `permission_id`: int, FK permission.id, required

### `user_role`
- `id`: int, PK
- `user_id`: int, FK user.id, required
- `role_id`: int, FK role.id, required
- `tenant_id`: str(36), FK tenant.id

### `tenant_feature`
- `id`: int, PK
- `tenant_id`: str(36), FK tenant.id, index, required
- `feature_name`: str(80), required
- `enabled`: bool, default `true`

### `audit_log`
- `id`: str(36), PK
- `tenant_id`: str(36)
- `user_id`: int
- `action`: str(200), required
- `details`: str(1000)
- `timestamp`: datetime, index

## 6.2 User/account tables

### `user` (unique: `tenant_id + username`)
- `id`: int, PK
- `tenant_id`: str(36)
- `username`: str(80), required
- `password_hash`: str(200)
- `password_plain`: str(200)
- `role`: str(20), default `user`
- `status`: str(20), default `active`
- Permission booleans: see section 4.3
- `restrict_backdated_edit`: bool, default `false`
- `created_at`: datetime

### `root_recovery_code`
- `id`: int, PK
- `username`: str(80), index, default `root`, required
- `code_hash`: str(255), required
- `created_at`: datetime, index
- `used_at`: datetime, nullable, index
- `generated_by`: str(80)
- `note`: str(300)

## 6.3 Master/directory tables

### `client` (unique: `tenant_id + code`)
- `id`: int, PK
- `code`: str(50), required
- `name`: str(100), required
- `phone`: str(20)
- `address`: str(200)
- `category`: str(50), default `General`
- `opening_balance`: float, default `0`
- `opening_balance_date`: datetime, index, default pk_now
- `is_active`: bool, default `true`
- `transferred_to_id`: int, FK client.id, nullable
- `require_manual_invoice`: bool, default `false`
- `created_at`: datetime, index

### `supplier` (unique: `tenant_id + name`)
- `id`: int, PK
- `name`: str(100), required
- `phone`: str(20)
- `address`: str(200)
- `opening_balance`: float, default `0`
- `opening_balance_date`: datetime, index, default pk_now
- `is_active`: bool, default `true`
- `created_at`: datetime, index

### `material_category`
- `id`: int, PK
- `name`: str(100), required
- `is_active`: bool, default `true`
- `created_at`: datetime, index

### `material` (unique: `tenant_id + code`)
- `id`: int, PK
- `code`: str(50), required
- `name`: str(100), required
- `category_id`: int, FK material_category.id, index
- `unit_price`: float, default `0`
- `total`: float, default `0`
- `unit`: str(20), default `Bags`
- `is_active`: bool, default `true`
- `created_at`: datetime, index

### `delivery_person`
- `id`: int, PK
- `name`: str(100), unique, required
- `is_active`: bool, default `true`
- `created_at`: datetime, index

## 6.4 Financial/sales/stock transaction tables

### `entry`
- `id`: int, PK
- `date`: str(20)
- `time`: str(20)
- `type`: str(10) (`IN`/`OUT`/`CANCEL`)
- `material`: str(100)
- `client`: str(100)
- `client_code`: str(50)
- `client_category`: str(50)
- `qty`: float, default `0`
- `bill_no`: str(50)
- `auto_bill_no`: str(50)
- `nimbus_no`: str(50)
- `invoice_id`: int, FK invoice.id
- `created_by`: str(80)
- `created_at`: datetime, index
- `is_void`: bool, default `false`
- `transaction_category`: str(50)
- `driver_name`: str(100)
- `note`: str(500)
- `booked_material`: str(100)
- `is_alternate`: bool, default `false`

### `pending_bill`
- `id`: int, PK
- `client_code`: str(50)
- `client_name`: str(100)
- `bill_no`: str(50)
- `nimbus_no`: str(50)
- `amount`: float, default `0`
- `reason`: str(200)
- `photo_url`: str(200)
- `photo_path`: str(200)
- `is_paid`: bool, default `false`
- `is_cash`: bool, default `false`
- `is_manual`: bool, default `false`
- `created_at`: str(50)
- `created_by`: str(80)
- `is_void`: bool, default `false`
- `note`: str(500)
- `risk_override`: str(20)

### `booking`
- `id`: int, PK
- `client_name`: str(100)
- `amount`: float, default `0`
- `paid_amount`: float, default `0`
- `manual_bill_no`: str(50)
- `auto_bill_no`: str(50)
- `photo_path`: str(200)
- `photo_url`: str(500)
- `date_posted`: datetime, index
- `is_void`: bool, default `false`
- `note`: str(500)
- `discount`: float, default `0`
- `discount_reason`: str(200)

### `booking_item`
- `id`: int, PK
- `booking_id`: int, FK booking.id, required
- `material_name`: str(100)
- `qty`: float, default `0`
- `price_at_time`: float, default `0`

### `payment`
- `id`: int, PK
- `client_name`: str(100)
- `amount`: float, default `0`
- `method`: str(50)
- `manual_bill_no`: str(50)
- `auto_bill_no`: str(50)
- `photo_path`: str(200)
- `photo_url`: str(500)
- `date_posted`: datetime, index
- `is_void`: bool, default `false`
- `note`: str(500)
- `discount`: float, default `0`
- `discount_reason`: str(200)
- `bank_name`: str(100)

### `waive_off`
- `id`: int, PK
- `payment_id`: int, FK payment.id, index
- `client_code`: str(50), index
- `client_name`: str(100), index
- `bill_no`: str(50), index
- `amount`: float, default `0`
- `reason`: str(300)
- `date_posted`: datetime, index
- `created_by`: str(80)
- `note`: str(500)
- `is_void`: bool, default `false`, index

### `invoice`
- `id`: int, PK
- `client_code`: str(50)
- `client_name`: str(100)
- `invoice_no`: str(50)
- `is_manual`: bool, default `false`
- `date`: date
- `total_amount`: float, default `0`
- `balance`: float, default `0`
- `status`: str(20), default `OPEN`
- `is_cash`: bool, default `false`
- `created_at`: str(50)
- `created_by`: str(80)
- `is_void`: bool, default `false`
- `note`: str(500)

### `bill_counter`
- `id`: int, PK
- `count`: int, default `1000`

### `direct_sale`
- `id`: int, PK
- `client_name`: str(100)
- `category`: str(50)
- `amount`: float, default `0`
- `paid_amount`: float, default `0`
- `discount`: float, default `0`
- `discount_reason`: str(200)
- `manual_bill_no`: str(50)
- `auto_bill_no`: str(50)
- `photo_path`: str(200)
- `photo_url`: str(500)
- `invoice_id`: int, FK invoice.id
- `date_posted`: datetime, index
- `is_void`: bool, default `false`
- `note`: str(500)
- `driver_name`: str(100)

### `direct_sale_item`
- `id`: int, PK
- `sale_id`: int, FK direct_sale.id, required
- `product_name`: str(100)
- `qty`: float, default `0`
- `price_at_time`: float, default `0`

### `delivery_rent`
- `id`: int, PK
- `sale_id`: int, FK direct_sale.id, index
- `delivery_person_name`: str(100), index, required
- `bill_no`: str(50), index
- `amount`: float, default `0`
- `note`: str(500)
- `date_posted`: datetime, index
- `created_by`: str(80)
- `is_void`: bool, default `false`, index

### `grn`
- `id`: int, PK
- `supplier_id`: int, FK supplier.id
- `supplier`: str(100)
- `manual_bill_no`: str(50)
- `auto_bill_no`: str(50)
- `photo_path`: str(200)
- `photo_url`: str(500)
- `loading_cost`: float, default `0`
- `freight_cost`: float, default `0`
- `other_expense`: float, default `0`
- `adjustment_amount`: float, default `0`
- `discount`: float, default `0`
- `paid_amount`: float, default `0`
- `payment_type`: str(50)
- `tax_percent`: float, default `0`
- `tax_amount`: float, default `0`
- `tax_type`: str(50)
- `bank_name`: str(100)
- `account_name`: str(100)
- `account_no`: str(50)
- `supplier_invoice_no`: str(50)
- `due_date`: date
- `bill_date`: date
- `date_posted`: datetime, index
- `is_void`: bool, default `false`
- `note`: str(500)

### `grn_item`
- `id`: int, PK
- `grn_id`: int, FK grn.id, required
- `mat_name`: str(100)
- `qty`: float, default `0`
- `price_at_time`: float, default `0`

### `supplier_payment`
- `id`: int, PK
- `supplier_id`: int, FK supplier.id, required
- `amount`: float, default `0`
- `method`: str(50)
- `date_posted`: datetime, index
- `note`: str(500)
- `is_void`: bool, default `false`
- `bank_name`: str(100)
- `account_name`: str(100)
- `account_no`: str(50)
- `manual_bill_no`: str(50)
- `auto_bill_no`: str(50)

### `delivery`
- `id`: int, PK
- `client_name`: str(100)
- `manual_bill_no`: str(50)
- `auto_bill_no`: str(50)
- `photo_path`: str(200)
- `date_posted`: datetime, index

### `delivery_item`
- `id`: int, PK
- `delivery_id`: int, FK delivery.id, required
- `product`: str(100)
- `qty`: float, default `0`

## 6.5 Settings/notification/support tables

### `settings`
- `id`: int, PK
- `currency`: str(10), default `PKR`
- `company_name`: str(100), default `FAZAL BUILDING MATERIALS`
- `company_address`: str(200), default `JALAL PUR SOBTIAN`
- `company_phone`: str(50), default `+92302-0000993 +92331-0000993`
- `company_email`: str(100)
- `tax_rate`: float, default `0`
- `invoice_prefix`: str(10), default `INV-`
- `bill_prefix`: str(10), default `#`
- `ui_theme`: str(20), default `dark_navy`
- `allow_global_negative_stock`: bool, default `false`, required
- SMTP/google fields with lengths:
  - `smtp_host`: str(200)
  - `smtp_port`: int default `587`
  - `smtp_user`: str(200)
  - `smtp_pass`: str(200)
  - `smtp_from`: str(200)
  - `smtp_use_tls`: bool default `true`
  - `notify_daily_time`: str(10), default `08:00`
  - `google_client_id`: str(500)
  - `google_client_secret`: str(500)
  - `google_refresh_token`: str(1000)
  - `google_access_token`: str(1000)
  - `google_token_expiry`: str(50)
  - `google_sender_email`: str(200)

### `staff_email`
- `id`: int, PK
- `email`: str(200), unique, required
- `is_active`: bool, default `true`
- `created_at`: datetime, index

### `follow_up_reminder`
- `id`: int, PK
- `pending_bill_id`: int, FK pending_bill.id, required
- `remind_at`: datetime, required
- `note`: str(500)
- `is_done`: bool, default `false`
- `alerted_at`: datetime
- `acknowledged_at`: datetime
- `created_at`: datetime, index

### `follow_up_contact`
- `id`: int, PK
- `pending_bill_id`: int, FK pending_bill.id, required
- `reminder_id`: int, FK follow_up_reminder.id
- `contacted_at`: datetime, required
- `channel`: str(30), default `Call`
- `response`: str(200)
- `note`: str(500)
- `created_by`: str(80)
- `created_at`: datetime, index

### `recon_basket`
- `id`: int, PK
- `bill_no`: str(50)
- `inv_date`: date
- `inv_client`: str(100)
- `fin_client`: str(100)
- `inv_material`: str(100)
- `inv_qty`: float, default `0`
- `status`: str(20)
- `match_score`: int, default `0`
- `created_at`: datetime, index

### `schema_version`
- `id`: int, PK
- `version`: int, default `1`
- `applied_at`: datetime

## 7. Frontend Structure Xerox

## 7.1 Base layout and navigation

Defined in `templates/layout.html`:
- Sidebar modules:
  - Dashboard
  - Inventory: GRN, Stock Summary, Daily Breakdown, History
  - Transactions: Bookings, Payments, Sales, Delivery Rent
  - Ledgers: Client, Supplier, Decision
  - Pending Bills
  - Reports: Unpaid/Paid, Profit
  - Notifications
  - Import & Export
  - Directory: Materials, Delivery Persons
  - System: Tenants (root), Void Audit, Settings

## 7.2 UI Forms and Field Catalog (Core Business Pages)

Notes:
- Most text inputs do not have HTML `maxlength`; server-side schema lengths govern storage.
- `step="0.01"` is used for money/qty numeric precision.

### A) Clients (`templates/clients.html`)
- Add/Edit client form fields:
  - `name` text required
  - `category` text + datalist options: `General`, `Open Khata`, `Walking-Customer`, `Misc`
  - `code` text required
  - `phone` text
  - `address` text
  - `opening_balance` number step `0.01`
  - `opening_balance_date` date
- Other forms:
  - search filters: `category`, `search`
  - activate all suspended
  - suspend, transfer, reclaim actions

### B) Client ledger opening balance (`templates/client_ledger.html`)
- `opening_balance` number step `0.01` required
- `opening_balance_date` date required

### C) Suppliers (`templates/suppliers.html`)
- Add/Edit supplier form fields:
  - `name` text required
  - `phone` text
  - `address` textarea
  - `opening_balance` number step `0.01`
  - `opening_balance_date` date
  - `is_active` checkbox (edit)

### D) Supplier ledger (`templates/supplier_ledger.html`)
- Opening balance modal:
  - `opening_balance` number step `0.01` required
  - `opening_balance_date` date required
- Add supplier payment:
  - `supplier_id` hidden
  - `date` date required
  - `amount` number step `0.01` required
  - `method` select options: `Cash`, `Bank Transfer`, `Check`
  - `bank_name`, `account_name`, `account_no` text
  - `note` textarea
- Edit supplier payment has same fields

### E) Bookings (`templates/bookings.html`)
- Add/Edit booking:
  - `client_code` text required (search-combobox)
  - line item arrays:
    - `material_name[]` select required
    - `qty[]` number step `0.01` required
    - `unit_rate[]` number step `0.01` required
  - `amount` number step `0.01` required readonly (computed)
  - `paid_amount` number step `0.01`
  - `discount` number step `0.01` min `0`
  - `discount_reason` text
  - `manual_bill_no` text
  - `photo` file accept `image/*`
  - `photo_url` url
  - `note` textarea

### F) Payments (`templates/payments.html`)
- Add/Edit client payment:
  - `client_code` text required
  - `amount` number step `0.01` required
  - `discount` number step `0.01` min `0`
  - `discount_reason` text
  - `method` select: `Cash`, `Bank`, `Check`
  - `manual_bill_no` text
  - `settle_leftover_with_discount` checkbox (add)
  - `photo` file accept `image/*`
  - `photo_url` url
  - `note` textarea
- Add/Edit supplier payment:
  - add includes `supplier_id` select required
  - shared fields: `date`, `amount`, `method`, bank/account fields, `note`
  - method options: `Cash`, `Bank Transfer`, `Check`

### G) Direct sales (`templates/direct_sales.html`)
- Add/Edit sale:
  - `category` select options:
    - `Booking Delivery`
    - `Mixed Transaction`
    - `Credit Customer`
    - `Open Khata`
    - `Cash`
  - `client_code` text required
  - `client_name` hidden
  - `manual_client_name` text (used for unregistered/open khata flow)
  - hidden flags:
    - `has_bill`
    - `create_invoice`
    - `track_as_cash`
  - `driver_name` text required (datalist `deliveryPersonsList`)
  - `sale_date` datetime-local
  - line item arrays:
    - `product_name[]` text required
    - `alternate_material[]` text optional (add form)
    - `qty[]` number step `0.01` required
    - `unit_rate[]` number step `0.01` required
  - rent fields:
    - `delivery_rent` number step `0.01`
    - `delivery_rent_note` text
    - `rent_in_bill` checkbox
  - bill/media fields:
    - `manual_bill_no` text
    - `photo` file accept `image/*`
    - `photo_url` url
    - `note` textarea
  - totals:
    - `amount` number step `0.01` required
    - `discount` number step `0.01`
    - `discount_reason` text
    - `paid_amount` number step `0.01`
  - `allow_negative_stock` checkbox

### H) GRN (`templates/grn.html`)
- Add GRN:
  - hidden `action=add`
  - `supplier` text required
  - `manual_bill_no` text
  - `photo` file accept `image/*`
  - line item arrays:
    - `mat_name[]` select required
    - `qty[]` number step `0.01` required
    - `price[]` number step `0.01`
- Delete/void GRN uses hidden:
  - `action=delete`
  - `id`

### I) Materials (`templates/materials.html`)
- Add material:
  - `material_name` text required
  - `category_id` select
  - `material_unit` text + datalist options: `Bags, KG, Foot, Litre, Meter, Sq. Ft, Ton, Pcs`
  - `material_code` text required
- Edit material:
  - `material_name`, `category_id`, `material_unit`, `material_code`
- Merge materials:
  - `source_material_id` select required
  - `target_material_id` select required
- Bulk update unit:
  - `category_id` select optional
  - `new_unit` text required

### J) Pending bills (`templates/pending_bills.html`)
- Filter form fields:
  - `client_code`
  - `bill_no`
  - `is_manual` options: all, 1/manual, 0/auto
  - `category` options:
    - `Booking Delivery`
    - `Mixed Transaction`
    - `Credit Customer`
    - `Open Khata`
    - `Cash`
    - `Cash Paid`
  - `bill_from`, `bill_to`
- Add/Edit pending bill:
  - `client_code` text required
  - `bill_no` text required
  - `nimbus_no` text
  - `amount` number step `0.01`
  - `reason` textarea
  - `photo_url` url
  - `photo` file accept `image/*`
  - `note` textarea (edit route supports note)
- Import pending bill:
  - `file` required
- Paid toggle:
  - role switch checkbox per row

### K) Notifications (`templates/notifications.html`, `notifications_detail.html`)
- Main filter fields:
  - `q` text
  - `category` select options: `all, billed, unbilled, cash_unbilled, open_khata, cash_paid`
  - `status` select options: `all, pending, paid`
  - `risk` select options: `all, very_high, high, medium, low`
- Staff email add:
  - `email` type email required
- Bill detail:
  - severity override options: `auto, medium, high, very_high, low`
  - set reminder:
    - `remind_at` datetime-local required
    - `note` text
  - log contact:
    - `channel` options: `Call, WhatsApp, SMS, Email, Visit, Other`
    - `response` text required
    - `contacted_at` datetime-local
    - `note` text
  - close reminder:
    - `channel` same options
    - `response` required
    - `note` optional

### L) Settings (`templates/settings.html`)
- General settings form:
  - company/currency/smtp fields
  - `smtp_use_tls` checkbox
  - `notify_daily_time` time
  - `allow_global_negative_stock` checkbox
- Material category management:
  - add/rename/toggle forms with `category_name`
- User management:
  - add user:
    - `username`, `password`, `role`
    - permission checkboxes (full matrix)
  - edit user:
    - `role`, optional `password`
    - permission checkboxes (full matrix)
- Data restore and reconciliation:
  - tenant DB restore file input `.db`
  - reconcile scan/apply-fixes forms
- Wipe data:
  - checkbox list `delete_targets`
  - confirm text `confirm_text`
  - acknowledgment checkbox
  - `hard_delete_override` checkbox

## 8. Database + Form Size/Constraint Rules

Use these implementation rules:

1. Persist length limits by model definition (String lengths above).
2. Keep frontend numeric precision at 2 decimals (`step=0.01`).
3. Keep opening balance date as `date` input and persist to `datetime`.
4. Keep same option sets for categories/methods/channels.
5. Do not remove hidden operational fields used by server logic.

## 9. Rebuild Procedure (Production Grade)

1. Clone code with same structure.
2. Install dependencies.
3. Set all env vars (replace all default passwords).
4. Boot app once to run bootstrap and schema patchers.
5. Verify root/admin/default tenant seeding.
6. Verify route auth by creating a non-admin user and testing restrictions.
7. Run form parity tests for sections in 7.2.
8. Run ledger parity tests:
   - opening balance row with date
   - running balance correctness after booking/payment/sale
9. Validate import/export and print/PDF behavior.

## 10. Acceptance Checklist (Xerox Match)

System is considered xerox-matched when:

1. All tables/columns in section 6 exist with matching types/lengths/defaults.
2. Route groups in section 5 are available and permission-gated.
3. Form fields/options in section 7.2 are present and posted under same names.
4. Client and supplier opening balance both support amount + date.
5. Tenant scoping works for non-root users.
6. Reports, notifications, and import/export flows execute without schema errors.

## SUGGESTIONS

`SUGGESTION 1`
Add a full route contract appendix with columns: `URL`, `Method`, `Permission`, `Input Fields`, `Success Redirect/Response`, `Error Response`.

`SUGGESTION 2`
Add an ER diagram image (or Mermaid block) for all core financial tables: `client`, `booking`, `payment`, `direct_sale`, `pending_bill`, `invoice`, `entry`.

`SUGGESTION 3`
Document all computed formulas explicitly, especially:
- ledger running balance
- pending bill generation rules
- discount and waive-off treatment
- stock delta per transaction type

`SUGGESTION 4`
Add a migration safety playbook with rollback checkpoints:
- pre-migration backup hash
- post-migration row-count diff
- reconciliation commands

`SUGGESTION 5`
Add API/JSON examples for current ajax endpoints:
- request sample
- response sample
- error payload sample

`SUGGESTION 6`
Define strict validation rules in one table (server-side truth):
- allowed enum values
- numeric min/max
- required conditions by transaction category

`SUGGESTION 7`
Add a production security hardening baseline:
- CSRF enabled status and rollout plan
- secure cookie flags
- session timeout policy
- password rotation and lockout policy

`SUGGESTION 8`
Add observability guidance:
- log format standard
- critical audit events list
- alert thresholds (failed logins, void spikes, import failures)

`SUGGESTION 9`
Add performance expectations and test dataset sizes:
- max rows for acceptable page latency
- index checklist for heavy filters
- report export timing targets

`SUGGESTION 10`
Add UAT scripts per module (step-by-step, expected result, pass/fail checkbox) so QA can execute consistently across environments.
