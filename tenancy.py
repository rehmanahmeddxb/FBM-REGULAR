import os
from datetime import datetime

from flask import g, abort
from werkzeug.security import generate_password_hash
from sqlalchemy import text

from models import (
    db,
    Tenant,
    TenantFeature,
    AuditLog,
    User,
    Role,
    Permission,
    RolePermission,
    UserRole,
    Settings,
    Supplier,
)

ROOT_USERNAME = os.environ.get('ROOT_USERNAME', 'root')
ROOT_PASSWORD = os.environ.get('ROOT_PASSWORD', 'ChangeMeNow!2026')
DEFAULT_TENANT_NAME = os.environ.get('DEFAULT_TENANT_NAME', 'Default Branch')
DEFAULT_ADMIN_USERNAME = os.environ.get('DEFAULT_ADMIN_USERNAME', 'admin')
DEFAULT_ADMIN_PASSWORD = os.environ.get('DEFAULT_ADMIN_PASSWORD', 'admin123')
DEFAULT_TENANT_ADMIN_USERNAME = os.environ.get('DEFAULT_TENANT_ADMIN_USERNAME', 'admin')
DEFAULT_TENANT_ADMIN_PASSWORD = os.environ.get('DEFAULT_TENANT_ADMIN_PASSWORD', 'Admin@12345')
TEST_TENANT_NAME = os.environ.get('TEST_TENANT_NAME', 'TEST-TENANT')


def init_tenancy(app):
    @app.before_request
    def _set_tenant_context():
        try:
            from flask_login import current_user
        except Exception:
            current_user = None

        if current_user and getattr(current_user, 'is_authenticated', False):
            g.user = current_user
            g.is_root = (current_user.role == 'root')
            g.tenant_id = current_user.tenant_id
            g.enforce_tenant = (not g.is_root) and (g.tenant_id is not None)

            if not g.is_root:
                tenant = db.session.get(Tenant, g.tenant_id) if g.tenant_id else None
                if not tenant or tenant.status != 'active':
                    abort(403, 'Tenant suspended')
        else:
            g.user = None
            g.is_root = False
            g.tenant_id = None
            g.enforce_tenant = False


def require_root():
    if not getattr(g, 'is_root', False):
        abort(403, 'Root access required')


def audit_log(user, tenant_id, action, details=None):
    try:
        db.session.add(AuditLog(
            user_id=(user.id if user else None),
            tenant_id=tenant_id,
            action=action,
            details=details
        ))
        db.session.commit()
    except Exception:
        db.session.rollback()


def _ensure_default_tenant():
    tenant = Tenant.query.filter_by(name=DEFAULT_TENANT_NAME).first()
    if not tenant:
        tenant = Tenant(name=DEFAULT_TENANT_NAME, status='active')
        db.session.add(tenant)
        db.session.commit()
    return tenant


def _sqlite_rebuild_table_for_tenant_unique(table_name, unique_col, new_index_name):
    engine = db.engine
    if not engine or engine.dialect.name != 'sqlite':
        return

    exists = db.session.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
        {'name': table_name}
    ).fetchone()
    if not exists:
        return

    indexes = db.session.execute(text(f"PRAGMA index_list('{table_name}')")).fetchall()
    old_unique = False
    for idx in indexes:
        if len(idx) >= 3 and idx[2]:
            idx_name = idx[1]
            cols = db.session.execute(text(f"PRAGMA index_info('{idx_name}')")).fetchall()
            col_names = [c[2] for c in cols]
            if col_names == [unique_col]:
                old_unique = True
                break

    if not old_unique:
        return

    cols = db.session.execute(text(f"PRAGMA table_info('{table_name}')")).fetchall()
    col_defs = []
    col_names = []
    for c in cols:
        name = c[1]
        col_type = c[2] or 'TEXT'
        notnull = bool(c[3])
        default = c[4]
        is_pk = bool(c[5])
        col_names.append(name)
        parts = [name, col_type]
        if notnull:
            parts.append("NOT NULL")
        if default is not None:
            parts.append(f"DEFAULT {default}")
        if is_pk:
            parts.append("PRIMARY KEY")
        col_defs.append(" ".join(parts))

    db.session.execute(text(f"ALTER TABLE {table_name} RENAME TO {table_name}_old"))
    db.session.execute(text(f"CREATE TABLE {table_name} ({', '.join(col_defs)})"))
    db.session.execute(text(
        f"INSERT INTO {table_name} ({', '.join(col_names)}) SELECT {', '.join(col_names)} FROM {table_name}_old"
    ))
    db.session.execute(text(f"DROP TABLE {table_name}_old"))
    db.session.execute(text(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {new_index_name} ON {table_name} (tenant_id, {unique_col})"
    ))
    db.session.commit()


def ensure_user_table_tenant_unique():
    _sqlite_rebuild_table_for_tenant_unique('user', 'username', 'uq_user_tenant_username')


def ensure_material_table_tenant_unique():
    _sqlite_rebuild_table_for_tenant_unique('material', 'code', 'uq_material_tenant_code')


def ensure_client_table_tenant_unique():
    _sqlite_rebuild_table_for_tenant_unique('client', 'code', 'uq_client_tenant_code')


def ensure_supplier_table_tenant_unique():
    _sqlite_rebuild_table_for_tenant_unique('supplier', 'name', 'uq_supplier_tenant_name')


def _backfill_tenant_ids(default_tenant_id):
    for table in db.metadata.sorted_tables:
        if 'tenant_id' in table.c and table.name != 'tenant':
            if table.name == 'user':
                db.session.execute(
                    table.update()
                    .where(table.c.tenant_id.is_(None))
                    .where(table.c.username != ROOT_USERNAME)
                    .values(tenant_id=default_tenant_id)
                )
            else:
                db.session.execute(
                    table.update().where(table.c.tenant_id.is_(None)).values(tenant_id=default_tenant_id)
                )
    db.session.commit()


def _seed_root_account():
    root_user = User.query.filter_by(username=ROOT_USERNAME).first()
    if not root_user:
        root_user = User(
            username=ROOT_USERNAME,
            password_hash=generate_password_hash(ROOT_PASSWORD),
            password_plain=ROOT_PASSWORD,
            role='root',
            status='active',
            tenant_id=None
        )
        db.session.add(root_user)
        db.session.commit()
        return root_user

    root_user.role = 'root'
    root_user.status = 'active'
    root_user.tenant_id = None
    if not root_user.password_hash:
        root_user.password_hash = generate_password_hash(ROOT_PASSWORD)
    if not root_user.password_plain:
        root_user.password_plain = ROOT_PASSWORD
    db.session.commit()
    return root_user


def _force_root_tenant_none():
    db.session.execute(
        User.__table__.update()
        .where(User.username == ROOT_USERNAME)
        .values(tenant_id=None, role='root', status='active')
    )
    db.session.commit()


def _seed_default_admin(default_tenant):
    admin_user = User.query.filter_by(username=DEFAULT_ADMIN_USERNAME).first()
    if not admin_user:
        admin_user = User(
            username=DEFAULT_ADMIN_USERNAME,
            password_hash=generate_password_hash(DEFAULT_ADMIN_PASSWORD),
            password_plain=DEFAULT_ADMIN_PASSWORD,
            role='admin',
            status='active',
            tenant_id=default_tenant.id
        )
        db.session.add(admin_user)
    else:
        if not admin_user.tenant_id:
            admin_user.tenant_id = default_tenant.id
        if not admin_user.password_hash:
            admin_user.password_hash = generate_password_hash(DEFAULT_ADMIN_PASSWORD)
        if not admin_user.password_plain:
            admin_user.password_plain = DEFAULT_ADMIN_PASSWORD
        if admin_user.role not in ('admin', 'root'):
            admin_user.role = 'admin'
    db.session.commit()
    return admin_user


def _seed_rbac():
    roles = ['tenant_admin', 'manager', 'cashier']
    perms = [
        ('manage_users', 'Create and manage tenant users'),
        ('view_reports', 'View reports'),
        ('manage_inventory', 'Manage inventory'),
        ('manage_sales', 'Manage sales'),
    ]

    for name, desc in perms:
        if not Permission.query.filter_by(name=name).first():
            db.session.add(Permission(name=name, description=desc))

    for role_name in roles:
        if not Role.query.filter_by(name=role_name).first():
            db.session.add(Role(name=role_name, scope='tenant'))

    db.session.commit()


def _ensure_default_settings(default_tenant):
    settings = Settings.query.first()
    if not settings:
        settings = Settings(currency='PKR', company_name='AMS SYSTEM FOR EASE', tenant_id=default_tenant.id)
        db.session.add(settings)
        db.session.commit()


def bootstrap_tenancy():
    ensure_user_table_tenant_unique()
    ensure_material_table_tenant_unique()
    ensure_client_table_tenant_unique()
    ensure_supplier_table_tenant_unique()
    default_tenant = _ensure_default_tenant()
    _backfill_tenant_ids(default_tenant.id)
    _seed_root_account()
    _force_root_tenant_none()
    _seed_default_admin(default_tenant)
    _seed_rbac()
    _ensure_default_settings(default_tenant)


def can_hard_delete_tenant(tenant):
    return bool(tenant and tenant.name == TEST_TENANT_NAME)


def hard_delete_tenant(tenant_id):
    for table in db.metadata.sorted_tables:
        if table.name == 'tenant':
            continue
        if 'tenant_id' in table.c:
            db.session.execute(
                table.delete().where(table.c.tenant_id == tenant_id)
            )
    db.session.execute(
        Tenant.__table__.delete().where(Tenant.id == tenant_id)
    )
    db.session.commit()
