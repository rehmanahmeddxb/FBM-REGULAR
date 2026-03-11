from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from datetime import date, datetime
from types import SimpleNamespace
from sqlalchemy import func, case, or_, and_
from models import db, Material, MaterialCategory, Entry, Client, PendingBill, DirectSale, Booking, Payment, GRN, Invoice

# Module configuration
MODULE_CONFIG = {
    'name': 'Inventory Module',
    'description': 'Stock and inventory management',
    'url_prefix': '/inventory',
    'enabled': True
}

inventory_bp = Blueprint('inventory', __name__)


def _entry_best_bill_ref(entry_obj):
    if not entry_obj:
        return ''
    primary = (getattr(entry_obj, 'bill_no', None) or '').strip()
    auto = (getattr(entry_obj, 'auto_bill_no', None) or '').strip()
    if primary and not primary.upper().startswith('UNBILLED'):
        return primary
    if auto and not auto.upper().startswith('UNBILLED'):
        return auto
    inv_id = getattr(entry_obj, 'invoice_id', None)
    if inv_id:
        inv = db.session.get(Invoice, inv_id)
        if inv and not inv.is_void and inv.invoice_no:
            return (inv.invoice_no or '').strip()
    return ''

@inventory_bp.route('/stock_summary')
@login_required
def stock_summary():
    date_from = request.args.get('date_from', date.today().strftime('%Y-%m-%d')).strip()
    date_to = request.args.get('date_to', date_from).strip()
    # Backward compatibility for old single-date link/query.
    single_date = request.args.get('date', '').strip()
    if single_date:
        date_from = single_date
        date_to = single_date
    category_id = request.args.get('material_category', '').strip()
    material_filter = request.args.get('material', '').strip()

    # Normalize invalid range silently to a safe value.
    if date_to < date_from:
        date_to = date_from

    sel_date = date_to

    prev_stats = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=-Entry.qty)).label('prev_net')
    ).filter(Entry.date < sel_date, Entry.is_void == False)

    if material_filter:
        prev_stats = prev_stats.filter(Entry.material == material_filter)
    prev_stats = prev_stats.group_by(Entry.material).all()
    prev_map = {row.material: float(row.prev_net or 0) for row in prev_stats}
    
    day_stats = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=0)).label('day_in'),
        func.sum(case((Entry.type == 'OUT', Entry.qty), else_=0)).label('day_out')
    ).filter(Entry.date == sel_date, Entry.is_void == False)
    if material_filter:
        day_stats = day_stats.filter(Entry.material == material_filter)
    day_stats = day_stats.group_by(Entry.material).all()
    day_map = {row.material: {'in': float(row.day_in or 0), 'out': float(row.day_out or 0)} for row in day_stats}
    
    all_material_objs = Material.query.all()
    category_map = {m.name: m.category.name if m.category else '' for m in all_material_objs}
    all_materials = set(prev_map.keys()) | set(day_map.keys())
    for mat in Material.query.with_entities(Material.name).all():
        all_materials.add(mat.name)

    if category_id:
        try:
            cat_id_int = int(category_id)
            allowed = {m.name for m in Material.query.filter(Material.category_id == cat_id_int).all()}
            all_materials = {m for m in all_materials if m in allowed}
        except ValueError:
            pass
    if material_filter:
        all_materials = {m for m in all_materials if m == material_filter}
    
    stats = []
    for mat_name in sorted([m for m in all_materials if m is not None]):
        prev_net = prev_map.get(mat_name, 0)
        day_in = day_map.get(mat_name, {}).get('in', 0)
        day_out = day_map.get(mat_name, {}).get('out', 0)
        
        stats.append({
            'name': mat_name,
            'category': category_map.get(mat_name, ''),
            'opening': int(prev_net),
            'in': int(day_in),
            'out': int(day_out),
            'closing': int(prev_net + day_in - day_out)
        })

    range_query = Entry.query.filter(
        Entry.is_void == False,
        Entry.date >= date_from,
        Entry.date <= date_to
    )
    if category_id:
        try:
            cat_id_int = int(category_id)
            allowed_materials = [m.name for m in Material.query.filter(Material.category_id == cat_id_int).all()]
            if allowed_materials:
                range_query = range_query.filter(Entry.material.in_(allowed_materials))
            else:
                range_query = range_query.filter(Entry.id == -1)
        except ValueError:
            pass
    if material_filter:
        range_query = range_query.filter(Entry.material == material_filter)

    totals_rows = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=0)).label('total_in'),
        func.sum(case((Entry.type == 'OUT', Entry.qty), else_=0)).label('total_out')
    ).filter(
        Entry.id.in_(range_query.with_entities(Entry.id))
    ).group_by(Entry.material).order_by(Entry.material.asc()).all()

    totals_by_material = [{
        'material': row.material,
        'received': float(row.total_in or 0),
        'delivered': float(row.total_out or 0),
        'net': float((row.total_in or 0) - (row.total_out or 0))
    } for row in totals_rows if row.material]

    daily_rows = db.session.query(
        Entry.date,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=0)).label('day_in'),
        func.sum(case((Entry.type == 'OUT', Entry.qty), else_=0)).label('day_out')
    ).filter(
        Entry.id.in_(range_query.with_entities(Entry.id))
    ).group_by(Entry.date).order_by(Entry.date.desc()).all()

    day_wise = [{
        'date': row.date,
        'received': float(row.day_in or 0),
        'delivered': float(row.day_out or 0),
        'net': float((row.day_in or 0) - (row.day_out or 0))
    } for row in daily_rows if row.date]

    grand_received = float(sum(r['received'] for r in totals_by_material))
    grand_delivered = float(sum(r['delivered'] for r in totals_by_material))
    grand_net = grand_received - grand_delivered

    categories = MaterialCategory.query.order_by(MaterialCategory.name.asc()).all()
    materials = Material.query.order_by(Material.name.asc()).all()
    return render_template(
        'stock_summary.html',
        stats=stats,
        sel_date=sel_date,
        categories=categories,
        materials=materials,
        category_filter=category_id,
        date_from=date_from,
        date_to=date_to,
        material_filter=material_filter,
        totals_by_material=totals_by_material,
        day_wise=day_wise,
        grand_received=grand_received,
        grand_delivered=grand_delivered,
        grand_net=grand_net
    )

@inventory_bp.route('/daily_transactions')
@login_required
def daily_transactions():
    # Support date range and category filtering
    date_from = request.args.get('date_from') or request.args.get('date') or date.today().strftime('%Y-%m-%d')
    date_to = request.args.get('date_to') or date_from
    category = request.args.get('category', '').strip()
    trans_category = request.args.get('transaction_category', '').strip()
    material = request.args.get('material', '').strip()
    material_category = request.args.get('material_category', '').strip()
    bill_no = request.args.get('bill_no', '').strip()
    client_filter = request.args.get('client', '').strip()
    show_mode = (request.args.get('show') or 'active').strip().lower()

    page = request.args.get('page', 1, type=int)
    per_page = 50  # Increased for better visibility

    # Fix: Ensure query uses models correctly
    q = Entry.query.filter(Entry.date >= date_from, Entry.date <= date_to)
    if show_mode == 'voided':
        q = q.filter(Entry.is_void == True)
    elif show_mode == 'all':
        q = q
    else:
        show_mode = 'active'
        q = q.filter(Entry.is_void == False)
    if category:
        q = q.filter(or_(
            Entry.client_category == category,
            Entry.client_code.in_(db.session.query(Client.code).filter(Client.category == category))
        ))
    if trans_category:
        tc_norm = trans_category.strip().lower()
        if tc_norm == 'billed':
            q = q.filter(func.lower(func.coalesce(Entry.transaction_category, '')) == 'billed')
        elif tc_norm == 'unbilled':
            q = q.filter(func.lower(func.coalesce(Entry.transaction_category, '')).in_(['unbilled', 'unbilled cash']))
        elif tc_norm == 'open khata':
            q = q.filter(func.lower(func.coalesce(Entry.transaction_category, '')) == 'open khata')
        else:
            q = q.filter(func.lower(func.coalesce(Entry.transaction_category, '')) == tc_norm)
    if material:
        q = q.filter(Entry.material == material)
    if material_category:
        try:
            cat_id_int = int(material_category)
            cat_materials = [m.name for m in Material.query.filter(Material.category_id == cat_id_int).all()]
            if cat_materials:
                q = q.filter(Entry.material.in_(cat_materials))
            else:
                q = q.filter(Entry.id == -1)
        except ValueError:
            pass
    if bill_no:
        q = q.filter(or_(Entry.bill_no.ilike(f'%{bill_no}%'), Entry.auto_bill_no.ilike(f'%{bill_no}%')))
    if client_filter:
        # If filter looks like a code, do an exact match on code.
        if client_filter.lower().startswith(('tmpc-', 'fbm-')):
            q = q.filter(Entry.client_code == client_filter)
        else: # Otherwise, do a 'contains' search on the name.
            q = q.filter(Entry.client.ilike(f'%{client_filter}%'))
        
    entries_list = q.order_by(Entry.date.desc(), Entry.time.desc()).all()
    for e in entries_list:
        e.bill_ref = _entry_best_bill_ref(e)
        e.source_type = 'Entry'
        e.void_reason_label = ''

    payment_rows = []
    include_payments = not material and not material_category
    if include_payments:
        pay_q = Payment.query.filter(
            func.date(Payment.date_posted) >= date_from,
            func.date(Payment.date_posted) <= date_to
        )
        if show_mode == 'voided':
            pay_q = pay_q.filter(Payment.is_void == True)
        elif show_mode == 'all':
            pay_q = pay_q
        else:
            pay_q = pay_q.filter(Payment.is_void == False)
        if bill_no:
            pay_q = pay_q.filter(or_(Payment.manual_bill_no.ilike(f'%{bill_no}%'), Payment.auto_bill_no.ilike(f'%{bill_no}%')))
        if client_filter:
            if client_filter.lower().startswith(('tmpc-', 'fbm-')):
                client_obj = Client.query.filter_by(code=client_filter).first()
                if client_obj:
                    pay_q = pay_q.filter(Payment.client_name == client_obj.name)
                else:
                    pay_q = pay_q.filter(Payment.id == -1)
            else:
                pay_q = pay_q.filter(Payment.client_name.ilike(f'%{client_filter}%'))
        if category:
            category_clients = [row[0] for row in db.session.query(Client.name).filter(Client.category == category).all() if row[0]]
            if category_clients:
                pay_q = pay_q.filter(Payment.client_name.in_(category_clients))
            else:
                pay_q = pay_q.filter(Payment.id == -1)

        client_code_map = {c.name: c.code for c in Client.query.with_entities(Client.name, Client.code).all()}
        for p in pay_q.order_by(Payment.date_posted.desc(), Payment.id.desc()).all():
            dt = p.date_posted or datetime.now()
            payment_rows.append(SimpleNamespace(
                id=p.id,
                date=dt.strftime('%Y-%m-%d'),
                time=dt.strftime('%H:%M:%S'),
                type='PAYMENT',
                client=(p.client_name or ''),
                client_code=(client_code_map.get(p.client_name, '') or ''),
                material='-',
                qty=float(p.amount or 0),
                bill_no=(p.manual_bill_no or ''),
                auto_bill_no=(p.auto_bill_no or ''),
                nimbus_no='Payment',
                created_by='System',
                is_void=bool(p.is_void),
                bill_ref=(p.manual_bill_no or p.auto_bill_no or f'PAY-{p.id}'),
                source_type='Payment'
            ))

    all_rows = entries_list + payment_rows
    all_rows.sort(
        key=lambda r: datetime.strptime(f"{(getattr(r, 'date', '') or '').strip()} {(getattr(r, 'time', '') or '').strip()}".strip(), '%Y-%m-%d %H:%M:%S')
        if (getattr(r, 'date', None) and getattr(r, 'time', None)) else datetime.min,
        reverse=True
    )
    total = len(all_rows)
    pages = max(1, (total + per_page - 1) // per_page)
    if page > pages:
        page = pages
    start = (page - 1) * per_page
    end_idx = start + per_page
    paged_rows = all_rows[start:end_idx]

    # Classify "voided by edit" rows for clearer audit readability.
    for row in paged_rows:
        if getattr(row, 'source_type', '') != 'Entry':
            continue
        if not bool(getattr(row, 'is_void', False)):
            continue
        if (getattr(row, 'nimbus_no', '') or '').strip().lower() != 'direct sale':
            continue
        bill_ref = (getattr(row, 'bill_no', '') or '').strip() or (getattr(row, 'auto_bill_no', '') or '').strip()
        if not bill_ref:
            continue
        replacement = Entry.query.filter(
            Entry.id != row.id,
            Entry.is_void == False,
            Entry.nimbus_no == row.nimbus_no,
            Entry.type == row.type,
            Entry.material == row.material,
            Entry.client == row.client,
            Entry.qty == row.qty,
            or_(Entry.bill_no == bill_ref, Entry.auto_bill_no == bill_ref)
        ).order_by(Entry.id.desc()).first()
        if replacement and replacement.id > row.id:
            row.void_reason_label = 'Voided by Edit'

    entries_pagination = SimpleNamespace(
        page=page,
        pages=pages,
        total=total,
        has_prev=(page > 1),
        has_next=(page < pages),
        prev_num=(page - 1),
        next_num=(page + 1),
        items=paged_rows
    )

    materials = Material.query.all()
    material_category_map = {m.name: (m.category.name if m.category else '') for m in materials}
    clients = Client.query.filter_by(is_active=True).order_by(Client.name.asc()).all()
    material_categories = MaterialCategory.query.order_by(MaterialCategory.name.asc()).all()
    
    # Build categories list for filter efficiently
    categories_query = db.session.query(Client.category).distinct().filter(Client.category != None, Client.category != '').all()
    categories = sorted([c[0] for c in categories_query])
    if 'Open Khata' not in categories:
        categories.append('Open Khata')
        categories = sorted(categories, key=lambda x: str(x).lower())
    
    # Add Transaction Categories
    transaction_categories = ['Billed', 'Unbilled', 'Open Khata']
    
    # Get bill metadata (photos/urls) for this date range's entries
    bill_numbers = set()
    for e in paged_rows:
        if e.bill_no: bill_numbers.add(e.bill_no)
        if e.auto_bill_no: bill_numbers.add(e.auto_bill_no)
    
    bill_meta = {}
    if bill_numbers:
        def populate_meta(model):
            records = model.query.filter(or_(model.manual_bill_no.in_(list(bill_numbers)), model.auto_bill_no.in_(list(bill_numbers)))).all()
            for r in records:
                meta = {'photo_path': r.photo_path, 'photo_url': r.photo_url}
                if r.manual_bill_no: bill_meta[r.manual_bill_no] = meta
                if r.auto_bill_no: bill_meta[r.auto_bill_no] = meta
        
        populate_meta(DirectSale)
        populate_meta(Booking)
        populate_meta(Payment)
        populate_meta(GRN)

    return render_template('daily_transactions.html', 
                           entries=paged_rows, 
                           pagination=entries_pagination, 
                           sel_date=date_from,
                           date_from=date_from,
                           date_to=date_to,
                           category_filter=category,
                           transaction_category_filter=trans_category,
                           material_filter=material,
                           material_category_filter=material_category,
                           bill_no_filter=bill_no,
                           client_filter=client_filter,
                           clients=clients,
                           materials=materials,
                           material_category_map=material_category_map,
                           categories=categories,
                           material_categories=material_categories,
                           transaction_categories=transaction_categories,
                           show_mode=show_mode,
                           bill_meta=bill_meta)

@inventory_bp.route('/inventory_log')
@login_required
def inventory_log():
    # Keep it for compatibility or redirect
    return redirect(url_for('inventory.stock_summary'))
