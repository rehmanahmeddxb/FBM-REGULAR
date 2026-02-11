from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from datetime import date
from sqlalchemy import func, case, or_
from models import db, Material, Entry, Client, PendingBill

# Module configuration
MODULE_CONFIG = {
    'name': 'Inventory Module',
    'description': 'Stock and inventory management',
    'url_prefix': '/inventory',
    'enabled': True
}

inventory_bp = Blueprint('inventory', __name__)

@inventory_bp.route('/stock_summary')
@login_required
def stock_summary():
    sel_date = request.args.get('date', date.today().strftime('%Y-%m-%d'))
    
    prev_stats = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=-Entry.qty)).label('prev_net')
    ).filter(Entry.date < sel_date, Entry.is_void == False).group_by(Entry.material).all()
    prev_map = {row.material: float(row.prev_net or 0) for row in prev_stats}
    
    day_stats = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=0)).label('day_in'),
        func.sum(case((Entry.type == 'OUT', Entry.qty), else_=0)).label('day_out')
    ).filter(Entry.date == sel_date, Entry.is_void == False).group_by(Entry.material).all()
    day_map = {row.material: {'in': float(row.day_in or 0), 'out': float(row.day_out or 0)} for row in day_stats}
    
    all_materials = set(prev_map.keys()) | set(day_map.keys())
    for mat in Material.query.with_entities(Material.name).all():
        all_materials.add(mat.name)
    
    stats = []
    for mat_name in sorted([m for m in all_materials if m is not None]):
        prev_net = prev_map.get(mat_name, 0)
        day_in = day_map.get(mat_name, {}).get('in', 0)
        day_out = day_map.get(mat_name, {}).get('out', 0)
        
        stats.append({
            'name': mat_name,
            'opening': int(prev_net),
            'in': int(day_in),
            'out': int(day_out),
            'closing': int(prev_net + day_in - day_out)
        })
        
    return render_template('stock_summary.html', stats=stats, sel_date=sel_date)

@inventory_bp.route('/daily_transactions')
@login_required
def daily_transactions():
    # Support date range and category filtering
    date_from = request.args.get('date_from') or request.args.get('date') or date.today().strftime('%Y-%m-%d')
    date_to = request.args.get('date_to') or date_from
    category = request.args.get('category', '').strip()
    trans_category = request.args.get('transaction_category', '').strip()

    page = request.args.get('page', 1, type=int)
    per_page = 50  # Increased for better visibility

    # Fix: Ensure query uses models correctly
    q = Entry.query.filter(Entry.date >= date_from, Entry.date <= date_to)
    if category:
        q = q.filter(or_(
            Entry.client_category == category,
            Entry.client_code.in_(db.session.query(Client.code).filter(Client.category == category))
        ))
    if trans_category:
        q = q.filter(Entry.transaction_category == trans_category)
        
    entries_pagination = q.order_by(Entry.date.desc(), Entry.time.desc()).paginate(page=page, per_page=per_page, error_out=False)

    materials = Material.query.all()
    
    # Build categories list for filter efficiently
    categories_query = db.session.query(Client.category).distinct().filter(Client.category != None, Client.category != '').all()
    categories = sorted([c[0] for c in categories_query])
    
    # Add Transaction Categories
    transaction_categories = ['BILLED', 'UNBILLED', 'OPEN KHATA']
    
    # Get pending bills with photos for this date range's entries
    bill_numbers = [e.bill_no for e in entries_pagination.items if e.bill_no]
    pending_photos = {}
    if bill_numbers:
        # Use .get() or handle missing columns more robustly if needed, but here we assume models.py is correct
        pending_photos = {b.bill_no: b.photo_url for b in PendingBill.query.filter(PendingBill.bill_no.in_(bill_numbers), PendingBill.photo_url != '').all() if hasattr(b, 'photo_url')}

    return render_template('daily_transactions.html', 
                           entries=entries_pagination.items, 
                           pagination=entries_pagination, 
                           sel_date=date_from,
                           date_from=date_from,
                           date_to=date_to,
                           category_filter=category,
                           transaction_category_filter=trans_category,
                           materials=materials,
                           categories=categories,
                           transaction_categories=transaction_categories,
                           pending_photos=pending_photos)

@inventory_bp.route('/inventory_log')
@login_required
def inventory_log():
    # Keep it for compatibility or redirect
    return redirect(url_for('inventory.stock_summary'))
