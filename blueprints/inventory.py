from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from datetime import date
from sqlalchemy import func, case
from models import db, Material, Entry

inventory_bp = Blueprint('inventory', __name__)

@inventory_bp.route('/stock_summary')
@login_required
def stock_summary():
    sel_date = request.args.get('date', date.today().strftime('%Y-%m-%d'))
    
    prev_stats = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=-Entry.qty)).label('prev_net')
    ).filter(Entry.date < sel_date).group_by(Entry.material).all()
    prev_map = {row.material: float(row.prev_net or 0) for row in prev_stats}
    
    day_stats = db.session.query(
        Entry.material,
        func.sum(case((Entry.type == 'IN', Entry.qty), else_=0)).label('day_in'),
        func.sum(case((Entry.type == 'OUT', Entry.qty), else_=0)).label('day_out')
    ).filter(Entry.date == sel_date).group_by(Entry.material).all()
    day_map = {row.material: {'in': float(row.day_in or 0), 'out': float(row.day_out or 0)} for row in day_stats}
    
    all_materials = set(prev_map.keys()) | set(day_map.keys())
    for mat in Material.query.with_entities(Material.name).all():
        all_materials.add(mat.name)
    
    stats = []
    for mat_name in sorted(all_materials):
        prev_net = prev_map.get(mat_name, 0)
        day_in = day_map.get(mat_name, {}).get('in', 0)
        day_out = day_map.get(mat_name, {}).get('out', 0)
        effective_opening = prev_net + day_in
        stats.append({
            'name': mat_name,
            'opening': int(effective_opening),
            'in': int(day_in),
            'out': int(day_out),
            'closing': int(effective_opening - day_out)
        })
        
    return render_template('stock_summary.html', stats=stats, sel_date=sel_date)

@inventory_bp.route('/daily_transactions')
@login_required
def daily_transactions():
    sel_date = request.args.get('date', date.today().strftime('%Y-%m-%d'))
    page = request.args.get('page', 1, type=int)
    per_page = 50  # Increased for better visibility
    
    entries_pagination = Entry.query.filter_by(date=sel_date).order_by(Entry.time.desc()).paginate(page=page, per_page=per_page)
    materials = Material.query.all()
    
    # Get pending bills with photos for this date's entries
    bill_numbers = [e.bill_no for e in entries_pagination.items if e.bill_no]
    from models import PendingBill
    pending_photos = {b.bill_no: b.photo_url for b in PendingBill.query.filter(PendingBill.bill_no.in_(bill_numbers), PendingBill.photo_url != '').all()}
    
    return render_template('daily_transactions.html', 
                           entries=entries_pagination.items, 
                           pagination=entries_pagination, 
                           sel_date=sel_date,
                           materials=materials,
                           pending_photos=pending_photos)

@inventory_bp.route('/inventory_log')
@login_required
def inventory_log():
    # Keep it for compatibility or redirect
    return redirect(url_for('inventory.stock_summary'))
