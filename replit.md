# Ahmed Cement - Integrated CRM & Inventory System

## Overview

Ahmed Cement Inventory Pro is a comprehensive sales and inventory management system built with Flask. It provides complete warehouse management capabilities including goods receipt (GRN), deliveries, stock tracking, client management, bookings, payments, invoicing, and financial ledgers. The application uses a dark-themed UI and is designed for cement/materials distribution businesses.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Framework
- **Flask** serves as the web framework with Jinja2 templating
- **Flask-SQLAlchemy** provides ORM functionality with SQLite as the database
- **Flask-Login** handles user authentication and session management

### Application Structure
- **main.py** - Primary application entry point containing routes and business logic
- **app.py** - Compatibility shim providing `create_app()` factory pattern for tests
- **models.py** - Database models using SQLAlchemy ORM

### Modular Blueprint System
The application uses an auto-discovery blueprint system located in `blueprints/`:
- **module_loader.py** in `utils/` automatically discovers and registers blueprints
- Blueprints follow naming convention `*_bp` or `bp` for auto-registration
- Each module can include a `MODULE_CONFIG` dict for metadata (name, description, url_prefix, enabled status)

### Key Blueprints
- **inventory** - Stock summary and daily transactions
- **import_export** - Bulk data import/export functionality
- **data_lab** - Data reconciliation and analysis tools
- **admin** - System administration (admin-only access)

### Database Models
Core entities include:
- **User** - Authentication with role-based permissions (admin/user)
- **Client** - Customer records with auto-generated codes (tmpc-XXXXXX format)
- **Material** - Inventory items with auto-generated codes (tmpm-XXXXX format)
- **Entry** - Stock IN/OUT transactions
- **GRN/GRNItem** - Goods Receipt Notes for incoming stock
- **Delivery/DeliveryItem** - Outbound deliveries reducing stock
- **Booking/BookingItem** - Customer booking records
- **Payment** - Payment tracking with multiple methods
- **Invoice** - Invoice management with status tracking
- **PendingBill** - Unpaid bills tracking
- **DirectSale/DirectSaleItem** - Quick sales entries

### Authentication & Authorization
- Password hashing via Werkzeug security utilities
- Role-based access: 'admin' and 'user' roles
- Granular permissions per user: can_view_stock, can_view_daily, can_view_history, can_import_export, can_manage_directory
- Backdated edit restrictions available per user

### Template Structure
- **layout.html** - Base template with dark-themed sidebar navigation
- Bootstrap 5 with dark theme styling
- Bootstrap Icons for UI elements
- Flatpickr for date picking

## External Dependencies

### Python Packages
- **Flask 3.0.0** - Web framework
- **Flask-SQLAlchemy 3.1.1** - Database ORM
- **Flask-Login 0.6.3** - User session management
- **Werkzeug 3.0.1** - WSGI utilities and password hashing
- **reportlab 4.0.7** - PDF report generation
- **pandas 2.1.4** - Data manipulation for imports/exports
- **openpyxl 3.1.2** - Excel file handling

### Frontend Libraries (CDN)
- Bootstrap 5.3.0 - UI framework
- Bootstrap Icons 1.10.0 - Icon set
- Flatpickr - Date picker component
- SheetJS (xlsx) - Client-side Excel parsing for imports

### Database
- **SQLite** - File-based database stored at `instance/ahmed_cement.db`
- Database auto-creates on first run via `db.create_all()`

### File Storage
- Uploaded files (photos for GRN/Delivery) stored locally
- Max upload size configured at 16MB