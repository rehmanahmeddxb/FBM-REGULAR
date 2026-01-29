# Ahmed Cement Inventory System

## Recent Changes (January 25, 2026)
- **Duplicate Prevention**: Clients and Materials are now differentiated by unique CODES
- **Manual Entry**: Code is REQUIRED when manually adding clients or materials
- **Import Auto-Code**: CSV/Excel imports auto-generate codes if missing:
  - Clients: `tmpc-000001`, `tmpc-000002`, etc.
  - Materials: `tmpm-00001`, `tmpm-00002`, etc.
- **Database Schema**: Both Client and Material models now have `code` as unique required field

## Master Rules & Architecture (January 26, 2026)
- **Pending Bills Master**: Single source of truth. Lightweight, indexed, and fast-loading.
- **Import Logic**: Harden imports to allow blank cells (except Bill No and Client ID). Auto-create clients if missing.
- **Granular Wipe**: Settings now allow selecting specific datasets (Clients, Bills, Entries, Materials) for deletion.
- **Modern UI**: Flatpickr implemented globally for a bug-free, modern date selection experience.
- **Data Consistency**: Edits propagate across all relevant modules (Pending Bills, Dispatching, Receiving, Reports).
- **Function-level Permissions**: Standard users restricted from editing back-dated data.

## Overview

This is a Flask-based inventory management system for a cement distribution business. The application tracks stock receiving (IN) and dispatching (OUT), manages client relationships, and provides reporting capabilities. It features user authentication with role-based access control (admin/user), a dark-themed modern UI built with Bootstrap 5, and SQLite database storage.

**Core Features:**
- Stock receiving and dispatching with material tracking
- Client directory with individual transaction ledgers
- Material/brand management
- Daily inventory log with physical count reconciliation
- Excel/CSV/PDF export capabilities
- Data import functionality (append or daily sync modes)
- Multi-user system with admin controls

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Template Engine:** Jinja2 templating integrated with Flask
- **UI Framework:** Bootstrap 5.3 with Bootstrap Icons
- **Design Pattern:** Server-side rendered pages using a shared layout template (`layout.html`)
- **Theme:** Dark mode design with custom CSS variables (navy/slate color scheme with gold accents)
- **Navigation:** Fixed sidebar navigation with responsive design

### Backend Architecture
- **Framework:** Flask (Python web framework)
- **Pattern:** Monolithic MVC-style architecture with routes defined in `main.py`
- **Authentication:** Flask-Login for session management with Werkzeug password hashing
- **Role System:** Two-tier access control (admin, user) stored in User model

### Data Storage
- **Database:** SQLite with SQLAlchemy ORM
- **Location:** `instance/ahmed_cement.db` (created automatically)
- **Models:**
  - `User` - Authentication and role management
  - `Client` - Customer directory (name, code, phone, address)
  - `Material` - Cement brands/types
  - `Entry` - Transaction records (IN/OUT movements)

### Key Design Decisions

1. **SQLite over PostgreSQL:** Chosen for simplicity and portability. The application creates the database file automatically in an `instance` folder. This can be migrated to PostgreSQL if scaling is needed.

2. **Server-Side Rendering:** All pages are rendered server-side using Jinja2 templates rather than a SPA approach. This keeps the stack simple and reduces frontend complexity.

3. **Modular Architecture:** The main routes are in `main.py` with specialized functionality split into blueprints (`blueprints/inventory.py`, `blueprints/import_export.py`).

4. **Session-Based Auth:** Uses Flask-Login sessions rather than JWT tokens, appropriate for a traditional web application with server-rendered pages.

5. **Optimized Database Queries:** All data aggregation uses SQLAlchemy's `func.sum()` and `case()` expressions instead of Python loops for maximum performance. This prevents memory issues with large datasets.

### Database Indexes

Composite indexes added to Entry model for fast lookups:
- `idx_entry_date_material` - Date and material combination queries
- `idx_entry_material_type` - Material and type aggregations
- `idx_entry_date_type` - Date and type filtering
- `idx_entry_client_date` - Client ledger queries

## External Dependencies

### Python Packages
- **Flask** - Web framework
- **Flask-SQLAlchemy** - ORM for database operations
- **Flask-Login** - User session management
- **Werkzeug** - Password hashing utilities
- **Pandas** - Data manipulation for imports/exports

### Frontend CDN Resources
- Bootstrap 5.3.0 CSS/JS
- Bootstrap Icons 1.10.0

### Database
- SQLite (file-based, no external server required)

### File Storage
- Local filesystem for database storage (`instance/` directory)
- No cloud storage integrations