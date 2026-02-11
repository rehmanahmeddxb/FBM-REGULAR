# 📘 COMPLETE APPLICATION BLUEPRINT
## Ahmed Cement Inventory Management System
### Full Functional Documentation - Every Page, Every Button, Every Flow

**Version:** 3.0
**Date:** February 3, 2026
**Total Pages:** 23
**Total Functions:** 67
**Total User Flows:** 45

================================================================================
## TABLE OF CONTENTS
================================================================================

1. **SECTION 1: SYSTEM OVERVIEW**
2. **SECTION 2: USER ROLES & PERMISSIONS**
3. **SECTION 3: NAVIGATION STRUCTURE**
4. **SECTION 4: PAGE-BY-PAGE DETAILED DOCUMENTATION**
5. **SECTION 5: COMPLETE USER WORKFLOWS**
6. **SECTION 6: DATA FLOW DIAGRAMS**
7. **SECTION 7: BUTTON & FUNCTION REFERENCE**
8. **SECTION 8: REPORTING & ANALYTICS**
9. **SECTION 9: TROUBLESHOOTING GUIDE**


================================================================================
## SECTION 1: SYSTEM OVERVIEW
================================================================================

### 1.1 SYSTEM PURPOSE
The Ahmed Cement Inventory Management System is designed to manage:
- Material stock (cement brands)
- Client bookings and orders
- Dispatching and deliveries
- Financial tracking (bills, payments)
- Inventory movements (IN/OUT)
- Client accounts and ledgers

### 1.2 CORE MODULES
1. **Inventory Management**
   - Goods Receipt Notes (GRN) - Receiving stock
   - Material tracking
   - Stock summary and reports

2. **Sales & Bookings**
   - Client bookings (advance orders)
   - Direct sales (immediate sales)
   - Dispatching (delivery)

3. **Financial Management**
   - Pending bills (accounts receivable)
   - Payments received
   - Client ledgers
   - Financial reports

4. **Client Management**
   - Client directory
   - Client categories
   - Client ledgers

5. **Administration**
   - User management
   - Settings
   - Data cleanup


================================================================================
## SECTION 2: USER ROLES & PERMISSIONS
================================================================================

### 2.1 ADMIN ROLE
**Permissions:**
✓ View all pages
✓ Create, edit, delete all records
✓ Add/edit/delete users
✓ Access settings
✓ Import/export data
✓ Delete historical data
✓ Back-date entries
✓ Manage client directory

### 2.2 USER ROLE (Standard)
**Permissions:**
✓ View stock summary (if enabled)
✓ View daily transactions (if enabled)
✓ View history (if enabled)
✗ Cannot back-date entries
✗ Cannot delete old records
✗ Cannot access settings
✗ Cannot import/export (unless enabled)
✗ Cannot manage directory (unless enabled)


================================================================================
## SECTION 3: NAVIGATION STRUCTURE
================================================================================

### 3.1 MAIN NAVIGATION MENU
```text
┌─────────────────────────────────────────────────────────────────────────┐
│  Logo: Ahmed Cement    [Dashboard] [Inventory] [Sales] [Finance] [More] │
└─────────────────────────────────────────────────────────────────────────┘
```

**TOP MENU BAR:**
1. Dashboard (Home)
2. Stock Summary
3. Daily Transactions
4. Clients
5. Materials
6. Bookings
7. Direct Sales
8. Dispatching
9. Pending Bills
10. Payments
11. Tracking
12. Settings
13. Logout

**DROPDOWN MENUS:**

**📦 Inventory Menu:**
  - Receiving (GRN)
  - Stock Summary
  - Daily Transactions
  - Material Ledger
  - Tracking

**💰 Sales Menu:**
  - Bookings
  - Direct Sales
  - Dispatching

**💵 Finance Menu:**
  - Pending Bills (Manual)
  - Pending Bills (Automatic)
  - Payments
  - Unpaid Transactions

**👥 Clients Menu:**
  - Client Directory
  - Client Ledger

**⚙️ Settings Menu:**
  - User Management
  - System Settings
  - Import/Export
  - Data Cleanup


================================================================================
## SECTION 4: PAGE-BY-PAGE DETAILED DOCUMENTATION
================================================================================

### PAGE 1: DASHBOARD (Landing Page)

**URL:** /
**Access:** All logged-in users

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ HEADER: Ahmed Cement Inventory System                                │
│ DATE: February 03, 2026                                              │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐    │
│ │ TOTAL      │  │ CLIENTS    │  │ UNPAID     │  │ TOTAL      │    │
│ │ STOCK      │  │ COUNT      │  │ BILLS      │  │ CREDIT     │    │
│ │ 1,250 bags │  │ 45         │  │ Rs 250,000 │  │ Rs 350,000 │    │
│ └────────────┘  └────────────┘  └────────────┘  └────────────┘    │
│                                                                       │
├──────────────────────────────────────────────────────────────────────┤
│ STOCK SUMMARY TABLE:                                                 │
│                                                                       │
│ Material Name    │ Received │ Dispatched │ Available Stock           │
│ ────────────────┼──────────┼────────────┼─────────────              │
│ DG Cement        │ 500      │ 350        │ 150                       │
│ Askari Cement    │ 300      │ 200        │ 100                       │
│ Lucky Cement     │ 450      │ 250        │ 200                       │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**COMPONENTS:**

1. **Statistics Cards (Top Row)**
   - **Card 1: Total Stock**
     - Shows: Sum of all available stock
     - Calculation: (Total IN - Total OUT)
     - Updates: Real-time on any stock movement
     - Click: Goes to Stock Summary page
   - **Card 2: Client Count**
     - Shows: Number of active clients
     - Calculation: COUNT(clients WHERE is_active=true)
     - Updates: When client added/deactivated
     - Click: Goes to Clients page
   - **Card 3: Unpaid Bills**
     - Shows: Total pending amount
     - Calculation: SUM(pending_bills WHERE is_paid=false)
     - Updates: When bill paid or new bill added
     - Click: Goes to Pending Bills page
   - **Card 4: Total Credit**
     - Shows: Total credit amount
     - Calculation: SUM(pending_bills WHERE is_paid=false)
     - Updates: Real-time
     - Click: Goes to Financial Reports

2. **Stock Summary Table**
   - **Columns:**
     - Material Name: Cement brand/type
     - Received: Total IN quantity
     - Dispatched: Total OUT quantity
     - Available Stock: IN - OUT
   - **Actions:**
     - Click material name → Goes to Material Ledger
     - Hover → Shows tooltip with more details
   - **Sorting:**
     - Alphabetical by material name
     - Can click column headers to sort

**HOW TO USE:**
1. Login to system (see Page 23: Login)
2. Dashboard automatically opens
3. View quick statistics in top cards
4. Scroll down to see material-wise stock
5. Click any statistic card to drill down
6. Click material name to see detailed ledger


### PAGE 2: STOCK SUMMARY

**URL:** /stock_summary
**Access:** Users with can_view_stock permission

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ STOCK SUMMARY                                          [Export Excel] │
├──────────────────────────────────────────────────────────────────────┤
│ Filter: [All Materials ▼] [Date From:____] [Date To:____] [Apply]   │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ Material          Opening  Received  Dispatched  Closing  Status     │
│ ────────────────  ───────  ────────  ──────────  ───────  ──────    │
│ DG Cement         100      150       120         130      ✓ OK       │
│ Askari Cement     50       100       80          70       ⚠ LOW      │
│ Lucky Cement      0        200       180         20       ⚠ LOW      │
│                                                                       │
│ ─────────────────────────────────────────────────────────────────── │
│ TOTALS:           150      450       380         220                 │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**FILTERS:**
1. **Material Filter**
   - Dropdown: Shows all materials + "All Materials" option
   - Default: All Materials
   - Effect: Shows only selected material when applied
2. **Date From**
   - Date picker
   - Default: Beginning of current month
   - Effect: Opening stock calculated from this date
3. **Date To**
   - Date picker
   - Default: Today
   - Effect: Closing stock calculated up to this date
4. **Apply Button**
   - Click to apply filters
   - Reloads table with filtered data

**ACTIONS:**
1. **Export Excel**
   - Generates Excel file with current view
   - Filename: stock_summary_YYYY-MM-DD.xlsx
   - Includes: All columns and totals
2. **Click Material Name**
   - Opens Material Ledger for that material
   - Shows all IN/OUT transactions
3. **Status Indicators**
   - ✓ OK: Stock > 50 bags (Green)
   - ⚠ LOW: Stock 10-50 bags (Yellow)
   - ✗ CRITICAL: Stock < 10 bags (Red)

**HOW TO USE:**
- **SCENARIO 1: Check current stock levels**
  - Click "Stock Summary" in menu -> View Closing column -> Check Status column
- **SCENARIO 2: Check stock for specific material**
  - Open Stock Summary -> Select material -> Click "Apply" -> View detailed numbers
- **SCENARIO 3: Export stock report for accounting**
  - Set date range -> Click "Apply" -> Click "Export Excel" -> Save file


### PAGE 3: DAILY TRANSACTIONS

**URL:** /daily_transactions
**Access:** Users with can_view_daily permission

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ DAILY TRANSACTIONS                                                    │
├──────────────────────────────────────────────────────────────────────┤
│ Select Date: [2026-02-03 ▼]                        [Print Report]    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ SUMMARY FOR: February 03, 2026                                       │
│                                                                       │
│ Opening Stock:    1,000 bags                                         │
│ Received Today:     150 bags  (+)                                    │
│ Dispatched Today:   120 bags  (-)                                    │
│ Closing Stock:    1,030 bags                                         │
│                                                                       │
├──────────────────────────────────────────────────────────────────────┤
│ DETAILED TRANSACTIONS:                                                │
│                                                                       │
│ Time  │ Type │ Material      │ Client          │ Qty │ Bill No       │
│ ──────┼──────┼───────────────┼─────────────────┼─────┼──────────    │
│ 09:30 │ IN   │ DG Cement     │ Supplier A      │ 100 │ GRN-001       │
│ 10:15 │ OUT  │ DG Cement     │ Zafar Builders  │ 50  │ #1001         │
│ 11:00 │ IN   │ Askari Cement │ Supplier B      │ 50  │ GRN-002       │
│ 14:30 │ OUT  │ Askari Cement │ Khan Traders    │ 30  │ 12345         │
│ 16:00 │ OUT  │ Lucky Cement  │ Ahmed Const.    │ 40  │ #1002         │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**COMPONENTS:**
1. **Date Selector**
   - Calendar dropdown
   - Shows dates with transactions in bold
   - Default: Today's date
   - Click date → Loads that day's transactions
2. **Summary Section**
   - Opening Stock: Closing stock of previous day
   - Received Today: Sum of all IN transactions (green)
   - Dispatched Today: Sum of all OUT transactions (red)
   - Closing Stock: Opening + Received - Dispatched
3. **Detailed Transactions Table**
   - Columns: Time, Type (IN/OUT), Material, Client, Qty, Bill No
   - Sorting: Default Time ascending

**ACTIONS:**
1. **Print Report Button:** Opens print dialog with company header
2. **Click Bill Number:** Opens bill detail view
3. **Navigation Arrows:** Jump between dates


### PAGE 4: CLIENTS (Client Directory)

**URL:** /clients
**Access:** All users

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ CLIENT DIRECTORY                               [+ Add New Client]     │
├──────────────────────────────────────────────────────────────────────┤
│ Search: [____________]  Category: [All ▼]  Status: [Active ▼] [Go]  │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ Code      │ Name              │ Phone        │ Category │ Actions    │
│ ──────────┼───────────────────┼──────────────┼──────────┼──────────  │
│ tmpc-0001 │ Zafar Builders    │ 0321-1234567 │ Credit   │ [View][Edit]│
│ tmpc-0002 │ Ahmed Construction│ 0333-7654321 │ Credit   │ [View][Edit]│
│ tmpc-0003 │ Khan Traders      │ 0345-9876543 │ General  │ [View][Edit]│
│ tmpc-0004 │ Walking Customer  │ -            │ Cash     │ [View][Edit]│
│                                                                       │
│ Showing 1-10 of 45 clients                      « 1 2 3 4 5 »        │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**ADD NEW CLIENT FORM (Modal):**
- **Client Code:** Auto-generated (tmpc-XXXXXX)
- **Client Name*:** Required
- **Phone Number:** Optional
- **Address:** Optional
- **Category*:** General, Credit Customer, Cash Customer
- **Require Manual Invoice:** Checkbox to force manual bill entry

**HOW TO USE:**
- **Add new client:** Click "+ Add New Client" -> Fill details -> Save
- **Find existing client:** Type name in search box
- **Update client:** Click "Edit" button
- **Deactivate client:** Edit -> Change status to "Inactive"


### PAGE 5: MATERIALS (Material Master)

**URL:** /materials
**Access:** All users

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ MATERIALS MASTER                              [+ Add New Material]    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ Code        │ Material Name    │ Unit Price │ Current Stock │ Actions│
│ ────────────┼──────────────────┼────────────┼───────────────┼─────── │
│ tmpm-00001  │ DG Cement        │ Rs 800     │ 150 bags      │ [Edit] │
│ tmpm-00002  │ Askari Cement    │ Rs 750     │ 100 bags      │ [Edit] │
│ tmpm-00003  │ Lucky Cement     │ Rs 780     │ 200 bags      │ [Edit] │
│ tmpm-00004  │ Maple Leaf Cement│ Rs 820     │ 50 bags       │ [Edit] │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**FIELDS:**
1. **Material Code:** Auto-generated (tmpm-XXXXX)
2. **Material Name*:** Required (e.g., "DG Cement")
3. **Unit Price*:** Price per bag in Rupees
4. **Current Stock:** Read-only, calculated from transactions

**ACTIONS:**
- **Add New Material:** Click "+ Add New Material" -> Enter details -> Save
- **Edit Material:** Click "Edit" -> Update price/name -> Save


### PAGE 6: BOOKINGS

**URL:** /bookings
**Access:** All users

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ BOOKINGS                                      [+ Add New Booking]     │
├──────────────────────────────────────────────────────────────────────┤
│ Filter: [All Clients ▼] [All Materials ▼] [Date From:__] [Date To:__]│
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ Date     │Client         │Material     │Qty │Amount  │Paid│Bill No  │
│ ─────────┼───────────────┼─────────────┼────┼────────┼────┼──────── │
│ 02-03-26 │Zafar Builders │DG Cement    │50  │40,000  │0   │MANUAL-01│
│ 02-03-26 │Khan Traders   │Askari Cement│30  │22,500  │5000│#1001    │
│ 02-02-26 │Ahmed Const.   │Lucky Cement │100 │78,000  │0   │12345    │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**ADD NEW BOOKING FORM:**
- **Client*:** Select from existing clients
- **Location:** Delivery address (optional)
- **Materials Section:**
  - Select Material, Enter Quantity
  - Rate auto-fills, Amount auto-calculates
  - Can add multiple rows
- **Total Amount:** Grand total
- **Paid Amount:** Advance payment (optional)
- **Manual Bill No:** Optional (auto-generated if empty)
- **Photo:** Optional upload

**BOOKING LIFECYCLE:**
1. **CREATED:** Booking saved, materials reserved, pending bill created.
2. **PARTIAL DISPATCH:** Some materials dispatched, booking quantity reduced.
3. **FULLY DISPATCHED:** All materials delivered, booking quantity 0.
4. **PAID:** Payment received, pending bill cleared.


### PAGE 7: DISPATCHING (Stock Out)

**URL:** /dispatching
**Access:** All users

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ DISPATCHING (Stock Out)                                               │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ Date*:            [2026-02-03]                                        │
│                                                                       │
│ Type*:            [● OUT    ○ IN]                                     │
│                                                                       │
│ Material*:        [Select Material ▼]                                │
│                   (DG Cement)                                         │
│                                                                       │
│ Client*:          [Select Client ▼]                                  │
│                   (Zafar Builders - tmpc-00001)                      │
│                                                                       │
│ Quantity*:        [___________] bags                                 │
│                                                                       │
│ Bill/Invoice No:  [___________]  (Optional)                          │
│                                                                       │
│ Nimbus/Ref No:    [___________]  (Optional)                          │
│                                                                       │
│                                                [Cancel] [Dispatch]    │
└──────────────────────────────────────────────────────────────────────┘
```

**DISPATCHING PROCESS:**
- **Date*:** Actual dispatch date
- **Type*:** Always OUT for dispatching
- **Material*:** Select material
- **Client*:** Select client
  - **CRITICAL VALIDATION:** System checks if client has booking for this material.
  - **If NO booking:** Shows error message.
- **Quantity*:** Bags to dispatch
  - **Validation:** Cannot exceed booking quantity.
- **Bill/Invoice No:** Optional link to invoice

**SCENARIOS:**
- **Dispatch against booking:** Select client/material -> Enter qty -> Dispatch. Booking reduced, stock reduced.
- **Partial dispatch:** Enter partial qty. Remaining booking stays open.
- **No booking (Error):** Try to dispatch to client without booking -> Error shown -> Use Direct Sale instead.
- **Over-dispatch (Error):** Try to dispatch more than booked -> Error shown.


### PAGE 8: DIRECT SALES

**URL:** /direct_sales
**Access:** All users

**PURPOSE:** Immediate cash sales without booking (Walking customers, one-time buyers).

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ DIRECT SALES                                [+ Add New Direct Sale]   │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ Date      │ Client        │ Materials       │ Amount  │ Paid  │ Bill │
│ ──────────┼───────────────┼─────────────────┼─────────┼───────┼───── │
│ 02-03-26  │ Walk-in       │ DG Cement (10)  │ 8,000   │ 8,000 │ #1050│
│ 02-03-26  │ Fahad Const.  │ Askari (20)     │ 15,000  │ 15,000│ CS-01│
│ 02-02-26  │ Ali Brothers  │ Lucky (5)       │ 3,900   │ 3,900 │ #1049│
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**ADD DIRECT SALE FORM:**
- **Client Name*:** Can type ANY name (doesn't need to be in directory).
- **Materials Section:** Select material, qty.
- **Paid Amount*:** Usually equals Total Amount for cash sales.
- **Payment Method*:** Cash, Bank Transfer, Cheque, Other.
- **Category:** Cash, Credit, General.

**KEY DIFFERENCES: DIRECT SALE vs BOOKING**
- **Client:** Booking requires existing client; Direct Sale accepts any name.
- **Stock Movement:** Booking reserves only; Direct Sale does immediate OUT.
- **Payment:** Booking can be 0; Direct Sale usually full payment.
- **Dispatch:** Booking needs separate dispatch; Direct Sale includes dispatch.


### PAGE 9: PENDING BILLS

**URL:** /pending_bills
**Access:** All users

**PURPOSE:** Accounts Receivable / Unpaid Bills.

**LAYOUT:**
```text
┌──────────────────────────────────────────────────────────────────────┐
│ PENDING BILLS                                                         │
├──────────────────────────────────────────────────────────────────────┤
│ View: [● Manual Bills  ○ Automatic Bills]                           │
├──────────────────────────────────────────────────────────────────────┤
│ Filters: [Client ▼] [Bill No: ___] [Status: Unpaid ▼] [Apply]      │
├──────────────────────────────────────────────────────────────────────┤
│                                                                       │
│ Bill No   │Client         │Amount  │Paid   │Balance │Status │Actions│
│ ──────────┼───────────────┼────────┼───────┼────────┼───────┼────── │
│ MANUAL-01 │Zafar Builders │40,000  │0      │40,000  │UNPAID │[View] │
│ 12345     │Khan Traders   │22,500  │5,000  │17,500  │PARTIAL│[View] │
│ BK-2026-1 │Ahmed Const.   │78,000  │78,000 │0       │PAID ✓ │[View] │
│                                                                       │
│ ──────────────────────────────────────────────────────────────────── │
│ TOTALS:   45 bills        │350,000 │120,000 │230,000 │               │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**TABS:**
1. **MANUAL BILLS:** Bills from physical bill book (official).
2. **AUTOMATIC BILLS:** System generated bills (#1001).

**COLUMNS:**
- **Status:** UNPAID (Red), PARTIAL (Yellow), PAID (Green).
- **Actions:** View, Mark as Paid, Edit.

**SCENARIOS:**
- **Check who owes money:** Filter by "Unpaid" status.
- **Check specific client:** Filter by Client name.
- **Mark as paid:** Click "Mark as Paid" or add record in Payments page.
- **Aging check:** Filter by date range to find old bills.
#   V S C O D E - A P P  
 