#!/usr/bin/env python3
"""
Test script to verify the module system is working correctly.
Run: python test_modules.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.module_loader import get_modules_info
from app import create_app


def test_module_discovery():
    """Test that module discovery works."""
    print("\n" + "=" * 60)
    print("TEST 1: Module Discovery")
    print("=" * 60)

    modules = get_modules_info('blueprints')

    if not modules:
        print("[FAIL] No modules found!")
        return False

    print(f"[OK] Found {len(modules)} modules:")
    for module_name, blueprints in modules:
        print(f"   - {module_name}: {', '.join(blueprints)}")

    return True


def test_app_creation():
    """Test that the Flask app creates successfully with modules."""
    print("\n" + "=" * 60)
    print("TEST 2: Flask App Creation with Auto-Loading")
    print("=" * 60)

    try:
        app = create_app()
        print("[OK] Flask app created successfully")

        # List registered blueprints
        print(f"[OK] Registered blueprints ({len(app.blueprints)}):")
        for name in sorted(app.blueprints.keys()):
            print(f"   - {name}")

        return True
    except Exception as e:
        print(f"[FAIL] Error creating app: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_blueprint_routes():
    """Test that blueprint routes are accessible."""
    print("\n" + "=" * 60)
    print("TEST 3: Blueprint Routes")
    print("=" * 60)

    try:
        app = create_app()

        # Get all routes
        routes = {}
        for rule in app.url_map.iter_rules():
            if rule.endpoint != 'static':
                routes.setdefault(rule.endpoint, []).append(str(rule))

        print(f"[OK] Found {len(routes)} route endpoints:")
        for endpoint in sorted(routes.keys()):
            print(f"   - {endpoint}")
            for route in routes[endpoint]:
                print(f"     -> {route}")

        return True
    except Exception as e:
        print(f"[FAIL] Error checking routes: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_module_config():
    """Test that MODULE_CONFIG is accessible."""
    print("\n" + "=" * 60)
    print("TEST 4: Module Configuration")
    print("=" * 60)

    try:
        from blueprints import data_lab
        from blueprints import import_export
        from blueprints import inventory

        modules_to_check = [
            ('data_lab', data_lab),
            ('import_export', import_export),
            ('inventory', inventory),
        ]

        configs_found = 0
        for name, module in modules_to_check:
            if hasattr(module, 'MODULE_CONFIG'):
                config = module.MODULE_CONFIG
                print(f"[OK] {name}:")
                print(f"   Name: {config.get('name', 'N/A')}")
                print(f"   URL: {config.get('url_prefix', 'N/A')}")
                print(f"   Enabled: {config.get('enabled', 'N/A')}")
                configs_found += 1
            else:
                print(f"[WARN] {name}: No MODULE_CONFIG")

        print(f"\n[OK] {configs_found}/{len(modules_to_check)} modules have configuration")
        return configs_found > 0
    except Exception as e:
        print(f"[FAIL] Error checking config: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n")
    print("+" + "=" * 58 + "+")
    print("|" + " MODULE SYSTEM TEST SUITE ".center(58) + "|")
    print("+" + "=" * 58 + "+")

    results = []

    results.append(("Module Discovery", test_module_discovery()))
    results.append(("App Creation", test_app_creation()))
    results.append(("Blueprint Routes", test_blueprint_routes()))
    results.append(("Module Config", test_module_config()))

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{status}: {test_name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\nAll tests passed! Module system is working correctly.")
        return 0

    print(f"\n{total - passed} test(s) failed. Check the errors above.")
    return 1


if __name__ == '__main__':
    sys.exit(main())
