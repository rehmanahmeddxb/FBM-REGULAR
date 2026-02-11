"""
Module loader for automatic blueprint registration.
Discovers and registers all blueprints from the blueprints directory.
"""
import os
import importlib
import inspect
from pathlib import Path
from flask import Blueprint


def load_modules(app, blueprint_dir='blueprints'):
    """
    Automatically discover and register all blueprints from the blueprint directory.
    
    Args:
        app: Flask application instance
        blueprint_dir: Directory containing blueprint modules
    """
    blueprint_path = Path(blueprint_dir)
    
    if not blueprint_path.exists():
        return
    
    # Discover all Python modules in the blueprints directory
    modules = []
    for file in blueprint_path.glob('*.py'):
        if file.name.startswith('_'):
            continue
        module_name = file.stem
        modules.append((module_name, file))
    
    # Import and register blueprints
    for module_name, module_path in sorted(modules):
        try:
            # Dynamically import the module
            spec = importlib.util.spec_from_file_location(
                f"{blueprint_dir}.{module_name}", 
                module_path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find all Blueprint objects in the module
            blueprints_found = []
            for name, obj in inspect.getmembers(module):
                if isinstance(obj, Blueprint):
                    blueprints_found.append((name, obj))
            
            # Register blueprints
            for bp_name, blueprint in blueprints_found:
                # Determine URL prefix from module name or blueprint name
                url_prefix = f"/{blueprint.name}"
                
                # Check if blueprint config exists
                if hasattr(module, 'MODULE_CONFIG'):
                    config = module.MODULE_CONFIG
                    if 'url_prefix' in config:
                        url_prefix = config['url_prefix']
                
                app.register_blueprint(blueprint, url_prefix=url_prefix)
                print(f"✓ Registered blueprint '{blueprint.name}' from module '{module_name}' at '{url_prefix}'")
                blueprints_found.append((bp_name, blueprint))
            
            if not blueprints_found:
                print(f"⚠ No blueprints found in module '{module_name}'")
                
        except Exception as e:
            print(f"✗ Error loading module '{module_name}': {str(e)}")


def get_modules_info(blueprint_dir='blueprints'):
    """
    Get information about all available modules.
    
    Returns:
        List of tuples containing (module_name, blueprints_in_module)
    """
    blueprint_path = Path(blueprint_dir)
    modules_info = []
    
    if not blueprint_path.exists():
        return modules_info
    
    for file in blueprint_path.glob('*.py'):
        if file.name.startswith('_'):
            continue
        module_name = file.stem
        
        try:
            spec = importlib.util.spec_from_file_location(
                f"{blueprint_dir}.{module_name}",
                file
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            blueprints = []
            for name, obj in inspect.getmembers(module):
                if isinstance(obj, Blueprint):
                    blueprints.append(obj.name)
            
            if blueprints:
                modules_info.append((module_name, blueprints))
        except Exception:
            pass
    
    return modules_info
