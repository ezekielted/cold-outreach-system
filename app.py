import os
import sys
import time
import importlib.util
from pathlib import Path

def load_module(module_name, module_path):
    try:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None:
            print(f"Error: Could not find module at {module_path}")
            return None
            
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        print(f"Error loading module {module_name} from {module_path}: {e}")
        return None

def run_module(module, module_name):
    if module is None:
        return False
        
    print(f"\n{'='*60}")
    print(f"RUNNING {module_name.upper()}")
    print(f"{'='*60}")
    
    try:
        # Check if the module has a main function
        if hasattr(module, 'main') and callable(module.main):
            start_time = time.time()
            result = module.main()
            elapsed_time = time.time() - start_time
            
            print(f"\n{'-'*60}")
            print(f"{module_name.upper()} COMPLETED in {elapsed_time:.2f} seconds")
            print(f"{'-'*60}")
            
            # If main returns a value, check if it indicates success
            if result is not None:
                return bool(result)
            return True
        else:
            print(f"Error: Module {module_name} does not have a main function")
            return False
    except Exception as e:
        print(f"Error executing {module_name}: {e}")
        return False

def main():
    """
    Main function that orchestrates the execution of the three modules
    """
    # Get the base directory
    base_dir = Path("venv")
    
    # Define the modules to run in sequence
    modules = [
        {"name": "leads", "path": base_dir / "leads.py"},
        {"name": "email_composer", "path": base_dir / "email_composer.py"},
        {"name": "email_sender", "path": base_dir / "email_sender.py"}
    ]
    
    print("\nSTARTING EMAIL MARKETING WORKFLOW")
    print("=" * 60)
    
    # Run each module in sequence
    for i, module_info in enumerate(modules, 1):
        module_name = module_info["name"]
        module_path = module_info["path"]
        
        print(f"\nStep {i}/{len(modules)}: Running {module_name}...")
        
        # Check if the module file exists
        if not module_path.exists():
            print(f"Error: Module file not found at {module_path}")
            print("Workflow stopped due to missing module.")
            return False
        
        # Load and run the module
        module = load_module(module_name, module_path)
        success = run_module(module, module_name)
        
        if not success:
            print(f"\nError: {module_name} failed to complete successfully.")
            print("Workflow stopped.")
            return False
        
        print(f"Step {i}/{len(modules)} completed successfully.")
    
    print("\n" + "=" * 60)
    print("EMAIL MARKETING WORKFLOW COMPLETED SUCCESSFULLY")
    print("=" * 60)
    return True

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nWorkflow interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        sys.exit(1)