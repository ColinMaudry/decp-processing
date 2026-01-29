#!/usr/bin/env python3
import ast
import os
import sys
from typing import List, Set


def get_src_modules(src_path: str) -> Set[str]:
    """Identify top-level modules and packages in the src directory."""
    modules = set()
    if not os.path.exists(src_path):
        return modules

    for item in os.listdir(src_path):
        full_path = os.path.join(src_path, item)
        if os.path.isdir(full_path):
            if (
                "__init__.py" in os.listdir(full_path) or item in ["flows", "tasks"]
            ):  # Include known packages even if __init__ is missing (namespace packages or just folder organization)
                modules.add(item)
        elif item.endswith(".py") and item != "__init__.py":
            modules.add(item[:-3])

    return modules


def check_file(file_path: str, src_modules: Set[str]) -> List[str]:
    """Check a single file for incorrect imports."""
    errors = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=file_path)
    except Exception as e:
        errors.append(f"Failed to parse {file_path}: {e}")
        return errors

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # Check regular imports: import tasks...
                base_module = alias.name.split(".")[0]
                if base_module in src_modules:
                    errors.append(
                        f"Line {node.lineno}: 'import {alias.name}' should be 'import src.{alias.name}'"
                    )

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                # Check from imports: from tasks import ...
                base_module = node.module.split(".")[0]
                if base_module in src_modules:
                    errors.append(
                        f"Line {node.lineno}: 'from {node.module} ...' should be 'from src.{node.module} ...'"
                    )

    return errors


def main():
    src_dir = os.path.join(os.getcwd(), "src")
    if not os.path.exists(src_dir):
        print("Error: src directory not found.")
        sys.exit(1)

    src_modules = get_src_modules(src_dir)
    print(f"Detected local modules in src: {', '.join(sorted(src_modules))}")

    files_to_check = []
    # If args are provided (pre-commit passes files), check those.
    # Otherwise check all files in src
    if len(sys.argv) > 1:
        files_to_check = sys.argv[1:]
    else:
        for root, _, files in os.walk(src_dir):
            for file in files:
                if file.endswith(".py"):
                    files_to_check.append(os.path.join(root, file))

    all_errors = {}
    for file_path in files_to_check:
        # Only check files that are likely part of the project source or tests
        # The pre-commit hook usually passes changed files.
        if not file_path.endswith(".py"):
            continue

        file_errors = check_file(file_path, src_modules)
        if file_errors:
            all_errors[file_path] = file_errors

    if all_errors:
        print("found improper imports:")
        for file_path, errors in all_errors.items():
            print(f"{file_path}:")
            for error in errors:
                print(f"  {error}")
        sys.exit(1)

    print("All imports valid.")
    sys.exit(0)


if __name__ == "__main__":
    main()
