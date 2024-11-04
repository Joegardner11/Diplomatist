import json
import re
import os
from typing import Dict, List

def extract_js_dependencies(path: str) -> Dict[str, str]:
    """Extract dependencies from a JavaScript package.json file."""
    with open(path, 'r') as file:
        data = json.load(file)
        dependencies = data.get("dependencies", {})
        dev_dependencies = data.get("devDependencies", {})
        dependencies.update(dev_dependencies)
    return dependencies

def extract_python_dependencies(path: str) -> List[str]:
    """Extract dependencies from a Python requirements.txt or setup.py file."""
    dependencies = []
    if path.endswith("requirements.txt"):
        with open(path, 'r') as file:
            dependencies = [line.strip() for line in file if line.strip() and not line.startswith('#')]
    elif path.endswith("setup.py"):
        with open(path, 'r') as file:
            content = file.read()
            dependencies = re.findall(r"['\"]([^'\"]+)['\"]", content)
    return dependencies

def extract_php_dependencies(path: str) -> Dict[str, str]:
    """Extract dependencies from a PHP composer.json file."""
    with open(path, 'r') as file:
        data = json.load(file)
        dependencies = data.get("require", {})
        dev_dependencies = data.get("require-dev", {})
        dependencies.update(dev_dependencies)
    return dependencies

def extract_ruby_dependencies(path: str) -> List[str]:
    """Extract dependencies from a Ruby Gemfile."""
    dependencies = []
    with open(path, 'r') as file:
        for line in file:
            match = re.match(r'^\s*gem\s+["\']([^"\']+)["\']', line)
            if match:
                dependencies.append(match.group(1))
    return dependencies

def extract_dependencies(project_dir: str) -> Dict[str, Dict[str, str]]:
    """Detects and extracts dependencies from different languages in a project."""
    result = {}

    for root, dirs, files in os.walk(project_dir):
        for file in files:
            path = os.path.join(root, file)

            # JavaScript dependencies
            if file == "package.json":
                result['JavaScript'] = extract_js_dependencies(path)

            # Python dependencies
            elif file == "requirements.txt" or file == "setup.py":
                result['Python'] = extract_python_dependencies(path)

            # PHP dependencies
            elif file == "composer.json":
                result['PHP'] = extract_php_dependencies(path)

            # Ruby dependencies
            elif file == "Gemfile":
                result['Ruby'] = extract_ruby_dependencies(path)

    return result

if __name__ == "__main__":
    # Define the project directory containing the configuration files
    project_dir = "/path/to/your/project"

    # Extract dependencies from the project directory
    dependencies = extract_dependencies(project_dir)
    
    # Print the extracted dependencies
    for language, deps in dependencies.items():
        print(f"\n{language} Dependencies:")
        if isinstance(deps, dict):
            for name, version in deps.items():
                print(f"  - {name}: {version}")
        else:
            for dep in deps:
                print(f"  - {dep}")
