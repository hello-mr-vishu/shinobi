import os

# Define folders and file types to ignore
EXCLUDE_DIRS = {'.git', '.github', '.vscode', '__pycache__', '.venv', 'env', 'venv', '.idea', 'build', 'dist'}
EXCLUDE_FILES = {'.gitignore', 'README.md', 'requirements.txt'}

def list_project_structure(start_path, indent=0):
    items = sorted(os.listdir(start_path))
    for item in items:
        item_path = os.path.join(start_path, item)

        # Skip excluded dirs and files
        if item in EXCLUDE_DIRS:
            continue
        if os.path.isfile(item_path) and item in EXCLUDE_FILES:
            continue

        print('    ' * indent + '├── ' + item)
        if os.path.isdir(item_path):
            list_project_structure(item_path, indent + 1)

# Start from the root of your project
project_root = os.getcwd()  # or set manually: "/path/to/your/project"
print(f"📂 Project structure of: {project_root}\n")
list_project_structure(project_root)
