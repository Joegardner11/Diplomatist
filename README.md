## Run Diplomatist
We open-sourced part of the code during the Double-Blind Peer Review.

- Step 1

Download the Jar and install the requirements.

```python
pip install requirements.txt
```

- Step 2

Create the database table and add the analyzed project info.

- Step 3

Guest language configuration files parsing.

```python
# extract_dependency_configuration.py
if __name__ == "__main__":
    project_dir = "/path/to/your/project"
    
    # Define the output CSV file path
    output_file = "dependencies.csv"

    # Extract dependencies from configuration files from the project directory
    dependencies = extract_dependencies(project_dir)
```

- Step 4

Cross-language invocation APIs analysis.

```python
# extract_dependency_APIs.py
if __name__ == "__main__":
    jar_path = "path/to/your.jar"
    jimple_output_dir = "path/to/output/jimple"

    #  Build Feature Database
    build_feature_database()
    
    # Convert JAR to Jimple
    convert_jar_to_jimple(jar_path, jimple_output_dir)
    
    # Identify Cross-Language Invocation APIs
    find_cross_language_apis(jimple_output_dir)
```


