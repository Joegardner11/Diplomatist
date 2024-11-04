import subprocess
import os
import re
import csv

# Define cross-language invocation APIs for each guest language
CROSS_LANGUAGE_INVOCATION_APIS = {
    "Java-JavaScript": ["HttpURLConnection.openConnection", "getClass().getResource", "Context:evaluateString", "ScriptEngine.eval", "V8:executeScript"],
    "Java-Python": ["PythonInterpreter.execfile", "ProcessBuilder.init", "Runtime:exec"],
    "Java-PHP": ["Runtime:exec", "ProcessBuilder.init", "QuercusEngine:execute"],
    "Java-Ruby": ["ScriptingContainer.runScriptlet", "OSGiScriptingContainer.runScriptlet", "Runtime:exec"]
}

def convert_jar_to_jimple(jar_path, output_dir):
    """
    Convert a JAR file to Jimple files using Soot.
    """
    command = [
        "java", "-cp", "path/to/soot.jar", "soot.Main", "-process-dir", jar_path, "-output-format", "jimple", "-d", output_dir
    ]
    subprocess.run(command, check=True)
    print(f"Converted {jar_path} to Jimple files in {output_dir}")

def find_cross_language_apis(jimple_dir, output_csv="detected_apis.csv"):
    """
    Scan Jimple files to identify cross-language invocation APIs and save results to CSV.
    """
    detected_apis = []
    
    for root, _, files in os.walk(jimple_dir):
        for file in files:
            if file.endswith(".jimple"):
                with open(os.path.join(root, file), 'r') as jimple_file:
                    content = jimple_file.read()
                    
                    # Check for cross-language invocation APIs
                    for api_type, apis in CROSS_LANGUAGE_INVOCATION_APIS.items():
                        for api in apis:
                            if re.search(rf'\b{api}\b', content):
                                detected_apis.append([file, api_type, api])

    # Write detected APIs to CSV
    with open(output_csv, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["File", "API Type", "API"])
        writer.writerows(detected_apis)
    
    print(f"Detected APIs saved to {output_csv}")

def build_feature_database(output_csv="feature_database.csv"):
    """
    Construct a feature database to track versions of guest libraries and save to CSV.
    """
    feature_database = [
        ["JavaScript", "axios", "0.21.1"],
        ["JavaScript", "axios", "0.22.0"],
        ["Python", "numpy", "1.21.0"],
        ["Python", "numpy", "1.22.0"],
        ["PHP", "composer", "2.0.0"],
        ["PHP", "composer", "2.1.0"],
        ["Ruby", "sinatra", "2.1.0"],
        ["Ruby", "sinatra", "2.2.0"]
    ]
    
    # Write feature database to CSV
    with open(output_csv, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Language", "Library", "Version"])
        writer.writerows(feature_database)
    
    print(f"Feature database saved to {output_csv}")

if __name__ == "__main__":
    # Path to the JAR file and output directory for Jimple files
    jar_path = "path/to/your.jar"
    jimple_output_dir = "path/to/output/jimple"
    
    # Step 1: Convert JAR to Jimple
    convert_jar_to_jimple(jar_path, jimple_output_dir)
    
    # Step 2: Identify Cross-Language Invocation APIs and save to CSV
    find_cross_language_apis(jimple_output_dir, output_csv="detected_apis.csv")
    
    # Step 3: Build Feature Database and save to CSV
    build_feature_database(output_csv="feature_database.csv")
    
    print("Process complete. Check the generated CSV files for results.")
