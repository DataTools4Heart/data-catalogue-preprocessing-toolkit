import json
import pandas as pd
import argparse
import sys
from pathlib import Path

def print_usage_examples():
    """Display examples of how to use the script"""
    examples = """
\033[1mExamples:\033[0m
  Basic conversion:
  python3 from_datasetMetadata_to_obibaFeaturesDict.py metadataDataset.json obibaDict.xlsx "my_table_name"

  Help message:
  python3 from_datasetMetadata_to_obibaFeaturesDict.py -h
"""
    print(examples)

def determine_value_type(feature):
    """Determine the valueType with proper error handling"""
    try:
        feature_name = feature["name"]
        data_type = feature.get("dataType", "").upper()
        if not data_type:
            raise ValueError(f"Missing dataType for feature {feature_name}")
            
        if data_type == "NOMINAL":
            if "valueSet" not in feature:
                raise ValueError(f"NOMINAL type requires valueSet for feature {feature_name}")
                
            concepts = feature["valueSet"].get("concept", [])
            if not concepts:
                raise ValueError(f"NOMINAL type requires case in valueSet for feature {feature_name}")
                
            first_code = concepts[0].get("code", "")
            if isinstance(first_code, str):
                return "text"
            elif isinstance(first_code, (int, float)):
                return "decimal"
            else:
                raise ValueError(f"Unsupported code type {type(first_code)} for feature {feature_name}")
                
        type_mapping = {
            "NUMERIC": "decimal",
            "BOOLEAN": "boolean",
            "DATETIME": "datetime"
        }
        
        value_type = type_mapping.get(data_type)
        if value_type is None:
            raise ValueError(f"Unknown dataType: {data_type} for feature {feature_name}")
        
        return value_type
        
    except Exception as e:
        print(f"Error determining valueType: {e}", file=sys.stderr)
        return "error"

def get_entity_type(entries):
    """Determine entityType from encounters_encounterClass feature"""
    try:
        # Find the encounters_encounterClass feature
        encounters_feature = next((f for f in entries.get('features', []) if f.get('name') == 'encounters_encounterClass'), None)

        if not encounters_feature:
            return "participant"

        # Get target code from datasetStats
        target_code = entries.get('datasetStats', {}).get('featureStats', {}).get('encounters_encounterClass', {}).get('valueSet', [None])[0]

        if not target_code:
            raise ValueError(f"Cannot find target code at datasetStats.featureStats.encounters_encounterClass.valueSet")

        # Find matching concept
        concepts = encounters_feature.get('valueSet', {}).get('concept', [])
        matching_concept = next((c for c in concepts if c.get('code') == target_code), None)

        if not matching_concept:
            raise ValueError(f"target code \"{target_code}\" not found in the valueSet for feature \"encounters_encounterClass\"")
        return matching_concept.get('display') if matching_concept else "participant"

    except Exception as e:
        print(f"Error determining entityType: {str(e)}", file=sys.stderr)
        return "participant"


def extract_variables(entries, table_name, entity_type):
    """Extract variables data with validation"""
    variables = []
    try:
        for feature in entries.get("features", []) + entries.get("outcomes", []):
            try:
                name = feature["name"]
                value_type = determine_value_type(feature)
                label_en = f"{feature.get('description', '')} {' '.join(feature.get('generatedDescription', []))}".strip()
                script = f"$('{name}')"
                
                variables.append([
                    table_name,
                    name,
                    value_type,
                    entity_type,
                    "",  # unit
                    label_en,
                    script
                ])
                
            except KeyError as e:
                print(f"Missing required key {e} in feature: {feature}", file=sys.stderr)
                
    except AttributeError:
        raise ValueError("Invalid entries structure")
        
    return variables

def extract_categories(entries, table_name):
    """Extract data for the Categories sheet."""
    categories = []
    try:
        for feature in entries.get("features", []):
            if feature.get("dataType", "").upper() == "NOMINAL" and "valueSet" in feature:
                for concept in feature["valueSet"].get("concept", []):
                    try:
                        categories.append([
                            table_name,
                            feature["name"],
                            concept.get("code", ""),
                            "",  # code column remains empty
                            0,   # missing column always 0
                            concept.get("display", "")
                        ])
                    except KeyError as e:
                        print(f"Missing concept key {e} in feature: {feature.get('name')}", file=sys.stderr)
    except AttributeError:
        raise ValueError("Invalid entries structure")

    return categories


def validate_json_structure(data):
    """Validate the JSON structure meets expectations"""
    if "entries" not in data:
        raise ValueError("JSON missing top-level 'entries' key")
        
    if not isinstance(data["entries"], list) or len(data["entries"]) == 0:
        raise ValueError("'entries' must be a non-empty list")
        
    first_entry = data["entries"][0]
    if not isinstance(first_entry, dict):
        raise ValueError("Entry must be a dictionary")
        
    required_sections = ["features", "outcomes"]
    for section in required_sections:
        if section not in first_entry:
            raise ValueError(f"Missing required section '{section}' in entry")

def main(json_file, xlsx_file, table_name):
    """Main conversion function with error handling"""
    try:
        # Validate input file
        input_path = Path(json_file)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {json_file}")
            
        # Read and parse JSON
        try:
            with open(input_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON format in input file")
            
        # Validate JSON structure
        validate_json_structure(data)
        entries = data["entries"][0]

        # Get entity type once for all variables
        entity_type = get_entity_type(entries)
        
        # Process data 
        variables = extract_variables(entries, table_name, entity_type)
        categories = extract_categories(entries, table_name)
        
        # Create DataFrames
        df_variables = pd.DataFrame(
            variables,
            columns=["table", "name", "valueType", "entityType", "unit", "label:en", "script"]
        )
        df_categories = pd.DataFrame(
            categories,
            columns=["table", "variable", "name", "code", "missing", "label:en"]
        )

        # Ensure output directory exists
        output_path = Path(xlsx_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to Excel
        try:
            with pd.ExcelWriter(xlsx_file) as writer:
                df_variables.to_excel(writer, sheet_name="Variables", index=False)
                df_categories.to_excel(writer, sheet_name="Categories", index=False)
        except PermissionError:
            raise RuntimeError(f"Permission denied for output file: {xlsx_file}")

        print(f"\n\033[32mSuccess:\033[0m Excel file '{xlsx_file}' created.\nPlease, check for parsing errors by finding the following keywords in the conflicting fields: \"error\" and \"unknown\"\n")

    except Exception as e:
        print(f"\n\033[31mError:\033[0m {e}", file=sys.stderr)
        print("\n\033[1mUsage instructions:\033[0m")
        parser.print_help()
        print_usage_examples()
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="Convert DT4H Dataset Metadata objects (JSON) into Obiba-compliant Variable dictionaries (Excel) for Opal"
    )
    parser.add_argument(
        "json_file",
        help="Path to input DT4H Dataset Metadata JSON file (dt4h_dataset_metadata format)"
    )
    parser.add_argument(
        "xlsx_file",
        help="Path to output XLSX Excel file (obiba_dictionary format)"
    )
    parser.add_argument(
        "table_name",
        help="Name of the Opal table to create"
    )
    
    # Handle help with examples
    if len(sys.argv) == 1:
        parser.print_help()
        print_usage_examples()
        sys.exit(0)
        
    args = parser.parse_args()
    main(args.json_file, args.xlsx_file, args.table_name)

