
"""
Neo4j Database Initialization Script
-----------------------------------
This script loads data from CSV files and populates a Neo4j database with nodes and relationships.
It is modular, readable, and follows professional Python coding standards.
"""

import os
import traceback
from typing import List, Dict, Any

import pandas as pd
from tqdm import tqdm
from neo4j_driver import Neo4jHandler


def get_env_variable(var_name: str, default: str) -> str:
    """Get environment variable or default."""
    return os.environ.get(var_name, default)


def fill_and_strip(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing values and strip whitespace from string columns."""
    df = df.fillna({
        col: ("missing" if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]) else -999)
        for col in df.columns
    })
    str_columns = df.select_dtypes(include=["object", "string"]).columns
    df[str_columns] = df[str_columns].apply(lambda x: x.str.strip())
    return df


def remove_all_occurrences(lst: List[Any], value: Any) -> List[Any]:
    """Remove all occurrences of value from the list lst."""
    return [x for x in lst if x != value]


def get_properties(df: pd.DataFrame, items: List[str], key_col: str, property_columns: List[str]) -> List[Dict[str, Any]]:
    """Extract properties for each item in items from df."""
    output = []
    for item in tqdm(items, desc=f"Processing {key_col}"):
        record = df.loc[df[key_col] == item].to_dict('records')
        if record:
            selected = {k: record[0][k] for k in property_columns if k in record[0]}
            selected["name"] = item
            output.append(selected)
    return output


def get_well_properties(df: pd.DataFrame, wells: List[str], property_columns: List[str]) -> List[Dict[str, Any]]:
    """Extract well properties for each well name."""
    output = []
    for item in tqdm(wells, desc="Processing Wells"):
        records = df.loc[(df['wlbWellboreName'] == item) & (df['wlbWell'] == item)].to_dict('records')
        if records:
            selected = {k: records[0][k] for k in property_columns if k in records[0]}
            selected["name"] = item
            output.append(selected)
    return output


def get_wellbore_properties(df: pd.DataFrame, wellbores: List[str], property_columns: List[str]) -> List[Dict[str, Any]]:
    """Extract wellbore properties for each wellbore name."""
    output = []
    for item in tqdm(wellbores, desc="Processing Wellbores"):
        record = df.loc[df['wlbWellboreName'] == item].to_dict('records')
        if record:
            selected = {k: record[0][k] for k in property_columns if k in record[0]}
            selected["name"] = item
            output.append(selected)
    return output


def main() -> None:
    """Main entry point for Neo4j database initialization."""
    # Environment
    URI = get_env_variable("NEO4J_URI", "bolt://neo4j:7687")
    USER = get_env_variable("NEO4J_USER", "neo4j")
    PASSWORD = get_env_variable("NEO4J_PASSWORD", "password")
    CSV_DIR = get_env_variable("CSV_DIR", "/data/csvs")
    #CSV_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'csvs'))

    # License
    license_path = os.path.join(CSV_DIR, "licence.csv")
    license_df = pd.read_csv(license_path)
    license_properties_columns = [
        'prlLicensingActivityName', 'prlMainArea', 'prlStatus', 'prlDateGranted', 'prlDateValidTo',
        'prlOriginalArea', 'prlCurrentArea', 'prlPhaseCurrent', 'prlFactPageUrl'
    ]
    license_df = fill_and_strip(license_df)
    licenses = remove_all_occurrences(list(license_df['prlName'].unique()), "missing")
    licenses_with_properties = get_properties(license_df, licenses, 'prlName', license_properties_columns)

    # Field
    field_path = os.path.join(CSV_DIR, "field.csv")
    field_df = pd.read_csv(field_path)
    field_useful_columns = [
        'fldName', 'cmpLongName', 'fldCurrentActivitySatus', 'wlbName', 'wlbCompletionDate',
        'fldMainArea', 'fldOwnerKind', 'fldMainSupplyBase', 'fldHcType'
    ]
    field_rename_mapping = {
        "fldCurrentActivitySatus": "fldCurrentActivityStatus",
        "wlbName": "Discovery_Wellbore"
    }
    field_properties_columns = [
        'cmpLongName', 'fldCurrentActivityStatus', 'Discovery_Wellbore', 'wlbCompletionDate',
        'fldMainArea', 'fldOwnerKind', 'fldMainSupplyBase', 'fldHcType'
    ]
    field_df = field_df[field_useful_columns].rename(columns=field_rename_mapping)
    field_df = fill_and_strip(field_df)
    fields = remove_all_occurrences(list(field_df['fldName'].unique()), "missing")
    fields_with_properties = get_properties(field_df, fields, 'fldName', field_properties_columns)
    supply_bases = remove_all_occurrences(list(field_df['fldMainSupplyBase'].unique()), "missing")

    # Well
    well_path = os.path.join(CSV_DIR, "wellbore_all_long.csv")
    wlb_df = pd.read_csv(well_path)
    useful_columns = [
        'wlbWellboreName', 'wlbWellType', 'wlbWell', 'wlbDrillingOperator', 'wlbPurpose', 'wlbStatus',
        'wlbContent', 'wlbSubSea', 'wlbField', 'wlbMaxInclation', 'wlbKellyBushElevation',
        'wlbFinalVerticalDepth', 'wlbTotalDepth', 'wlbProductionLicence', 'wlbWaterDepth',
        'wlbKickOffPoint', 'wlbMultilateral', 'wlbDiskosWellboreType', 'wlbFactPageUrl',
        'wlbNsDecDeg', 'wlbEwDecDeg', 'wlbDrillingDays', 'wlbEntryYear', 'wlbCompletionYear',
        'wlbMainArea', 'wlbDrillingFacility', 'wlbAgeAtTd', 'wlbDiscovery', 'wlbFacilityTypeDrilling'
    ]
    properties_columns = [
        'wlbPurpose', 'wlbStatus', 'wlbContent', 'latitude', 'longitude', 'wlbSubSea',
        'wlbMaxInclination', 'wlbKellyBushElevation', 'wlbFinalVerticalDepth', 'wlbTotalDepth',
        'wlbWaterDepth', 'wlbKickOffPoint', 'wlbMultilateral', 'wlbDiskosWellboreType',
        'wlbFactPageUrl', 'wlbDrillingDays', 'wlbEntryYear', 'wlbCompletionYear', 'wlbAgeAtTd',
        'wlbFacilityTypeDrilling'
    ]
    well_rename_mapping = {
        "wlbNsDecDeg": "latitude",
        "wlbEwDecDeg": "longitude"
    }
    well_df = wlb_df[useful_columns].rename(columns=well_rename_mapping)
    well_df = fill_and_strip(well_df)
    areas = remove_all_occurrences(list(well_df['wlbMainArea'].unique()), "missing")
    field_names = remove_all_occurrences(list(well_df['wlbField'].unique()), "missing")
    discoveries = remove_all_occurrences(list(well_df['wlbDiscovery'].unique()), "missing")
    wells = remove_all_occurrences(list(well_df['wlbWell'].unique()), "missing")
    wells_with_properties = get_well_properties(well_df, wells, properties_columns)
    wellbores = remove_all_occurrences(list(well_df['wlbWellboreName'].unique()), "missing")
    wellbore_with_properties = get_wellbore_properties(well_df, wellbores, properties_columns)
    drilling_facilities = remove_all_occurrences(list(well_df['wlbDrillingFacility'].unique()), "missing")
    facilities = remove_all_occurrences(list(well_df['wlbFacilityTypeDrilling'].unique()), "missing")
    operators = remove_all_occurrences(list(well_df['wlbDrillingOperator'].unique()), "missing")

    print("Data loaded and cleaned successfully.")

    handler = Neo4jHandler(URI, USER, PASSWORD)
    try:
        handler.create_nodes("Area", areas, property_key="name")
        handler.create_nodes_with_props("License", licenses_with_properties)
        handler.create_nodes("Discovery", discoveries, property_key="name")
        handler.create_nodes_with_props("Well", wells_with_properties)
        handler.create_nodes_with_props("Wellbore", wellbore_with_properties)
        handler.create_nodes("DrillingFacility", drilling_facilities, property_key="name")
        handler.create_nodes("FacilityType", facilities, property_key="name")
        handler.create_nodes("Operator", operators, property_key="name")
        handler.create_nodes_with_props("Field", fields_with_properties)
        handler.create_nodes("Base", supply_bases, property_key="name")

        # Relationships for wells
        missed_indexes = []
        for index, row in tqdm(well_df.iterrows(), total=well_df.shape[0], desc="Creating Well Relationships"):
            try:
                if row["wlbMainArea"] != 'missing' and row['wlbProductionLicence'] != 'missing':
                    handler.create_edge("Area", "name", row["wlbMainArea"], "License", "name", row['wlbProductionLicence'], "HAS_LICENSE")
                if row["wlbProductionLicence"] != 'missing' and row["wlbField"] != "missing":
                    handler.create_edge("License", "name", row["wlbProductionLicence"], "Field", "name", row['wlbField'], "HAS_FIELD")
                if row["wlbField"] != 'missing' and row["wlbDiscovery"] != "missing":
                    handler.create_edge("Field", "name", row["wlbField"], "Discovery", "name", row['wlbDiscovery'], "HAS_DISCOVERY")
                if row["wlbDiscovery"] != "missing":
                    handler.create_edge("Field", "name", row["wlbField"], "Discovery", "name", row['wlbDiscovery'], "HAS_DISCOVERY")
                if row['wlbWell'] != row['wlbWellboreName']:
                    handler.create_edge("Well", "name", row["wlbWell"], "Wellbore", "name", row['wlbWellboreName'], "HAS_WELLBORE")
                if row['wlbDrillingFacility'] != "missing":
                    handler.create_edge("DrillingFacility", "name", row["wlbDrillingFacility"], "Wellbore", "name", row['wlbWellboreName'], "HAS_WELL")
                if row['wlbFacilityTypeDrilling'] != "missing":
                    handler.create_edge("Wellbore", "name", row["wlbWellboreName"], "FacilityType", "name", row['wlbFacilityTypeDrilling'], "HAS_FACILITY_TYPE_OF")
                if row['wlbDrillingOperator'] != "missing":
                    handler.create_edge("Wellbore", "name", row["wlbWellboreName"], "Operator", "name", row['wlbDrillingOperator'], "WAS_OPERATED_BY")
            except Exception as err:
                traceback.print_exc()
                print(f"Error occurred in index {index}: {err}")
                missed_indexes.append(index)

        # Relationships for fields
        for index, row in tqdm(field_df.iterrows(), total=field_df.shape[0], desc="Creating Field Relationships"):
            try:
                if row["cmpLongName"] != 'missing':
                    handler.create_edge("Operator", "name", row["cmpLongName"], "Field", "name", row['fldName'], "MANAGES")
                if row["fldMainSupplyBase"] != "missing":
                    handler.create_edge("Field", "name", row["fldName"], "Base", "name", row['fldMainSupplyBase'], "HAS_SUPPLYBASE_OF")
            except Exception as err:
                traceback.print_exc()
                print(f"Error occurred in field index {index}: {err}")
    finally:
        handler.close()


if __name__ == "__main__":
    main()
