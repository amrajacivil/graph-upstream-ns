
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
MISSING_VALUE = "" #MISSING_VALUE = "missing", Initially I removed missing values, but later I decided to keep them for relationship creation.

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

def unique_non_missing(series):
    return series[series != MISSING_VALUE].unique().tolist()

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
    # Filter only rows where wlbWell is in wells
    filtered = df[df['wlbWell'].isin(wells)]
    # Drop duplicates to get one row per well (keep first occurrence)
    deduped = filtered.drop_duplicates(subset=['wlbWell'])
    # Build the list of dicts
    output = []
    for _, row in tqdm(deduped.iterrows(), total=deduped.shape[0], desc="Processing Wells"):
        selected = {k: row[k] for k in property_columns if k in row}
        selected["name"] = row['wlbWell']
        output.append(selected)
    return output


def get_wellbore_properties(df: pd.DataFrame, wellbores: List[str], property_columns: List[str]) -> List[Dict[str, Any]]:
    """Extract wellbore properties for each wellbore name."""
    # Filter rows where wlbWellboreName is in the wellbores list
    filtered_df = df[df['wlbWellboreName'].isin(wellbores)]
    # Drop duplicates to ensure one row per wellbore
    deduped_df = filtered_df.drop_duplicates(subset=['wlbWellboreName'])
    # Select only the required columns
    selected_df = deduped_df[['wlbWellboreName'] + property_columns]
    # Rename 'wlbWellboreName' to 'name' for the output
    selected_df = selected_df.rename(columns={'wlbWellboreName': 'name'})
    # Convert the dataframe to a list of dictionaries
    output = selected_df.to_dict('records')
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
    licenses = unique_non_missing(license_df['prlName'])
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
    fields = unique_non_missing(field_df['fldName'])
    fields_with_properties = get_properties(field_df, fields, 'fldName', field_properties_columns)
    supply_bases = unique_non_missing(field_df['fldMainSupplyBase'])

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
        "wlbEwDecDeg": "longitude",
        "wlbMaxInclation":"wlbMaxInclination"
    }
    well_df = wlb_df[useful_columns].rename(columns=well_rename_mapping)
    well_df = fill_and_strip(well_df)
    

    areas = unique_non_missing(well_df['wlbMainArea'])
    discoveries = unique_non_missing(well_df['wlbDiscovery'])
    wells = unique_non_missing(well_df['wlbWell'])
    wells_with_properties = get_well_properties(well_df, wells, properties_columns)
    wellbores = unique_non_missing(well_df['wlbWellboreName'])
    wellbore_with_properties = get_wellbore_properties(well_df, wellbores, properties_columns)
    drilling_facilities = unique_non_missing(well_df['wlbDrillingFacility'])
    facilities = unique_non_missing(well_df['wlbFacilityTypeDrilling'])
    operators = unique_non_missing(well_df['wlbDrillingOperator'])

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

        well_relationships = []
        for _, row in tqdm(well_df.iterrows(), total=well_df.shape[0], desc="Preparing Well Relationships"):
            well_relationships.append({
                "wlbMainArea": row["wlbMainArea"],
                "wlbProductionLicence": row["wlbProductionLicence"],
                "wlbField": row["wlbField"],
                "wlbDiscovery": row["wlbDiscovery"],
                "wlbWell": row["wlbWell"],
                "wlbWellboreName": row["wlbWellboreName"],
                "wlbDrillingFacility": row["wlbDrillingFacility"],
                "wlbFacilityTypeDrilling": row["wlbFacilityTypeDrilling"],
                "wlbDrillingOperator": row["wlbDrillingOperator"]
            })

        cypher = '''
        UNWIND $rows AS row
        MERGE (a:Area {name: row.wlbMainArea})
        MERGE (l:License {name: row.wlbProductionLicence})
        MERGE (f:Field {name: row.wlbField})
        MERGE (d:Discovery {name: row.wlbDiscovery})
        MERGE (w:Well {name: row.wlbWell})
        MERGE (wb:Wellbore {name: row.wlbWellboreName})
        MERGE (df:DrillingFacility {name: row.wlbDrillingFacility})
        MERGE (ft:FacilityType {name: row.wlbFacilityTypeDrilling})
        MERGE (op:Operator {name: row.wlbDrillingOperator})
        MERGE (a)-[:HAS_LICENSE]->(l)
        MERGE (l)-[:HAS_FIELD]->(f)
        MERGE (f)-[:HAS_DISCOVERY]->(d)
        MERGE (d)-[:HAS_WELL]->(w)
        MERGE (w)-[:HAS_WELLBORE]->(wb)
        MERGE (wb)-[:HAS_DRILLING_FACILITY]->(df)
        MERGE (wb)-[:HAS_FACILITY_TYPE_OF]->(ft)
        MERGE (wb)-[:WAS_OPERATED_BY]->(op)
        '''
        try:
            with handler.driver.session() as session:
                session.run(cypher, rows=well_relationships)
            print(f"Batch created {len(well_relationships)} well relationships.")
        except Exception as err:
            traceback.print_exc()
            print(f"Error occurred during batch well relationship creation: {err}")

        # Relationships for fields
        for index, row in tqdm(field_df.iterrows(), total=field_df.shape[0], desc="Creating Field Relationships"):
            try:
                handler.create_edge("Operator", "name", row["cmpLongName"], "Field", "name", row['fldName'], "MANAGES")
                handler.create_edge("Field", "name", row["fldName"], "Base", "name", row['fldMainSupplyBase'], "HAS_SUPPLYBASE_OF")
            except Exception as err:
                traceback.print_exc()
                print(f"Error occurred in field index {index}: {err}")
    finally:
        handler.close()


if __name__ == "__main__":
    main()
