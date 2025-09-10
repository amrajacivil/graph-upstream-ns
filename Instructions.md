# Instructions

## Data Source

The CSV files in the `csvs` folder were downloaded from [Sodir Factpages Wellbore AllLong](https://factpages.sodir.no/en/wellbore/TableView/AllLong) on **9th September 2025**. If you need the latest data, you can download it from the same link and replace the files in the `csvs` directory.

**Note:**

- Do **not** change anything in the `db` or `scripts` folders unless you know exactly what you are updating :)

## Running the Project

Open a terminal (bash/zsh) in this folder and run:

```
docker compose up
```

This will start the services and show logs in the terminal.

To run in the background (detached mode):

```
docker compose up --detach
```

**First time setup:**

- Please wait **3 minutes** after starting. This allows time for nodes and relationships to be created in the database.
- On subsequent runs, the services should start almost immediately.

## Accessing Neo4j

Once the services are running, you have two options:

1. **Local Browser:**

   - Open [http://localhost:7474/browser/](http://localhost:7474/browser/) in your browser.
   - Login with:
     - **Username:** `neo4j`
     - **Password:** `password`
   - You can run Cypher queries directly here.

2. **Neo4j Desktop (Advanced):**
   - For advanced querying and Bloom features, use [Neo4j Desktop](https://neo4j.com/docs/desktop/current/operations/connections/) if you have it installed.

## Example Cypher Query

You can try the following query to find "Extreme Subsea wells":

```
WITH "Equinor Energy AS" as operator_name, 2020 as cutOffYear, 3500 as kickOffThreshold
MATCH (op:Operator {name: operator_name })
MATCH (wb:Wellbore)-[r1:WAS_OPERATED_BY]->(op)
WHERE wb.wlbEntryYear >= cutOffYear
	AND wb.wlbKickOffPoint > kickOffThreshold
	AND wb.wlbSubSea = "YES"
OPTIONAL MATCH (df:DrillingFacility)-[r2:HAS_WELL]->(wb)
OPTIONAL MATCH (wb)-[r3:HAS_FACILITY_TYPE_OF]->(ft:FacilityType)
RETURN wb, r1, r2, r3, df;
```

Feel free to play around with your own queries!
