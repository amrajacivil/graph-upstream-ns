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

1. Short supply-chain paths between Areas and Operators

```
// find example shortest connections between any Area and any Operator (path length limited to 4 for clarity)
MATCH (a:Area), (op:Operator)
WHERE a.name IS NOT NULL AND op.name IS NOT NULL
MATCH p = shortestPath((a)-[*..4]-(op))
RETURN p
LIMIT 25;

```

2. Recent producing wells with high kickoff depth (Area → … → Wellbore view)

```
MATCH path = (area:Area)-[:HAS_LICENSE]->(:License)-[:HAS_FIELD]->(:Field)-[:HAS_DISCOVERY]->(:Discovery)
             -[:HAS_WELL]->(w:Well)-[:HAS_WELLBORE]->(wb:Wellbore)
WHERE wb.wlbKickOffPoint IS NOT NULL
  AND toFloat(wb.wlbKickOffPoint) > 2500.0
  AND wb.wlbEntryYear IS NOT NULL
  AND toInteger(wb.wlbEntryYear) >= 2025
  AND (wb.wlbStatus = 'PRODUCING' OR w.wlbStatus = 'Producing')
OPTIONAL MATCH (wb)-[:WAS_OPERATED_BY]->(op:Operator)
OPTIONAL MATCH (wb)-[:HAS_DRILLING_FACILITY]->(df:DrillingFacility)
RETURN DISTINCT path, op, df
LIMIT 25;

```

Feel free to play around with your own queries!
