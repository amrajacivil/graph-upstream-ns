# Graph Database for Upstream Data

This repository provides a setup for extracting, transforming, and loading data into a Neo4j database using Docker Compose. Below is an overview of the main components:

## Folder Structure

- **csvs/**: Contains data files downloaded from FactPages. These CSV files are the raw data source for the project.
- **db/**: Local mount volume used by Docker to persist Neo4j database data. This ensures your data is not lost when containers are stopped or removed.
- **scripts/**: Python modules and scripts to extract data from the CSVs and ingest it into the Neo4j database running in Docker.
- **docker-compose.yml**: Orchestrates the setup, connecting the Neo4j database with the scripts and mounting the necessary volumes. This file makes it easy to spin up the entire stack with a single command.

## Setup Instructions

For detailed steps to set up and run this project locally, please refer to the [`Instructions.md`](./Instructions.md) file in the same directory.
