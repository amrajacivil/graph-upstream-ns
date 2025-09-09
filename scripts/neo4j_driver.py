from neo4j import GraphDatabase

class Neo4jHandler:
    """
    Handles Neo4j database operations such as creating nodes and relationships.
    """

    def __init__(self, uri: str, user: str, password: str) -> None:
        """
        Initialize the Neo4jHandler with connection parameters.

        Args:
            uri (str): The Neo4j URI.
            user (str): The username for authentication.
            password (str): The password for authentication.
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            with self.driver.session() as session:
                session.run("RETURN 1")
            print("Connection to Neo4j established.")
        except Exception as exc:
            print(f"Failed to connect to Neo4j: {exc}")
            raise

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        self.driver.close()

    def create_nodes(self, label: str, values: list, property_key: str = "id") -> None:
        """
        Create nodes with a given label and property.

        Args:
            label (str): Node label.
            values (list): List of property values.
            property_key (str): Property key for the node.
        """
        query = (
            f"UNWIND $values AS val "
            f"MERGE (n:{label} {{ {property_key}: val }}) "
            "RETURN count(n) AS created"
        )
        with self.driver.session() as session:
            result = session.run(query, values=values)
            created_nodes = result.single()["created"]
            print(f"Created {created_nodes} nodes for label '{label}'.")

    def create_nodes_with_props(self, label: str, list_of_props: list) -> None:
        """
        Create nodes with properties.

        Args:
            label (str): Node label.
            list_of_props (list): List of property dictionaries.
        """
        query = (
            f"UNWIND $batch AS props "
            f"MERGE (n:{label} {{ name: props.name }}) "
            "SET n += props "
            "RETURN count(n) AS created"
        )
        with self.driver.session() as session:
            result = session.run(query, batch=list_of_props)
            created_nodes = result.single()["created"]
            print(f"Created {created_nodes} nodes with properties for label '{label}'.")

    def create_edge(
        self,
        label1: str,
        key1: str,
        value1,
        label2: str,
        key2: str,
        value2,
        rel_type: str,
        rel_props: dict = None
    ) -> str:
        """
        Create a relationship between two nodes.

        Args:
            label1 (str): Label of the first node.
            key1 (str): Property key of the first node.
            value1: Property value of the first node.
            label2 (str): Label of the second node.
            key2 (str): Property key of the second node.
            value2: Property value of the second node.
            rel_type (str): Relationship type.
            rel_props (dict, optional): Relationship properties.

        Returns:
            str: The type of relationship created.
        """
        if rel_props:
            query = (
                f"MERGE (a:{label1} {{ {key1}: $val1 }}) "
                f"MERGE (b:{label2} {{ {key2}: $val2 }}) "
                f"MERGE (a)-[r:{rel_type}]->(b) "
                "SET r += $props "
                "RETURN type(r) AS relationship"
            )
            params = {"val1": value1, "val2": value2, "props": rel_props}
        else:
            query = (
                f"MERGE (a:{label1} {{ {key1}: $val1 }}) "
                f"MERGE (b:{label2} {{ {key2}: $val2 }}) "
                f"MERGE (a)-[r:{rel_type}]->(b) "
                "RETURN type(r) AS relationship"
            )
            params = {"val1": value1, "val2": value2}

        with self.driver.session() as session:
            result = session.run(query, **params)
            record = result.single()
            return record["relationship"] if record else None