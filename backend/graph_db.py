import logging
import json
import hashlib
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

class GraphDBClient:
    def __init__(self, uri="bolt://neo4j:7687", user="", password=""):
        # We assume NEO4J_AUTH=none in the docker environment based on the compose
        self.driver = GraphDatabase.driver(uri)
        
    def close(self):
        self.driver.close()

    def generate_id(self, name: str) -> str:
        """Create a reproducible ID from a name."""
        return hashlib.md5(name.lower().strip().encode('utf-8')).hexdigest()

    def _merge_lists(self, current: list, new_val) -> list:
        """Helper to merge values into a list removing duplicates."""
        if not isinstance(new_val, list):
            new_val = [new_val]
            
        for v in new_val:
            if v not in current:
                current.append(v)
        return current

    def get_or_create_node(self, node_type: str, name: str) -> dict:
        """Creates or returns a python dict representing a node (to be flushed later)."""
        node = {
            "node_id": f"{node_type}_{self.generate_id(name)}",
            "type": node_type,
            "name": name,
            "attributes": {},
            "source_facts": [],
            "relationships": []
        }
        return node
        
    def update_node_attributes(self, node: dict, new_attributes: dict) -> dict:
        """Intelligently merge new attributes with existing ones locally in the dict."""
        for attr_key, new_value in new_attributes.items():
            # Only skip if explicit None or empty string. 0 or False are valid data points.
            if new_value is None or new_value == "":
                continue
                
            current_values = node["attributes"].get(attr_key, [])
            if not isinstance(current_values, list):
                # Ensure it's always a list locally for easy processing
                current_values = [current_values]
            else:
                # Use a copy to avoid mutating old lists unexpectedly
                current_values = list(current_values)
                
            # Handle different attribute types
            if isinstance(new_value, list):
                # Merge lists, remove duplicates
                current_values = self._merge_lists(current_values, new_value)
            elif isinstance(new_value, (int, float)):
                # Keep highest value for numerical attributes
                num_only = [x for x in current_values if isinstance(x, (int, float))]
                if not num_only or new_value > max(num_only):
                    # We keep the number but keep other strings if they existed? 
                    # Neo4j won't like it. Let's stick to homogeneous scalars if it's supposed to be a number.
                    # If we already have strings, we might want to keep them.
                    current_values.append(new_value)
            else:
                # For strings or other types, accumulate unique values
                if new_value not in current_values:
                    current_values.append(new_value)
            
            node["attributes"][attr_key] = current_values
            
        return node

    def create_relationship(self, source_node: dict, relationship_type: str, target_node: dict, fact_data: dict) -> dict:
        """Create relationships between nodes with fact provenance"""
        # Ensure 'fact_id', 'source_page' or other tracking vars exist safely
        fact_id = fact_data.get("fact_id", str(hash(fact_data.get("evidence_text", ""))))
        
        relationship = {
            "relationship_id": f"{source_node['node_id']}_{relationship_type}_{target_node['node_id']}",
            "type": relationship_type,
            "source": source_node["node_id"],
            "target": target_node["node_id"],
            "attributes": fact_data.get("attributes", {}),
            "evidence": fact_data.get("evidence_text", ""),
            "confidence": fact_data.get("confidence", ""),
            "source_page": fact_data.get("page_no", ""),
            "fact_id": fact_id
        }
        
        # Add to both nodes' relationship lists
        source_node["relationships"].append(relationship)
        target_node["relationships"].append(relationship)
        
        return relationship
        
    # --- CYPHER FLUSH EXECUTION ---

    def flush_nodes(self, nodes: dict):
        """Flushes a dictionary of cached nodes into Neo4j using Cypher queries."""
        with self.driver.session() as session:
            for node_id, node in nodes.items():
                # To dump lists properly as properties in Cypher we can just pass them in as parameters
                props = {
                    "node_id": node["node_id"],
                    "name": node["name"]
                }
                
                # Add attributes
                for k, v in node["attributes"].items():
                    if isinstance(v, list):
                        if len(v) == 0:
                            continue # Skip empty lists
                        if len(v) == 1:
                            props[k] = v[0]
                        else:
                            # Neo4j requires homogeneous lists.
                            types = set(type(x) for x in v)
                            if len(types) > 1:
                                # Mixed types! Fallback to string array
                                props[k] = [str(x) for x in v]
                            else:
                                props[k] = v
                    else:
                        props[k] = v
                        
                # Create the node with a dynamic label (node_type)
                # Using apoc or strict python parameterization
                cypher = f"""
                MERGE (n:`{node['type']}` {{ node_id: $props.node_id }})
                SET n += $props
                """
                try:
                    session.run(cypher, props=props)
                except Exception as e:
                    logger.error(f"Failed to flush node {node_id} of type {node['type']}: {e}")
                    logger.error(f"Payload: {props}")
                    raise e
                
    def flush_relationships(self, relationships: list):
        """Flushes the list of cached relationships to Neo4j."""
        with self.driver.session() as session:
            for rel in relationships:
                # Merge relationship
                cypher = f"""
                MATCH (s {{ node_id: $source_id }})
                MATCH (t {{ node_id: $target_id }})
                MERGE (s)-[r:`{rel['type']}`]->(t)
                SET r.evidence = $evidence, r.confidence = $confidence, r.source_page = $source_page, r.fact_id = $fact_id
                """
                try:
                    session.run(cypher, 
                        source_id=rel["source"],
                        target_id=rel["target"],
                        evidence=rel["evidence"],
                        confidence=rel["confidence"],
                        source_page=rel["source_page"],
                        fact_id=rel["fact_id"]
                    )
                except Exception as e:
                    logger.error(f"Failed to flush relationship {rel['type']}: {e}")
                    logger.error(f"Payload: {rel}")
                    raise e

    def get_node(self, node_type: str, node_name: str) -> dict:
        """Fetch a node, its attributes, and its immediate relationships/facts for querying."""
        with self.driver.session() as session:
            # We use an exact or case-insensitive name match
            cypher = f"""
            MATCH (n:`{node_type.lower()}`)
            WHERE toLower(n.name) = toLower($name)
            
            // Get relationships dynamically
            OPTIONAL MATCH (n)-[r]->(target)
            
            RETURN n, collect({{
                type: type(r),
                target_name: target.name,
                target_type: labels(target)[0],
                confidence: r.confidence,
                evidence: r.evidence,
                source_document: r.source_document,
                year: r.year
            }}) as relationships
            LIMIT 1
            """
            result = session.run(cypher, name=node_name.lower())
            record = result.single()
            
            if not record or not record.get("n"):
                return None
                
            n = record["n"]
            # Neo4j python driver nodes can be cast to dicts
            attributes = dict(n)
            
            # Extract standard ID/Name
            node_id = attributes.pop("node_id", None)
            name = attributes.pop("name", node_name)
            
            # Clean up relationships list
            rels = record["relationships"] if record["relationships"] else []
            clean_rels = [r for r in rels if r.get('type') is not None]
            
            return {
                "node_id": node_id,
                "name": name,
                "type": node_type.lower(),
                "attributes": attributes,
                "relationships": clean_rels
            }
