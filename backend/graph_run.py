import os
import time
import json
import logging
import glob
from graph_db import GraphDBClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/output")
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")

FIELD_PRIORITIES = {
    "Country": ["country", "state_province", "resource_tonnage", "mine_output"],
    "Mineral": ["mineral", "chemical_formula", "global_reserves", "applications"],
    "Company": ["company_name", "parent_company", "operator", "production_capacity"],
    "Facility": ["facility_name", "facility_type", "processing_capability", "location"],
    "Deposit": ["deposit_name", "deposit_type", "grade_value", "resource_status"]
}

def process_extractions(jsonl_path, db: GraphDBClient):
    """Read a JSONL file, merge node records locally, then flush to Neo4j."""
    logger.info(f"Processing {jsonl_path}...")
    
    nodes_cache = {}
    relationships_cache = []
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        # Load all facts from the JSONL output file
        facts = []
        for line in f:
            if line.strip():
                try:
                    facts.append(json.loads(line))
                except Exception as e:
                    logger.error(f"Failed to parse line: {e}")
    
    logger.info(f"Loaded {len(facts)} facts from {jsonl_path}")
                    
    # Sort strictly by confidence descending based on User Rules
    sorted_facts = sorted(
        facts,
        key=lambda x: (x.get("confidence", 0) if isinstance(x.get("confidence"), (int, float)) else 0.0),
        reverse=True
    )
    
    facts_processed = 0
    for fact in sorted_facts:
        try:
            # Flattened dict layout from pipeline.py looks like:
            # {"fact_type": "...", "subject_name": "...", "subject_type": "...", "relation": "...", "object_name": "...", "attr_country": "..."}
            
            # 1. Subject Node
            subj_name = fact.get("subject_name")
            subj_type = fact.get("subject_type")
            
            if subj_name and subj_type:
                s_id = f"{subj_type}_{db.generate_id(subj_name)}"
                if s_id not in nodes_cache:
                    nodes_cache[s_id] = db.get_or_create_node(subj_type, subj_name)
            else:
                logger.warning(f"Fact missing subject: {fact.get('fact_id')}")
                continue
                    
            # 2. Object Node
            obj_name = fact.get("object_name")
            obj_type = fact.get("object_type")
            
            if obj_name and obj_type:
                o_id = f"{obj_type}_{db.generate_id(obj_name)}"
                if o_id not in nodes_cache:
                    nodes_cache[o_id] = db.get_or_create_node(obj_type, obj_name)
            else:
                logger.warning(f"Fact missing object: {fact.get('fact_id')}")
                continue
            
            facts_processed += 1
                    
            # Extract Attributes (flattened with 'attr_' prefix)
            attrs = {k.replace('attr_', ''): v for k, v in fact.items() if k.startswith('attr_')}
            
            # Map Attributes back into the cached Subject and Object
            if subj_name and subj_type:
                db.update_node_attributes(nodes_cache[s_id], attrs)
            if obj_name and obj_type:
                db.update_node_attributes(nodes_cache[o_id], attrs)
                
            # 3. Create the Relationship
            if subj_name and subj_type and obj_name and obj_type and fact.get('relation'):
                rel = db.create_relationship(
                    nodes_cache[s_id], 
                    fact['relation'].upper().replace(' ', '_'), 
                    nodes_cache[o_id], 
                    fact
                )
                relationships_cache.append(rel)
                
        except Exception as e:
            logger.error(f"Error mapping row to graph: {e}")

    # FLUSH TO DB
    if nodes_cache:
        logger.info(f"Flushing {len(nodes_cache)} nodes to Neo4j...")
        db.flush_nodes(nodes_cache)
        
    if relationships_cache:
        logger.info(f"Flushing {len(relationships_cache)} relationships to Neo4j...")
        db.flush_relationships(relationships_cache)
        
    logger.info(f"Successfully processed {facts_processed} facts from {jsonl_path}.")
    logger.info(f"Done processing {jsonl_path}.")

def main():
    logger.info("Starting Graph Ingestion Service...")
    
    # Wait for Neo4j to be ready
    db = None
    retries = 30
    while retries > 0:
        try:
            db = GraphDBClient(NEO4J_URI)
            # test query
            with db.driver.session() as s:
                s.run("RETURN 1")
            break
        except Exception as e:
            logger.warning(f"Waiting for Neo4j... ({retries} retries left)")
            time.sleep(5)
            retries -= 1
            
    if not db:
        logger.error("Could not connect to Neo4j. Exiting.")
        return

    logger.info("Connected to Neo4j. Watching output directory...")
    
    processed_files = set()
    
    # Simple continuous polling loop to watch the output folder
    while True:
        try:
            # Tailing flattened CSV records as JSON lines
            files = glob.glob(os.path.join(OUTPUT_DIR, "facts_*.jsonl"))
            
            for f in files:
                if f not in processed_files:
                    # Basic check to wait until pipeline finishes writing
                    if time.time() - os.path.getmtime(f) > 5:
                        process_extractions(f, db)
                        processed_files.add(f)
        except Exception as e:
            logger.error(f"Error in Watcher loop: {e}")
            
        time.sleep(5)

if __name__ == "__main__":
    main()
