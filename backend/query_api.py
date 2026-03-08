import os
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

from llm_client import call_ollama
from graph_db import GraphDBClient

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
PORT = int(os.environ.get("PORT", 5051))
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud")

# Initialize DB connection lazy or upfront wrapper
def get_db():
    return GraphDBClient(NEO4J_URI)

# --- 1. Completeness Scoring System ---
class InformationCompletenessScorer:
    def __init__(self):
        # Define required fields for each node type
        self.required_fields = {
            "Country": {
                "mandatory": ["country", "resource_tonnage", "mine_output"],
                "important": ["trade_partners", "facilities", "policies", "esg_flags"],
                "nice_to_have": ["coordinates", "infrastructure_status"]
            },
            "Mineral": {
                "mandatory": ["mineral", "global_reserves", "primary_applications"],
                "important": ["processing_methods", "price_trends", "substitution_risk"],
                "nice_to_have": ["chemical_formula", "recycling_rate"]
            },
            "Company": {
                "mandatory": ["company_name", "operations_count"],
                "important": ["production_capacity", "facilities", "ownership_structure"],
                "nice_to_have": ["financial_data", "esg_ratings"]
            }
        }
        
        # Field weights for scoring
        self.field_weights = {
            "mandatory": 5,
            "important": 3, 
            "nice_to_have": 1
        }

    def calculate_completeness_score(self, node_type, attributes):
        """Calculate completeness percentage for a node"""
        # We lowercase node_type for mapping since our schema uses lowercase dynamic types
        schema_type = next((k for k in self.required_fields.keys() if k.lower() == node_type.lower()), None)
        required_set = self.required_fields.get(schema_type, {})
        
        total_possible_score = 0
        actual_score = 0
        
        for category, fields in required_set.items():
            weight = self.field_weights[category]
            total_possible_score += len(fields) * weight
            
            for field in fields:
                if self._field_has_data(attributes, field):
                    actual_score += weight
        
        if total_possible_score == 0:
            return 0
        
        completeness_percentage = (actual_score / total_possible_score) * 100
        return round(completeness_percentage, 2)
    
    def _field_has_data(self, attributes, field_name):
        """Check if field has meaningful data"""
        field_data = attributes.get(field_name)
        if not field_data:
            return False
        if isinstance(field_data, list):
            return len(field_data) > 0
        return bool(field_data and str(field_data).strip())


# --- 2. Data Freshness and Source Quality Scoring ---
class DataQualityScorer:
    def __init__(self):
        self.source_quality_weights = {
            "official_report": 1.0,      # Government, BGS, USGS
            "company_disclosure": 0.9,    # Annual reports
            "academic_paper": 0.8,        # Peer-reviewed
            "industry_report": 0.7,       # Roskill, CRU
            "news_article": 0.5,          # Media
            "inferred": 0.3               # Estimated/lower confidence
        }
    
    def calculate_data_freshness(self, facts):
        """Score based on how recent the data is"""
        if not facts:
            return 0
        
        years = []
        for fact in facts:
            year = fact.get('year')
            if year and str(year).isdigit():
                years.append(int(year))
        
        if not years:
            return 0
        
        current_year = datetime.now().year
        avg_year = sum(years) / len(years)
        freshness = max(0, 100 - (current_year - avg_year) * 10)
        return min(100, freshness)
    
    def calculate_source_quality(self, facts):
        """Score based on source reliability"""
        if not facts:
            return 0
        
        scores = []
        for fact in facts:
            source_type = self._infer_source_type(fact)
            score = self.source_quality_weights.get(source_type, 0.5)
            # Default confidence to 0.5 if not present or coercible
            conf = fact.get('confidence', 0.5)
            try: conf = float(conf)
            except: conf = 0.5
            scores.append(score * conf)
        
        return sum(scores) / len(scores) * 100
    
    def _infer_source_type(self, fact):
        """Infer source type from evidence text"""
        evidence = str(fact.get('evidence', '')).lower()
        doc_source = str(fact.get('source_document', '')).lower()
        
        if any(x in doc_source for x in ['bgs', 'usgs', 'government', 'ukcmic']):
            return "official_report"
        elif 'annual report' in evidence or 'sec filing' in evidence:
            return "company_disclosure" 
        elif 'journal' in evidence or 'university' in evidence:
            return "academic_paper"
        elif any(x in evidence for x in ['roskill', 'cru', 'wood macenzie', 'woodmac']):
            return "industry_report"
        elif 'inferred' in evidence or 'estimated' in evidence:
            return "inferred"
        else:
            return "industry_report"  # default


# --- 3. Enhanced Node Functions ---

def identify_data_gaps(node_type, attributes):
    """Identify missing important information"""
    required_fields = {
        "Country": ["trade_partners", "mine_output", "reserves", "policies"],
        "Mineral": ["applications", "processing_methods", "price_data", "substitutes"],
        "Company": ["facilities", "production_data", "ownership", "esg_info"]
    }
    
    # Capitalize for mapping
    schema_type = node_type.capitalize()
    gaps = []
    
    for field in required_fields.get(schema_type, []):
        if not attributes.get(field):
            gaps.append(field)
    
    return gaps

def calculate_overall_quality(metrics):
    """Composite quality score"""
    weights = {
        "completeness_score": 0.4,
        "data_freshness": 0.25, 
        "source_quality": 0.35
    }
    
    overall_score = (
        metrics["completeness_score"] * weights["completeness_score"] +
        metrics["data_freshness"] * weights["data_freshness"] +
        metrics["source_quality"] * weights["source_quality"]
    )
    
    return round(overall_score, 2)

def get_confidence_level(score):
    """Convert score to human-readable confidence level"""
    if score >= 90: return "Very High"
    elif score >= 75: return "High" 
    elif score >= 60: return "Moderate"
    elif score >= 40: return "Low"
    else: return "Very Low"

def generate_completeness_warnings(response):
    """Generate specific warnings about data completeness"""
    warnings = []
    metrics = response["information_completeness"]
    limitations = response["data_limitations"]
    
    if metrics["overall_score"] < 60:
        warnings.append("⚠️ Information is incomplete - consider gathering more sources")
    
    if metrics["data_freshness"] < 50:
        warnings.append("🕐 Data may be outdated - recent information needed")
    
    if limitations["facts_count"] < 3:
        warnings.append("📊 Limited supporting facts - low evidence base")
    
    if limitations["missing_critical_info"]:
        warnings.append(f"❌ Missing critical information: {', '.join(limitations['missing_critical_info'])}")
    
    return warnings

# --- 4. Main Query Routines ---

def extract_entities_from_query(query_text: str):
    """Use the LLM to parse the natural language query into `node_type` and `node_name`."""
    
    sys_prompt = f"""
You are a Graph Query Router. Your job is to extract the primary subject of the user's question.
Return ONLY a valid JSON object matching this schema:
{{
  "node_type": "<Country, Mineral, Company, Facility, Deposit, Technology, Product>",
  "node_name": "<the entity name>"
}}
Example User: "Tell me everything about the mineral Cobalt"
Example Output: {{"node_type": "mineral", "node_name": "Cobalt"}}
"""
    raw_response = call_ollama(sys_prompt, query_text, model=OLLAMA_MODEL)
    try:
        # Extract json chunk
        if "```json" in raw_response:
            chunk = raw_response.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_response:
            chunk = raw_response.split("```")[1].split("```")[0].strip()
        else:
            chunk = raw_response.strip()
            
        parsed = json.loads(chunk)
        return parsed.get("node_type", "").lower(), parsed.get("node_name", "")
    except Exception as e:
        logger.error(f"Failed to route question: {e}")
        return None, None

def query_node_with_completeness(node_type, node_name):
    """Enhanced query that returns completeness information"""
    db = get_db()
    node_data = db.get_node(node_type, node_name)
    db.close()
    
    if not node_data:
        return {
            "found": False,
            "message": f"No {node_type} named '{node_name}' found in database."
        }
        
    attributes = node_data.get("attributes", {})
    relationships = node_data.get("relationships", [])
    
    # We treat relationships as contributing facts to establish quality metrics
    contributing_facts = relationships
    
    completeness_scorer = InformationCompletenessScorer()
    quality_scorer = DataQualityScorer()

    metrics = {
        "completeness_score": completeness_scorer.calculate_completeness_score(node_type, attributes),
        "data_freshness": quality_scorer.calculate_data_freshness(contributing_facts),
        "source_quality": quality_scorer.calculate_source_quality(contributing_facts),
        "attribute_count": len([v for v in attributes.values() if v]),
        "facts_count": len(contributing_facts),
        "last_updated": datetime.now().isoformat(),
        "data_gaps": identify_data_gaps(node_type, attributes)
    }
    
    metrics["overall_quality_score"] = calculate_overall_quality(metrics)

    # Base response
    response = {
        "found": True,
        "node_type": node_type.capitalize(),
        "node_name": node_data.get("name", node_name),
        "node_id": node_data.get("node_id"),
        
        "information_completeness": {
            "overall_score": metrics["overall_quality_score"],
            "completeness_score": metrics["completeness_score"],
            "data_freshness": metrics["data_freshness"],
            "source_quality": metrics["source_quality"],
            "confidence_level": get_confidence_level(metrics["overall_quality_score"]),
            "last_updated": metrics["last_updated"]
        },
        
        "data_limitations": {
            "missing_critical_info": metrics["data_gaps"],
            "facts_count": metrics["facts_count"],
            "attribute_coverage": f"{metrics['attribute_count']} attributes populated"
        },
        
        "attributes": attributes,
        "relationships": relationships
    }
    
    response["warnings"] = generate_completeness_warnings(response)
    
    return response

# --- HTTP API ---

@app.route("/api/query", methods=["POST"])
def perform_query():
    data = request.json
    question = data.get("question")
    
    if not question:
        return jsonify({"error": "No question provided."}), 400
        
    # 1. Natural Language -> Graph Entity
    node_type, node_name = extract_entities_from_query(question)
    
    if not node_type or not node_name:
        return jsonify({
            "found": False,
            "message": "I couldn't identify the main entity (Country, Mineral, Company, etc) in your question."
        })
        
    # 2. Lookup and Score
    result = query_node_with_completeness(node_type, node_name)
    
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
