from typing import Dict, List, Tuple, Optional, Any
from core.logger import get_logger
import re

logger = get_logger()


class HierarchyExtractor:
    """
    Extracts hierarchical structures from multi-level concepts.
    
    Handles:
    - Multi-level headers (Total > Sector > Subsector)
    - Geographic hierarchies (Country > State > City)
    - Time hierarchies (Year > Quarter > Month)
    - Category hierarchies (Industry > Sector > Company)
    """
    
    def __init__(self):
        # Common hierarchy indicators
        self.GEOGRAPHIC_LEVELS = ['country', 'region', 'state', 'province', 'city', 'district']
        self.TIME_LEVELS = ['year', 'quarter', 'month', 'week', 'day']
        self.CATEGORY_LEVELS = ['total', 'aggregate', 'category', 'subcategory', 'item']
        
    def extract_hierarchy(self, concepts: List[Dict], metadata: Dict = None) -> Dict:
        """
        Extract hierarchical relationships from concept list.
        
        Args:
            concepts: List of concept dictionaries with primary/secondary/third/fourth concepts
            metadata: Additional context
            
        Returns:
            Hierarchy structure with levels and relationships
        """
        logger.info(f"Extracting hierarchy from {len(concepts)} concepts")
        
        hierarchy = {
            "levels": [],
            "relationships": [],
            "root_nodes": [],
            "leaf_nodes": [],
            "hierarchy_type": None
        }
        
        if not concepts:
            return hierarchy
        
        # Identify hierarchy levels from concept structure
        sample = concepts[0]
        levels = []
        
        if 'primary_concept' in sample and sample['primary_concept']:
            levels.append("primary")
        if 'secondary_concept' in sample and sample['secondary_concept']:
            levels.append("secondary")
        if 'third_concept' in sample and sample['third_concept']:
            levels.append("third")
        if 'fourth_concept' in sample and sample['fourth_concept']:
            levels.append("fourth")
        
        hierarchy["levels"] = levels
        
        # Detect hierarchy type
        hierarchy["hierarchy_type"] = self._detect_hierarchy_type(concepts)
        
        # Build relationship tree
        tree = self._build_hierarchy_tree(concepts)
        hierarchy["tree"] = tree
        
        # Identify root and leaf nodes
        hierarchy["root_nodes"] = self._find_root_nodes(tree)
        hierarchy["leaf_nodes"] = self._find_leaf_nodes(tree)
        
        # Extract parent-child relationships
        hierarchy["relationships"] = self._extract_relationships(tree)
        
        logger.info(f"Hierarchy extracted: Type={hierarchy['hierarchy_type']}, "
                   f"Levels={len(levels)}, Relationships={len(hierarchy['relationships'])}")
        
        return hierarchy
    
    def _detect_hierarchy_type(self, concepts: List[Dict]) -> str:
        """Detect the type of hierarchy (geographic, temporal, categorical, etc.)"""
        
        # Sample first few concepts
        sample = concepts[:min(5, len(concepts))]
        
        # Check for geographic indicators
        geographic_score = 0
        for concept_dict in sample:
            for level_key in ['primary_concept', 'secondary_concept', 'third_concept', 'fourth_concept']:
                concept = str(concept_dict.get(level_key, '')).lower()
                for geo_term in self.GEOGRAPHIC_LEVELS:
                    if geo_term in concept:
                        geographic_score += 1
        
        # Check for temporal indicators
        temporal_score = 0
        for concept_dict in sample:
            for level_key in ['primary_concept', 'secondary_concept', 'third_concept', 'fourth_concept']:
                concept = str(concept_dict.get(level_key, '')).lower()
                for time_term in self.TIME_LEVELS:
                    if time_term in concept:
                        temporal_score += 1
        
        # Check for categorical indicators
        categorical_score = 0
        for concept_dict in sample:
            for level_key in ['primary_concept', 'secondary_concept', 'third_concept', 'fourth_concept']:
                concept = str(concept_dict.get(level_key, '')).lower()
                for cat_term in self.CATEGORY_LEVELS:
                    if cat_term in concept:
                        categorical_score += 1
        
        # Determine type based on scores
        max_score = max(geographic_score, temporal_score, categorical_score)
        
        if max_score == 0:
            return "unknown"
        elif geographic_score == max_score:
            return "geographic"
        elif temporal_score == max_score:
            return "temporal"
        else:
            return "categorical"
    
    def _build_hierarchy_tree(self, concepts: List[Dict]) -> Dict:
        """Build hierarchical tree structure from concepts"""
        tree = {}
        
        for concept_dict in concepts:
            primary = self._normalize_concept(concept_dict.get('primary_concept'))
            secondary = self._normalize_concept(concept_dict.get('secondary_concept'))
            third = self._normalize_concept(concept_dict.get('third_concept'))
            fourth = self._normalize_concept(concept_dict.get('fourth_concept'))
            
            if not primary:
                continue
            
            # Initialize primary level
            if primary not in tree:
                tree[primary] = {
                    "level": "primary",
                    "children": {},
                    "series_codes": []
                }
            
            # Add series code to primary
            if 'series_code' in concept_dict:
                tree[primary]["series_codes"].append(concept_dict['series_code'])
            
            # Add secondary level
            if secondary:
                if secondary not in tree[primary]["children"]:
                    tree[primary]["children"][secondary] = {
                        "level": "secondary",
                        "children": {},
                        "series_codes": []
                    }
                
                if 'series_code' in concept_dict:
                    tree[primary]["children"][secondary]["series_codes"].append(concept_dict['series_code'])
                
                # Add third level
                if third:
                    if third not in tree[primary]["children"][secondary]["children"]:
                        tree[primary]["children"][secondary]["children"][third] = {
                            "level": "third",
                            "children": {},
                            "series_codes": []
                        }
                    
                    if 'series_code' in concept_dict:
                        tree[primary]["children"][secondary]["children"][third]["series_codes"].append(concept_dict['series_code'])
                    
                    # Add fourth level
                    if fourth:
                        if fourth not in tree[primary]["children"][secondary]["children"][third]["children"]:
                            tree[primary]["children"][secondary]["children"][third]["children"][fourth] = {
                                "level": "fourth",
                                "children": {},
                                "series_codes": []
                            }
                        
                        if 'series_code' in concept_dict:
                            tree[primary]["children"][secondary]["children"][third]["children"][fourth]["series_codes"].append(concept_dict['series_code'])
        
        return tree
    
    def _normalize_concept(self, concept: Any) -> Optional[str]:
        """Normalize concept string"""
        if concept is None or concept == '':
            return None
        
        concept_str = str(concept).strip()
        
        if not concept_str or concept_str.lower() in ['nan', 'none', 'null', '']:
            return None
        
        return concept_str
    
    def _find_root_nodes(self, tree: Dict) -> List[str]:
        """Find root nodes (top-level concepts)"""
        return list(tree.keys())
    
    def _find_leaf_nodes(self, tree: Dict) -> List[str]:
        """Find leaf nodes (bottom-level concepts with no children)"""
        leaves = []
        
        def traverse(node_dict, path=[]):
            for key, value in node_dict.items():
                current_path = path + [key]
                
                if isinstance(value, dict):
                    children = value.get("children", {})
                    
                    if not children:
                        # Leaf node
                        leaves.append(" > ".join(current_path))
                    else:
                        # Continue traversing
                        traverse(children, current_path)
        
        traverse(tree)
        return leaves
    
    def _extract_relationships(self, tree: Dict) -> List[Dict]:
        """Extract parent-child relationships"""
        relationships = []
        
        def traverse(node_dict, parent=None, level=0):
            for key, value in node_dict.items():
                # Record relationship
                if parent:
                    relationships.append({
                        "parent": parent,
                        "child": key,
                        "level": level,
                        "series_codes": value.get("series_codes", []) if isinstance(value, dict) else []
                    })
                
                # Continue traversing
                if isinstance(value, dict) and "children" in value:
                    traverse(value["children"], parent=key, level=level + 1)
        
        traverse(tree)
        return relationships
    
    def visualize_hierarchy(self, hierarchy: Dict) -> str:
        """Generate text visualization of hierarchy"""
        if not hierarchy.get("tree"):
            return "No hierarchy found"
        
        lines = []
        lines.append(f"Hierarchy Type: {hierarchy.get('hierarchy_type', 'unknown').upper()}")
        lines.append(f"Levels: {', '.join(hierarchy.get('levels', []))}")
        lines.append("")
        lines.append("Tree Structure:")
        lines.append("=" * 60)
        
        def render_tree(node_dict, indent=0):
            for key, value in node_dict.items():
                series_count = len(value.get("series_codes", [])) if isinstance(value, dict) else 0
                lines.append("  " * indent + f"├─ {key} ({series_count} series)")
                
                if isinstance(value, dict) and "children" in value and value["children"]:
                    render_tree(value["children"], indent + 1)
        
        render_tree(hierarchy["tree"])
        
        lines.append("")
        lines.append(f"Root Nodes: {len(hierarchy.get('root_nodes', []))}")
        lines.append(f"Leaf Nodes: {len(hierarchy.get('leaf_nodes', []))}")
        lines.append(f"Relationships: {len(hierarchy.get('relationships', []))}")
        
        return "\n".join(lines)
    
    def get_series_by_path(self, hierarchy: Dict, path: List[str]) -> List[str]:
        """
        Get all series codes for a specific concept path.
        
        Example: path = ["Total", "Manufacturing", "Textiles"]
        """
        if not hierarchy.get("tree"):
            return []
        
        current = hierarchy["tree"]
        
        for concept in path:
            if concept in current:
                current = current[concept]
                
                # If this is the final concept in path
                if concept == path[-1]:
                    return current.get("series_codes", [])
                
                # Move to children
                current = current.get("children", {})
            else:
                return []
        
        return []
    
    def flatten_hierarchy(self, hierarchy: Dict) -> List[Dict]:
        """
        Flatten hierarchy into list of paths with series codes.
        
        Useful for indexing and searching.
        """
        flattened = []
        
        def traverse(node_dict, path=[]):
            for key, value in node_dict.items():
                current_path = path + [key]
                
                if isinstance(value, dict):
                    series_codes = value.get("series_codes", [])
                    
                    if series_codes:
                        flattened.append({
                            "path": " > ".join(current_path),
                            "depth": len(current_path),
                            "concept": key,
                            "series_codes": series_codes,
                            "series_count": len(series_codes)
                        })
                    
                    # Continue traversing
                    children = value.get("children", {})
                    if children:
                        traverse(children, current_path)
        
        if hierarchy.get("tree"):
            traverse(hierarchy["tree"])
        
        return flattened
