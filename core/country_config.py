"""
CountryConfigLoader   Plug-and-play country format support (Problem 4 Fix)

Loads country-specific configuration from config/countries/<country>.json.
Preprocessing uses this config instead of hardcoded Spanish keywords,
enabling zero-code addition of new country formats.

Usage:
    loader = CountryConfigLoader()
    config = loader.detect_and_load(input_file, mapping_file)
    # config contains: header_keywords, footer_keywords, time_keywords, etc.
"""

import os
import json
from typing import Dict, Optional, List
from core.logger import get_logger

logger = get_logger()

COUNTRIES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "countries")

# Universal fallback keywords (covers English + Spanish + Portuguese)
UNIVERSAL_HEADER_KEYWORDS = [
    # Spanish
    "C digo", "Divisi n", "Ponderaci n", "Grupo", "Secci n", "Actividad", "Rama",
    # English
    "Code", "Division", "Weight", "Group", "Section", "Activity",
    # Portuguese
    "C digo", "Divis o", "Pondera o", "Grupo", "Se o",
]

UNIVERSAL_FOOTER_KEYWORDS = [
    "Fuente:", "Nota:", "FUENTE:", "NOTA:", "Source:", "Note:", "Fonte:", "Nota:",
    "Trimestre", "Quarter"
]

UNIVERSAL_TIME_KEYWORDS = [
    # Spanish months
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
    # English months
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
    # Portuguese months
    "janeiro", "fevereiro", "mar o", "abril", "maio", "junho",
    "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
    # Quarters
    "q1", "q2", "q3", "q4", "trimestre", "quarter", "trim",
]


class CountryConfigLoader:
    """
    Loads country-specific preprocessing configuration.
    Falls back to universal keywords if no country config is found.
    """

    def __init__(self):
        self._configs: Dict[str, Dict] = {}
        self._load_all_configs()

    def _load_all_configs(self):
        """Load all country configs from config/countries/ directory."""
        if not os.path.isdir(COUNTRIES_DIR):
            logger.warning(f"[WARN] Countries config directory not found: {COUNTRIES_DIR}")
            return

        for fname in os.listdir(COUNTRIES_DIR):
            if fname.endswith(".json"):
                fpath = os.path.join(COUNTRIES_DIR, fname)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        cfg = json.load(f)
                    country_code = cfg.get("country_code", fname.replace(".json", "")).upper()
                    self._configs[country_code] = cfg
                    # Silent load, only log summary
                except Exception as e:
                    logger.warning(f"  [WARN] Could not load country config '{fname}': {e}")

        logger.info(f"[INIT] Loaded {len(self._configs)} country formats: {list(self._configs.keys())}")

    def detect_and_load(self, input_file: str, mapping_file: str = None) -> Dict:
        """
        Auto-detect country from mapping file name or input file name,
        then return the matching country config.

        Falls back to universal config if no match found.

        Args:
            input_file: Path to the input Excel file
            mapping_file: Path to the mapping Excel file (optional)

        Returns:
            Country config dictionary with preprocessing parameters
        """
        # Try to detect country from input file name first
        country_code = self._detect_country_from_filename(input_file or "")
        
        # Then fall back to mapping file name if not found
        if not country_code:
            country_code = self._detect_country_from_filename(mapping_file or "")

        if country_code and country_code in self._configs:
            cfg = self._configs[country_code]
            logger.info(f"[OK] Working with country format: {cfg.get('country_name', country_code)} ({country_code})")
            return cfg

        # Fallback: universal config
        logger.warning(
            f"[WARN] Could not detect country from files. Using universal keyword config. "
            f"(Hint: add a config/countries/<country>.json for '{os.path.basename(input_file)}')"
        )
        return self._universal_config()

    def get_config(self, country_code: str) -> Optional[Dict]:
        """Get config for a specific country code (e.g., 'AR', 'CO')."""
        return self._configs.get(country_code.upper())

    def list_supported_countries(self) -> List[str]:
        """Return list of supported country codes."""
        return list(self._configs.keys())

    def _detect_country_from_filename(self, filepath: str) -> Optional[str]:
        """Detect country code from filename using known country codes."""
        if not filepath:
            return None
        basename = os.path.basename(filepath).upper()

        # Check each country config for a match
        for code, cfg in self._configs.items():
            country_name = cfg.get("country_name", "").upper()
            # Match country code or country name in filename
            if code in basename or country_name in basename:
                return code

        # Common country code patterns in filenames
        common_codes = {
            "ARG": "AR", "ARGENTINA": "AR",
            "COL": "CO", "COLOMBIA": "CO", "EMMET": "CO", "GEIH": "CO", "CHV": "CO",
            "BRA": "BR", "BRAZIL": "BR", "BRASIL": "BR",
            "MEX": "MX", "MEXICO": "MX", "M XICO": "MX",
            "CHL": "CL", "CHILE": "CL",
            "PER": "PE", "PERU": "PE", "PER ": "PE",
            "URY": "UY", "URUGUAY": "UY",
            "PRY": "PY", "PARAGUAY": "PY",
            "BOL": "BO", "BOLIVIA": "BO",
            "ECU": "EC", "ECUADOR": "EC",
            "VEN": "VE", "VENEZUELA": "VE",
        }
        for pattern, code in common_codes.items():
            if pattern in basename:
                return code

        return None

    def _universal_config(self) -> Dict:
        """Universal fallback config covering all supported languages."""
        return {
            "country_code": "UNIVERSAL",
            "country_name": "Universal (Auto-detect)",
            "language": "auto",
            "mapping_file": None,
            "header_detection_keywords": UNIVERSAL_HEADER_KEYWORDS,
            "footer_keywords": UNIVERSAL_FOOTER_KEYWORDS,
            "code_column_names": ["C digo", "Code", "C digo", "Division", "Divisi n"],
            "description_column_names": ["Grupo", "Group", "Divisi n", "Description", "Descripci n"],
            "time_keywords": UNIVERSAL_TIME_KEYWORDS,
            "code_pad_length": 8,
            "aggregation_strategy": "mean",
            "group_code_prefix_length": 2,
        }
