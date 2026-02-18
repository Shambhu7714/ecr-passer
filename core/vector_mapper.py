import os
import json
from typing import Optional, Dict, Any, List

# Try to use chromadb + sentence-transformers when available. Fall back to
# the lightweight JSON-based shim implemented below.
_HAS_CHROMA = False
try:
	import chromadb
	from chromadb.config import Settings
	_HAS_CHROMA = True
except Exception:
	_HAS_CHROMA = False


class VectorMapper:
	"""Lightweight VectorMapper shim for pattern storage and simple matching.

	This implementation stores patterns as JSON under `db_path/patterns.json`.
	It provides the minimal API used by the supervisor:
	  - match_pattern(file_structure, source_file=None)
	  - save_pattern(...)
	  - map_multiple_fields(headers)
	  - list_patterns()

	It's intentionally simple and deterministic so it works without an
	external vector DB dependency in the test environment.
	"""

	def __init__(self, db_path: str = "./chroma_db", use_fallback: bool = True, embed_fn=None):
		self.db_path = os.path.abspath(db_path)
		os.makedirs(self.db_path, exist_ok=True)
		self.pattern_file = os.path.join(self.db_path, "patterns.json")
		self.use_fallback = use_fallback
		self.pattern_collection: Dict[str, Dict[str, Any]] = {}
		self.collection = self.pattern_collection
		self._load_patterns()

		# Chromadb-backed collection (optional). Accept an embed_fn callable that
		# takes List[str] -> List[List[float]] (embedding vectors). We do not
		# depend on sentence-transformers here; the caller should provide an LLM
		# embedding function when available.
		self._chroma_client = None
		self._chroma_collection = None
		self._embed_fn = embed_fn
		if _HAS_CHROMA and (embed_fn is not None) and not use_fallback:
			try:
				client = chromadb.Client(Settings(persist_directory=self.db_path))
				# create or get collection named 'mappings' (no embedding_function set)
				self._chroma_collection = client.get_or_create_collection(name="mappings")
				self._chroma_client = client
			except Exception:
				self._chroma_client = None
				self._chroma_collection = None

	def _load_patterns(self):
		if os.path.exists(self.pattern_file):
			try:
				with open(self.pattern_file, "r", encoding="utf-8") as f:
					arr = json.load(f)
					for p in arr:
						name = p.get("pattern_name") or p.get("id")
						if name:
							self.pattern_collection[name] = p
			except Exception:
				# If file is corrupted, ignore and start fresh
				self.pattern_collection = {}

	def _save_patterns(self):
		try:
			with open(self.pattern_file, "w", encoding="utf-8") as f:
				json.dump(list(self.pattern_collection.values()), f, indent=2, ensure_ascii=False)
			return True
		except Exception:
			return False

	def match_pattern(self, file_structure: Dict[str, Any], source_file: Optional[str] = None) -> Optional[Dict[str, Any]]:
		"""Try to find a stored pattern that matches the file_structure or source_file.

		Returns a dict with keys like `pattern_name`, `confidence`, `layout`, etc., or None.
		"""
		# Prefer exact source_file match in metadata
		base = os.path.basename(source_file) if source_file else None
		best = None
		best_score = 0.0

		for name, meta in self.pattern_collection.items():
			score = 0.0
			# If source_file matches stored metadata, strong match
			msrc = meta.get("metadata", {}).get("source_file") or meta.get("file_structure", {}).get("sheet_name")
			if base and msrc and base.lower() in str(msrc).lower():
				score += 0.8

			# Compare headers overlap if available
			stored_headers = meta.get("file_structure", {}).get("headers", [])
			headers = file_structure.get("headers", [])
			if stored_headers and headers:
				shared = len(set([str(x).lower() for x in stored_headers]) & set([str(x).lower() for x in headers]))
				denom = max(1, min(len(stored_headers), len(headers)))
				score += (shared / denom) * 0.2

			if score > best_score:
				best_score = score
				best = meta

		if best and best_score >= 0.6:
			return {
				"pattern_name": best.get("pattern_name") or best.get("id"),
				"description": best.get("description", ""),
				"pattern_type": best.get("pattern_type", "vector"),
				"layout": best.get("layout", {}),
				"confidence": round(best_score, 2)
			}

		return None

	def save_pattern(self, pattern_name: str, update_name: str, layout: Dict[str, Any], file_structure: Dict[str, Any], metadata: Dict[str, Any]) -> bool:
		"""Save a pattern entry to disk and update in-memory collection."""
		entry = {
			"pattern_name": pattern_name,
			"update_name": update_name,
			"layout": layout,
			"file_structure": file_structure,
			"metadata": metadata,
			"pattern_type": layout.get("orientation", "unknown")
		}
		self.pattern_collection[pattern_name] = entry
		return self._save_patterns()

	def map_multiple_fields(self, headers: List[str]) -> Dict[str, Dict[str, Any]]:
		"""Return a simple identity mapping with confidence scores for headers."""
		result = {}
		for h in headers:
			# Simple heuristic: if header contains 'code' or looks numeric, mark as index
			label = str(h)
			conf = 0.8
			result[label] = {"mapped_to": label, "confidence": conf}
		return result

	def list_patterns(self) -> List[Dict[str, Any]]:
		return list(self.pattern_collection.values())

	# --- Legacy seeding + simple mapping API ---
	def seed_from_legacy_mapper(self, mapping_file: str, sheet: str = 'Mapping Rules + Checks', max_rows: Optional[int] = None) -> bool:
		"""Seed vector DB mappings from an Excel mapping file.

		Tries to find sensible source/target columns in the provided sheet.
		Supports both simple (source_field/target_field) and the project's
		comprehensive mapping layout (PRIMARY CONCEPT, SERIES CODE, Update Name).
		"""
		import pandas as pd

		if not os.path.exists(mapping_file):
			raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

		df = pd.read_excel(mapping_file, sheet_name=sheet, engine='openpyxl')
		if max_rows:
			df = df.head(max_rows)

		# Normalize column names
		cols = {c.lower().strip(): c for c in df.columns}

		# Preferred simple columns
		if 'source_field' in cols and 'target_field' in cols:
			# If caller provided explicit source/target columns, use them
			src_cols = [cols['source_field']]
			tgt_col = cols['target_field']
		else:
			# Try to discover the series code (target) and the concept columns (sources)
			tgt_col = None
			for candidate in ('series code', 'series_code', 'seriescode', 'series code '):
				if candidate in cols:
					tgt_col = cols[candidate]
					break
			if not tgt_col and 'update name' in cols:
				tgt_col = cols['update name']

			# Source concept columns: PRIMARY, SECONDARY, THIRD, FOURTH, Update Name
			src_candidates = [
				('primary concept', 'primary_concept', 'primaryconcept', 'primary concept '),
				('secondary concept', 'secondary_concept', 'secondaryconcept'),
				('third concept', 'third_concept', 'thirdconcept'),
				('fourth concept', 'fourth_concept', 'fourthconcept'),
				('update name', 'update_name')
			]
			src_cols = []
			for group in src_candidates:
				for candidate in group:
					if candidate in cols:
						src_cols.append(cols[candidate])
						break

		# If still not found, raise informative error
		if not src_cols or not tgt_col:
			raise ValueError("Mapping sheet missing required columns. Ensure it has source concept columns (PRIMARY/SECONDARY/THIRD/FOURTH) and SERIES CODE column.")

		# Build mappings list: one entry per non-empty concept column per row.
		mappings = []
		for idx, row in df.iterrows():
			# Serialize full row into a simple dict of strings/None
			row_dict_base = {}
			for c in df.columns:
				v = row.get(c)
				if pd.isna(v):
					row_dict_base[c] = None
				else:
					row_dict_base[c] = str(v).strip()

			tgt = row_dict_base.get(tgt_col)
			if not tgt:
				# Skip rows without a target series code
				continue

			for s_col in src_cols:
				src_val = row_dict_base.get(s_col)
				if not src_val:
					continue

				entry = dict(row_dict_base)
				entry['source_field'] = src_val
				entry['target_field'] = tgt
				entry['concept_column'] = s_col
				entry['row_index'] = int(idx)
				mappings.append(entry)

		# Save as mappings.json inside db_path for simple lookup
		mappings_path = os.path.join(self.db_path, 'mappings.json')
		try:
			with open(mappings_path, 'w', encoding='utf-8') as f:
				json.dump(mappings, f, ensure_ascii=False, indent=2)
		except Exception as e:
			raise IOError(f"Could not write mappings to {mappings_path}: {e}")

		# Also keep in-memory for map_field
		self._mappings = mappings

		# If chroma is available and we have an embed function, add documents there too
		if getattr(self, '_chroma_collection', None) is not None and callable(getattr(self, '_embed_fn', None)):
			try:
				# Prepare documents (use source_field as the document text)
				docs = [m.get('source_field', '') for m in mappings]
				metadatas = [ {k:v for k,v in m.items() if k not in ('source_field',)} for m in mappings]
				ids = [f"map-{i}" for i in range(len(mappings))]
				embs = self._embed_fn(docs)
				# Add to chroma collection (guarding for API differences)
				try:
					self._chroma_collection.add(ids=ids, documents=docs, metadatas=metadatas, embeddings=embs)
				except TypeError:
					# Some chroma client variants expect different param names
					self._chroma_collection.add(documents=docs, metadatas=metadatas)
			except Exception:
				# Non-fatal: leave JSON fallback intact
				pass

		return True

	def map_field(self, query: str) -> Optional[Dict[str, Any]]:
		"""Simple semantic lookup: return best mapping row for a source query.

		Returns the full mapping row (as seeded) with an added `confidence` key,
		or None if no reasonable match was found.
		"""
		q = str(query).strip().lower()

		# If chroma collection available and embed_fn provided, prefer vector lookup
		if getattr(self, '_chroma_collection', None) is not None and callable(getattr(self, '_embed_fn', None)):
			try:
				emb = self._embed_fn([query])
				if emb and len(emb) > 0:
					q_emb = emb[0]
					# Query by embedding; include metadatas and distances
					res = self._chroma_collection.query(query_embeddings=[q_emb], n_results=3, include=['metadatas', 'distances'])
					metas = (res.get('metadatas') or [[]])[0]
					dists = (res.get('distances') or [[]])[0]
					if metas:
						best_meta = metas[0]
						conf = None
						if dists and len(dists) > 0 and dists[0] is not None:
							try:
								conf = float(1.0 - dists[0])
							except Exception:
								conf = None
						if conf is None:
							conf = 0.85
						best_meta['confidence'] = conf
						return best_meta
			except Exception:
				# fall back to file-based lookup
				pass
		best = None
		best_score = 0.0
		mappings = getattr(self, '_mappings', None)
		if mappings is None:
			# Try to load from disk
			mp = os.path.join(self.db_path, 'mappings.json')
			if os.path.exists(mp):
				try:
					with open(mp, 'r', encoding='utf-8') as f:
						mappings = json.load(f)
						self._mappings = mappings
				except Exception:
					mappings = []
			else:
				mappings = []

		for m in mappings:
			src = str(m.get('source_field', '')).lower()
			tgt = m.get('target_field')
			if q == src:
				row_copy = dict(m)
				row_copy['confidence'] = 1.0
				return row_copy
			# substring match
			if q in src or src in q:
				score = 0.8
			else:
				# token overlap
				shared = len(set(q.split()) & set(src.split()))
				denom = max(1, min(len(q.split()), len(src.split())))
				score = shared / denom if denom else 0.0
			if score > best_score:
				best_score = score
				best = dict(m)
				best['confidence'] = score

		return best

	def get_stats(self) -> Dict[str, Any]:
		mp = os.path.join(self.db_path, 'mappings.json')
		count = 0
		if os.path.exists(mp):
			try:
				with open(mp, 'r', encoding='utf-8') as f:
					arr = json.load(f)
					count = len(arr)
			except Exception:
				count = 0
		return {'count': count, 'db_path': self.db_path}

