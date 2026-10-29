"""
Inverted Index - Full-text search with TF-IDF scoring

Simple tokenization, optional stemming, and relevance ranking.
"""

import re
import math
from collections import Counter, defaultdict
from typing import List, Tuple, Dict, Set


class InvertedIndex:
    """
    Full-text search engine with inverted index.
    
    Features:
    - Whitespace + punctuation tokenization
    - Lowercase normalization
    - TF-IDF scoring for relevance ranking
    - Document length normalization
    """
    
    # Common English stopwords (optional filtering)
    STOPWORDS: Set[str] = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
        'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'shall', 'can', 'need', 'dare',
        'it', 'its', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she',
        'we', 'they', 'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his',
    }
    
    def __init__(self, use_stopwords: bool = True, min_term_length: int = 2):
        self.index: Dict[str, Dict[str, int]] = {}  # term -> {doc_id: frequency}
        self.doc_lengths: Dict[str, int] = {}       # doc_id -> total terms
        self.doc_count: int = 0
        self.use_stopwords = use_stopwords
        self.min_term_length = min_term_length
    
    def add_document(self, doc_id: str, text: str):
        """Add or update a document in the index."""
        tokens = self._tokenize(text)
        
        # Remove old document if updating
        if doc_id in self.doc_lengths:
            self._remove_document(doc_id)
        
        self.doc_lengths[doc_id] = len(tokens)
        self.doc_count += 1
        
        term_freq = Counter(tokens)
        for term, freq in term_freq.items():
            if term not in self.index:
                self.index[term] = {}
            self.index[term][doc_id] = freq
    
    def _remove_document(self, doc_id: str):
        """Remove a document from the index."""
        # Find all terms in this document and remove references
        terms_to_remove = []
        for term, docs in self.index.items():
            if doc_id in docs:
                del docs[doc_id]
                if not docs:
                    terms_to_remove.append(term)
        
        for term in terms_to_remove:
            del self.index[term]
        
        del self.doc_lengths[doc_id]
        self.doc_count -= 1
    
    def remove_document(self, doc_id: str):
        """Public method to remove a document."""
        if doc_id in self.doc_lengths:
            self._remove_document(doc_id)
    
    def search(self, query: str, limit: int = 20) -> List[Tuple[str, float]]:
        """
        Search for documents matching query.
        
        Returns list of (doc_id, score) tuples sorted by relevance.
        Uses TF-IDF scoring:
            score = sum( (tf / doc_length) * log(N / df) ) for each query term
        """
        query_terms = self._tokenize(query)
        if not query_terms:
            return []
        
        scores: Dict[str, float] = defaultdict(float)
        N = max(1, self.doc_count)
        
        for term in query_terms:
            if term not in self.index:
                continue
            
            posting_list = self.index[term]
            df = len(posting_list)  # Document frequency
            idf = math.log(N / df) if df > 0 else 0
            
            for doc_id, tf in posting_list.items():
                doc_len = max(1, self.doc_lengths.get(doc_id, 1))
                # Normalized TF-IDF
                scores[doc_id] += (tf / doc_len) * idf
        
        # Sort by score descending
        results = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return results[:limit]
    
    def _tokenize(self, text: str) -> List[str]:
        """
        Tokenize text into terms.
        
        - Lowercase
        - Split on non-alphanumeric
        - Filter short terms and stopwords
        """
        text = text.lower()
        tokens = re.findall(r'\b[a-z0-9]+\b', text)
        
        filtered = []
        for token in tokens:
            if len(token) < self.min_term_length:
                continue
            if self.use_stopwords and token in self.STOPWORDS:
                continue
            filtered.append(token)
        
        return filtered
    
    def get_terms(self, doc_id: str) -> List[str]:
        """Get all terms for a document (for debugging)."""
        terms = []
        for term, docs in self.index.items():
            if doc_id in docs:
                terms.append(term)
        return terms
    
    def stats(self) -> dict:
        """Return index statistics."""
        return {
            'document_count': self.doc_count,
            'unique_terms': len(self.index),
            'avg_doc_length': sum(self.doc_lengths.values()) / max(1, self.doc_count),
        }
    
    def clear(self):
        """Clear the entire index."""
        self.index.clear()
        self.doc_lengths.clear()
        self.doc_count = 0
