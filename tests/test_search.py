"""Tests for InvertedIndex full-text search"""
import pytest
from ignisdb.search import InvertedIndex


class TestInvertedIndex:
    def test_add_and_search(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'The quick brown fox')
        idx.add_document('doc2', 'A lazy brown dog')
        
        results = idx.search('brown')
        doc_ids = [d for d, s in results]
        assert 'doc1' in doc_ids
        assert 'doc2' in doc_ids
    
    def test_search_relevance(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'python programming tutorial')
        idx.add_document('doc2', 'python is great for programming')
        idx.add_document('doc3', 'java programming language')
        
        results = idx.search('python programming')
        # doc1 and doc2 should rank higher than doc3
        doc_ids = [d for d, s in results]
        assert doc_ids.index('doc1') < doc_ids.index('doc3')
        assert doc_ids.index('doc2') < doc_ids.index('doc3')
    
    def test_no_results(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'hello world')
        
        results = idx.search('nonexistent')
        assert results == []
    
    def test_case_insensitive(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'Hello World PYTHON')
        
        results = idx.search('python')
        assert len(results) == 1
        
        results = idx.search('HELLO')
        assert len(results) == 1
    
    def test_stopword_filtering(self):
        idx = InvertedIndex(use_stopwords=True)
        idx.add_document('doc1', 'the quick brown fox')
        
        # 'the' is a stopword, should not match
        results = idx.search('the')
        assert len(results) == 0
        
        # 'quick' should match
        results = idx.search('quick')
        assert len(results) == 1
    
    def test_min_term_length(self):
        idx = InvertedIndex(min_term_length=3)
        idx.add_document('doc1', 'I am a go developer')
        
        # 'go' is too short (2 chars < min 3)
        results = idx.search('go')
        assert len(results) == 0
        
        # 'developer' should work
        results = idx.search('developer')
        assert len(results) == 1
    
    def test_remove_document(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'hello world')
        idx.add_document('doc2', 'hello python')
        
        idx.remove_document('doc1')
        
        results = idx.search('hello')
        doc_ids = [d for d, s in results]
        assert 'doc1' not in doc_ids
        assert 'doc2' in doc_ids
    
    def test_update_document(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'original content')
        idx.add_document('doc1', 'updated content here')
        
        results = idx.search('original')
        assert len(results) == 0
        
        results = idx.search('updated')
        assert len(results) == 1
    
    def test_stats(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'hello world')
        idx.add_document('doc2', 'hello python')
        
        stats = idx.stats()
        assert stats['document_count'] == 2
        assert stats['unique_terms'] > 0
    
    def test_limit_results(self):
        idx = InvertedIndex()
        for i in range(50):
            idx.add_document(f'doc{i}', f'common term document {i}')
        
        results = idx.search('common', limit=5)
        assert len(results) == 5
    
    def test_clear(self):
        idx = InvertedIndex()
        idx.add_document('doc1', 'test')
        idx.clear()
        
        assert idx.doc_count == 0
        assert idx.search('test') == []
