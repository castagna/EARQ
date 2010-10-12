package org.openjena.earq.searchers;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.openjena.earq.Document;
import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.IndexSearcher;
import org.openjena.earq.LuceneConstants;

public class LuceneIndexSearcher extends IndexSearcherBase implements IndexSearcher {

	private Directory dir = null;
	private IndexReader indexReader = null;
	private QueryParser queryParser = null ;
	
	public final static int NUM_RESULTS = 10000;
	
	public LuceneIndexSearcher (String path) {
		super();
        try {
			dir = FSDirectory.open(new File(path));
	        indexReader = IndexReader.open(dir, true) ;
	        queryParser = new QueryParser(Version.LUCENE_29, EARQ.fText, new StandardAnalyzer(Version.LUCENE_29));
		} catch (IOException e) {
			throw new EARQException(e.getMessage(), e);
		}
	}
	
	public LuceneIndexSearcher (Directory dir) {
		this.dir = dir;
        try {
        	// TODO: remove duplication!
	        indexReader = IndexReader.open(dir, true) ;
	        queryParser = new QueryParser(Version.LUCENE_29, EARQ.fText, new StandardAnalyzer(Version.LUCENE_29));
		} catch (IOException e) {
			throw new EARQException(e.getMessage(), e);
		}		
	}

	@Override
	public Iterator<Document> search(String query) {
		Searcher indexSearcher = new org.apache.lucene.search.IndexSearcher(indexReader); 
		Query luceneQuery = null;
		ArrayList<Document> hits = new ArrayList<Document>();
		try {
			luceneQuery = queryParser.parse(query);
			TopDocs docs = indexSearcher.search(luceneQuery, LuceneConstants.NUM_RESULTS);
			for (int i = 0; i < docs.totalHits; i++) {
				org.apache.lucene.document.Document luceneDocument = indexSearcher.doc(i);
				Document doc = new Document();
				List<Fieldable> fields = luceneDocument.getFields();
				for (Fieldable field : fields) {
					doc.set(field.name(), field.stringValue());
				}
				doc.set(EARQ.fScore, String.valueOf(docs.scoreDocs[i].score));
				hits.add(doc);
			}
		} catch (Exception e) {
			throw new EARQException (e.getMessage(), e);
		} finally {
			try {
				indexSearcher.close();
			} catch (IOException e) { 
				// TODO 
			}
		}
		
		return hits.iterator();
	}

}
