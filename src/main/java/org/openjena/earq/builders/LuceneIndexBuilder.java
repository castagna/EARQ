package org.openjena.earq.builders;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.openjena.earq.Document;
import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.IndexSearcher;
import org.openjena.earq.searchers.IndexSearcherFactory;

public class LuceneIndexBuilder extends IndexBuilderBase {

    private String path = null;
	private Directory dir = null;
    private IndexWriter indexWriter = null ;
	
	public LuceneIndexBuilder(String path) {
		super();
		this.path = path;
		try {
			dir = FSDirectory.open(new File(path));
			makeIndexWriter();
		} catch (Exception e) {
			throw new EARQException(e.getMessage(), e);
		}
	}
	
    private void makeIndexWriter() throws CorruptIndexException, LockObtainFailedException, IOException {
    	indexWriter = new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_29), MaxFieldLength.UNLIMITED) ;
    }

    public Directory getDirectory() {
    	return this.dir;
    }
    
	@Override
	public void add(Document doc) {
		org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
		for (String name : doc.getNames()) {
			Field field = null;
			Field.Store store = null;
			Field.Index index = null;
			if ( EARQ.fId.equals(name) ) {
				store = Field.Store.NO;
				index = Field.Index.NOT_ANALYZED;
			} else if ( EARQ.fText.equals(name) )  {
				store = Field.Store.NO;
				index = Field.Index.ANALYZED;
			} else {
				store = Field.Store.YES;
				index = Field.Index.NO;
			}
			
			if ( ( store == null ) || (index == null) ) {
				throw new EARQException("Unknown field name.");
			}
			field = new Field(name, doc.get(name), store, index) ;
			luceneDoc.add(field);
		}
		try {
			indexWriter.addDocument(luceneDoc);
		} catch (Exception e) {
			throw new EARQException(e.getMessage(), e);
		}
	}

	@Override
	public void delete(String id) {
		BooleanQuery query = new BooleanQuery();
		query.add(new TermQuery(new Term(EARQ.fText, id)) , Occur.MUST);
		try {
			indexWriter.deleteDocuments(query);
		} catch (Exception e) {
			throw new EARQException(e.getMessage(), e);
		}
	}

	@Override
	public IndexSearcher getIndexSearcher() {
		// TODO: we should flush only if the index have been changed, see index version 
		flushIndexWriter();
		closeIndexWriter(true);
		return IndexSearcherFactory.create(EARQ.Type.LUCENE, path);
	}
	
	@Override
	public void close() {
		closeIndexWriter(true) ;
	}

	public void deleteAll() {
		try {
			indexWriter.deleteAll();
		} catch (Exception e) {
			throw new EARQException(e.getMessage(), e);
		}
	}
	
    private void closeIndexWriter(boolean optimize) {
        if ( optimize ) 
            flushIndexWriter() ;
        try {
            if ( indexWriter != null ) indexWriter.close();
        } catch (IOException e) { 
        	throw new EARQException("closeIndex", e) ; 
        }
        indexWriter = null ;
    }
    
    private void flushIndexWriter() { 
        try { 
        	if ( indexWriter != null ) indexWriter.optimize(); 
        } catch (IOException e) { 
        	throw new EARQException("flushWriter", e) ; 
        }
    }

}
