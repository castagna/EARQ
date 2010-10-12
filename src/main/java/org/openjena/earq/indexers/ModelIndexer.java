package org.openjena.earq.indexers;

import org.openjena.earq.IndexSearcher;

import com.hp.hpl.jena.rdf.model.ModelChangedListener;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public interface ModelIndexer extends ModelChangedListener {

    public void indexStatement(Statement s) ;
    public void unindexStatement(Statement s) ;

    public void indexStatements(StmtIterator sIter) ; 
    public void unindexStatements(StmtIterator sIter) ;

    public IndexSearcher getIndexSearcher();
    public void close();

}
