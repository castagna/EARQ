package org.openjena.earq.searchers;

import java.util.Iterator;

import org.openjena.earq.Document;
import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.IndexSearcher;

import com.hp.hpl.jena.graph.Node;

public abstract class IndexSearcherBase implements IndexSearcher {

	@Override public abstract Iterator<Document> search(String query);
	
	@Override
    public Document contains(Node node, String query) {
        try {
            Iterator<Document> iter = search(query) ;
            for ( ; iter.hasNext() ; ) {
                Document x = iter.next();
                if ( x != null && EARQ.build(x).equals(node)) {
                    return x ;
                }
            }
            return null ;
        } catch (Exception e) { 
        	throw new EARQException("contains", e) ; 
        }   	
    }

}
