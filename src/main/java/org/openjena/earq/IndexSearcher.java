package org.openjena.earq;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Node;

public interface IndexSearcher {

	public Iterator<Document> search(String query);
	public Document contains(Node node, String query);
	
}
