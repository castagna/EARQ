/*
 * Copyright © 2010 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openjena.earq.builders;

import java.io.Reader;

import org.openjena.earq.Document;
import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.IndexBuilder;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.RDFNode;

public abstract class IndexBuilderBase implements IndexBuilder {

	@Override public abstract void add(Document doc);
	@Override public abstract void delete(String id);

    @Override public abstract void close();

	@Override
	public void index(RDFNode rdfNode, String indexStr) {
		Document doc = new Document() ;
	    EARQ.store(doc, rdfNode.asNode()) ;
	    EARQ.index(doc, rdfNode.asNode(), indexStr) ;
	    add(doc) ;
	}

	@Override
	public void index(RDFNode rdfNode, Reader indexStream) {
		Document doc = new Document() ;
		EARQ.store(doc, rdfNode.asNode()) ;
	    EARQ.index(doc, rdfNode.asNode(), indexStream) ;
	    add(doc) ;
	}

	@Override
	public void index(Node node, String indexStr) {
		Document doc = new Document() ;
	    EARQ.store(doc, node) ;
	    EARQ.index(doc, node, indexStr) ;
	    add(doc) ;
	}

	@Override
	public void index(Node node, Reader indexStream) {
		Document doc = new Document() ;
	    EARQ.store(doc, node) ;
	    EARQ.index(doc, node, indexStream) ;
	    add(doc) ;
	}

	@Override
	public void unindex(RDFNode node, Reader indexStream) {
		unindex(node.asNode(), indexStream);
	}

	@Override
	public void unindex(RDFNode node, String indexStr) {
		unindex(node.asNode(), indexStr);
	}

	public void unindex(Node node, Reader indexStream) {
		try {
			String id = EARQ.unindex(node, indexStream);
			delete(id);			
	    } catch (Exception ex) { 
	    	throw new EARQException("unindex", ex) ; 
	    } 
	}

	@Override
	public void unindex(Node node, String indexStr) {
		try {
			String id = EARQ.unindex(node, indexStr);
			delete(id);
	    } catch (Exception ex) { 
	    	throw new EARQException("unindex", ex) ; 
	    } 
	}


}