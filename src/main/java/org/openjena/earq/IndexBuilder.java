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

package org.openjena.earq;

import java.io.Reader;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.RDFNode;

public interface IndexBuilder {

	public void add(Document doc);
	public void delete(String id);

	public IndexSearcher getIndexSearcher();

	public void close();

	public void index(RDFNode rdfNode, String indexStr);
	public void index(RDFNode rdfNode, Reader indexStream);
	public void index(Node node, String indexStr);
	public void index(Node node, Reader indexStream);
	public void unindex(RDFNode node, Reader indexStream);
	public void unindex(RDFNode node, String indexStr);
	public void unindex(Node node, Reader indexStream);
	public void unindex(Node node, String indexStr);
	
}