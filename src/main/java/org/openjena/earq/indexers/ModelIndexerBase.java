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

package org.openjena.earq.indexers;

import org.openjena.earq.EARQ;
import org.openjena.earq.IndexBuilder;
import org.openjena.earq.IndexSearcher;
import org.openjena.earq.builders.IndexBuilderFactory;
import org.openjena.earq.searchers.IndexSearcherFactory;

import com.hp.hpl.jena.rdf.listeners.StatementListener;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public abstract class ModelIndexerBase extends StatementListener implements ModelIndexer {

    protected IndexBuilder builder ;
//    protected IndexSearcher searcher ;
    
    public ModelIndexerBase(String location) { 
    	builder = IndexBuilderFactory.create(EARQ.TYPE, location);
//    	searcher = builder.getIndexSearcher();
    }

    @Override public abstract void indexStatement(Statement s) ;
    @Override public abstract void unindexStatement(Statement s) ;
    
    @Override
    public void addedStatement(Statement s) { 
    	indexStatement(s) ; 
    }
    
    @Override
    public void removedStatement(Statement s) { 
    	unindexStatement(s) ; 
    }

    @Override 
    public void indexStatements(StmtIterator sIter) {
        for ( ; sIter.hasNext() ; ) {
            indexStatement(sIter.nextStatement()) ;
        }
    }
    
    @Override 
    public void unindexStatements(StmtIterator sIter) {
        for ( ; sIter.hasNext() ; ) {
            unindexStatement(sIter.nextStatement()) ;
        }
    }
    
    @Override
    public IndexSearcher getIndexSearcher() {
    	// TODO do something clever here... if the index has not been changed return the same object?
    	return IndexSearcherFactory.create(builder);
    }
    
    @Override
    public void close() {
    	builder.close();
    }

}
