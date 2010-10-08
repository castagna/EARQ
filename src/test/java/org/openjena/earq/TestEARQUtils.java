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

import java.util.Iterator;

import org.openjena.atlas.lib.StrUtils;
import org.openjena.earq.indexers.ModelIndexer;
import org.openjena.earq.searchers.IndexSearcherFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.iterator.NiceIterator;

public class TestEARQUtils {

    public static QueryExecution query(Model model, String pattern) { 
    	return query(model, pattern, null) ; 
    }

    public static QueryExecution query(Model model, String pattern, IndexSearcher index) {
        String queryString = StrUtils.strjoin("\n", 
            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>" ,
            "PREFIX : <http://example/>" ,
            "PREFIX earq: <http://openjena.org/EARQ/property#>",
            "PREFIX dc:<http://purl.org/dc/elements/1.1/>",
            "SELECT *",
            pattern ) ;
        Query query = QueryFactory.create(queryString) ;
        QueryExecution qExec = QueryExecutionFactory.create(query, model) ;
        if ( index != null )
            EARQ.setDefaultIndex(qExec.getContext(), index) ;
        return qExec ;
    }
    
    public static int count(ResultSet rs) {
        return ResultSetFormatter.consume(rs) ;
    }
    
    public static int count(Iterator<?> iter) {
        int count = 0 ; 
        for ( ; iter.hasNext() ; ) {
            iter.next();
            count++ ;
        }
        NiceIterator.close(iter) ;
        return count ;
    }
    
    public static IndexSearcher createIndex(String datafile, ModelIndexer indexer) { 
    	return createIndex(ModelFactory.createDefaultModel(), datafile, indexer) ; 
    }
    
    public static IndexSearcher createIndex(Model model, String datafile, ModelIndexer indexer) {
        model.register(indexer) ;
        FileManager.get().readModel(model, datafile) ;
        model.unregister(indexer) ;
        indexer.close() ;

        return IndexSearcherFactory.create(EARQ.DEFAULT_TYPE, "test");
    }

}