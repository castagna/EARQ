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

import static org.junit.Assert.assertTrue;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openjena.earq.indexers.ModelIndexer;
import org.openjena.earq.indexers.ModelIndexerString;
import org.openjena.earq.indexers.ModelIndexerSubject;
import org.openjena.earq.searchers.IndexSearcherFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.junit.QueryTest;
import com.hp.hpl.jena.sparql.resultset.ResultSetRewindable;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.DC;

public class TestEARQ_Script {

    static final String root = "src/test/resources/EARQ/" ;
    static final String location = "test";
    
    private static Node node = null;
    
    @BeforeClass public static void startCluster() {
    	node = NodeBuilder.nodeBuilder().node();
    	node.start();
    }
    
    @AfterClass public static void stopCluster() {
    	node.stop();
    }
    
    static void runTestScript(String queryFile, String dataFile, String resultsFile, ModelIndexer indexer) {
        Query query = QueryFactory.read(root+queryFile) ;
        Model model = ModelFactory.createDefaultModel() ; 
        model.register(indexer) ;
        FileManager.get().readModel(model, root+dataFile) ;
        model.unregister(indexer) ;
        indexer.close();

        IndexSearcher searcher = IndexSearcherFactory.create(EARQ.DEFAULT_TYPE, location) ;
        EARQ.setDefaultIndex(searcher) ;

        QueryExecution qe = QueryExecutionFactory.create(query, model) ;
        ResultSetRewindable rsExpected = ResultSetFactory.makeRewindable(ResultSetFactory.load(root+resultsFile)) ;
        ResultSetRewindable rsActual = ResultSetFactory.makeRewindable(qe.execSelect()) ;
        boolean b = QueryTest.resultSetEquivalent(query, rsActual, rsExpected) ;
        if ( ! b ) {
            rsActual.reset() ;
            rsExpected.reset() ;
            System.out.println("==== Different (EARQ)") ;
            System.out.println("== Actual") ;
            ResultSetFormatter.out(rsActual) ;
            System.out.println("== Expected") ;
            ResultSetFormatter.out(rsExpected) ;
        }
        
        assertTrue(b) ;
        qe.close() ; 
        EARQ.removeDefaultIndex() ;
    }
    
    @Test public void test_larq_1() { 
    	runTestScript("larq-q-1.rq", "data-1.ttl", "results-1.srj", new ModelIndexerString(location)) ; 
    }

    @Test public void test_larq_2() { 
    	runTestScript("larq-q-2.rq", "data-1.ttl", "results-2.srj", new ModelIndexerString(DC.title, location)) ; 
    }

    @Test public void test_larq_3() { 
    	runTestScript("larq-q-3.rq", "data-1.ttl", "results-3.srj", new ModelIndexerSubject(DC.title, location)) ; 
    }
    
    @Test public void test_larq_4() { 
    	runTestScript("larq-q-4.rq", "data-1.ttl", "results-4.srj", new ModelIndexerString(location)) ; 
    }
    
    @Test public void test_larq_5() { 
    	runTestScript("larq-q-5.rq", "data-1.ttl", "results-5.srj", new ModelIndexerString(location)) ; 
    }

    @Test public void test_larq_6() { 
    	runTestScript("larq-q-6.rq", "data-1.ttl", "results-6.srj", new ModelIndexerString(location)) ; 
   	}

    @Test public void test_larq_7() { 
    	runTestScript("larq-q-7.rq", "data-1.ttl", "results-7.srj", new ModelIndexerString(location)) ; 
    }

}
