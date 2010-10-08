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

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Statement;

public class ModelIndexerString extends ModelIndexerLiteralBase {
    private Property property = null ;

    public ModelIndexerString(String location) { 
    	super(location) ; 
    }

    public ModelIndexerString(Property property, String url) { 
    	super(url) ; 
    	setProperty(property) ; 
    }

    @Override
    protected boolean indexThisStatement(Statement stmt) { 
        if ( property == null ) {
            return true ;
        }
        return stmt.getPredicate().equals(property) ;
    }

    private void setProperty(Property p) { 
    	property = p ; 
    }
    
    @Override
    protected boolean indexThisLiteral(Literal literal) { 
    	return EARQ.isString(literal) ; 
    }

}
