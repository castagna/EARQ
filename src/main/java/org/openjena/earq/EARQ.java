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

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.openjena.earq.pfunction.search;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Node_Blank;
import com.hp.hpl.jena.graph.Node_Literal;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.sparql.ARQConstants;
import com.hp.hpl.jena.sparql.pfunction.PropertyFunctionRegistry;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.sparql.util.NodeFactory;
import com.hp.hpl.jena.sparql.util.Symbol;

public class EARQ {

	/** The EARQ property function library URI space */
    public static final String EARQPropertyFunctionLibraryURI = "http://openjena.org/EARQ/property#" ;
    
    public static enum Type { LUCENE, SOLR, ELASTICSEARCH };
    
    public static Type TYPE = Type.ELASTICSEARCH;
    
    static {
    	PropertyFunctionRegistry.get().put(EARQPropertyFunctionLibraryURI + "search", search.class);
    }

    public static final String fId                 = "id" ;

    public static final String fScore              = "score" ;
    
    // The field that is the index
    public static final String fText               = "text" ;

    // Object literals
    public static final String fLex                 = "lex" ;
    public static final String fLang                = "lang" ;
    public static final String fDataType            = "datatype" ;
    // Object URI
    public static final String fURI                 = "uri" ;
    // Object bnode
    public static final String fBNodeID             = "bnode" ;

    // The symbol used to register the index in the query context
    public static final Symbol indexKey     = ARQConstants.allocSymbol(TYPE.toString()) ;

    public static void setDefaultIndex(IndexSearcher index) { 
    	setDefaultIndex(ARQ.getContext(), index) ; 
    }
    
    public static void setDefaultIndex(Context context, IndexSearcher index) { 
    	context.set(EARQ.indexKey, index) ; 
    }
    
    public static IndexSearcher getDefaultIndex() { 
    	return getDefaultIndex(ARQ.getContext()) ; 
    }
    
    public static IndexSearcher getDefaultIndex(Context context) { 
    	return (IndexSearcher)context.get(EARQ.indexKey) ; 
    }
    
    public static void removeDefaultIndex() { 
    	removeDefaultIndex(ARQ.getContext()) ; 
    }
    
    public static void removeDefaultIndex(Context context) { 
    	context.unset(EARQ.indexKey) ; 
    }

    public static void index(Document doc, Node indexNode) {
        if ( ! indexNode.isLiteral() ) {
            throw new EARQException("Not a literal: " + indexNode) ;
        }
        index(doc, indexNode, indexNode.getLiteralLexicalForm()) ;
    }        
     
    public static void index(Document doc, Node node, String indexContent) {
    	doc.set(EARQ.fId, hash(node, indexContent));
        doc.set(EARQ.fText, indexContent);
    }        
     
    public static void index(Document doc, Node node, Reader indexContent) {
    	String content = read(indexContent);
    	doc.set(EARQ.fId, hash(node, content));
        doc.set(EARQ.fText, content) ;
    }

	public static String unindex(Node node, String indexStr) {
		return hash(node, indexStr);
	}

	public static String unindex(Node node, Reader indexContent) {
		return hash(node, indexContent);
	}
    
    public static void store(Document doc, Node node) {
        if ( node.isLiteral() ) {
            storeLiteral(doc, (Node_Literal)node) ;
        } else if ( node.isURI() ) {
            storeURI(doc, (Node_URI)node) ;
        } else if ( node.isBlank() ) {
            storeBNode(doc, (Node_Blank)node) ;
        } else {
            throw new EARQException("Can't store: "+node) ;
        }
    }

    public static Node build(Document doc) {
        String lex = doc.get(EARQ.fLex) ;
        if ( lex != null ) {
            return buildLiteral(doc) ;
        }
        String uri = doc.get(EARQ.fURI) ;
        if ( uri != null ) {
            return Node.createURI(uri) ;
        }
        String bnode = doc.get(EARQ.fBNodeID) ;
        if ( bnode != null ) {
            return Node.createAnon(new AnonId(bnode)) ;
        }
        throw new EARQException("Can't build: " + doc) ;
    }
    
    public static boolean isString(Literal literal) {
        RDFDatatype dtype = literal.getDatatype() ;
        if ( dtype == null ) {
            return true ;
        }
        if ( dtype.equals(XSDDatatype.XSDstring) ) {
            return true ;
        }
        return false ;
    }
    
    private static void storeURI(Document doc, Node_URI node) { 
        String x = node.getURI() ;
        // TODO check this
        // doc.set(EARQ.fText, x) ;
        doc.set(EARQ.fURI, x) ;
    }

    private static void storeBNode(Document doc, Node_Blank node) { 
        String x = node.getBlankNodeLabel() ;
        // TODO check this
        // doc.set(EARQ.fText, x) ;
        doc.set(EARQ.fBNodeID, x) ;
    }
    
    private static void storeLiteral(Document doc, Node_Literal node) {
        String lex = node.getLiteralLexicalForm() ;
        String datatype = node.getLiteralDatatypeURI() ;
        String lang = node.getLiteralLanguage() ;

        doc.set(EARQ.fLex, lex) ;
        
        if ( lang != null ) {
            doc.set(EARQ.fLang, lang) ;
        }

        if ( datatype != null ) {
            doc.set(EARQ.fDataType, datatype) ;
        }
    }
    
    private static Node buildLiteral(Document doc) {
        String lex = doc.get(EARQ.fLex) ;
        if ( lex == null ) {
            return null ;
        }
        String datatype = doc.get(EARQ.fDataType) ;
        String lang = doc.get(EARQ.fLang) ;
        return NodeFactory.createLiteralNode(lex, lang, datatype) ;
    }

    private static String hash (String str) 
    {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
            digest.update(str.getBytes("UTF8"));
            byte[] hash = digest.digest();
            BigInteger bigInt = new BigInteger(hash);
            return Long.toString(bigInt.longValue());
        } catch (NoSuchAlgorithmException e) {
        	new EARQException("hash", e);
        } catch (UnsupportedEncodingException e) {
        	new EARQException("hash", e);
        }

        return null;
    }
    
    private static String hash (Node node, String str) 
    {
        String lexForm = null ; 
        String datatypeStr = "" ;
        String langStr = "" ;
        
        if ( node.isURI() ) {
        	lexForm = node.getURI() ;
        } else if ( node.isLiteral() ) {
        	lexForm = node.getLiteralLexicalForm() ;
            datatypeStr = node.getLiteralDatatypeURI() ;
            langStr = node.getLiteralLanguage() ;
        } else if ( node.isBlank() ) {
        	lexForm = node.getBlankNodeLabel() ;
        } else {
        	throw new EARQException("Unable to hash node:"+node) ;
        }

        return hash (lexForm + "|" + langStr + "|" + datatypeStr + "|" + str);
    }
    
    private static String hash (Node node, Reader reader)
    {
        String lexForm = null ; 
        String datatypeStr = "" ;
        String langStr = "" ;
        
        if ( node.isURI() ) {
        	lexForm = node.getURI() ;
        } else if ( node.isLiteral() ) {
        	lexForm = node.getLiteralLexicalForm() ;
            datatypeStr = node.getLiteralDatatypeURI() ;
            langStr = node.getLiteralLanguage() ;
        } else if ( node.isBlank() ) {
        	lexForm = node.getBlankNodeLabel() ;
        } else {
        	throw new EARQException("Unable to hash node:"+node) ;
        }
    	
    	StringBuffer sb = new StringBuffer();
		try {
	        int charsRead;
			do {
		    	char[] buffer = new char[1024];
		        int offset = 0;
		        int length = buffer.length;
		        charsRead = 0;
				while (offset < buffer.length) {
					charsRead = reader.read(buffer, offset, length);
					if (charsRead == -1)
						break;
					offset += charsRead;
					length -= charsRead;
				}
				sb.append(buffer);
			} while (charsRead != -1);
			reader.reset();
		} catch (IOException e) {
			new EARQException("hash", e);
		}
		
		return hash (lexForm + "|" + langStr + "|" + datatypeStr + "|" + sb.toString());
    }
    
    private static String read(Reader reader) {
    	StringBuffer sb = new StringBuffer();
		try {
	        int charsRead;
			do {
		    	char[] buffer = new char[1024];
		        int offset = 0;
		        int length = buffer.length;
		        charsRead = 0;
				while (offset < buffer.length) {
					charsRead = reader.read(buffer, offset, length);
					if (charsRead == -1)
						break;
					offset += charsRead;
					length -= charsRead;
				}
				sb.append(buffer);
			} while (charsRead != -1);
			reader.reset();
		} catch (IOException e) {
			new EARQException("read", e);
		}

		return sb.toString();
    }
    
}