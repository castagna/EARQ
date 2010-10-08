package org.openjena.earq;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Document {

	private Map<String, String> fields = new HashMap<String, String>();

	public void set (String name, String value) { 
		fields.put(name, value); 
	}
	
	public String get (String name) { 
		return fields.get(name); 
	}
	
	public Set<String> getNames() {
		return fields.keySet();
	}
	
}
