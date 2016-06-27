package org.geotools.data.complex;

import java.util.HashMap;

public class NestedMappings extends HashMap<String, FeatureTypeMapping>{
	private FeatureTypeMapping rootMapping;

	public NestedMappings(FeatureTypeMapping rootMapping) {
		super();
		this.rootMapping = rootMapping;
	}

	public FeatureTypeMapping getRootMapping() {
		return rootMapping;
	}

	public void setRootMapping(FeatureTypeMapping rootMapping) {
		this.rootMapping = rootMapping;
	}
	
	
}
