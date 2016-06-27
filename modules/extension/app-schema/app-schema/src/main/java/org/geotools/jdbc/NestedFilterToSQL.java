package org.geotools.jdbc;

import static org.geotools.filter.capability.FunctionNameImpl.parameter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.geotools.data.complex.AppSchemaDataAccess;
import org.geotools.data.complex.AttributeMapping;
import org.geotools.data.complex.FeatureTypeMapping;
import org.geotools.data.complex.NestedMappings;
import org.geotools.data.complex.filter.XPathUtil;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.joining.JoiningNestedAttributeMapping;
import org.geotools.data.joining.JoiningQuery.QueryJoin;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.filter.FilterFactoryImplNamespaceAware;
import org.geotools.filter.FunctionImpl;
import org.geotools.filter.NestedAttributeExpression;
import org.geotools.filter.capability.FunctionNameImpl;
import org.geotools.filter.visitor.DuplicatingFilterVisitor;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;

public class NestedFilterToSQL extends FilterToSQL {
	private FilterToSQL filterToSQL;
	NestedMappings nestedMappings;

	public NestedFilterToSQL(FilterToSQL filterToSQL, NestedMappings nestedMappings) {
		super();
		this.filterToSQL = filterToSQL;
		this.nestedMappings = nestedMappings;
	}

	
	protected class NestedFieldEncoder implements FilterToSQL.FieldEncoder {
        
        private String tableName;
        private JDBCDataStore store;
        
        public NestedFieldEncoder(String tableName, JDBCDataStore store) {
            this.tableName = tableName;
            this.store = store;
        }
        
        public String encode(String s) {
           StringBuffer buf = new StringBuffer();
           try {
			store.encodeTableName(tableName, buf, null);
           } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
           }           
           buf.append(".");
           buf.append(s);
           return buf.toString();
        }
    }
	
	
	private Object[] getMapping(NestedMappings mappings, int count, String[] attributesPath) {
		FeatureTypeMapping mapping = null;
		while(mapping == null && count < attributesPath.length) {
			mapping = mappings.get(attributesPath[count]);
			count += 1;
		}
		return new Object[] {mapping, count};
	}
	
	public Object visit(PropertyIsEqualTo filter, Object extraData) {
		try {

	        FilterAttributeExtractor extractor = new FilterAttributeExtractor();
	        filter.accept(extractor, null);
			String xpath = extractor.getAttributeNames()[0];
			
			if(xpath.indexOf("/") == -1) {
				return super.visit(filter, extraData);
			}
			out.write("EXISTS (");
			
			
			FeatureTypeMapping rootMapping = nestedMappings.getRootMapping();
			List<AttributeMapping> mappings = new ArrayList<AttributeMapping>();
			List<AttributeMapping> rootAttributes = rootMapping.getAttributeMappings();
			
			List<AttributeMapping> currentAttributes = rootAttributes;
			String currentXPath = xpath;
			FeatureTypeMapping currentTypeMapping = rootMapping;
			boolean found = true;
			while (currentXPath.indexOf("/") != -1 && found) {
				found = false;
				int pos = 0;
				AttributeMapping currentAttribute = null;
				for(AttributeMapping attributeMapping : currentAttributes) {
					String targetXPath = attributeMapping.getTargetXPath().toString();
					if(currentXPath.startsWith(targetXPath)) {
						if(attributeMapping instanceof JoiningNestedAttributeMapping) {
							String nestedFeatureType = ((JoiningNestedAttributeMapping)attributeMapping).getNestedFeatureTypeName(null).toString();
							 
							if(currentXPath.startsWith(targetXPath + "/" + nestedFeatureType)) {
								pos += targetXPath.length() + nestedFeatureType.length() + 2;
								currentAttribute = attributeMapping;
								found = true;
							}
						}
					}

				}
				if(currentAttribute != null) {
					mappings.add(currentAttribute);
					currentTypeMapping = ((JoiningNestedAttributeMapping)currentAttribute).getNestedFeatureType();
				}
				currentXPath = currentXPath.substring(pos);
				currentAttributes = currentTypeMapping.getAttributeMappings();
			}
			
			
			SimpleFeatureType lastType = (SimpleFeatureType)currentTypeMapping.getSource().getSchema();
			JDBCDataStore store = (JDBCDataStore)currentTypeMapping.getSource().getDataStore();
			
			StringBuffer sql = encodeSelectKeyFrom(lastType, store);
	        for(int i = 0; i < mappings.size() - 1; i++) {
	        	JoiningNestedAttributeMapping leftJoin = (JoiningNestedAttributeMapping)mappings.get(i);
	        	String leftTableName = leftJoin.getNestedFeatureType().getSource().getSchema().getName().getLocalPart();
	        	JoiningNestedAttributeMapping rightJoin = (JoiningNestedAttributeMapping)mappings.get(i + 1);
	        	String rightTableName = rightJoin.getNestedFeatureType().getSource().getSchema().getName().getLocalPart();
	        	sql.append(" INNER JOIN ");
	        	
				store.encodeTableName(leftTableName, sql, null);
	        	sql.append(" ON ");
	        	encodeColumnName(store, rightJoin.getSourceExpression().toString(), leftTableName, sql, null);
	        	sql.append(" = ");
	        	encodeColumnName(store, rightJoin.getMapping(rightJoin.getNestedFeatureType()).getSourceExpression().toString(), rightTableName, sql, null);
	        }
	        if(!currentXPath.equals("")) {
	        	createWhereClause(filter, xpath, currentXPath,
						currentTypeMapping, lastType, store, sql);
	        	
	        	JoiningNestedAttributeMapping firstJoin = (JoiningNestedAttributeMapping)mappings.get(0);
	        	
	        	sql.append(" AND ");
	        	encodeColumnName(store, firstJoin.getSourceExpression().toString(), rootMapping.getSource().getSchema().getName().getLocalPart(), sql, null);
	        	sql.append(" = ");
	        	encodeColumnName(store, firstJoin.getMapping(firstJoin.getNestedFeatureType()).getSourceExpression().toString() , firstJoin.getNestedFeatureType().getSource().getSchema().getName().getLocalPart(), sql, null);
			}
	        out.write(sql.toString());
	        out.write(")");
	        return extraData;
			
		} catch (java.io.IOException ioe) {
            throw new RuntimeException("Problem writing filter: ", ioe);
        } catch (SQLException e) {
        	throw new RuntimeException("Problem writing filter: ", e);
		} catch (FilterToSQLException e) {
			throw new RuntimeException("Problem writing filter: ", e);
		}
	}

	private void createWhereClause(PropertyIsEqualTo filter, String filterProperty,
			String newFilterProperty, FeatureTypeMapping currentTypeMapping,
			SimpleFeatureType lastType, JDBCDataStore store, StringBuffer sql)
			throws FilterToSQLException {

		NestedToSimpleFilterVisitor duplicate = new NestedToSimpleFilterVisitor(filterProperty, newFilterProperty);
		
		Filter duplicated = (Filter)filter.accept(duplicate, null);
		Filter unrolled = AppSchemaDataAccess.unrollFilter(duplicated, currentTypeMapping);
		NestedFieldEncoder fieldEncoder = new NestedFieldEncoder(lastType.getTypeName(), store);
		FilterToSQL nestedFilterToSQL = store.createFilterToSQL(lastType);
		nestedFilterToSQL.setFieldEncoder(fieldEncoder);
		sql.append(" ").append(nestedFilterToSQL.encodeToString(unrolled));
	}

	private StringBuffer encodeSelectKeyFrom(SimpleFeatureType lastType,
			JDBCDataStore store) throws SQLException {
		// primary key
		PrimaryKey key = null;

		try {
		    key = store.getPrimaryKey(lastType);
		} catch (IOException e) {
		    throw new RuntimeException(e);
		}
		StringBuffer sql = new StringBuffer();
		sql.append("SELECT ");
		StringBuffer sqlKeys = new StringBuffer();
		String colName;
		for ( PrimaryKeyColumn col : key.getColumns() ) {
		    colName = col.getName();
		    sqlKeys.append(",");
		    encodeColumnName(store, colName, lastType.getTypeName(), sqlKeys, null);
		    
		}
		sql.append(sqlKeys.substring(1));
		sql.append(" FROM ");
		store.encodeTableName(lastType.getTypeName(), sql, null);
		return sql;
	}

	
	
	/*@Override
	public Object visit(PropertyIsEqualTo filter, Object extraData) {
		try {

	        FilterAttributeExtractor extractor = new FilterAttributeExtractor();
			String[] nestedAttribute = extractNestedAttribute(filter, extractor);
			if(nestedAttribute == null) {
				return super.visit(filter, extraData);
			}
			out.write("EXISTS (");
			List<Object[]> mappings = new ArrayList<Object[]>();
			Object[] result = getMapping(nestedMappings, 0, nestedAttribute);
			FeatureTypeMapping mapping = (FeatureTypeMapping)result[0];
			int count = (Integer)result[1];
			int rootCount = count;
			FeatureTypeMapping rootMapping = nestedMappings.getRootMapping();
			while(mapping != null && count<nestedAttribute.length) {
				Object[] newResult = getMapping(getNestedMappings(mapping), count, nestedAttribute);
				if(newResult[0] != null) {
					mapping = (FeatureTypeMapping)newResult[0];
					mappings.add(result);
					result = newResult;
				}
	    		count = (Integer)newResult[1];
			}
			
			AttributeMapping foundAttribute = null;
			for(AttributeMapping attribute : mapping.getAttributeMappings()) {
				if(attribute.getTargetXPath().toString().startsWith("FEATURE_LINK[")) {
					foundAttribute = attribute;
				}
			}
			SimpleFeatureType nestedType = (SimpleFeatureType)mapping.getSource().getSchema();
			JDBCDataStore store = (JDBCDataStore)mapping.getSource().getDataStore();
			 // primary key
	        PrimaryKey key = null;
	
	        try {
	            key = store.getPrimaryKey(nestedType);
	        } catch (IOException e) {
	            throw new RuntimeException(e);
	        }
	        Set<String> pkColumnNames = new HashSet<String>();
	        StringBuffer sql = new StringBuffer();
	        sql.append("SELECT ");
	        StringBuffer sqlKeys = new StringBuffer();
	        String colName;
	        for ( PrimaryKeyColumn col : key.getColumns() ) {
	            colName = col.getName();
	            sqlKeys.append(",");
	            encodeColumnName(store, colName, nestedType.getTypeName(), sqlKeys, null);
	            
	        }
	        sql.append(sqlKeys.substring(1));
	        sql.append(" FROM ");
	        store.encodeTableName(nestedType.getTypeName(), sql, null);
	        AttributeMapping lastAttribute = foundAttribute;
	        String lastTableName = nestedType.getTypeName();
	        for(Object[] current : mappings) {
	        	FeatureTypeMapping currentMapping = (FeatureTypeMapping)current[0];
	        	sql.append(" INNER JOIN ");
	        	store.encodeTableName(currentMapping.getSource().getSchema().getName().getLocalPart(), sql, null);
	        	sql.append(" ON ");
	        	encodeColumnName(store, lastAttribute.getSourceExpression().toString(), lastTableName, sql, null);
	        	sql.append(" = ");
	        	
	        	lastTableName = currentMapping.getSource().getSchema().getName().getLocalPart();
	        	encodeColumnName(store, lastAttribute.getSourceExpression().toString(), lastTableName, sql, null);
	        	lastAttribute = null;
	    		for(AttributeMapping attribute : currentMapping.getAttributeMappings()) {
	    			if(attribute.getTargetXPath().toString().startsWith("FEATURE_LINK[")) {
	    				lastAttribute = attribute;
	    			}
	    		}
	        }                
	        
	        final String filterProperty = StringUtils.join(nestedAttribute, "/");
	        final String newFilterProperty = nestedAttribute[nestedAttribute.length - 1];
	        NestedToSimpleFilterVisitor duplicate = new NestedToSimpleFilterVisitor(filterProperty, newFilterProperty);
	        
	        Filter duplicated = (Filter)filter.accept(duplicate, null);
	        Filter unrolled = AppSchemaDataAccess.unrollFilter(duplicated, mapping);
	        NestedFieldEncoder fieldEncoder = new NestedFieldEncoder(nestedType.getTypeName(), store);
	        FilterToSQL nestedFilterToSQL = store.createFilterToSQL(nestedType);
	        nestedFilterToSQL.setFieldEncoder(fieldEncoder);
	        sql.append(" ").append(nestedFilterToSQL.encodeToString(unrolled));
	        
	        AttributeMapping foundRootAttribute = findAttribute(
					nestedAttribute, 0, rootCount - 1 , rootMapping);
	        if(foundRootAttribute != null) {
	        	sql.append(" AND ");
	        	encodeColumnName(store, foundRootAttribute.getSourceExpression().toString(), rootMapping.getSource().getSchema().getName().getLocalPart(), sql, null);
	        	sql.append(" = ");
	        	encodeColumnName(store, lastAttribute.getSourceExpression().toString(), lastTableName, sql, null);
	        }
	       
	        out.write(sql.toString());
	        //filter.accept(this, null);
	        out.write(")");
	        return extraData;
		} catch (java.io.IOException ioe) {
            throw new RuntimeException("Problem writing filter: ", ioe);
        } catch (SQLException e) {
        	throw new RuntimeException("Problem writing filter: ", e);
		} catch (FilterToSQLException e) {
			throw new RuntimeException("Problem writing filter: ", e);
		}
	}*/



	/*private String[] extractNestedAttribute(PropertyIsEqualTo filter,
			FilterAttributeExtractor extractor) {
		String attribute = null;
		filter.accept(extractor, null);
		String[] nestedAttribute = null;
		for(String attribute : extractor.getAttributeNames()) {
			if(attribute.indexOf("/") != -1) {
				nestedAttribute = attribute.split("/");
			}
		}
		return nestedAttribute;
	}*/

	private AttributeMapping findAttribute(String[] nestedAttribute,
			int startCount, int endCount, FeatureTypeMapping rootMapping) {
		AttributeMapping foundRootAttribute = null;
		for(AttributeMapping attribute : rootMapping.getAttributeMappings()) {
			if(attribute.getTargetXPath().toString().equals(joinPath(nestedAttribute, startCount, endCount))) {
				foundRootAttribute = attribute;
			}
		}
		return foundRootAttribute;
	}
	
    private String joinPath(String[] path, int start, int end) {
		return StringUtils.join(path, "/", start, end);
	}

	private NestedMappings getNestedMappings(FeatureTypeMapping mapping) throws IOException {
    	NestedMappings nestedMappings = new NestedMappings(mapping);
        // NC - joining nested atts
        for (AttributeMapping attMapping : mapping.getAttributeMappings()) {

            if (attMapping instanceof JoiningNestedAttributeMapping) {
            	JoiningNestedAttributeMapping joiningMapping = (JoiningNestedAttributeMapping) attMapping;
            	FeatureTypeMapping ftm = joiningMapping.getNestedFeatureType();
            	nestedMappings.put(joiningMapping.getNestedFeatureTypeName(null).toString(), ftm);
            }

        }
        return nestedMappings;
	}

	public void encodeColumnName(JDBCDataStore store, String colName, String typeName, StringBuffer sql, Hints hints) throws SQLException{
        
        store.encodeTableName(typeName, sql, hints);                
        sql.append(".");
        store.dialect.encodeColumnName(colName, sql);
        
    }
    
	/**
     * Common implementation for BinaryComparisonOperator filters.  This way
     * they're all handled centrally.
     *  
     *  DJB: note, postgis overwrites this implementation because of the way
     *       null is handled.  This is for <PropertyIsNull> filters and <PropertyIsEqual> filters
     *       are handled.  They will come here with "property = null".  
     *       NOTE: 
     *        SELECT * FROM <table> WHERE <column> isnull;  -- postgresql
     *        SELECT * FROM <table> WHERE isnull(<column>); -- oracle???
     *
     * @param filter the comparison to be turned into SQL.
     *
     * @throws RuntimeException for io exception with writer
     */
    protected void visitBinaryComparisonOperator(BinaryComparisonOperator filter, Object extraData) throws RuntimeException {

        Expression left = filter.getExpression1();
        Expression right = filter.getExpression2();
        Class leftContext = null, rightContext = null;
        if (left instanceof PropertyName) {
            // aha!  It's a propertyname, we should get the class and pass it in
            // as context to the tree walker.
            AttributeDescriptor attType = (AttributeDescriptor)left.evaluate(featureType);
            if (attType != null) {
                rightContext = attType.getType().getBinding();
            }
        }
        
        
        if (right instanceof PropertyName) {
            AttributeDescriptor attType = (AttributeDescriptor)right.evaluate(featureType);
            if (attType != null) {
                leftContext = attType.getType().getBinding();
            }
        }
        

        //case sensitivity
        boolean matchCase = true;
        if ( !filter.isMatchingCase() ) {
            //we only do for = and !=
            if ( filter instanceof PropertyIsEqualTo || 
                    filter instanceof PropertyIsNotEqualTo ) {
                //and only for strings
                if ( String.class.equals( leftContext ) 
                        || String.class.equals( rightContext ) ) {
                    matchCase = false;
                }
            }
        }
        
        String type = (String) extraData;

        try {
            if ( matchCase ) {
                left.accept(this, leftContext);
                
                out.write(" " + type + " ");

                right.accept(this, rightContext);
            }
            else {
                // wrap both sides in "lower"
                FunctionImpl f = new FunctionImpl() {
                    {
                        functionName = new FunctionNameImpl("lower",
                                parameter("lowercase", String.class),
                                parameter("string", String.class));
                    }
                };
                f.setName("lower");
                
                f.setParameters(Arrays.asList(left));
                f.accept(this, Arrays.asList(leftContext));
                
                out.write(" " + type + " ");
                
                f.setParameters(Arrays.asList(right));
                f.accept(this, Arrays.asList(rightContext));
            }
            
        } catch (java.io.IOException ioe) {
            throw new RuntimeException(IO_ERROR, ioe);
        }
    }
}
