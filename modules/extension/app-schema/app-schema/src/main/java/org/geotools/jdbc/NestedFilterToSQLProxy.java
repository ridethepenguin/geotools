package org.geotools.jdbc;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.geotools.data.complex.AppSchemaDataAccess;
import org.geotools.data.complex.AttributeMapping;
import org.geotools.data.complex.FeatureTypeMapping;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.joining.JoiningNestedAttributeMapping;
import org.geotools.factory.Hints;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.jdbc.JoiningJDBCFeatureSource.JoiningFieldEncoder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.Filter;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNull;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class NestedFilterToSQLProxy implements MethodInterceptor {
	FeatureTypeMapping rootMapping;
	
	
	
	public NestedFilterToSQLProxy(FeatureTypeMapping rootMapping) {
		super();
		this.rootMapping = rootMapping;
	}
	private String[] getAttributesXPath(Filter filter) {
		FilterAttributeExtractor extractor = new FilterAttributeExtractor();
        filter.accept(extractor, null);
		return extractor.getAttributeNames();
	}
	private boolean hasNestedAttributes(String[] xpaths) {
		for(String xpath : xpaths) {
			if(xpath.indexOf("/") != -1) {
				return true;
			}
		}
		return false;
	}
	
	public static Field getField(Class clazz, String fieldName) throws NoSuchFieldException {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                throw e;
            } else {
                return getField(superClass, fieldName);
            }
        }
    }
    public static void makeAccessible(Field field) {
        if (!Modifier.isPublic(field.getModifiers()) ||
            !Modifier.isPublic(field.getDeclaringClass().getModifiers()))
        {
            field.setAccessible(true);
        }
    }
	
    private Object visitComparison(Filter filter, Writer out, Object extraData, String xpath) {
		try {

	        
			out.write("EXISTS (");
			
			
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
    
    private void createWhereClause(Filter filter, String filterProperty,
			String newFilterProperty, FeatureTypeMapping currentTypeMapping,
			SimpleFeatureType lastType, JDBCDataStore store, StringBuffer sql)
			throws FilterToSQLException {
		NestedToSimpleFilterVisitor duplicate = new NestedToSimpleFilterVisitor(filterProperty, newFilterProperty);
		
		Filter duplicated = (Filter)filter.accept(duplicate, null);
		Filter unrolled = AppSchemaDataAccess.unrollFilter(duplicated, currentTypeMapping);
		JoiningFieldEncoder fieldEncoder = new JoiningFieldEncoder(lastType.getTypeName(), store);
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



	public void encodeColumnName(JDBCDataStore store, String colName, String typeName, StringBuffer sql, Hints hints) throws SQLException{
        
        store.encodeTableName(typeName, sql, hints);                
        sql.append(".");
        store.dialect.encodeColumnName(colName, sql);
        
    }
	@Override
	public Object intercept(Object obj, Method method, Object[] arguments,
			MethodProxy proxy) throws Throwable {
		Field field = getField(obj.getClass(), "out");
		field.setAccessible(true);
		Writer writer = (Writer)field.get(obj);
		if(method.getName().equals("visit") && arguments.length > 1 && isComparison(arguments[0])) {
			Filter filter = (Filter)arguments[0];
			String[] xpath = getAttributesXPath(filter);
			if(!hasNestedAttributes(xpath)) {
				return proxy.invokeSuper(obj, arguments);
			}
			return visitComparison(filter, writer, arguments[1], xpath[0]);
		}
		return proxy.invokeSuper(obj, arguments);
	}
	
	private boolean isComparison(Object filter) {
		return filter instanceof BinaryComparisonOperator ||
				filter instanceof PropertyIsLike ||
				filter instanceof PropertyIsBetween ||
				filter instanceof PropertyIsNull;
	}

}
