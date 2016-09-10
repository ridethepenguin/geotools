/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 * 
 *    (C) 2016, Open Source Geospatial Foundation (OSGeo)
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */

package org.geotools.jdbc;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.List;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.geotools.data.complex.FeatureTypeMapping;
import org.geotools.data.complex.NestedAttributeMapping;
import org.geotools.data.complex.filter.FeatureChainedAttributeVisitor;
import org.geotools.data.complex.filter.FeatureChainedAttributeVisitor.FeatureChainLink;
import org.geotools.data.complex.filter.FeatureChainedAttributeVisitor.FeatureChainedAttributeDescriptor;
import org.geotools.data.complex.filter.UnmappingFilterVisitor;
import org.geotools.data.complex.filter.XPath;
import org.geotools.data.complex.filter.XPathUtil.StepList;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.factory.Hints;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.filter.FilterFactoryImplNamespaceAware;
import org.geotools.jdbc.JoiningJDBCFeatureSource.JoiningFieldEncoder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.xml.sax.helpers.NamespaceSupport;

public class NestedFilterToSQLProxy implements MethodInterceptor {
    FeatureTypeMapping rootMapping;

    FilterFactory ff;

    public NestedFilterToSQLProxy(FeatureTypeMapping rootMapping) {
        super();
        this.rootMapping = rootMapping;
        this.ff = new FilterFactoryImplNamespaceAware(rootMapping.getNamespaces());
    }

    private String[] getAttributesXPath(Filter filter) {
        FilterAttributeExtractor extractor = new FilterAttributeExtractor();
        filter.accept(extractor, null);
        return extractor.getAttributeNames();
    }

    private boolean hasNestedAttributes(String[] xpaths) {
        for (String xpath : xpaths) {
            if (xpath.indexOf("/") != -1) {
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
        if (!Modifier.isPublic(field.getModifiers())
                || !Modifier.isPublic(field.getDeclaringClass().getModifiers())) {
            field.setAccessible(true);
        }
    }

    private Object visitComparison(Filter filter, Writer out, Object extraData, String xpath) {
        try {

            FeatureChainedAttributeVisitor nestedMappingsExtractor = new FeatureChainedAttributeVisitor(
                    rootMapping);
            nestedMappingsExtractor.visit(ff.property(xpath), null);
            List<FeatureChainedAttributeDescriptor> attributes = nestedMappingsExtractor.getFeatureChainedAttributes();
            // encoding of filters on multiple nested attributes is not (yet) supported
            if (attributes.size() == 1) {
                FeatureChainedAttributeDescriptor nestedAttrDescr = attributes.get(0);

                int numMappings = nestedAttrDescr.chainSize();
                if (numMappings > 0 && nestedAttrDescr.isJoiningEnabled()) {
                    out.write("EXISTS (");

                    FeatureChainLink lastMappingStep = nestedAttrDescr.getLastLink();
                    StringBuffer sql = encodeSelectKeyFrom(lastMappingStep);

                    for (int i = numMappings - 2; i > 0; i--) {
                        FeatureChainLink mappingStep = nestedAttrDescr.getLink(i);
                        if (mappingStep.hasNestedFeature()) {
                            FeatureTypeMapping parentFeature = mappingStep.getFeatureTypeMapping();
                            JDBCDataStore store = (JDBCDataStore) parentFeature.getSource()
                                    .getDataStore();
                            String parentTableName = parentFeature.getSource().getSchema()
                                    .getName().getLocalPart();

                            sql.append(" INNER JOIN ");
                            store.encodeTableName(parentTableName, sql, null);
                            sql.append(" ");
                            store.dialect.encodeTableName(mappingStep.getAlias(), sql);
                            sql.append(" ON ");
                            encodeJoinCondition(nestedAttrDescr, i, sql);
                        }
                    }

                    if (nestedAttrDescr.getAttributePath() != null) {
                        createWhereClause(filter, xpath, nestedAttrDescr, sql);

                        sql.append(" AND ");
                    } else {
                        sql.append(" WHERE ");
                    }

                    // join with root table
                    encodeJoinCondition(nestedAttrDescr, 0, sql);

                    out.write(sql.toString());
                    out.write(")");
                }
            }

            return extraData;

        } catch (java.io.IOException ioe) {
            throw new RuntimeException("Problem writing filter: ", ioe);
        } catch (SQLException e) {
            throw new RuntimeException("Problem writing filter: ", e);
        } catch (FilterToSQLException e) {
            throw new RuntimeException("Problem writing filter: ", e);
        }
    }

    private void encodeJoinCondition(FeatureChainedAttributeDescriptor nestedAttrDescr,
            int stepIdx, StringBuffer sql) throws SQLException, IOException {
        FeatureChainLink parentStep = nestedAttrDescr.getLink(stepIdx);
        FeatureChainLink nestedStep = nestedAttrDescr.getLink(stepIdx + 1);
        FeatureTypeMapping parentFeature = parentStep.getFeatureTypeMapping();
        JDBCDataStore store = (JDBCDataStore) parentFeature.getSource().getDataStore();
        NestedAttributeMapping nestedFeatureAttr = parentStep.getNestedFeatureAttribute();
        FeatureTypeMapping nestedFeature = nestedFeatureAttr.getFeatureTypeMapping(null);

        String parentTableName = parentFeature.getSource().getSchema().getName().getLocalPart();
        String parentTableAlias = parentStep.getAlias();
        // TODO: what if source expression is not a literal?
        String parentTableColumn = nestedFeatureAttr.getSourceExpression().toString();
        // String nestedTableName = nestedFeature.getSource().getSchema().getName().getLocalPart();
        String nestedTableAlias = nestedStep.getAlias();
        // TODO: what if source expression is not a literal?
        String nestedTableColumn = nestedFeatureAttr.getMapping(nestedFeature)
                .getSourceExpression().toString();

        if (stepIdx == 0) {
            encodeColumnName(store, parentTableColumn, parentTableName, sql, null);
        } else {
            encodeAliasedColumnName(store, parentTableColumn, parentTableAlias, sql, null);
        }
        sql.append(" = ");
        encodeAliasedColumnName(store, nestedTableColumn, nestedTableAlias, sql, null);
    }

    private void createWhereClause(Filter filter, String nestedProperty,
            FeatureChainedAttributeDescriptor nestedAttrDescr, StringBuffer sql)
            throws FilterToSQLException {
        FeatureChainLink lastLink = nestedAttrDescr.getLastLink();
        String simpleProperty = nestedAttrDescr.getAttributePath().toString();
        FeatureTypeMapping featureMapping = lastLink.getFeatureTypeMapping();
        JDBCDataStore store = (JDBCDataStore) featureMapping.getSource().getDataStore();
        FeatureTypeMapping featureMappingForUnrolling = nestedAttrDescr
                .getFeatureTypeOwningAttribute();
        SimpleFeatureType sourceType = (SimpleFeatureType) featureMapping.getSource().getSchema();

        NamespaceAwareAttributeRenameVisitor duplicate = new NamespaceAwareAttributeRenameVisitor(
                nestedProperty, simpleProperty);
        Filter duplicated = (Filter) filter.accept(duplicate, null);
        Filter unrolled = unrollFilter(duplicated, featureMappingForUnrolling);

        JoiningFieldEncoder fieldEncoder = new JoiningFieldEncoder(lastLink.getAlias(), store);
        FilterToSQL nestedFilterToSQL = null;
        if (store.getSQLDialect() instanceof PreparedStatementSQLDialect) {
            PreparedFilterToSQL preparedFilterToSQL = store.createPreparedFilterToSQL(sourceType);
            // disable prepared statements to have literals actually encoded in the SQL
            preparedFilterToSQL.setPrepareEnabled(false);
            nestedFilterToSQL = preparedFilterToSQL;
        } else {
            nestedFilterToSQL = store.createFilterToSQL(sourceType);
        }
        nestedFilterToSQL.setFieldEncoder(fieldEncoder);
        String encodedFilter = nestedFilterToSQL.encodeToString(unrolled);
        sql.append(" ").append(encodedFilter);
    }

    private StringBuffer encodeSelectKeyFrom(FeatureChainLink lastMappingStep) throws SQLException {
        FeatureTypeMapping lastTypeMapping = lastMappingStep.getFeatureTypeMapping();
        JDBCDataStore store = (JDBCDataStore) lastTypeMapping.getSource().getDataStore();
        SimpleFeatureType lastType = (SimpleFeatureType) lastTypeMapping.getSource().getSchema();

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
        for (PrimaryKeyColumn col : key.getColumns()) {
            colName = col.getName();
            sqlKeys.append(",");
            encodeAliasedColumnName(store, colName, lastMappingStep.getAlias(), sqlKeys, null);

        }
        if (sqlKeys.length() <= 0) {
            sql.append("*");
        } else {
            sql.append(sqlKeys.substring(1));
        }
        sql.append(" FROM ");
        store.encodeTableName(lastType.getTypeName(), sql, null);
        sql.append(" ");
        store.dialect.encodeTableName(lastMappingStep.getAlias(), sql);
        return sql;
    }

    private void encodeColumnName(JDBCDataStore store, String colName, String typeName,
            StringBuffer sql, Hints hints) throws SQLException {
        store.encodeTableName(typeName, sql, hints);
        sql.append(".");
        store.dialect.encodeColumnName(colName, sql);
    }

    private void encodeAliasedColumnName(JDBCDataStore store, String colName, String typeName,
            StringBuffer sql, Hints hints) throws SQLException {
        store.dialect.encodeTableName(typeName, sql);
        sql.append(".");
        store.dialect.encodeColumnName(colName, sql);
    }

    @Override
    public Object intercept(Object obj, Method method, Object[] arguments, MethodProxy proxy)
            throws Throwable {
        Field field = getField(obj.getClass(), "out");
        field.setAccessible(true);
        Writer writer = (Writer) field.get(obj);
        if (method.getName().equals("visit") && arguments.length > 1 && isComparison(arguments[0])) {
            Filter filter = (Filter) arguments[0];
            String[] xpath = getAttributesXPath(filter);
            if (!hasNestedAttributes(xpath)) {
                return proxy.invokeSuper(obj, arguments);
            }
            return visitComparison(filter, writer, arguments[1], xpath[0]);
        }
        return proxy.invokeSuper(obj, arguments);
    }

    private boolean isComparison(Object filter) {
        return filter instanceof BinaryComparisonOperator || filter instanceof PropertyIsLike
                || filter instanceof PropertyIsBetween || filter instanceof PropertyIsNull;
    }

    private Filter unrollFilter(Filter complexFilter, FeatureTypeMapping mappings) {
        UnmappingFilterVisitorExcludingNestedMappings visitor = new UnmappingFilterVisitorExcludingNestedMappings(
                mappings);
        Filter unrolledFilter = (Filter) complexFilter.accept(visitor, null);
        return unrolledFilter;
    }

    private class UnmappingFilterVisitorExcludingNestedMappings extends UnmappingFilterVisitor {

        public UnmappingFilterVisitorExcludingNestedMappings(FeatureTypeMapping mappings) {
            super(mappings);
        }

        @Override
        public List<Expression> visit(PropertyName expr, Object arg1) {
            String targetXPath = expr.getPropertyName();
            NamespaceSupport namespaces = mappings.getNamespaces();
            AttributeDescriptor root = mappings.getTargetFeature();

            // break into single steps
            StepList simplifiedSteps = XPath.steps(root, targetXPath, namespaces);

            List<Expression> matchingMappings = mappings.findMappingsFor(simplifiedSteps, false);

            if (matchingMappings.size() == 0) {
                throw new IllegalArgumentException("Can't find source expression for: "
                        + targetXPath);
            }

            return matchingMappings;
        }

    }
}
