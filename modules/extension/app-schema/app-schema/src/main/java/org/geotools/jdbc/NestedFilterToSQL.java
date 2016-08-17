package org.geotools.jdbc;

import static org.geotools.filter.capability.FunctionNameImpl.parameter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.geotools.data.complex.AppSchemaDataAccess;
import org.geotools.data.complex.AttributeMapping;
import org.geotools.data.complex.FeatureTypeMapping;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.joining.JoiningNestedAttributeMapping;
import org.geotools.factory.Hints;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.FunctionImpl;
import org.geotools.filter.capability.FunctionNameImpl;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.And;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Not;
import org.opengis.filter.Or;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNil;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.NilExpression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.filter.temporal.After;
import org.opengis.filter.temporal.AnyInteracts;
import org.opengis.filter.temporal.Before;
import org.opengis.filter.temporal.Begins;
import org.opengis.filter.temporal.BegunBy;
import org.opengis.filter.temporal.During;
import org.opengis.filter.temporal.EndedBy;
import org.opengis.filter.temporal.Ends;
import org.opengis.filter.temporal.Meets;
import org.opengis.filter.temporal.MetBy;
import org.opengis.filter.temporal.OverlappedBy;
import org.opengis.filter.temporal.TContains;
import org.opengis.filter.temporal.TEquals;
import org.opengis.filter.temporal.TOverlaps;

public class NestedFilterToSQL extends FilterToSQL {
    FeatureTypeMapping rootMapping;

    FilterToSQL original;

    public NestedFilterToSQL(FeatureTypeMapping rootMapping, FilterToSQL original) {
        super();
        this.rootMapping = rootMapping;
        this.original = original;
    }

    public void encode(Filter filter) throws FilterToSQLException {
        if (out == null)
            throw new FilterToSQLException("Can't encode to a null writer.");
        original.setWriter(out);
        if (original.getCapabilities().fullySupports(filter)) {

            try {
                if (!inline) {
                    out.write("WHERE ");
                }

                filter.accept(this, null);

                // out.write(";");
            } catch (java.io.IOException ioe) {
                throw new FilterToSQLException("Problem writing filter: ", ioe);
            }
        } else {
            throw new FilterToSQLException("Filter type not supported");
        }
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

    protected Object visitBinaryComparison(Filter filter, Object extraData, String xpath) {
//        try {

//            out.write("EXISTS (");
//
//            List<AttributeMapping> mappings = new ArrayList<AttributeMapping>();
//            List<AttributeMapping> rootAttributes = rootMapping.getAttributeMappings();
//
//            List<AttributeMapping> currentAttributes = rootAttributes;
//            String currentXPath = xpath;
//            FeatureTypeMapping currentTypeMapping = rootMapping;
//            boolean found = true;
//            while (currentXPath.indexOf("/") != -1 && found) {
//                found = false;
//                int pos = 0;
//                AttributeMapping currentAttribute = null;
//                for (AttributeMapping attributeMapping : currentAttributes) {
//                    String targetXPath = attributeMapping.getTargetXPath().toString();
//                    if (currentXPath.startsWith(targetXPath)) {
//                        if (attributeMapping instanceof JoiningNestedAttributeMapping) {
//                            String nestedFeatureType = ((JoiningNestedAttributeMapping) attributeMapping)
//                                    .getNestedFeatureTypeName(null).toString();
//
//                            if (currentXPath.startsWith(targetXPath + "/" + nestedFeatureType)) {
//                                pos += targetXPath.length() + nestedFeatureType.length() + 2;
//                                currentAttribute = attributeMapping;
//                                found = true;
//                            }
//                        }
//                    }
//
//                }
//                if (currentAttribute != null) {
//                    mappings.add(currentAttribute);
//                    currentTypeMapping = ((JoiningNestedAttributeMapping) currentAttribute)
//                            .getNestedFeatureType();
//                }
//                currentXPath = currentXPath.substring(pos);
//                currentAttributes = currentTypeMapping.getAttributeMappings();
//            }
//
//            SimpleFeatureType lastType = (SimpleFeatureType) currentTypeMapping.getSource()
//                    .getSchema();
//            JDBCDataStore store = (JDBCDataStore) currentTypeMapping.getSource().getDataStore();
//
//            StringBuffer sql = encodeSelectKeyFrom(lastType, store);
//            for (int i = 0; i < mappings.size() - 1; i++) {
//                JoiningNestedAttributeMapping leftJoin = (JoiningNestedAttributeMapping) mappings
//                        .get(i);
//                String leftTableName = leftJoin.getNestedFeatureType().getSource().getSchema()
//                        .getName().getLocalPart();
//                JoiningNestedAttributeMapping rightJoin = (JoiningNestedAttributeMapping) mappings
//                        .get(i + 1);
//                String rightTableName = rightJoin.getNestedFeatureType().getSource().getSchema()
//                        .getName().getLocalPart();
//                sql.append(" INNER JOIN ");
//
//                store.encodeTableName(leftTableName, sql, null);
//                sql.append(" ON ");
//                encodeColumnName(store, rightJoin.getSourceExpression().toString(), leftTableName,
//                        sql, null);
//                sql.append(" = ");
//                encodeColumnName(store, rightJoin.getMapping(rightJoin.getNestedFeatureType())
//                        .getSourceExpression().toString(), rightTableName, sql, null);
//            }
//            if (!currentXPath.equals("")) {
//                createWhereClause(filter, xpath, currentXPath, currentTypeMapping, lastType, store,
//                        sql);
//
//                JoiningNestedAttributeMapping firstJoin = (JoiningNestedAttributeMapping) mappings
//                        .get(0);
//
//                sql.append(" AND ");
//                encodeColumnName(store, firstJoin.getSourceExpression().toString(), rootMapping
//                        .getSource().getSchema().getName().getLocalPart(), sql, null);
//                sql.append(" = ");
//                encodeColumnName(store, firstJoin.getMapping(firstJoin.getNestedFeatureType())
//                        .getSourceExpression().toString(), firstJoin.getNestedFeatureType()
//                        .getSource().getSchema().getName().getLocalPart(), sql, null);
//            }
//            out.write(sql.toString());
//            out.write(")");
            return extraData;

//        } catch (java.io.IOException ioe) {
//            throw new RuntimeException("Problem writing filter: ", ioe);
//        } catch (SQLException e) {
//            throw new RuntimeException("Problem writing filter: ", e);
//        } catch (FilterToSQLException e) {
//            throw new RuntimeException("Problem writing filter: ", e);
//        }
    }

    @Override
    public Object visit(PropertyIsEqualTo filter, Object extraData) {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }

        return visitBinaryComparison(filter, extraData, xpath[0]);

    }

    private boolean hasNestedAttributes(String[] xpaths) {
        for (String xpath : xpaths) {
            if (xpath.indexOf("/") != -1) {
                return true;
            }
        }
        return false;
    }

    private String[] getAttributesXPath(Filter filter) {
        FilterAttributeExtractor extractor = new FilterAttributeExtractor();
        filter.accept(extractor, null);
        return extractor.getAttributeNames();
    }

    @Override
    public Object visit(PropertyIsBetween filter, Object extraData) throws RuntimeException {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(PropertyIsLike filter, Object extraData) {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(PropertyIsGreaterThanOrEqualTo filter, Object extraData) {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(PropertyIsGreaterThan filter, Object extraData) {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(PropertyIsLessThan filter, Object extraData) {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(PropertyIsLessThanOrEqualTo filter, Object extraData) {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(PropertyIsNotEqualTo filter, Object extraData) {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(PropertyIsNull filter, Object extraData) throws RuntimeException {
        String[] xpath = getAttributesXPath(filter);
        if (!hasNestedAttributes(xpath)) {
            return original.visit(filter, extraData);
        }
        return visitBinaryComparison(filter, extraData, xpath[0]);
    }

    @Override
    public Object visit(Id filter, Object extraData) {
        return original.visit(filter, extraData);
    }

    private void createWhereClause(Filter filter, String filterProperty, String newFilterProperty,
            FeatureTypeMapping currentTypeMapping, SimpleFeatureType lastType, JDBCDataStore store,
            StringBuffer sql) throws FilterToSQLException {

        NestedToSimpleFilterVisitor duplicate = new NestedToSimpleFilterVisitor(filterProperty,
                newFilterProperty);

        Filter duplicated = (Filter) filter.accept(duplicate, null);
        Filter unrolled = AppSchemaDataAccess.unrollFilter(duplicated, currentTypeMapping);
        NestedFieldEncoder fieldEncoder = new NestedFieldEncoder(lastType.getTypeName(), store);
        FilterToSQL nestedFilterToSQL = store.createFilterToSQL(lastType);
        nestedFilterToSQL.setFieldEncoder(fieldEncoder);
        sql.append(" ").append(nestedFilterToSQL.encodeToString(unrolled));
    }

    private StringBuffer encodeSelectKeyFrom(SimpleFeatureType lastType, JDBCDataStore store)
            throws SQLException {
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
            encodeColumnName(store, colName, lastType.getTypeName(), sqlKeys, null);

        }
        sql.append(sqlKeys.substring(1));
        sql.append(" FROM ");
        store.encodeTableName(lastType.getTypeName(), sql, null);
        return sql;
    }

    public void encodeColumnName(JDBCDataStore store, String colName, String typeName,
            StringBuffer sql, Hints hints) throws SQLException {

        store.encodeTableName(typeName, sql, hints);
        sql.append(".");
        store.dialect.encodeColumnName(colName, sql);

    }
}
