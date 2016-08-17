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

import org.geotools.data.complex.AppSchemaDataAccess;
import org.geotools.data.complex.FeatureTypeMapping;
import org.geotools.data.complex.NestedAttributeMapping;
import org.geotools.data.complex.filter.NestedMappingsExtractor;
import org.geotools.data.complex.filter.XPath;
import org.geotools.data.complex.filter.NestedMappingsExtractor.MappingStep;
import org.geotools.data.complex.filter.NestedMappingsExtractor.MappingStepList;
import org.geotools.data.complex.filter.XPathUtil.StepList;
import org.geotools.data.complex.filter.UnmappingFilterVisitor;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.factory.Hints;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.filter.FilterFactoryImplNamespaceAware;
import org.geotools.filter.NestedAttributeExpression;
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

            NestedMappingsExtractor nestedMappingsExtractor = new NestedMappingsExtractor(
                    rootMapping);
            nestedMappingsExtractor.visit(ff.property(xpath), null);
            MappingStepList mappingSteps = nestedMappingsExtractor.getMappingSteps();

            int numMappings = mappingSteps.size();
            if (numMappings > 0 && mappingSteps.isJoiningEnabled()) {
                out.write("EXISTS (");

                MappingStep lastMappingStep = mappingSteps.getLastStep();
                StringBuffer sql = encodeSelectKeyFrom(lastMappingStep);

                for (int i = numMappings - 2; i > 0; i--) {
                    MappingStep mappingStep = mappingSteps.getStep(i);
                    if (mappingStep.hasNestedFeature()) {
                        FeatureTypeMapping parentFeature = mappingStep.getFeatureTypeMapping();
                        JDBCDataStore store = (JDBCDataStore) parentFeature.getSource()
                                .getDataStore();
                        String parentTableName = parentFeature.getSource().getSchema().getName()
                                .getLocalPart();

                        sql.append(" INNER JOIN ");
                        store.encodeTableName(parentTableName, sql, null);
                        sql.append(" AS ");
                        store.dialect.encodeTableName(mappingStep.getAlias(), sql);
                        sql.append(" ON ");
                        encodeJoinCondition(mappingSteps, i, sql);
                    }
                }

                if (lastMappingStep.hasOwnAttribute()) {
                    createWhereClause(filter, xpath, lastMappingStep, sql);

                    sql.append(" AND ");
                } else {
                    sql.append(" WHERE ");
                }

                // join with root table
                encodeJoinCondition(mappingSteps, 0, sql);

                out.write(sql.toString());
                out.write(")");
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

    private void encodeJoinCondition(MappingStepList mappingStepList, int stepIdx, StringBuffer sql)
            throws SQLException, IOException {
        MappingStep parentStep = mappingStepList.getStep(stepIdx);
        MappingStep nestedStep = mappingStepList.getStep(stepIdx + 1);
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

    private void createWhereClause(Filter filter, String nestedProperty, MappingStep mapping,
            StringBuffer sql) throws FilterToSQLException {
        String simpleProperty = mapping.getOwnAttributeXPath();
        FeatureTypeMapping featureMapping = mapping.getFeatureTypeMapping();
        JDBCDataStore store = (JDBCDataStore) featureMapping.getSource().getDataStore();
        FeatureTypeMapping featureMappingForUnrolling = featureMapping;
        if (mapping.isChainingByReference()) {
            // last attribute xpath should be resolved against the parent feature
            if (mapping.previous() != null) {
                featureMappingForUnrolling = mapping.previous().getFeatureTypeMapping();
            }
        }
        SimpleFeatureType sourceType = (SimpleFeatureType) featureMapping.getSource().getSchema();

        NestedToSimpleFilterVisitor duplicate = new NestedToSimpleFilterVisitor(nestedProperty,
                simpleProperty);
        Filter duplicated = (Filter) filter.accept(duplicate, null);
        Filter unrolled = unrollFilter(duplicated, featureMappingForUnrolling);

        JoiningFieldEncoder fieldEncoder = new JoiningFieldEncoder(mapping.getAlias(), store);
        FilterToSQL nestedFilterToSQL = store.createFilterToSQL(sourceType);
        nestedFilterToSQL.setFieldEncoder(fieldEncoder);
        String encodedFilter = nestedFilterToSQL.encodeToString(unrolled);
        sql.append(" ").append(encodedFilter);
    }

    private StringBuffer encodeSelectKeyFrom(MappingStep lastMappingStep) throws SQLException {
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
        sql.append(" AS ");
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
        UnmappingFilterVisitorExcludingNestedMappings visitor = new UnmappingFilterVisitorExcludingNestedMappings(mappings);
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
                throw new IllegalArgumentException("Can't find source expression for: " + targetXPath);
            }

            return matchingMappings;
        }

    }
}
