package org.geotools.jdbc;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.SQLException;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.geotools.data.complex.AppSchemaDataAccess;
import org.geotools.data.complex.FeatureTypeMapping;
import org.geotools.data.complex.NestedAttributeMapping;
import org.geotools.data.complex.filter.NestedMappingsExtractor;
import org.geotools.data.complex.filter.NestedMappingsExtractor.MappingStep;
import org.geotools.data.complex.filter.NestedMappingsExtractor.MappingStepList;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.factory.Hints;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.filter.FilterFactoryImplNamespaceAware;
import org.geotools.jdbc.JoiningJDBCFeatureSource.JoiningFieldEncoder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNull;

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
                FeatureTypeMapping lastTypeMapping = lastMappingStep.getFeatureTypeMapping();
                SimpleFeatureType lastSourceType = (SimpleFeatureType) lastTypeMapping.getSource()
                        .getSchema();
                JDBCDataStore store = (JDBCDataStore) lastTypeMapping.getSource().getDataStore();

                StringBuffer sql = encodeSelectKeyFrom(lastSourceType, store);

                for (int i = numMappings - 2; i > 0; i--) {
                    MappingStep mappingStep = mappingSteps.getStep(i);
                    if (mappingStep.hasNestedFeature()) {
                        FeatureTypeMapping parentFeature = mappingStep.getFeatureTypeMapping();
                        String parentTableName = parentFeature.getSource().getSchema().getName()
                                .getLocalPart();

                        sql.append(" INNER JOIN ");
                        store.encodeTableName(parentTableName, sql, null);
                        sql.append(" ON ");
                        encodeJoinCondition(mappingStep, sql, store);
                    }
                }

                if (lastMappingStep.hasOwnAttribute()) {
                    String lastAttrXPath = lastMappingStep.getOwnAttributeXPath();
                    // TODO: investigate why table names are not schema-qualified in where clause
                    createWhereClause(filter, xpath, lastAttrXPath, lastTypeMapping,
                            lastSourceType, store, sql);

                    sql.append(" AND ");
                } else {
                    sql.append(" WHERE ");
                }

                // join with root table
                encodeJoinCondition(mappingSteps.getFirstStep(), sql, store);

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

    private void encodeJoinCondition(MappingStep mappingStep, StringBuffer sql, JDBCDataStore store)
            throws SQLException, IOException {
        FeatureTypeMapping parentFeature = mappingStep.getFeatureTypeMapping();
        NestedAttributeMapping nestedFeatureAttr = mappingStep.getNestedFeatureAttribute();
        FeatureTypeMapping nestedFeature = nestedFeatureAttr.getNestedFeatureType();

        String parentTableName = parentFeature.getSource().getSchema().getName().getLocalPart();
        // TODO: what if source expression is not a literal?
        String parentTableColumn = nestedFeatureAttr.getSourceExpression().toString();
        String nestedTableName = nestedFeature.getSource().getSchema().getName().getLocalPart();
        // TODO: what if source expression is not a literal?
        String nestedTableColumn = nestedFeatureAttr.getMapping(nestedFeature)
                .getSourceExpression().toString();

        encodeColumnName(store, parentTableColumn, parentTableName, sql, null);
        sql.append(" = ");
        encodeColumnName(store, nestedTableColumn, nestedTableName, sql, null);
    }

    private void createWhereClause(Filter filter, String filterProperty, String newFilterProperty,
            FeatureTypeMapping currentTypeMapping, SimpleFeatureType lastType, JDBCDataStore store,
            StringBuffer sql) throws FilterToSQLException {
        NestedToSimpleFilterVisitor duplicate = new NestedToSimpleFilterVisitor(filterProperty,
                newFilterProperty);

        Filter duplicated = (Filter) filter.accept(duplicate, null);
        Filter unrolled = AppSchemaDataAccess.unrollFilter(duplicated, currentTypeMapping);
        JoiningFieldEncoder fieldEncoder = new JoiningFieldEncoder(lastType.getTypeName(), store);
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
        if (sqlKeys.length() <= 0) {
            sql.append("*");
        } else {
            sql.append(sqlKeys.substring(1));
        }
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

}
