package org.geotools.data.complex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataAccess;
import org.geotools.data.DataAccessFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.complex.config.Types;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.FilterFactoryImplNamespaceAware;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.FilterFactory2;
import org.xml.sax.helpers.NamespaceSupport;

public class DefaultGeometryTest {

    private static final String STATIONS_NS = "http://www.stations.org/1.0";

    static final Name STATION_FEATURE_TYPE = Types.typeName(STATIONS_NS, "StationType");

    static final Name STATION_FEATURE = Types.typeName(STATIONS_NS, "Station");

    private static final String schemaBase = "/test-data/";

    static FilterFactory2 ff;

    private NamespaceSupport namespaces = new NamespaceSupport();

    public DefaultGeometryTest() {
        namespaces.declarePrefix("st", STATIONS_NS);
        ff = new FilterFactoryImplNamespaceAware(namespaces);
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        loadDataAccesses();
    }

    @Test
    public void testDefaultGeometry() {

    }

    /**
     * Load all the data accesses.
     *
     * @return
     * @throws Exception
     */
    private static void loadDataAccesses() throws Exception {
        /**
         * Load mapped feature data access
         */
        Map dsParams = new HashMap();
        URL url = FeatureChainingTest.class.getResource(schemaBase + "stations.xml");
        assertNotNull(url);

        dsParams.put("dbtype", "app-schema");
        dsParams.put("url", url.toExternalForm());
        DataAccess<FeatureType, Feature> mfDataAccess = DataAccessFinder.getDataStore(dsParams);
        assertNotNull(mfDataAccess);

        FeatureType mappedFeatureType = mfDataAccess.getSchema(STATION_FEATURE);
        assertNotNull(mappedFeatureType);
        assertNotNull(mappedFeatureType.getGeometryDescriptor());

        FeatureSource fs = (FeatureSource) mfDataAccess.getFeatureSource(STATION_FEATURE);
        FeatureCollection stationFeatures = (FeatureCollection) fs.getFeatures();
        assertEquals(3, size(stationFeatures));
    }

    private static int size(FeatureCollection<FeatureType, Feature> features) {
        int size = 0;
        FeatureIterator<Feature> iterator = features.features();
        while (iterator.hasNext()) {
            iterator.next();
            size++;
        }
        iterator.close();
        return size;
    }
}
