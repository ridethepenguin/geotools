package org.geotools.ysld.parse;

import org.geotools.process.function.ProcessFunction;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.PolygonSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.SLD;
import org.geotools.styling.StyledLayerDescriptor;
import org.geotools.ysld.Ysld;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.geotools.styling.ColorMap;
import org.opengis.style.RasterSymbolizer;

import java.awt.*;
import java.io.IOException;
import java.util.Arrays;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.geotools.ysld.TestUtils.appliesToScale;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.describedAs;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class YsldParseTest {

    @Test
    public void testAnchor() throws Exception {
        String yaml =
        "blue: &blue rgb(0,0,255)\n" +
        "point: \n"+
        "  symbols: \n" +
        "  - mark: \n" +
        "      fill-color: *blue\n";

        StyledLayerDescriptor sld = Ysld.parse(yaml);
        PointSymbolizer p = SLD.pointSymbolizer(SLD.defaultStyle(sld));
        assertEquals(Color.BLUE, SLD.color(SLD.fill(p)));
    }

    @Test
    public void testNamedColor() throws Exception {
        String yaml =
        "point: \n"+
        "  symbols: \n" +
        "  - mark: \n" +
        "      fill-color: blue\n";

        StyledLayerDescriptor sld = Ysld.parse(yaml);
        PointSymbolizer p = SLD.pointSymbolizer(SLD.defaultStyle(sld));
        assertEquals(Color.BLUE, SLD.color(SLD.fill(p)));
    }

    @Test
    public void testFunction() throws Exception {
        String yaml =
        "rules: \n"+
        "- filter: strEndsWith(foo,'bar') = true\n";

        StyledLayerDescriptor sld = Ysld.parse(yaml);
        Rule r = SLD.defaultStyle(sld).featureTypeStyles().get(0).rules().get(0);

        PropertyIsEqualTo f = (PropertyIsEqualTo) r.getFilter();
        Function func = (Function) f.getExpression1();
        assertEquals("strEndsWith", func.getName());
        assertTrue(func.getParameters().get(0) instanceof PropertyName);
        assertTrue(func.getParameters().get(1) instanceof Literal);

        Literal lit = (Literal) f.getExpression2();
    }

    @Test
    public void testRenderingTransformation() throws IOException {
        String yaml =
        "feature-styles: \n"+
        "- transform:\n" +
        "    name: ras:Contour\n" +
        "    params:\n" +
        "      levels:\n" +
        "      - 1000\n" +
        "      - 1100\n" +
        "      - 1200\n";

        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);

        Expression tx = fs.getTransformation();
        assertNotNull(tx);

        ProcessFunction pf = (ProcessFunction) tx;
        assertEquals(2, pf.getParameters().size());

        Function e1 = (Function) pf.getParameters().get(0);
        assertEquals(1, e1.getParameters().size());
        assertTrue(e1.getParameters().get(0) instanceof Literal);
        assertEquals("data", e1.getParameters().get(0).evaluate(null, String.class));

        Function e2 = (Function) pf.getParameters().get(1);
        assertEquals(4, e2.getParameters().size());
        assertTrue(e2.getParameters().get(0) instanceof Literal);
        assertEquals("levels", e2.getParameters().get(0).evaluate(null, String.class));
        assertEquals(1000, e2.getParameters().get(1).evaluate(null, Integer.class).intValue());
        assertEquals(1100, e2.getParameters().get(2).evaluate(null, Integer.class).intValue());
        assertEquals(1200, e2.getParameters().get(3).evaluate(null, Integer.class).intValue());
    }

    @Test
    public void testRenderingTransformationHeatmap() throws IOException {
        String yaml =
            "feature-styles: \n"+
            "- transform:\n" +
            "    name: vec:Heatmap\n" +
            "    params:\n" +
            "      weightAttr: pop2000\n" +
            "      radius: 100\n" +
            "      pixelsPerCell: 10\n" +
            "      outputBBOX: wms_bbox\n" +
            "      outputWidth: wms_width\n" +
            "      outputHeight: wms_height\n";


        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);

        Expression tx = fs.getTransformation();
        assertNotNull(tx);

        ProcessFunction pf = (ProcessFunction) tx;
        assertEquals(7, pf.getParameters().size());

        Function e = (Function) pf.getParameters().get(0);
        assertEquals(1, e.getParameters().size());
        assertTrue(e.getParameters().get(0) instanceof Literal);
        assertEquals("data", e.getParameters().get(0).evaluate(null, String.class));

        e = (Function) pf.getParameters().get(1);
        assertEquals(2, e.getParameters().size());
        assertTrue(e.getParameters().get(0) instanceof Literal);
        assertEquals("weightAttr", e.getParameters().get(0).evaluate(null, String.class));
        assertEquals("pop2000", e.getParameters().get(1).evaluate(null, String.class));
    }
    
    @Test
    public void testLabelShield() throws Exception {
        String yaml =
                "feature-styles:\n"+
                "- name: name\n"+
                " rules:\n"+
                " - symbolizers:\n"+
                "   - line:\n"+
                "       stroke-color: '#555555'\n"+
                "       stroke-width: 1.0\n"+
                "    - text:\n"+
                "       label: name\n"+
                "       symbols:\n"+
                "        - mark:\n"+
                "           shape: circle\n"+
                "           fill-color: '#995555'\n"+
                "       geometry: geom";
                        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testZoomSimple() throws IOException {
        String yaml =
            "grid:\n"+
            "  initial-scale: 5000000\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            "  - zoom: (0,0)\n";
        
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        assertThat((Iterable<Rule>)fs.rules(), hasItems(
                allOf(
                        appliesToScale(5000000),
                        not(appliesToScale(5000000/2)),
                        not(appliesToScale(5000000*2))
                    )));
        
    }
    
    @Test
    public void testZoomSimpleRange() throws IOException {
        String yaml =
            "grid:\n"+
            "  initial-scale: 5000000\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            "  - zoom: (1,2)\n";
        
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        Rule r = fs.rules().get(0);
        
        assertThat(r, appliesToScale(5000000/2)); // Z=1
        assertThat(r, appliesToScale(5000000/4)); // Z=2
        assertThat(r, not(appliesToScale(5000000)));  // Z=0
        assertThat(r, not(appliesToScale(5000000/8))); // Z=3
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testZoomSimpleWithDifferentInitial() throws IOException {
        String yaml =
            "grid:\n"+
            "  initial-scale: 5000000\n"+
            "  initial-level: 3\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            "  - zoom: (0,0)\n"+
            "  - zoom: (3,3)\n";
        
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        assertThat((Iterable<Rule>)fs.rules(), hasItems(
                allOf(
                        appliesToScale(5000000*8),
                        not(appliesToScale(5000000/2*8)),
                        not(appliesToScale(5000000*2*8))
                    ),
                allOf(
                        appliesToScale(5000000*8),
                        not(appliesToScale(5000000/2*8)),
                        not(appliesToScale(5000000*2*8))
                    )
                ));
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testZoomList() throws IOException {
        String yaml =
            "grid:\n"+
            "  scales:\n"+
            "  - 5000000\n"+
            "  - 2000000\n"+
            "  - 1000000\n"+
            "  - 500000\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            "  - zoom: (0,0)\n"+
            "  - zoom: (2,2)\n";
        
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        assertThat((Iterable<Rule>)fs.rules(), hasItems(
                allOf(
                        appliesToScale(5000000d),
                        not(appliesToScale(2000000d))
                    ),
                allOf(
                        appliesToScale(1000000d),
                        not(appliesToScale(2000000d)),
                        not(appliesToScale(500000d))
                    )
                ));
        
    }
    @SuppressWarnings("unchecked")
    @Test
    public void testZoomListWithInitial() throws IOException {
        String yaml =
            "grid:\n"+
            "  initial-level: 3\n"+
            "  scales:\n"+
            "  - 5000000\n"+
            "  - 2000000\n"+
            "  - 1000000\n"+
            "  - 500000\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            "  - zoom: (3,3)\n"+
            "  - zoom: (5,5)\n";
        
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        assertThat((Iterable<Rule>)fs.rules(), hasItems(
                allOf(
                        appliesToScale(5000000d),
                        not(appliesToScale(2000000d))
                    ),
                allOf(
                        appliesToScale(1000000d),
                        not(appliesToScale(2000000d)),
                        not(appliesToScale(500000d))
                    )
                ));
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testZoomListWithRanges() throws IOException {
        String yaml =
            "grid:\n"+
            "  scales:\n"+
            "  - 5000000\n"+
            "  - 2000000\n"+
            "  - 1000000\n"+
            "  - 500000\n"+
            "  - 200000\n"+
            "  - 100000\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            "  - zoom: (,1)\n"+
            "  - zoom: (2,3)\n"+
            "  - zoom: (4,)\n";
        
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        assertThat((Iterable<Rule>)fs.rules(), hasItems(
                allOf(
                        appliesToScale(5000000d),
                        appliesToScale(2000000d),
                        not(appliesToScale(1000000d)),
                        not(appliesToScale(500000d)),
                        not(appliesToScale(200000d)),
                        not(appliesToScale(100000d))
                        ),
                allOf(
                        not(appliesToScale(5000000d)),
                        not(appliesToScale(2000000d)),
                        appliesToScale(1000000d),
                        appliesToScale(500000d),
                        not(appliesToScale(200000d)),
                        not(appliesToScale(100000d))
                        ),
                allOf(
                        not(appliesToScale(5000000d)),
                        not(appliesToScale(2000000d)),
                        not(appliesToScale(1000000d)),
                        not(appliesToScale(500000d)),
                        appliesToScale(200000d),
                        appliesToScale(100000d)
                        )
                ));
        
    }
    final static String GOOGLE_MERCATOR_TEST_RULES=
            "  - zoom: (0,0)\n"+
            "  - zoom: (1,1)\n"+
            "  - zoom: (2,2)\n"+
            "  - zoom: (3,3)\n"+
            "  - zoom: (4,4)\n"+
            "  - zoom: (5,5)\n"+
            "  - zoom: (6,6)\n"+
            "  - zoom: (7,7)\n"+
            "  - zoom: (8,8)\n"+
            "  - zoom: (9,9)\n"+
            "  - zoom: (10,10)\n"+
            "  - zoom: (11,11)\n"+
            "  - zoom: (12,12)\n"+
            "  - zoom: (13,13)\n"+
            "  - zoom: (14,14)\n"+
            "  - zoom: (15,15)\n"+
            "  - zoom: (16,16)\n"+
            "  - zoom: (17,17)\n"+
            "  - zoom: (18,18)\n"+
            "  - zoom: (19,19)\n";
    
    final static String WGS84_TEST_RULES=
            "  - zoom: (0,0)\n"+
            "    name: WGS84:00\n"+
            "  - zoom: (1,1)\n"+
            "    name: WGS84:01\n"+
            "  - zoom: (2,2)\n"+
            "    name: WGS84:02\n"+
            "  - zoom: (3,3)\n"+
            "    name: WGS84:03\n"+
            "  - zoom: (4,4)\n"+
            "    name: WGS84:04\n"+
            "  - zoom: (5,5)\n"+
            "    name: WGS84:05\n"+
            "  - zoom: (6,6)\n"+
            "    name: WGS84:06\n"+
            "  - zoom: (7,7)\n"+
            "    name: WGS84:07\n"+
            "  - zoom: (8,8)\n"+
            "    name: WGS84:08\n"+
            "  - zoom: (9,9)\n"+
            "    name: WGS84:09\n"+
            "  - zoom: (10,10)\n"+
            "    name: WGS84:10\n"+
            "  - zoom: (11,11)\n"+
            "    name: WGS84:11\n"+
            "  - zoom: (12,12)\n"+
            "    name: WGS84:12\n"+
            "  - zoom: (13,13)\n"+
            "    name: WGS84:13\n"+
            "  - zoom: (14,14)\n"+
            "    name: WGS84:14\n"+
            "  - zoom: (15,15)\n"+
            "    name: WGS84:15\n"+
            "  - zoom: (16,16)\n"+
            "    name: WGS84:16\n"+
            "  - zoom: (17,17)\n"+
            "    name: WGS84:17\n"+
            "  - zoom: (18,18)\n"+
            "    name: WGS84:18\n"+
            "  - zoom: (19,19)\n"+
            "    name: WGS84:19\n"+
            "  - zoom: (20,20)\n"+
            "    name: WGS84:20\n";
    
    // m/px
    double[] GOOGLE_MERCATOR_PIXEL_SIZES = {
            156543.0339280410,
            78271.51696402048,
            39135.75848201023,
            19567.87924100512,
            9783.939620502561,
            4891.969810251280,
            2445.984905125640,
            1222.992452562820,
            611.4962262814100,
            305.7481131407048,
            152.8740565703525,
            76.43702828517624,
            38.21851414258813,
            19.10925707129406,
            9.554628535647032,
            4.777314267823516,
            2.388657133911758,
            1.194328566955879,
            0.5971642834779395,
            0.29858214173896974,
            0.14929107086948487
    };
    
    double INCHES_PER_METRE = 39.3701;
    double OGC_DPI = 90;
    
    final double[] WGS84_SCALE_DENOMS = {
            279_541_132.0143589,
            139_770_566.00717944,
            69_885_283.00358972,
            34_942_641.50179486,
            17_471_320.75089743,
            8_735_660.375448715,
            4_367_830.1877243575,
            2_183_915.0938621787,
            1_091_957.5469310894,
            545_978.7734655447,
            272_989.38673277234,
            136_494.69336638617,
            68_247.34668319309,
            34_123.67334159654,
            17_061.83667079827,
            8_530.918335399136,
            4_265.459167699568,
            2_132.729583849784,
            1_066.364791924892,
            533.182395962446,
            266.591197981223
    };
    
    @Test
    public void testZoomDefault() throws IOException {
        String yaml =
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            WGS84_TEST_RULES;
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        doTestForWGS84(sld);
        
    }
    @Test
    public void testNamed() throws IOException {
        String yaml =
            "grid:\n"+
            "  name: WebMercator\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            GOOGLE_MERCATOR_TEST_RULES;
        
        StyledLayerDescriptor sld = Ysld.parse(yaml);
        doTestForGoogleMercator(sld);
        
    }
    
    @SuppressWarnings("unchecked")
    private void doTestForGoogleMercator(StyledLayerDescriptor sld) throws IOException {
        double scaleDenominators[] = new double[GOOGLE_MERCATOR_PIXEL_SIZES.length];
        for(int i=0; i<GOOGLE_MERCATOR_PIXEL_SIZES.length; i++){
            scaleDenominators[i]=OGC_DPI*INCHES_PER_METRE*GOOGLE_MERCATOR_PIXEL_SIZES[i];
        }
        

        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        for(int i=0; i<GOOGLE_MERCATOR_PIXEL_SIZES.length; i++){
            scaleDenominators[i]=OGC_DPI*INCHES_PER_METRE*GOOGLE_MERCATOR_PIXEL_SIZES[i];
        }
        
        assertThat(fs.rules().size(), is(20));
        
        assertThat(fs.rules().get(0), allOf(
                appliesToScale(scaleDenominators[0]),
                not(appliesToScale(scaleDenominators[1]))
        ));
        assertThat(fs.rules().get(19) ,allOf(
                appliesToScale(scaleDenominators[19]),
                not(appliesToScale(scaleDenominators[18]))
            ));
        for(int i = 1; i<19; i++){
            assertThat(fs.rules().get(i) ,describedAs("rule applies only to level %0 (%1)",
                    allOf(
                        appliesToScale(scaleDenominators[i]),
                        not(appliesToScale(scaleDenominators[i+1])),
                        not(appliesToScale(scaleDenominators[i+-1]))
                    ),
                    i,scaleDenominators[i]
               ));
        }
    }
    
    @Test
    public void testWGS84Scales() throws Exception {
        ZoomContext context = WellKnownZoomContextFinder.getInstance().get("DEFAULT");
        
        for(int i=0; i<WGS84_SCALE_DENOMS.length; i++) {
            assertThat(context.getScaleDenominator(i), closeTo(WGS84_SCALE_DENOMS[i], 0.000000001));
        }
    }
    
    @SuppressWarnings("unchecked")
    private void doTestForWGS84(StyledLayerDescriptor sld) throws IOException {
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        
        assertThat(fs.rules().size(), is(21));
        
        Rule first = fs.rules().get(0);
        assertThat(first, allOf(
                appliesToScale(WGS84_SCALE_DENOMS[0]),
                not(appliesToScale(WGS84_SCALE_DENOMS[1]))
        ));

        for(int i = 1; i<20; i++){
            Rule r = fs.rules().get(i);
            assertThat(r ,describedAs("rule applies only to level %0 (%1)",
                    allOf(
                        appliesToScale(WGS84_SCALE_DENOMS[i]),
                        not(appliesToScale(WGS84_SCALE_DENOMS[i+1])),
                        not(appliesToScale(WGS84_SCALE_DENOMS[i+-1]))
                    ),
                    i,WGS84_SCALE_DENOMS[i]
               ));
        }
        
        Rule last = fs.rules().get(20);
        assertThat(last ,allOf(
                appliesToScale(WGS84_SCALE_DENOMS[20]),
                not(appliesToScale(WGS84_SCALE_DENOMS[19]))
            ));

    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testNamedWithFinder() throws IOException {
        String yaml =
            "grid:\n"+
            "  name: test\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            "  - zoom: (0,0)";
        
        ZoomContextFinder finder = createMock(ZoomContextFinder.class);
        ZoomContext context = createMock(ZoomContext.class);
        
        expect(finder.get("test")).andReturn(context);
        expect(context.getRange(0, 0)).andReturn(new ScaleRange(42, 64));
        
        replay(finder, context);
        
        StyledLayerDescriptor sld = Ysld.parse(yaml, Arrays.asList(finder));
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        fs.rules().get(0).getMaxScaleDenominator();
        assertThat((Iterable<Rule>)fs.rules(), hasItems(
                allOf(
                        Matchers.<Rule>hasProperty("maxScaleDenominator", Matchers.closeTo(64,0.0000001d)),
                        Matchers.<Rule>hasProperty("minScaleDenominator", Matchers.closeTo(42,0.0000001d))
                    )));
        
        verify(finder, context);
    }
    
    @Test
    public void testWellKnownWithCustomFinder() throws IOException {
        String yaml =
            "grid:\n"+
            "  name: WebMercator\n"+
            "feature-styles: \n"+
            "- name: name\n"+
            "  rules:\n"+
            GOOGLE_MERCATOR_TEST_RULES;
        
        ZoomContextFinder finder = createMock(ZoomContextFinder.class);
        
        expect(finder.get("WebMercator")).andReturn(null);
        
        replay(finder);
        
        StyledLayerDescriptor sld = Ysld.parse(yaml, Arrays.asList(finder));
        doTestForGoogleMercator(sld); // The additional finder doesn't have a WebMercator context and so should not interfere.
        
        verify(finder);
    }
    @SuppressWarnings("unchecked")
    @Test
    public void testCustomFinderOverridesWellKnown() throws IOException {
        String yaml =
                "grid:\n"+
                "  name: WebMercator\n"+
                "feature-styles: \n"+
                "- name: name\n"+
                "  rules:\n"+
                "  - zoom: (0,0)";
            
            ZoomContextFinder finder = createMock(ZoomContextFinder.class);
            ZoomContext context = createMock(ZoomContext.class);
            
            expect(finder.get("WebMercator")).andReturn(context);
            expect(context.getRange(0, 0)).andReturn(new ScaleRange(42, 64));
            
            replay(finder, context);
            
            StyledLayerDescriptor sld = Ysld.parse(yaml, Arrays.asList(finder));
            FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
            fs.rules().get(0).getMaxScaleDenominator();
            assertThat((Iterable<Rule>)fs.rules(), hasItems(
                    allOf(
                            Matchers.<Rule>hasProperty("maxScaleDenominator", Matchers.closeTo(64,0.0000001d)),
                            Matchers.<Rule>hasProperty("minScaleDenominator", Matchers.closeTo(42,0.0000001d))
                        )));
            
            verify(finder, context);
    }

    @Test
    public void testParseNoStrokeFillDefaults() throws Exception {
        String yaml =
            "polygon: \n"+
            "  fill-color: blue\n";

        StyledLayerDescriptor sld = Ysld.parse(yaml);
        PolygonSymbolizer p = (PolygonSymbolizer) SLD.symbolizers(SLD.defaultStyle(sld))[0];
        assertEquals(Color.BLUE, SLD.color(SLD.fill(p)));
        assertNull(SLD.stroke(p));

        yaml =
            "polygon: \n"+
            "  stroke-color: blue\n";

        sld = Ysld.parse(yaml);
        p = (PolygonSymbolizer) SLD.symbolizers(SLD.defaultStyle(sld))[0];
        assertEquals(Color.BLUE, SLD.color(SLD.stroke(p)));
        assertNull(SLD.fill(p));
    }

    @Test
    public void testColourMapValuesWithoutHash() throws Exception {
        String yaml =
                "raster: \n" +
                        "  color-map:\n" +
                        "    type: values\n" +
                        "    entries:\n" +
                        "    - (ff0000, 1.0, 0, start)\n" +
                        "    - (00ff00, 1.0, 500, middle)\n" +
                        "    - (0000ff, 1.0, 1000, end)\n" +
                        "";

        StyledLayerDescriptor sld = Ysld.parse(yaml);
        FeatureTypeStyle fs = SLD.defaultStyle(sld).featureTypeStyles().get(0);
        RasterSymbolizer symb = (RasterSymbolizer) fs.rules().get(0).symbolizers().get(0);

        // need to use the geotools.styling interface as it provides the accessors for the entries.
        ColorMap map = (ColorMap) symb.getColorMap();

        Color colour1 = (Color) map.getColorMapEntry(0).getColor().evaluate(null);
        Color colour2 = (Color) map.getColorMapEntry(1).getColor().evaluate(null);
        Color colour3 = (Color) map.getColorMapEntry(2).getColor().evaluate(null);

        assertThat(colour1, is(Color.RED));
        assertThat(colour2, is(Color.GREEN));
        assertThat(colour3, is(Color.BLUE));
    }

    @Test
    public void testBadExpression() throws Exception {
        String yaml =
            "polygon: \n"+
            "  stroke-width: round(foo) 1000\n";
        try {
            Ysld.parse(yaml);
            fail("Bad expression should have thrown exception");
        }
        catch(IllegalArgumentException e) {
            // expected
        }
    }
}
