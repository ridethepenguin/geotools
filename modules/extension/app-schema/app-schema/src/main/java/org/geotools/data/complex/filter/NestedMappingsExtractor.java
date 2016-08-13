package org.geotools.data.complex.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.geotools.data.complex.AttributeMapping;
import org.geotools.data.complex.FeatureTypeMapping;
import org.geotools.data.complex.NestedAttributeMapping;
import org.geotools.data.complex.filter.XPathUtil.StepList;
import org.geotools.data.joining.JoiningNestedAttributeMapping;
import org.geotools.feature.type.Types;
import org.geotools.filter.visitor.DefaultExpressionVisitor;
import org.geotools.util.logging.Logging;
import org.opengis.feature.Feature;
import org.opengis.feature.type.Name;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.xml.sax.helpers.NamespaceSupport;

/**
 * Expression visitor that uses the attribute and mapping information provided by a {@link FeatureTypeMapping} object to determine which nested
 * features / attributes must be traversed to reach the attribute identified by the provided {@link PropertyName} expression.
 * 
 * <p>
 * The provided {@link FeatureTypeMapping} object is regarded as the root mapping against which the expression is evaluated.
 * </p>
 * 
 * <p>
 * The nested mappings are returned as a list of {@link MappingStep} objects; the first one in the list always refers to the root mapping.
 * </p>
 * 
 * @author Stefano Costa, GeoSolutions
 *
 */
public class NestedMappingsExtractor extends DefaultExpressionVisitor {

    private static final Logger LOGGER = Logging.getLogger(NestedMappingsExtractor.class);

    private FeatureTypeMapping rootMapping;

    private MappingStepList mappingSteps;

    public NestedMappingsExtractor(FeatureTypeMapping root) {
        if (root == null) {
            throw new NullPointerException("root mapping is null");
        }
        this.mappingSteps = new MappingStepList();
        this.rootMapping = root;
    }

    @Override
    public Object visit(PropertyName expression, Object data) {
        if (expression == null) {
            throw new NullPointerException("expression is null");
        }
        if (data != null && !(data instanceof Feature)) {
            throw new IllegalArgumentException("data must be a Feature object");
        }
        Feature feature = (Feature) data;

        try {
            splitXPath(expression.getPropertyName(), feature);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Exception occurred splitting XPath expression into mapping steps", e);
        }

        return getMappingSteps();
    }

    void splitXPath(String xpath, Feature feature) throws IOException {
        FeatureTypeMapping currentType = rootMapping;
        List<NestedAttributeMapping> currentAttributes = rootMapping.getNestedMappings();
        String currentXPath = xpath;
        boolean found = true;
        while (currentXPath.indexOf("/") != -1 && found) {
            found = false;
            for (NestedAttributeMapping nestedAttr : currentAttributes) {
                String targetXPath = nestedAttr.getTargetXPath().toString();
                if (currentXPath.startsWith(targetXPath)) {
                    if (nestedAttr.isConditional() && feature == null) {
                        LOGGER.fine("Conditional nested mapping found, but no feature to evaluate "
                                + "against was provided: nested feature type cannot be determined");
                        mappingSteps.clear();
                    } else {
                        FeatureTypeMapping nestedType = nestedAttr.getFeatureTypeMapping(feature);
                        if (nestedType != null) {
                            String nestedTypeName = getPrefixedFeatureTypeName(nestedType);
                            String nestedTypeXPath = targetXPath + "/" + nestedTypeName;
                            if (currentXPath.startsWith(nestedTypeXPath)) {
                                LOGGER.finer("Nested feature type found: " + nestedTypeName);
                                mappingSteps.addStep(new MappingStep(currentType, nestedAttr));

                                // update currentType to the nested type just found
                                currentType = nestedType;
                                // update currentXPath by removing the path to the nested type just found
                                int startIdx = nestedTypeXPath.length() + 1;
                                if (startIdx < currentXPath.length()) {
                                    String xpathFromNestedType = currentXPath.substring(startIdx);
                                    currentXPath = xpathFromNestedType;
                                } else {
                                    currentXPath = "";
                                }

                                // nested type was found: stop looping through attributes
                                found = true;
                                break;
                            }
                        } else {
                            LOGGER.fine("Nested feature type could not be determined");
                        }
                    }
                }
            }
            currentAttributes = currentType.getNestedMappings();
        }
        // add last attribute mapping, which is a direct child of the last nested feature found
        if (currentXPath != null && !currentXPath.isEmpty()) {
            StepList lastAttrPath = XPathUtil.steps(currentType.getTargetFeature(), currentXPath,
                    currentType.getNamespaces());
            List<Expression> lastAttrExpressions = currentType.findMappingsFor(lastAttrPath, false);
            if (lastAttrExpressions != null && lastAttrExpressions.size() > 0) {
                mappingSteps.addStep(new MappingStep(currentType, currentXPath));
            }
        }
    }

    private String getPrefixedFeatureTypeName(FeatureTypeMapping featureTypeMapping) {
        Name featureTypeName = featureTypeMapping.getTargetFeature().getName();
        NamespaceSupport nsSupport = featureTypeMapping.getNamespaces();

        if (nsSupport != null) {
            String prefix = nsSupport.getPrefix(featureTypeName.getNamespaceURI());
            if (prefix != null && !prefix.isEmpty()) {
                return prefix + ":" + featureTypeName.getLocalPart();
            }
        }

        LOGGER.warning("No prefix found for namespace URI: " + featureTypeName.getNamespaceURI());

        return featureTypeName.getLocalPart();
    }

    /**
     * Returns the mappings steps the visited expression was split into.
     * 
     * @return the mapping steps
     */
    public MappingStepList getMappingSteps() {
        return mappingSteps;
    }

    public static class MappingStepList {

        private List<MappingStep> mappingSteps;

        public MappingStepList() {
            mappingSteps = new ArrayList<>();
        }

        /**
         * Returns the mappings steps the visited expression was split into.
         * 
         * @return the mapping steps
         */
        public List<MappingStep> getMappingSteps() {
            return new ArrayList<>(mappingSteps);
        }

        public void addStep(MappingStep mappingStep) {
            if (mappingStep == null) {
                throw new NullPointerException("mappingStep is null");
            }
            mappingSteps.add(mappingStep);
        }

        public MappingStep getStep(int stepIdx) {
            if (stepIdx < 0) {
                throw new IllegalArgumentException("stepIdx must be > 0");
            }
            if (stepIdx >= mappingSteps.size()) {
                throw new IndexOutOfBoundsException("stepIdx " + stepIdx + " is not present");
            }
            return mappingSteps.get(stepIdx);
        }

        public MappingStep getFirstStep() {
            if (mappingSteps.size() == 0) {
                throw new IndexOutOfBoundsException("the list is empty");
            }
            return mappingSteps.get(0);
        }

        public MappingStep getLastStep() {
            if (mappingSteps.size() == 0) {
                throw new IndexOutOfBoundsException("the list is empty");
            }
            return mappingSteps.get(mappingSteps.size()-1);
        }

        public boolean isJoiningEnabled() {
            boolean joiningEnabled = true;

            for (MappingStep mappingStep : mappingSteps) {
                joiningEnabled = joiningEnabled
                        && (!mappingStep.hasNestedFeature() || mappingStep.isJoiningNestedMapping());
            }

            return joiningEnabled;
        }

        public void clear() {
            mappingSteps.clear();
        }

        public int size() {
            return mappingSteps.size();
        }

    }

    /**
     * Represents a single step in the "chain" of feature types that need to be linked to go from the root type to a nested attribute.
     * 
     * @author Stefano Costa, GeoSolutions
     *
     */
    public static class MappingStep {

        private FeatureTypeMapping featureTypeMapping;

        private NestedAttributeMapping nestedFeatureAttribute;

        private String ownAttributeXPath;

        public MappingStep(FeatureTypeMapping featureType,
                NestedAttributeMapping nestedFeatureAttribute) {
            if (featureType == null) {
                throw new NullPointerException("featureType is null");
            }
            if (nestedFeatureAttribute == null) {
                throw new NullPointerException("nestedFeatureAttribute is null");
            }
            this.featureTypeMapping = featureType;
            this.nestedFeatureAttribute = nestedFeatureAttribute;
            this.ownAttributeXPath = null;
        }

        public MappingStep(FeatureTypeMapping featureType, String ownAttributeXPath) {
            if (featureType == null) {
                throw new NullPointerException("featureType is null");
            }
            if (ownAttributeXPath == null || ownAttributeXPath.trim().isEmpty()) {
                throw new IllegalArgumentException("ownAttributeXPath must not be null or empty");
            }
            this.featureTypeMapping = featureType;
            this.nestedFeatureAttribute = null;
            this.ownAttributeXPath = ownAttributeXPath;
        }

        public FeatureTypeMapping getFeatureTypeMapping() {
            return featureTypeMapping;
        }

        public NestedAttributeMapping getNestedFeatureAttribute() {
            return nestedFeatureAttribute;
        }

        public String getOwnAttributeXPath() {
            return ownAttributeXPath;
        }

        public <T extends NestedAttributeMapping> T getNestedFeatureAttribute(
                Class<T> attributeMappingClass) {
            return attributeMappingClass.cast(nestedFeatureAttribute);
        }

        public boolean isJoiningNestedMapping() {
            return nestedFeatureAttribute != null
                    && nestedFeatureAttribute instanceof JoiningNestedAttributeMapping;
        }

        public boolean hasNestedFeature() {
            return nestedFeatureAttribute != null;
        }

        public boolean hasOwnAttribute() {
            return ownAttributeXPath != null && !ownAttributeXPath.trim().isEmpty();
        }

    }
}
