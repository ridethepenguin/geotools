/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 * 
 *    (C) 2005-2008, Open Source Geospatial Foundation (OSGeo)
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.GeoTools;
import org.geotools.filter.NestedAttributeExpression;
import org.geotools.filter.visitor.DuplicatingFilterVisitor;
import org.opengis.filter.And;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.FilterVisitor;
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
import org.opengis.filter.expression.ExpressionVisitor;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.InternalFunction;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.NilExpression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.BBOX3D;
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

/**
 * Used to duplication Filters and/or Expressions - returned object is a copy.
 * <p>
 * Extra data can be used to provide a {@link FilterFactory2} but this is NOT required.
 * This class is thread safe.
 * </ul>
 * @author Jesse
 *
 *
 *
 *
 * @source $URL$
 */
public class NestedToSimpleFilterVisitor extends DuplicatingFilterVisitor {

	// protected final FilterFactory2 ff;
	String filterProperty;
	String newFilterProperty;

	public NestedToSimpleFilterVisitor(String filterProperty, String newFilterProperty) {
		super();
		this.filterProperty = filterProperty;
		this.newFilterProperty = newFilterProperty;
		
	}
		
	public Expression visit(NestedAttributeExpression expression, Object extraData) {
	    if(expression == null)
	        return null;
	    if(expression.getPropertyName().equals(filterProperty)) {
	    	return getFactory(extraData).property(newFilterProperty);
	    }
	    return expression;
	}
	
	/*public Expression visit(NestedAttributeExpression expression, Object extraData) {
	    if(expression == null)
	        return null;
	    if(expression.getPropertyName().equals(filterProperty) && !newFilterProperty.equalsIgnoreCase("@gml:id")) {
	    	return getFactory(extraData).property(newFilterProperty);
	    }
	    return expression;
	}
	
	public Object visit(PropertyIsBetween filter, Object extraData) {
		Expression expr= visit(filter.getExpression(), extraData);
		Expression lower= visit(filter.getLowerBoundary(), extraData);
		Expression upper= visit(filter.getUpperBoundary(), extraData);
		return getFactory(extraData).between(expr, lower, upper, filter.getMatchAction());
	}

	public Object visit(PropertyIsEqualTo filter, Object extraData) {
		Expression expr1= visit(filter.getExpression1(), extraData);
		Expression expr2= visit(filter.getExpression2(), extraData);
		if(expr1 instanceof PropertyName && newFilterProperty.equalsIgnoreCase("@gml:id")) {
			return getFactory(extraData).id(getFactory(extraData).featureId(expr2.toString()));
		}
		if(expr2 instanceof PropertyName && newFilterProperty.equalsIgnoreCase("@gml:id")) {
			return getFactory(extraData).id(getFactory(extraData).featureId(expr1.toString()));
		}
		
		boolean matchCase=filter.isMatchingCase();
		return getFactory(extraData).equal(expr1, expr2, matchCase, filter.getMatchAction());
	}

	public Object visit(PropertyIsNotEqualTo filter, Object extraData) {
	    Expression expr1= visit(filter.getExpression1(), extraData);
        Expression expr2= visit(filter.getExpression2(), extraData);
		boolean matchCase=filter.isMatchingCase();
		return getFactory(extraData).notEqual(expr1, expr2, matchCase, filter.getMatchAction());
	}

	public Object visit(PropertyIsGreaterThan filter, Object extraData) {
	    Expression expr1= visit(filter.getExpression1(), extraData);
        Expression expr2= visit(filter.getExpression2(), extraData);
		return getFactory(extraData).greater(expr1, expr2, filter.isMatchingCase(), filter.getMatchAction());
	}

	public Object visit(PropertyIsGreaterThanOrEqualTo filter, Object extraData) {
	    Expression expr1= visit(filter.getExpression1(), extraData);
        Expression expr2= visit(filter.getExpression2(), extraData);
		return getFactory(extraData).greaterOrEqual(expr1, expr2, filter.isMatchingCase(), filter.getMatchAction());
	}

	public Object visit(PropertyIsLessThan filter, Object extraData) {
	    Expression expr1= visit(filter.getExpression1(), extraData);
        Expression expr2= visit(filter.getExpression2(), extraData);
		return getFactory(extraData).less(expr1, expr2, filter.isMatchingCase(), filter.getMatchAction());
	}

	public Object visit(PropertyIsLessThanOrEqualTo filter, Object extraData) {
	    Expression expr1= visit(filter.getExpression1(), extraData);
        Expression expr2= visit(filter.getExpression2(), extraData);
		return getFactory(extraData).lessOrEqual(expr1, expr2, filter.isMatchingCase(), filter.getMatchAction());
	}

	public Object visit(PropertyIsLike filter, Object extraData) {
		Expression expr= visit(filter.getExpression(), extraData);
		String pattern=filter.getLiteral();
		String wildcard=filter.getWildCard();
		String singleChar=filter.getSingleChar();
		String escape=filter.getEscape();
                boolean matchCase = filter.isMatchingCase();
		return getFactory(extraData).like(expr, pattern, wildcard, singleChar, escape, matchCase, filter.getMatchAction());
	}

	public Object visit(PropertyIsNull filter, Object extraData) {
		Expression expr= visit(filter.getExpression(), extraData);
		return getFactory(extraData).isNull(expr);
	}

    public Object visit(PropertyIsNil filter, Object extraData) {
        Expression expr = visit(filter.getExpression(), extraData);
        return getFactory(extraData).isNil(expr, filter.getNilReason());
    }*/

	public Object visit(PropertyName expression, Object extraData) {
		if(expression.getPropertyName().equals(filterProperty)) {
			return getFactory(extraData).property(newFilterProperty, expression.getNamespaceContext());
	    }
	    //NC - namespace support
	    return getFactory(extraData).property(expression.getPropertyName(), expression.getNamespaceContext());	    
	}
}
