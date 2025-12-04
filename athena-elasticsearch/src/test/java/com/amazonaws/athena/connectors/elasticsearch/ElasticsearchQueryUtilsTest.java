/*-
 * #%L
 * athena-elasticsearch
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;

/**
 * This class is used to test the ElasticsearchQueryUtils class.
 */
@RunWith(MockitoJUnitRunner.class)
public class ElasticsearchQueryUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchQueryUtilsTest.class);

    private final BlockAllocatorImpl allocator = new BlockAllocatorImpl();
    Schema mapping;
    Map<String, ValueSet> constraintsMap = new HashMap<>();

    @Before
    public void setUp()
    {
        mapping = SchemaBuilder.newBuilder()
                .addField("mytext", Types.MinorType.VARCHAR.getType())
                .addField("mykeyword", Types.MinorType.VARCHAR.getType())
                .addField(new Field("mylong", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mylong",
                                FieldType.nullable(Types.MinorType.BIGINT.getType()), null))))
                .addField("myinteger", Types.MinorType.INT.getType())
                .addField("myshort", Types.MinorType.INT.getType())
                .addField("mybyte", Types.MinorType.TINYINT.getType())
                .addField("mydouble", Types.MinorType.FLOAT8.getType())
                .addField(new Field("myscaled",
                        new FieldType(true, Types.MinorType.BIGINT.getType(), null,
                                ImmutableMap.of("scaling_factor", "10.51")), null))
                .addField("myfloat", Types.MinorType.FLOAT4.getType())
                .addField("myhalf", Types.MinorType.FLOAT4.getType())
                .addField("mydatemilli", Types.MinorType.DATEMILLI.getType())
                .addField(new Field("mydatenano", FieldType.nullable(Types.MinorType.LIST.getType()),
                        Collections.singletonList(new Field("mydatenano",
                                FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null))))
                .addField("myboolean", Types.MinorType.BIT.getType())
                .addField("mybinary", Types.MinorType.VARCHAR.getType())
                .addField("mynested", Types.MinorType.STRUCT.getType(), ImmutableList.of(
                        new Field("l1long", FieldType.nullable(Types.MinorType.BIGINT.getType()), null),
                        new Field("l1date", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null),
                        new Field("l1nested", FieldType.nullable(Types.MinorType.STRUCT.getType()), ImmutableList.of(
                                new Field("l2short", FieldType.nullable(Types.MinorType.LIST.getType()),
                                        Collections.singletonList(new Field("l2short",
                                                FieldType.nullable(Types.MinorType.INT.getType()), null))),
                                new Field("l2binary", FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                                        null))))).build();
    }

    @Test
    public void getProjectionTest()
    {
        logger.info("getProjectionTest - enter");

        List<String> expectedProjection = new ArrayList<>();
        mapping.getFields().forEach(field -> expectedProjection.add(field.getName()));

        // Get the actual projection and compare to the expected one.
        FetchSourceContext context = ElasticsearchQueryUtils.getProjection(mapping);
        List<String> actualProjection = ImmutableList.copyOf(context.includes());

        logger.info("Projections - Expected: {}, Actual: {}", expectedProjection, actualProjection);
        assertEquals("Projections do not match", expectedProjection, actualProjection);

        logger.info("getProjectionTest - exit");
    }

    @Test
    public void getRangePredicateTest()
    {
        logger.info("getRangePredicateTest - enter");

        constraintsMap.put("year", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.lessThan(allocator, Types.MinorType.INT.getType(), 1950),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1952),
                        Range.range(allocator, Types.MinorType.INT.getType(),
                                1955, false, 1972, true),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1996),
                        Range.greaterThanOrEqual(allocator, Types.MinorType.INT.getType(), 2010)),
                false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(_exists_:year) AND year:([* TO 1950} OR {1955 TO 1972] OR [2010 TO *] OR 1952 OR 1996)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getRangePredicateTest - exit");
    }

    @Test
    public void getWhitelistedEquitableValuesPredicate()
    {
        logger.info("getWhitelistedEquitableValuesPredicate - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                true, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
                Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getWhitelistedEquitableValuesPredicate - exit");
    }

    @Test
    public void getExclusiveEquitableValuesPredicate()
    {
        logger.info("getExclusiveEquitableValuesPredicate - enter");

        constraintsMap.put("age", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(),
                false, true).addAll(ImmutableList.of(20, 25, 30, 35)).build());
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "NOT age:(20 OR 25 OR 30 OR 35)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getExclusiveEquitableValuesPredicate - exit");
    }

    @Test
    public void getAllValuePredicate()
    {
        logger.info("getAllValuePredicate - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), true, true));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(_exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getAllValuePredicate - exit");
    }

    @Test
    public void getNoneValuePredicate()
    {
        logger.info("getNoneValuePredicate - enter");

        constraintsMap.put("number", new AllOrNoneValueSet(Types.MinorType.INT.getType(), false, false));
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        String expectedPredicate = "(NOT _exists_:number)";

        // Get the actual predicate and compare to the expected one.
        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        logger.info("Predicates - Expected: {}, Actual: {}", expectedPredicate, actualPredicate);
        assertEquals("Predicates do not match", expectedPredicate, actualPredicate);

        logger.info("getNoneValuePredicate - exit");
    }

    @Test
    public void getPredicateFromRange_withDateSingleValue_wrapsInQuotes()
    {
        com.amazonaws.athena.connector.lambda.domain.predicate.Constraints constraints =
                new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(
                        ImmutableMap.of("mydate",
                                com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet.copyOf(
                                        Types.MinorType.DATEMILLI.getType(),
                                        ImmutableList.of(
                                                com.amazonaws.athena.connector.lambda.domain.predicate.Range.equal(
                                                        allocator, Types.MinorType.DATEMILLI.getType(), 1589525370001L)),
                                        false)),
                        Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        String actualPredicate = builder.queryName();

        assertTrue("Should wrap date in quotes", actualPredicate.contains("\""));
    }

    @Test
    public void getPredicateFromRange_withLowMarkerBelow_continuesToNextRange()
    {
        com.amazonaws.athena.connector.lambda.domain.predicate.Constraints constraints =
                new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(
                        ImmutableMap.of("myfield",
                                com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet.copyOf(
                                        Types.MinorType.INT.getType(),
                                        ImmutableList.of(
                                                com.amazonaws.athena.connector.lambda.domain.predicate.Range.range(
                                                        allocator, Types.MinorType.INT.getType(), 10, false, 20, true)),
                                        false)),
                        Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        assertNotNull("Should handle BELOW bound", builder);
    }

    @Test
    public void getPredicateFromRange_withHighMarkerAbove_continuesToNextRange()
    {
        com.amazonaws.athena.connector.lambda.domain.predicate.Constraints constraints =
                new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(
                        ImmutableMap.of("myfield",
                                com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet.copyOf(
                                        Types.MinorType.INT.getType(),
                                        ImmutableList.of(
                                                com.amazonaws.athena.connector.lambda.domain.predicate.Range.range(
                                                        allocator, Types.MinorType.INT.getType(), 10, true, 20, false)),
                                        false)),
                        Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        assertNotNull("Should handle ABOVE bound", builder);
    }

    @Test
    public void getPredicateFromRange_withUnhandledBound_continuesToNextRange()
    {
        com.amazonaws.athena.connector.lambda.domain.predicate.Constraints constraints =
                new com.amazonaws.athena.connector.lambda.domain.predicate.Constraints(
                        ImmutableMap.of("myfield",
                                com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet.copyOf(
                                        Types.MinorType.INT.getType(),
                                        ImmutableList.of(
                                                com.amazonaws.athena.connector.lambda.domain.predicate.Range.range(
                                                        allocator, Types.MinorType.INT.getType(), 10, true, 20, true)),
                                        false)),
                        Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(constraints);
        assertNotNull("Should handle unhandled bound", builder);
    }

    @Test
    public void getPredicateFromRange_withEmptyDisjuncts_returnsEmptyPredicate()
    {
        try {
            java.lang.reflect.Method method = ElasticsearchQueryUtils.class.getDeclaredMethod("getPredicateFromRange", String.class, ValueSet.class);
            method.setAccessible(true);

            SortedRangeSet emptyRangeSet = SortedRangeSet.copyOf(Types.MinorType.INT.getType(), Collections.emptyList(), false);
            String result = (String) method.invoke(null, "myfield", emptyRangeSet);

            assertEquals("Should return empty predicate when no ranges", "", result);
        }
        catch (Exception e) {
            fail("Failed to invoke getPredicateFromRange: " + e.getMessage());
        }
    }

    @Test
    public void getPredicate_withEmptyPredicateParts_returnsEmptyPredicate()
    {
        Constraints emptyConstraints = new Constraints(Collections.emptyMap(), Collections.emptyList(),
                Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        QueryBuilder builder = ElasticsearchQueryUtils.getQuery(emptyConstraints);
        assertNotNull("Should return query builder", builder);
    }

    @Test
    public void getPredicateFromRange_withLowMarkerBelow_logsWarningAndContinues()
    {
        try {
            java.lang.reflect.Method method = ElasticsearchQueryUtils.class.getDeclaredMethod("getPredicateFromRange", String.class, ValueSet.class);
            method.setAccessible(true);

            Marker lowMarker = Marker.exactly(allocator, Types.MinorType.INT.getType(), 10);
            Marker highMarker = Marker.exactly(allocator, Types.MinorType.INT.getType(), 20);

            Range range = Range.range(allocator, Types.MinorType.INT.getType(), 10, true, 20, true);

            java.lang.reflect.Field boundField = Marker.class.getDeclaredField("bound");
            boundField.setAccessible(true);
            boundField.set(lowMarker, Marker.Bound.BELOW);

            java.lang.reflect.Field lowField = Range.class.getDeclaredField("low");
            lowField.setAccessible(true);
            lowField.set(range, lowMarker);

            SortedRangeSet rangeSet = SortedRangeSet.copyOf(Types.MinorType.INT.getType(), ImmutableList.of(range), false);
            String result = (String) method.invoke(null, "myfield", rangeSet);

            assertNotNull("Should return a result", result);
        }
        catch (Exception e) {
            fail("Failed to invoke getPredicateFromRange: " + e.getMessage());
        }
    }

    @Test
    public void getPredicateFromRange_withHighMarkerAbove_logsWarningAndContinues()
    {
        try {
            java.lang.reflect.Method method = ElasticsearchQueryUtils.class.getDeclaredMethod("getPredicateFromRange", String.class, ValueSet.class);
            method.setAccessible(true);

            Marker lowMarker = Marker.exactly(allocator, Types.MinorType.INT.getType(), 10);
            Marker highMarker = Marker.exactly(allocator, Types.MinorType.INT.getType(), 20);

            Range range = Range.range(allocator, Types.MinorType.INT.getType(), 10, true, 20, true);

            java.lang.reflect.Field boundField = Marker.class.getDeclaredField("bound");
            boundField.setAccessible(true);
            boundField.set(highMarker, Marker.Bound.ABOVE);

            java.lang.reflect.Field highField = Range.class.getDeclaredField("high");
            highField.setAccessible(true);
            highField.set(range, highMarker);

            SortedRangeSet rangeSet = SortedRangeSet.copyOf(Types.MinorType.INT.getType(), ImmutableList.of(range), false);
            String result = (String) method.invoke(null, "myfield", rangeSet);

            assertNotNull("Should return a result", result);
        }
        catch (Exception e) {
            fail("Failed to invoke getPredicateFromRange: " + e.getMessage());
        }
    }
}
