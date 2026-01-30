/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream.query;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PredicateBuilderTest
{
    private static final ArrowType INT_TYPE = Types.MinorType.INT.getType();
    private static final ArrowType VARCHAR_TYPE = Types.MinorType.VARCHAR.getType();
    private static final ArrowType DATEMILLI_TYPE = Types.MinorType.DATEMILLI.getType();

    private BlockAllocator allocator;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void buildConjucts_WithSingleValue_ReturnsEqualityPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 42), Marker.exactly(allocator, INT_TYPE, 42)))
                .build();
        constraintsMap.put("intCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain intCol", conjuncts.get(0).contains("intCol"));
        assertTrue("Conjunct should contain =", conjuncts.get(0).contains("="));
        assertTrue("Conjunct should contain 42", conjuncts.get(0).contains("42"));
    }

    @Test
    public void buildConjucts_WithMultipleValues_ReturnsInPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        SortedRangeSet.Builder builder = SortedRangeSet.newBuilder(INT_TYPE, false);
        for (Object value : new Object[]{1, 2, 3}) {
            builder.add(new Range(Marker.exactly(allocator, INT_TYPE, value), Marker.exactly(allocator, INT_TYPE, value)));
        }
        ValueSet valueSet = builder.build();
        constraintsMap.put("intCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IN", conjuncts.get(0).contains("IN"));
        assertTrue("Conjunct should contain 1", conjuncts.get(0).contains("1"));
        assertTrue("Conjunct should contain 2", conjuncts.get(0).contains("2"));
        assertTrue("Conjunct should contain 3", conjuncts.get(0).contains("3"));
    }

    @Test
    public void buildConjucts_WithRange_ReturnsRangePredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 20)))
                .build();
        constraintsMap.put("intCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain >=", conjuncts.get(0).contains(">="));
        assertTrue("Conjunct should contain <=", conjuncts.get(0).contains("<="));
        assertTrue("Conjunct should contain 10", conjuncts.get(0).contains("10"));
        assertTrue("Conjunct should contain 20", conjuncts.get(0).contains("20"));
    }

    @Test
    public void buildConjucts_WithNullValue_ReturnsNullPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet = SortedRangeSet.newBuilder(VARCHAR_TYPE, true).build();
        constraintsMap.put("varcharCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
    }

    @Test
    public void buildConjucts_WithNotNullValue_ReturnsNotNullPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintsMap.put("intCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NOT NULL", conjuncts.get(0).contains("IS NOT NULL"));
    }

    @Test
    public void buildConjucts_WithEquatableValueSet_ReturnsInPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet = EquatableValueSet.newBuilder(allocator, VARCHAR_TYPE, true, true)
                .add("val1")
                .add("val2")
                .build();
        constraintsMap.put("varcharCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IN", conjuncts.get(0).contains("IN"));
        assertTrue("Conjunct should contain val1", conjuncts.get(0).contains("val1"));
        assertTrue("Conjunct should contain val2", conjuncts.get(0).contains("val2"));
    }

    @Test
    public void buildConjucts_WithTimestampValue_FormatsTimestampCorrectly()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        LocalDateTime timestamp = LocalDateTime.of(2024, 4, 5, 9, 31, 12, 142000000);
        ValueSet valueSet = SortedRangeSet.newBuilder(DATEMILLI_TYPE, false)
                .add(new Range(Marker.exactly(allocator, DATEMILLI_TYPE, timestamp), Marker.exactly(allocator, DATEMILLI_TYPE, timestamp)))
                .build();
        constraintsMap.put("timeCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain timestamp format", conjuncts.get(0).contains("2024-04-05 09:31:12.142000000"));
    }

    @Test
    public void buildConjucts_WithMultipleColumns_ReturnsMultipleConjuncts()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet1 = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 1), Marker.exactly(allocator, INT_TYPE, 1)))
                .build();
        ValueSet valueSet2 = SortedRangeSet.newBuilder(VARCHAR_TYPE, false)
                .add(new Range(Marker.exactly(allocator, VARCHAR_TYPE, "test"), Marker.exactly(allocator, VARCHAR_TYPE, "test")))
                .build();
        constraintsMap.put("intCol", valueSet1);
        constraintsMap.put("varcharCol", valueSet2);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have two conjuncts", 2, conjuncts.size());
    }

    @Test
    public void buildConjucts_WithNoConstraints_ReturnsEmptyList()
    {
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have no conjuncts", 0, conjuncts.size());
    }

    @Test
    public void buildConjucts_WithGreaterThan_ReturnsGreaterThanPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintsMap.put("intCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain >", conjuncts.get(0).contains(">"));
        assertTrue("Conjunct should contain 10", conjuncts.get(0).contains("10"));
    }

    @Test
    public void buildConjucts_WithLessThan_ReturnsLessThanPredicate()
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        ValueSet valueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintsMap.put("intCol", valueSet);

        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        List<String> conjuncts = PredicateBuilder.buildConjucts(constraints);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain <", conjuncts.get(0).contains("<"));
        assertTrue("Conjunct should contain 20", conjuncts.get(0).contains("20"));
    }
}
