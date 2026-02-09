/*-
 * #%L
 * athena-timestream
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
package com.amazonaws.athena.connectors.timestream.query;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static org.junit.Assert.*;

public class SelectQueryBuilderTest
{
    private static final Logger logger = LoggerFactory.getLogger(SelectQueryBuilderTest.class);
    private QueryFactory queryFactory = new QueryFactory();
    private BlockAllocator allocator;
    private Map<String, ValueSet> constraintsMap;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
        constraintsMap = new HashMap<>();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void build()
    {
        logger.info("build: enter");

        String expected = "SELECT col1, col2, col3, col4 FROM \"myDatabase\".\"myTable\" WHERE (\"col4\" IN ('val1','val2')) AND ((\"col2\" < 1)) AND (\"col3\" IN (20000,10000)) AND ((\"col1\" > 1))";

        constraintsMap.put("col1", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));
        constraintsMap.put("col2", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.lessThan(allocator, Types.MinorType.INT.getType(), 1)), false));
        constraintsMap.put("col3", EquatableValueSet.newBuilder(allocator, Types.MinorType.INT.getType(), true, true)
                .add(20000L)
                .add(10000L)
                .build());
        constraintsMap.put("col4", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("val1")
                .add("val2")
                .build());

        Schema schema = SchemaBuilder.newBuilder()
                //types shouldn't matter
                .addStringField("col1")
                .addIntField("col2")
                .addBigIntField("col3")
                .addStringField("col4")
                .build();

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName("myDatabase")
                .withTableName("myTable")
                .withProjection(schema)
                .withConjucts(createConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT))
                .build().replace("\n", "");

        logger.info("build: actual[{}]", actual);
        assertEquals(expected, actual);

        logger.info("build: exit");
    }

    @Test
    public void buildWithTime() {
        logger.info("build: enter");

        String expected = "SELECT val FROM \"myDatabase\".\"myTable\" WHERE ((\"time1\" > '2024-04-05 09:31:12.000000000')) AND ((\"time0\" > '2024-04-05 09:31:12.142000000'))";

        constraintsMap.put("time0", SortedRangeSet.copyOf(Types.MinorType.DATEMILLI.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(),
                        LocalDateTime.of(2024, 4, 5, 9, 31, 12, 142000000))), false));
        constraintsMap.put("time1", SortedRangeSet.copyOf(Types.MinorType.DATEMILLI.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(),
                        LocalDateTime.of(2024, 4, 5, 9, 31, 12))), false));

        Schema schema = SchemaBuilder.newBuilder()
                .addField("val", Types.MinorType.DATEMILLI.getType())
                .build();

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName("myDatabase")
                .withTableName("myTable")
                .withProjection(schema)
                .withConjucts(createConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT))
                .build().replace("\n", "");

        logger.info("build: actual[{}]", actual);
        assertEquals(expected, actual);

        logger.info("build: exit");
    }

    @Test
    public void buildWithView()
    {
        logger.info("build: buildWithView");

        String expected = "WITH t1 AS ( SELECT col1 from test_table )  SELECT col1, col2, col3, col4 FROM t1 WHERE ((\"col2\" < 1)) AND ((\"col1\" > 1))";

        Schema schema = SchemaBuilder.newBuilder()
                //types shouldn't matter
                .addStringField("col1")
                .addIntField("col2")
                .addBigIntField("col3")
                .addStringField("col4")
                .addMetadata(VIEW_METADATA_FIELD, "SELECT col1 from test_table")
                .build();

        constraintsMap.put("col1", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));
        constraintsMap.put("col2", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.lessThan(allocator, Types.MinorType.INT.getType(), 1)), false));

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName("myDatabase")
                .withTableName("myTable")
                .withProjection(schema)
                .withConjucts(createConstraints(constraintsMap, Collections.emptyList(), DEFAULT_NO_LIMIT))
                .build().replace("\n", "");

        logger.info("build: actual[{}]", actual);
        assertEquals(expected, actual);

        logger.info("build: buildWithView");
    }

    @Test
    public void build_WithOrderBy_IncludesOrderByClause()
    {
        logger.info("build_WithOrderBy_IncludesOrderByClause: enter");

        String expected = "SELECT col1, col2 FROM \"myDatabase\".\"myTable\" ORDER BY \"col1\" ASC NULLS FIRST, \"col2\" DESC NULLS LAST";

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .addStringField("col2")
                .build();

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField("col1", OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField("col2", OrderByField.Direction.DESC_NULLS_LAST));

        Constraints constraints = createConstraints(Collections.emptyMap(), orderByFields, DEFAULT_NO_LIMIT);

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName("myDatabase")
                .withTableName("myTable")
                .withProjection(schema)
                .withConjucts(constraints)
                .withOrderByClause(constraints)
                .build().replace("\n", "");

        logger.info("build_WithOrderBy_IncludesOrderByClause: actual[{}]", actual);
        assertEquals(expected, actual);

        logger.info("build_WithOrderBy_IncludesOrderByClause: exit");
    }

    @Test
    public void build_WithLimit_IncludesLimitClause()
    {
        logger.info("build_WithLimit_IncludesLimitClause: enter");

        String expected = "SELECT col1 FROM \"myDatabase\".\"myTable\" LIMIT 10";

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .build();

        Constraints constraints = createConstraints(Collections.emptyMap(), Collections.emptyList(), 10L);

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName("myDatabase")
                .withTableName("myTable")
                .withProjection(schema)
                .withConjucts(constraints)
                .withLimitClause(constraints)
                .build().replace("\n", "");

        logger.info("build_WithLimit_IncludesLimitClause: actual[{}]", actual);
        assertEquals(expected, actual);

        logger.info("build_WithLimit_IncludesLimitClause: exit");
    }

    @Test
    public void build_WithOrderByAndLimit_IncludesBothClauses()
    {
        logger.info("build_WithOrderByAndLimit_IncludesBothClauses: enter");

        String expected = "SELECT col1 FROM \"myDatabase\".\"myTable\" ORDER BY \"col1\" DESC NULLS LAST LIMIT 5";

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .build();

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField("col1", OrderByField.Direction.DESC_NULLS_LAST));

        Constraints constraints = createConstraints(Collections.emptyMap(), orderByFields, 5L);

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName("myDatabase")
                .withTableName("myTable")
                .withProjection(schema)
                .withConjucts(constraints)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
                .build().replace("\n", "");

        logger.info("build_WithOrderByAndLimit_IncludesBothClauses: actual[{}]", actual);
        assertEquals(expected, actual);

        logger.info("build_WithOrderByAndLimit_IncludesBothClauses: exit");
    }

    @Test
    public void build_WithWhereAndOrderByAndLimit_IncludesAllClauses()
    {
        logger.info("build_WithWhereAndOrderByAndLimit_IncludesAllClauses: enter");

        String expected = "SELECT col1 FROM \"myDatabase\".\"myTable\" WHERE ((\"col1\" > 1)) ORDER BY \"col1\" ASC NULLS FIRST LIMIT 100";

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("col1")
                .build();

        constraintsMap.put("col1", SortedRangeSet.copyOf(Types.MinorType.INT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 1)), false));

        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField("col1", OrderByField.Direction.ASC_NULLS_FIRST));

        Constraints constraints = createConstraints(constraintsMap, orderByFields, 100L);

        String actual = queryFactory.createSelectQueryBuilder(VIEW_METADATA_FIELD)
                .withDatabaseName("myDatabase")
                .withTableName("myTable")
                .withProjection(schema)
                .withConjucts(constraints)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
                .build().replace("\n", "");

        logger.info("build_WithWhereAndOrderByAndLimit_IncludesAllClauses: actual[{}]", actual);
        assertEquals(expected, actual);

        logger.info("build_WithWhereAndOrderByAndLimit_IncludesAllClauses: exit");
    }

    private Constraints createConstraints(Map<String, ValueSet> constraintsMap, List<OrderByField> orderByFields, long limit)
    {
        return new Constraints(constraintsMap, Collections.emptyList(), orderByFields, limit, Collections.emptyMap(), null);
    }
}
