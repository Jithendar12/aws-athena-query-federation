/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.dynamodb.qpt.DDBQueryPassthrough;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.amazonaws.util.json.Jackson;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.paginators.GetDatabasesIterable;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.COLUMN_NAME_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.SOURCE_TABLE_PROPERTY;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler.DYNAMO_DB_FLAG;
import static com.amazonaws.athena.connectors.dynamodb.DynamoDBMetadataHandler.MAX_SPLITS_PER_REQUEST;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.DEFAULT_SCHEMA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_NAMES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.EXPRESSION_VALUES_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.HASH_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.INDEX_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.NON_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.PARTITION_TYPE_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.QUERY_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_FILTER_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.RANGE_KEY_NAME_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SCAN_PARTITION_TYPE;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_COUNT_METADATA;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.SEGMENT_ID_PROPERTY;
import static com.amazonaws.athena.connectors.dynamodb.constants.DynamoDBConstants.TABLE_METADATA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Glue logic is tested by GlueMetadataHandlerTest in SDK
 */
@RunWith(MockitoJUnitRunner.class)
public class DynamoDBMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBMetadataHandlerTest.class);

    @Rule
    public TestName testName = new TestName();

    @Mock
    private GlueClient glueClient;

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient athena;

    private DynamoDBMetadataHandler handler;

    private BlockAllocator allocator;

    @Before
    public void setup()
    {
        logger.info("{}: enter", testName.getMethodName());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        allocator = new BlockAllocatorImpl();
        handler = new DynamoDBMetadataHandler(new LocalKeyFactory(), secretsManager, athena, "spillBucket", "spillPrefix", ddbClient, glueClient, com.google.common.collect.ImmutableMap.of());
    }

    @After
    public void tearDown()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNamesGlueError()
            throws Exception
    {
        when(glueClient.getDatabasesPaginator(any(GetDatabasesRequest.class))).thenThrow(new AmazonServiceException(""));

        ListSchemasRequest req = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertThat(new ArrayList<>(res.getSchemas()), equalTo(Collections.singletonList(DEFAULT_SCHEMA)));
    }

    @Test
    public void doListSchemaNamesGlue()
            throws Exception
    {
        GetDatabasesResponse response = GetDatabasesResponse.builder()
                .databaseList(
                        Database.builder().name(DEFAULT_SCHEMA).build(),
                        Database.builder().name("ddb").locationUri(DYNAMO_DB_FLAG).build(),
                        Database.builder().name("s3").locationUri("blah").build())
                .build();

        GetDatabasesIterable mockIterable = mock(GetDatabasesIterable.class);
        when(mockIterable.stream()).thenReturn(Collections.singletonList(response).stream());
        when(glueClient.getDatabasesPaginator(any(GetDatabasesRequest.class))).thenReturn(mockIterable);

        ListSchemasRequest req = new ListSchemasRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());

        assertThat(res.getSchemas().size(), equalTo(2));
        assertThat(res.getSchemas().contains("default"), is(true));
        assertThat(res.getSchemas().contains("ddb"), is(true));
    }

    @Test
    public void doListTablesGlueAndDynamo()
            throws Exception
    {
        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        List<Table> tableList = new ArrayList<>();
        tableList.add(Table.builder().name("table1")
                .parameters(ImmutableMap.of("classification", "dynamodb"))
                .storageDescriptor(StorageDescriptor.builder()
                        .location("some.location")
                        .build())
                .build());
        tableList.add(Table.builder().name("table2")
                .parameters(ImmutableMap.of())
                .storageDescriptor(StorageDescriptor.builder()
                        .location("some.location")
                        .parameters(ImmutableMap.of("classification", "dynamodb"))
                        .build())
                .build());
        tableList.add(Table.builder().name("table3")
                .parameters(ImmutableMap.of())
                .storageDescriptor(StorageDescriptor.builder()
                        .location("arn:aws:dynamodb:us-east-1:012345678910:table/table3")
                        .build())
                .build());
        tableList.add(Table.builder().name("notADynamoTable")
                .parameters(ImmutableMap.of())
                .storageDescriptor(StorageDescriptor.builder()
                        .location("some_location")
                        .parameters(ImmutableMap.of())
                        .build())
                .build());
        GetTablesResponse mockResponse = GetTablesResponse.builder()
                .tableList(tableList)
                .build();
        when(glueClient.getTables(any(GetTablesRequest.class))).thenReturn(mockResponse);

        ListTablesRequest req = new ListTablesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, DEFAULT_SCHEMA,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());

        List<TableName> expectedTables = tableNames.stream().map(table -> new TableName(DEFAULT_SCHEMA, table)).collect(Collectors.toList());
        expectedTables.add(TEST_TABLE_NAME);
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table2"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table3"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table4"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table5"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table6"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table7"));
        expectedTables.add(new TableName(DEFAULT_SCHEMA, "test_table8"));

        assertThat(new HashSet<>(res.getTables()), equalTo(new HashSet<>(expectedTables)));
    }

    @Test
    public void doListPaginatedTables()
            throws Exception
    {
        ListTablesRequest req = new ListTablesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, DEFAULT_SCHEMA,
                null, 2);
        ListTablesResponse res = handler.doListTables(allocator, req);
        assertThat(res.getNextToken(), not(equalTo(null)));
        assertThat(res.getTables().size(), equalTo(2));
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName().getSchemaName(), equalTo(DEFAULT_SCHEMA));
        assertThat(res.getTableName().getTableName(), equalTo(TEST_TABLE));
        assertThat(res.getSchema().getFields().size(), equalTo(12));
    }

    @Test
    public void doGetEmptyTable()
            throws Exception
    {
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE_2_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetEmptyTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(TEST_TABLE_2_NAME));
        assertThat(res.getSchema().getFields().size(), equalTo(2));
    }

    @Test
    public void testCaseInsensitiveResolve()
            throws Exception
    {
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(new AmazonServiceException(""));

        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, TEST_TABLE_2_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        logger.info("doGetTable - {}", res.getSchema());

        assertThat(res.getTableName(), equalTo(TEST_TABLE_2_NAME));
    }

    @Test(expected = AthenaConnectorException.class)
    public void testCaseInsensitiveResolve_whenTableNotFound_throwsAthenaConnectorException()
            throws Exception
    {
        lenient().when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(new AmazonServiceException(""));

        // Request a table that doesn't exist in our test setup
        TableName nonExistentTable = new TableName(DEFAULT_SCHEMA, "NonExistentTable");
        GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, nonExistentTable, Collections.emptyMap());
        
        handler.doGetTable(allocator, req);
    }

    @Test
    public void testCaseInsensitiveResolve_whenDifferentCase()
    {
        try {
            lenient().when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(new AmazonServiceException(""));

            // Test with lowercase version of Test_table2 (existing table with mixed case)
            TableName tableWithDifferentCase = new TableName(DEFAULT_SCHEMA, "test_table2");
            GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableWithDifferentCase, Collections.emptyMap());
            GetTableResponse res = handler.doGetTable(allocator, req);

            assertThat(res.getTableName().getTableName(), equalTo("test_table2"));
            assertThat(res.getSchema().getFields().size(), equalTo(2));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test
    public void testCaseInsensitiveResolve_whenGetTableMetadata()
    {
        try {
            lenient().when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(new AmazonServiceException(""));

            // Test the getTableMetadata path through getTableLayout
            GetTableLayoutRequest req = new GetTableLayoutRequest(TEST_IDENTITY,
                    TEST_QUERY_ID,
                    TEST_CATALOG_NAME,
                    new TableName(DEFAULT_SCHEMA, "test_table2"), // Different case from Test_table2
                    new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                    SchemaBuilder.newBuilder().build(),
                    Collections.emptySet());

            GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

            // Should successfully resolve and contain the correct table metadata
            assertThat(res.getPartitions().getSchema().getCustomMetadata().get(TABLE_METADATA), equalTo(TEST_TABLE2));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test
    public void testCaseInsensitiveResolve_whenMixedCaseTableName()
    {
        try {
            lenient().when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenThrow(new AmazonServiceException(""));

            // Use a different case variation of TEST_TABLE2
            TableName mixedCaseTable = new TableName(DEFAULT_SCHEMA, "test_table2"); // lowercase version of Test_table2
            GetTableRequest req = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, mixedCaseTable, Collections.emptyMap());
            GetTableResponse res = handler.doGetTable(allocator, req);

            logger.info("testCaseInsensitiveResolve_whenMixedCaseTableName - {}", res.getSchema());

            // Schema should be successfully inferred, proving case-insensitive resolution worked
            assertThat(res.getTableName().getTableName(), equalTo("test_table2"));
            assertThat(res.getSchema().getFields().size(), equalTo(2));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test
    public void doGetTableLayoutScan()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col_3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());
        GetTableLayoutRequest req = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                new TableName(TEST_CATALOG_NAME, TEST_TABLE),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);
        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(res.getPartitions().getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(res.getPartitions().getRowCount(), equalTo(1));

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_3 = :v0 OR attribute_not_exists(#col_3) OR #col_3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_3", "col_3");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(true), ":v1", DDBTypeUtils.toAttributeValue(null));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(EnhancedDocument.fromAttributeValueMap(expressionValues).toJson()));
    }

    @Test
    public void doGetTableLayoutQueryIndex()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        SortedRangeSet.Builder dateValueSet = SortedRangeSet.newBuilder(Types.MinorType.DATEDAY.getType(), false);
        SortedRangeSet.Builder timeValueSet = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
        LocalDateTime dateTime = LocalDateTime.of(2019, 9, 23, 11, 18, 37);
        Instant epoch = Instant.MIN; //Set to Epoch time
        dateValueSet.add(Range.equal(allocator, Types.MinorType.DATEDAY.getType(), ChronoUnit.DAYS.between(epoch, dateTime.toInstant(ZoneOffset.UTC))));
        LocalDateTime dateTime2 = dateTime.plusHours(26);
        dateValueSet.add(Range.equal(allocator, Types.MinorType.DATEDAY.getType(), ChronoUnit.DAYS.between(epoch, dateTime2.toInstant(ZoneOffset.UTC))));
        long startTime = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTime = dateTime2.toInstant(ZoneOffset.UTC).toEpochMilli();
        timeValueSet.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime, true,
                endTime, true));
        constraintsMap.put("col_4", dateValueSet.build());
        constraintsMap.put("col_5", timeValueSet.build());

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));

        logger.info("doGetTableLayout schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayout partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(QUERY_PARTITION_TYPE));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().containsKey(INDEX_METADATA), is(true));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(INDEX_METADATA), equalTo("test_index"));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(HASH_KEY_NAME_METADATA), equalTo("col_4"));
        assertThat(res.getPartitions().getRowCount(), equalTo(2));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_NAME_METADATA), equalTo("col_5"));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 BETWEEN :v0 AND :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col_4", "col_4", "#col_5", "col_5");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(startTime), ":v1", DDBTypeUtils.toAttributeValue(endTime));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(EnhancedDocument.fromAttributeValueMap(expressionValues).toJson()));

        // Tests to validate that we correctly generate predicates that avoid this error:
        //    "KeyConditionExpressions must only contain one condition per key"
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
                true /* inclusive lowerbound */, endTime, false /* exclusive upperbound */));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            // Verify that only the upper bound is present
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 < :v0)"));
        }

        // For the same filters that we applied above, validate that we still get two conditions for non sort keys
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
                true /* inclusive lowerbound */, endTime, false /* exclusive upperbound */));
            constraintsMap.put("col_6", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            // Verify that both bounds are present for col_6 which is not a sort key
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_6 < :v1 AND #col_6 >= :v2)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
              false /* exclusive lowerbound */, endTime, true /* inclusive upperbound*/));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            // Verify that only the upper bound is present
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 <= :v0)"));
        }

        // For the same filters that we applied above, validate that we still get two conditions for non sort keys
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.range(allocator, Types.MinorType.DATEMILLI.getType(), startTime,
                false /* exclusive lowerbound */, endTime, true /* inclusive upperbound */));
            constraintsMap.put("col_6", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            // Verify that both bounds are present for col_6 which is not a sort key
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col_6 <= :v1 AND #col_6 > :v2)"));
        }

        // -------------------------------------------------------------------------
        // Single bound constraint tests
        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.greaterThan(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 > :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.greaterThanOrEqual(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 >= :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.lessThan(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 < :v0)"));
        }

        {
            SortedRangeSet.Builder timeValueSet2 = SortedRangeSet.newBuilder(Types.MinorType.DATEMILLI.getType(), false);
            timeValueSet2.add(Range.lessThanOrEqual(allocator, Types.MinorType.DATEMILLI.getType(), startTime));
            constraintsMap.put("col_5", timeValueSet2.build());
            GetTableLayoutResponse res2 = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));
            assertThat(res2.getPartitions().getSchema().getCustomMetadata().get(RANGE_KEY_FILTER_METADATA), equalTo("(#col_5 <= :v0)"));
        }
    }

    @Test
    public void doGetSplitsScan()
            throws Exception
    {
        GetTableLayoutResponse layoutResponse = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));

        GetSplitsRequest req = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                layoutResponse.getPartitions(),
                ImmutableList.of(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                null);
        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertThat(rawResponse.getRequestType(), equalTo(MetadataRequestType.GET_SPLITS));

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(continuationToken == null, is(true));

        Split split = Iterables.getOnlyElement(response.getSplits());
        assertThat(split.getProperty(SEGMENT_ID_PROPERTY), equalTo("0"));

        logger.info("doGetSplitsScan: exit");
    }

    @Test
    public void doGetSplitsQuery()
            throws Exception
    {
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        EquatableValueSet.Builder valueSet = EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, false);
        for (int i = 0; i < 2000; i++) {
            valueSet.add("test_str_" + i);
        }
        constraintsMap.put("col_0", valueSet.build());
        GetTableLayoutResponse layoutResponse = handler.doGetTableLayout(allocator, new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET));

        GetSplitsRequest req = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                layoutResponse.getPartitions(),
                ImmutableList.of("col_0"),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                null);
        logger.info("doGetSplits: req[{}]", req);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);
        assertThat(response.getRequestType(), equalTo(MetadataRequestType.GET_SPLITS));

        String continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(continuationToken, equalTo(String.valueOf(MAX_SPLITS_PER_REQUEST - 1)));
        assertThat(response.getSplits().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplits().stream().map(split -> split.getProperty("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));

        response = handler.doGetSplits(allocator, new GetSplitsRequest(req, continuationToken));

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]", continuationToken, response.getSplits().size());

        assertThat(response.getContinuationToken(), equalTo(null));
        assertThat(response.getSplits().size(), equalTo(MAX_SPLITS_PER_REQUEST));
        assertThat(response.getSplits().stream().map(split -> split.getProperty("col_0")).distinct().count(), equalTo((long) MAX_SPLITS_PER_REQUEST));
    }

    @Test
    public void validateSourceTableNamePropagation()
            throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col1").type("int").build());
        columns.add(Column.builder().name("col2").type("bigint").build());
        columns.add(Column.builder().name("col3").type("string").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1 , col2=Col2 ,col3=Col3",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=datetime1,col3=datetime3 ");
        Table table = Table.builder()
                .parameters(param)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .partitionKeys(Collections.EMPTY_SET)
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(tableResponse);

        TableName tableName = new TableName(DEFAULT_SCHEMA, "glueTableForTestTable");
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
        GetTableResponse getTableResponse = handler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = getTableResponse.getSchema().getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(TEST_TABLE));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,Col3=datetime3"));

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                tableName,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                getTableResponse.getSchema(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse getTableLayoutResponse = handler.doGetTableLayout(allocator, getTableLayoutRequest);
        logger.info("validateSourceTableNamePropagation: GetTableLayoutResponse[{}]", getTableLayoutResponse);
        assertThat(getTableLayoutResponse.getPartitions().getSchema().getCustomMetadata().get(TABLE_METADATA), equalTo(TEST_TABLE));
    }

    @Test
    public void doGetTableLayoutScanWithTypeOverride()
            throws Exception
    {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col1").type("int").build());
        columns.add(Column.builder().name("col2").type("timestamptz").build());
        columns.add(Column.builder().name("col3").type("string").build());

        Map<String, String> param = ImmutableMap.of(
                SOURCE_TABLE_PROPERTY, TEST_TABLE,
                COLUMN_NAME_MAPPING_PROPERTY, "col1=Col1",
                DATETIME_FORMAT_MAPPING_PROPERTY, "col1=datetime1,col3=datetime3 ");
        Table table = Table.builder()
                .parameters(param)
                .partitionKeys(Collections.EMPTY_SET)
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();
        software.amazon.awssdk.services.glue.model.GetTableResponse tableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glueClient.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(tableResponse);

        TableName tableName = new TableName(DEFAULT_SCHEMA, "glueTableForTestTable");
        GetTableRequest getTableRequest = new GetTableRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME, tableName, Collections.emptyMap());
        GetTableResponse getTableResponse = handler.doGetTable(allocator, getTableRequest);
        logger.info("validateSourceTableNamePropagation: GetTableResponse[{}]", getTableResponse);
        Map<String, String> customMetadata = getTableResponse.getSchema().getCustomMetadata();
        assertThat(customMetadata.get(SOURCE_TABLE_PROPERTY), equalTo(TEST_TABLE));
        assertThat(customMetadata.get(DATETIME_FORMAT_MAPPING_PROPERTY_NORMALIZED), equalTo("Col1=datetime1,col3=datetime3"));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());
        constraintsMap.put("col2",
                EquatableValueSet.newBuilder(allocator, new ArrowType.Bool(), true, true)
                        .add(true).build());

        GetTableLayoutRequest getTableLayoutRequest = new GetTableLayoutRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                tableName,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                getTableResponse.getSchema(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, getTableLayoutRequest);

        logger.info("doGetTableLayoutScanWithTypeOverride schema - {}", res.getPartitions().getSchema());
        logger.info("doGetTableLayoutScanWithTypeOverride partitions - {}", res.getPartitions());

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(PARTITION_TYPE_METADATA), equalTo(SCAN_PARTITION_TYPE));
        // no hash key constraints, so look for segment count column
        assertThat(res.getPartitions().getSchema().findField(SEGMENT_COUNT_METADATA) != null, is(true));
        assertThat(res.getPartitions().getRowCount(), equalTo(1));

        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(NON_KEY_FILTER_METADATA), equalTo("(#col3 = :v0 OR attribute_not_exists(#col3) OR #col3 = :v1)"));

        ImmutableMap<String, String> expressionNames = ImmutableMap.of("#col3", "col3", "#col2", "col2");
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_NAMES_METADATA), equalTo(Jackson.toJsonString(expressionNames)));

        ImmutableMap<String, AttributeValue> expressionValues = ImmutableMap.of(":v0", DDBTypeUtils.toAttributeValue(true), ":v1", DDBTypeUtils.toAttributeValue(null));
        assertThat(res.getPartitions().getSchema().getCustomMetadata().get(EXPRESSION_VALUES_METADATA), equalTo(EnhancedDocument.fromAttributeValueMap(expressionValues).toJson()));
    }

    @Test
    public void testDoGetQueryPassthroughSchema()
    {
        try {
            Map<String, String> queryPassthroughArguments = new HashMap<>();
            queryPassthroughArguments.put("schemaFunctionName", "system.query");
            queryPassthroughArguments.put(DDBQueryPassthrough.QUERY, "SELECT col_0, col_1 FROM " + TEST_TABLE + " WHERE col_0 = 'test_str_0'");

            GetTableRequest request = new GetTableRequest(
                    TEST_IDENTITY,
                    TEST_QUERY_ID,
                    TEST_CATALOG_NAME,
                    TEST_TABLE_NAME,
                    queryPassthroughArguments
            );

            // Execute test
            GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);

            // Verify response
            assertNotNull("Response should not be null", response);
            assertEquals(TEST_CATALOG_NAME, response.getCatalogName());
            assertEquals(TEST_TABLE_NAME, response.getTableName());

            Schema schema = response.getSchema();
            assertNotNull("Schema should not be null", schema);

            // Verify schema fields exist
            Field col0Field = schema.findField("col_0");
            assertNotNull("col_0 field should exist", col0Field);
            assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(col0Field.getType()));

            Field col1Field = schema.findField("col_1");
            assertNotNull("col_1 field should exist", col1Field);
            assertEquals(Types.MinorType.DECIMAL, Types.getMinorTypeForArrowType(col1Field.getType()));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test(expected = AthenaConnectorException.class)
    public void testDoGetQueryPassthroughSchema_withoutPassthrough_throwsAthenaConnectorException()
            throws Exception
    {
        // Setup request parameters without query passthrough
        Map<String, String> queryPassthroughArguments = new HashMap<>();
        GetTableRequest request = new GetTableRequest(
                TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                queryPassthroughArguments
        );

        // This should throw an AthenaConnectorException
        handler.doGetQueryPassthroughSchema(allocator, request);
    }

    @Test
    public void testDoGetSplits_withQueryPassthrough()
    {
        // Setup QPT arguments
        String partiQLQuery = "SELECT * FROM " + TEST_TABLE;
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(DDBQueryPassthrough.QUERY, partiQLQuery);

        // Create schema for request
        Schema schema = SchemaBuilder.newBuilder()
                .addField("col_0", Types.MinorType.VARCHAR.getType())
                .build();

        // Create constraints with QPT arguments
        Constraints constraints = new Constraints(
            new HashMap<>(),
            Collections.emptyList(),
            Collections.emptyList(),
            0L,
            qptArgs // queryPassthroughArguments
        );

        // Create GetSplitsRequest
        GetSplitsRequest getSplitsRequest = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                allocator.createBlock(schema),
                Collections.emptyList(),
                constraints,
                null);

        // Get splits response
        GetSplitsResponse response = handler.doGetSplits(allocator, getSplitsRequest);

        // Verify we get exactly one split for QPT
        assertNotNull("Response is null", response);
        assertEquals("Should be exactly one split", 1, response.getSplits().size());

        // Verify the split has our QPT query
        Split split = response.getSplits().iterator().next();
        assertEquals("Query mismatch", partiQLQuery, split.getProperties().get(DDBQueryPassthrough.QUERY));
    }

    @Test
    public void testDoGetDataSourceCapabilities_withQueryPassthroughEnabled()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        // Verify that capabilities map contains query passthrough capability
        assertNotNull("Capabilities map should not be null", response.getCapabilities());
        assertTrue("Capabilities map should contain SYSTEM.QUERY",
            response.getCapabilities().containsKey("SYSTEM.QUERY"));
    }

    @Test
    public void testDoGetDataSourceCapabilities_withQueryPassthroughDisabled()
    {
        // Create handler with query passthrough disabled
        DynamoDBMetadataHandler handlerWithoutQpt = new DynamoDBMetadataHandler(
                new LocalKeyFactory(),
                secretsManager,
                athena,
                "spillBucket",
                "spillPrefix",
                ddbClient,
                glueClient,
                ImmutableMap.of("enable_query_passthrough", "false"));
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(TEST_IDENTITY, TEST_QUERY_ID, TEST_CATALOG_NAME);
        GetDataSourceCapabilitiesResponse response = handlerWithoutQpt.doGetDataSourceCapabilities(allocator, request);

        assertFalse("Capabilities map should not contain SYSTEM.QUERY",
            response.getCapabilities().containsKey("SYSTEM.QUERY"));
    }

    @Test(expected = AthenaConnectorException.class)
    public void testDoGetSplits_withMissingPartitionTypeMetadata_throwsAthenaConnectorException()
    {
        // Create a schema without partition type metadata
        Schema schema = SchemaBuilder.newBuilder()
                .addField(TEST_FIELD, Types.MinorType.VARCHAR.getType())
                .build();

        Block partitions = allocator.createBlock(schema);
        partitions.setRowCount(1);

        GetSplitsRequest request = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                partitions,
                ImmutableList.of(),
                new Constraints(ImmutableMap.of(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                null);

        // This should throw AthenaConnectorException
        handler.doGetSplits(allocator, request);
    }

    @Test(expected = AthenaConnectorException.class)
    public void testDoGetSplits_withInvalidPartitionType_throwsAthenaConnectorException()
    {
        // Create a schema with invalid partition type
        Schema schema = SchemaBuilder.newBuilder()
                .addField(TEST_FIELD, Types.MinorType.VARCHAR.getType())
                .addMetadata(PARTITION_TYPE_METADATA, "INVALID_TYPE")
                .build();

        Block partitions = allocator.createBlock(schema);
        partitions.setRowCount(1);

        GetSplitsRequest request = new GetSplitsRequest(TEST_IDENTITY,
                TEST_QUERY_ID,
                TEST_CATALOG_NAME,
                TEST_TABLE_NAME,
                partitions,
                ImmutableList.of(),
                new Constraints(ImmutableMap.of(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                null);

        // This should throw AthenaConnectorException
        handler.doGetSplits(allocator, request);
    }
}
