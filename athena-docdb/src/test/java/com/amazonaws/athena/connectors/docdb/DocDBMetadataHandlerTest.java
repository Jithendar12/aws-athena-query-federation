/*-
 * #%L
 * athena-mongodb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
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
import com.google.common.collect.ImmutableList;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DocDBMetadataHandlerTest
    extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBMetadataHandlerTest.class);

    private static final String STRING_COL = "stringCol";
    private static final String STRING_COL2 = "stringCol2";
    private static final String INT_COL = "intCol";
    private static final String INT_COL2 = "intCol2";
    private static final String DOUBLE_COL = "doubleCol";
    private static final String DOUBLE_COL2 = "doubleCol2";
    private static final String LONG_COL = "longCol";
    private static final String LONG_COL2 = "longCol2";
    private static final String BOOLEAN_COL = "booleanCol";
    private static final String BOOLEAN_COL2 = "booleanCol2";
    private static final String FLOAT_COL = "floatCol";
    private static final String FLOAT_COL2 = "floatCol2";
    private static final String DATE_COL = "dateCol";
    private static final String DATE_COL2 = "dateCol2";
    private static final String TIMESTAMP_COL = "timestampCol";
    private static final String TIMESTAMP_COL2 = "timestampCol2";
    private static final String OBJECT_ID_COL = "objectIdCol";
    private static final String OBJECT_ID_COL2 = "objectIdCol2";
    
    private static final String STRING_VALUE = "stringVal";
    private static final double DOUBLE_VALUE = 2.2D;
    private static final int INT_VALUE = 1;
    private static final long LONG_VALUE = 100L;
    private static final float FLOAT_VALUE1 = 1.5F;
    private static final float FLOAT_VALUE2 = 2.5F;
    private static final float FLOAT_VALUE3 = 3.5F;
    private static final String ENABLE_QUERY_PASSTHROUGH = "enable_query_passthrough";
    private static final String SYSTEM_QUERY = "system.query";
    private static final String DOCDB_METADATA_FLAG = "docdb-metadata-flag";

    private DocDBMetadataHandler handler;
    private BlockAllocator allocator;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private DocDBConnectionFactory connectionFactory;

    @Mock
    private MongoClient mockClient;

    @Mock
    private GlueClient awsGlue;

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());

        when(connectionFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        handler = new DocDBMetadataHandler(awsGlue, connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena, SPILL_BUCKET, SPILL_PREFIX, com.google.common.collect.ImmutableMap.of());
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNames() throws Exception
    {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema1");
        schemaNames.add("schema2");
        schemaNames.add("schema3");

        when(mockClient.listDatabaseNames()).thenReturn(StubbingCursor.iterate(schemaNames));

        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());
        assertEquals(schemaNames, new ArrayList<>(res.getSchemas()));
    }

    @Test
    public void doListTables() throws Exception
    {
        List<String> tableNames = new ArrayList<>();
        tableNames.add("table1");
        tableNames.add("table2");
        tableNames.add("table3");

        Document tableNamesDocument = new Document("cursor",
                new Document("firstBatch",
                        Arrays.asList(new Document("name", "table1"),
                                new Document("name", "table2"),
                                new Document("name", "table3"))));

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.runCommand(any())).thenReturn(tableNamesDocument);

        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                null, UNLIMITED_PAGE_SIZE_VALUE);

        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());

        for (TableName next : res.getTables()) {
            assertEquals(DEFAULT_SCHEMA, next.getSchemaName());
            assertTrue(tableNames.contains(next.getTableName()));
        }
        assertEquals(tableNames.size(), res.getTables().size());
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        List<Document> documents = createTestDocuments();

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(TEST_TABLE))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        Mockito.lenient().when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        assertEquals(19, res.getSchema().getFields().size());

        Field stringCol = res.getSchema().findField(STRING_COL);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol.getType()));

        Field stringCol2 = res.getSchema().findField(STRING_COL2);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol2.getType()));

        Field intCol = res.getSchema().findField(INT_COL);
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol.getType()));

        Field intCol2 = res.getSchema().findField(INT_COL2);
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol2.getType()));

        Field doubleCol = res.getSchema().findField(DOUBLE_COL);
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol.getType()));

        Field doubleCol2 = res.getSchema().findField(DOUBLE_COL2);
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol2.getType()));

        Field longCol = res.getSchema().findField(LONG_COL);
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol.getType()));

        Field longCol2 = res.getSchema().findField(LONG_COL2);
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol2.getType()));

        Field booleanCol = res.getSchema().findField(BOOLEAN_COL);
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol.getType()));

        Field booleanCol2 = res.getSchema().findField(BOOLEAN_COL2);
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol2.getType()));

        Field floatCol = res.getSchema().findField(FLOAT_COL);
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol.getType()));

        Field floatCol2 = res.getSchema().findField(FLOAT_COL2);
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol2.getType()));

        Field dateCol = res.getSchema().findField(DATE_COL);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol.getType()));

        Field dateCol2 = res.getSchema().findField(DATE_COL2);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol2.getType()));

        Field timestampCol = res.getSchema().findField(TIMESTAMP_COL);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol.getType()));

        Field timestampCol2 = res.getSchema().findField(TIMESTAMP_COL2);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol2.getType()));

        Field objectIdCol = res.getSchema().findField(OBJECT_ID_COL);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(objectIdCol.getType()));

        Field objectIdCol2 = res.getSchema().findField(OBJECT_ID_COL2);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(objectIdCol2.getType()));

        Field unsupported = res.getSchema().findField("unsupported");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(unsupported.getType()));
    }

    @Test
    public void doGetTableCaseInsensitiveMatch()
            throws Exception
    {

        DocDBMetadataHandler caseInsensitiveHandler = new DocDBMetadataHandler(awsGlue,
                connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena,
                SPILL_BUCKET, SPILL_PREFIX, com.google.common.collect.ImmutableMap.of("enable_case_insensitive_match", "true"));
        List<Document> documents = createTestDocuments();

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);

        MongoIterable mockListDatabaseNamesIterable = mock(MongoIterable.class);
        when(mockClient.listDatabaseNames()).thenReturn(mockListDatabaseNamesIterable);

        when(mockListDatabaseNamesIterable.spliterator()).thenReturn(ImmutableList.of(DEFAULT_SCHEMA).spliterator());

        MongoIterable mockListCollectionsNamesIterable = mock(MongoIterable.class);
        when(mockDatabase.listCollectionNames()).thenReturn(mockListCollectionsNamesIterable);
        when(mockListCollectionsNamesIterable.spliterator()).thenReturn(ImmutableList.of(TEST_TABLE).spliterator());

        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(TEST_TABLE))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        Mockito.lenient().when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        TableName tableNameInput = new TableName("DEfault", TEST_TABLE.toUpperCase());
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameInput, Collections.emptyMap());
        GetTableResponse res = caseInsensitiveHandler.doGetTable(allocator, req);

        assertEquals(DEFAULT_SCHEMA, res.getTableName().getSchemaName());
        assertEquals(TEST_TABLE, res.getTableName().getTableName());
        logger.info("doGetTable - {}", res);

        assertEquals(19, res.getSchema().getFields().size());

        Field stringCol = res.getSchema().findField(STRING_COL);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol.getType()));

        Field stringCol2 = res.getSchema().findField(STRING_COL2);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol2.getType()));

        Field intCol = res.getSchema().findField("intCol");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol.getType()));

        Field intCol2 = res.getSchema().findField(INT_COL2);
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol2.getType()));

        Field doubleCol = res.getSchema().findField(DOUBLE_COL);
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol.getType()));

        Field doubleCol2 = res.getSchema().findField(DOUBLE_COL2);
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol2.getType()));

        Field longCol = res.getSchema().findField("longCol");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol.getType()));

        Field longCol2 = res.getSchema().findField(LONG_COL2);
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol2.getType()));

        Field booleanCol = res.getSchema().findField(BOOLEAN_COL);
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol.getType()));

        Field booleanCol2 = res.getSchema().findField(BOOLEAN_COL2);
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol2.getType()));

        Field floatCol = res.getSchema().findField(FLOAT_COL);
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol.getType()));

        Field floatCol2 = res.getSchema().findField(FLOAT_COL2);
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol2.getType()));

        Field dateCol = res.getSchema().findField(DATE_COL);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol.getType()));

        Field dateCol2 = res.getSchema().findField(DATE_COL2);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol2.getType()));

        Field timestampCol = res.getSchema().findField(TIMESTAMP_COL);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol.getType()));

        Field timestampCol2 = res.getSchema().findField(TIMESTAMP_COL2);
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol2.getType()));

        Field objectIdCol = res.getSchema().findField(OBJECT_ID_COL);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(objectIdCol.getType()));

        Field objectIdCol2 = res.getSchema().findField(OBJECT_ID_COL2);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(objectIdCol2.getType()));

        Field unsupported = res.getSchema().findField("unsupported");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(unsupported.getType()));
    }


    @Test
    public void doGetTableCaseInsensitiveMatchMultipleMatch()
            throws Exception
    {

        DocDBMetadataHandler caseInsensitiveHandler = new DocDBMetadataHandler(awsGlue,
                connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena,
                SPILL_BUCKET, SPILL_PREFIX, com.google.common.collect.ImmutableMap.of("enable_case_insensitive_match", "true"));

        MongoIterable mockListDatabaseNamesIterable = mock(MongoIterable.class);
        when(mockClient.listDatabaseNames()).thenReturn(mockListDatabaseNamesIterable);
        when(mockListDatabaseNamesIterable.spliterator()).thenReturn(ImmutableList.of(DEFAULT_SCHEMA, DEFAULT_SCHEMA.toUpperCase()).spliterator());

        TableName tableNameInput = new TableName("deFAULT", TEST_TABLE.toUpperCase());
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameInput, Collections.emptyMap());
        try {
            GetTableResponse res = caseInsensitiveHandler.doGetTable(allocator, req);
            fail("doGetTableCaseInsensitiveMatchMultipleMatch should failed");
        } catch(IllegalArgumentException ex){
            assertEquals("Schema name is empty or more than 1 for case insensitive match. schemaName: deFAULT, size: 2", ex.getMessage());
        }
    }

    @Test
    public void doGetTableCaseInsensitiveMatchNotEnable()
            throws Exception
    {

        String mixedCaseSchemaName = "deFAULT";
        String mixedCaseTableName = "tesT_Table";
        List<Document> documents = createTestDocuments();

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(mixedCaseSchemaName))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(mixedCaseTableName))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        Mockito.lenient().when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        TableName tableNameInput = new TableName(mixedCaseSchemaName, mixedCaseTableName);
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameInput, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        assertEquals(mixedCaseSchemaName, res.getTableName().getSchemaName());
        assertEquals(mixedCaseTableName, res.getTableName().getTableName());
        assertEquals(19, res.getSchema().getFields().size());

        Map<String, Field> fields = new HashMap<>();
        res.getSchema().getFields().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("stringCol").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("stringCol2").getType()));
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("intCol").getType()));
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("intCol2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("doubleCol").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("doubleCol2").getType()));
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(fields.get("longCol").getType()));
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(fields.get("longCol2").getType()));
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(fields.get("booleanCol").getType()));
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(fields.get("floatCol").getType()));
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(fields.get("dateCol").getType()));
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(fields.get("timestampCol").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("objectIdCol").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("unsupported").getType()));

        verify(mockClient, Mockito.never()).listDatabaseNames();
        verify(mockDatabase, Mockito.never()).listCollectionNames();

    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        Schema schema = SchemaBuilder.newBuilder().build();
        GetTableLayoutRequest req = new GetTableLayoutRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                schema,
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res);
        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
        }

        assertTrue(partitions.getRowCount() > 0);
    }

    @Test
    public void doGetSplits()
    {
        List<String> partitionCols = new ArrayList<>();

        Block partitions = BlockUtils.newBlock(allocator, PARTITION_ID, Types.MinorType.INT.getType(), 0);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                partitionCols,
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        assertTrue("Continuation criteria violated", response.getSplits().size() == 1);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);
    }

    @Test
    public void doGetDataSourceCapabilities_whenQueryPassthroughEnabled_returnsCapabilities()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(ENABLE_QUERY_PASSTHROUGH, "true");

        DocDBMetadataHandler handlerWithQPT = new DocDBMetadataHandler(awsGlue,
                connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena,
                SPILL_BUCKET, SPILL_PREFIX, configOptions);

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        GetDataSourceCapabilitiesResponse response = handlerWithQPT.doGetDataSourceCapabilities(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        assertFalse("Capabilities should not be empty", response.getCapabilities().isEmpty());
    }

    @Test
    public void doGetQueryPassthroughSchema_whenCalledWithValidParams_returnsQueryPassthroughSchema() throws Exception
    {
        List<Document> documents = new ArrayList<>();
        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put("title", "Bill of Rights");
        doc1.put("year", 1791);
        doc1.put("type", "document");

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq("example"))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq("tpcds"))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        Map<String, String> queryPassthroughParameters = new HashMap<>();
        queryPassthroughParameters.put("schemaFunctionName", SYSTEM_QUERY);
        queryPassthroughParameters.put("DATABASE", "example");
        queryPassthroughParameters.put("COLLECTION", "tpcds");
        queryPassthroughParameters.put("FILTER", "{\"title\": \"Bill of Rights\"}");
        queryPassthroughParameters.put(ENABLE_QUERY_PASSTHROUGH, "true");

        GetTableRequest request = new GetTableRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                new TableName("example", "tpcds"),
                queryPassthroughParameters
        );
        GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);

        // Verify response
        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        // Verify that the schema and table names from query passthrough are used
        assertEquals(new TableName("example", "tpcds"), response.getTableName());

        Schema schema = response.getSchema();
        assertNotNull("Schema should not be null", schema);

        // Verify schema fields match the document structure
        Field titleField = schema.findField("title");
        assertNotNull("Title field should not be null", titleField);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(titleField.getType()));

        Field yearField = schema.findField("year");
        assertNotNull("Year field should not be null", yearField);
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(yearField.getType()));

        Field typeField = schema.findField("type");
        assertNotNull("Type field should not be null", typeField);
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(typeField.getType()));
    }

    @Test(expected = AthenaConnectorException.class)
    public void doGetQueryPassthroughSchema_whenCalledWithInvalidParams_throwsAthenaConnectorException() throws Exception
    {
        // Test with missing required parameters
        Map<String, String> invalidParams = new HashMap<>();
        invalidParams.put("schemaFunctionName", SYSTEM_QUERY);
        invalidParams.put(ENABLE_QUERY_PASSTHROUGH, "true");
        // Missing DATABASE and COLLECTION parameters
        GetTableRequest invalidRequest = new GetTableRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                invalidParams
        );
        handler.doGetQueryPassthroughSchema(allocator, invalidRequest);
    }

    @Test
    public void doListSchemaNames_whenGlueDataPresent_returnsCombinedSchemas() throws Exception
    {
        List<String> schemaNames = new ArrayList<>();
        schemaNames.add("schema1");
        schemaNames.add("schema2");

        // Mock Glue client behavior to return some databases
        software.amazon.awssdk.services.glue.model.Database glueDb = software.amazon.awssdk.services.glue.model.Database.builder()
                .name("glue_schema1")
                .locationUri(DOCDB_METADATA_FLAG)
                .build();
        software.amazon.awssdk.services.glue.model.GetDatabasesResponse glueResponse = software.amazon.awssdk.services.glue.model.GetDatabasesResponse.builder()
                .databaseList(Collections.singletonList(glueDb))
                .build();
        
        // Mock the paginator
        software.amazon.awssdk.services.glue.paginators.GetDatabasesIterable mockPaginator = mock(software.amazon.awssdk.services.glue.paginators.GetDatabasesIterable.class);
        when(awsGlue.getDatabasesPaginator(any(software.amazon.awssdk.services.glue.model.GetDatabasesRequest.class)))
                .thenReturn(mockPaginator);
        when(mockPaginator.stream()).thenReturn(Stream.of(glueResponse));
        
        // Mock MongoDB client behavior
        when(mockClient.listDatabaseNames()).thenReturn(StubbingCursor.iterate(schemaNames));

        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        // Verify that both Glue and MongoDB schemas are combined
        Set<String> expectedSchemas = new HashSet<>(schemaNames);
        expectedSchemas.add("glue_schema1");
        assertEquals(expectedSchemas, new HashSet<>(res.getSchemas()));
    }

    @Test
    public void doGetTable_whenGlueDataPresent_returnsGlueTableSchema() throws Exception
    {
        // Mock Glue client behavior - return a table with docdb-metadata-flag
        Map<String, String> tableParameters = new HashMap<>();
        tableParameters.put(DOCDB_METADATA_FLAG, "true");
        
        software.amazon.awssdk.services.glue.model.Column glueColumn = software.amazon.awssdk.services.glue.model.Column.builder()
                .name("glue_field")
                .type("string")
                .build();
        
        software.amazon.awssdk.services.glue.model.StorageDescriptor storageDescriptor = 
                software.amazon.awssdk.services.glue.model.StorageDescriptor.builder()
                        .columns(Collections.singletonList(glueColumn))
                        .build();
        
        software.amazon.awssdk.services.glue.model.Table glueTable = software.amazon.awssdk.services.glue.model.Table.builder()
                .name(TEST_TABLE)
                .databaseName(DEFAULT_SCHEMA)
                .parameters(tableParameters)
                .storageDescriptor(storageDescriptor)
                .build();
        
        software.amazon.awssdk.services.glue.model.GetTableResponse glueTableResponse = 
                software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                        .table(glueTable)
                        .build();
        
        when(awsGlue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenReturn(glueTableResponse);

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        assertNotNull("Schema should not be null", res.getSchema());
        assertFalse("Schema fields should not be empty", res.getSchema().getFields().isEmpty());
        // Verify the schema contains the glue field
        Field glueField = res.getSchema().findField("glue_field");
        assertNotNull("Glue field should not be null", glueField);
    }

    /**
     * Helper method to create a document with the first set of test data (doc1 pattern)
     */
    private Document createDocument1()
    {
        Document doc = new Document();
        doc.put(STRING_COL, STRING_VALUE);
        doc.put(INT_COL, INT_VALUE);
        doc.put(DOUBLE_COL, DOUBLE_VALUE);
        doc.put(LONG_COL, LONG_VALUE);
        doc.put(BOOLEAN_COL, true);
        doc.put(FLOAT_COL, FLOAT_VALUE1);
        doc.put(DATE_COL, new Date());
        doc.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc.put(OBJECT_ID_COL, ObjectId.get());
        doc.put("unsupported", new UnsupportedType());
        return doc;
    }

    /**
     * Helper method to create a document with the second set of test data (doc2 pattern)
     */
    private Document createDocument2()
    {
        Document doc = new Document();
        doc.put(STRING_COL2, STRING_VALUE);
        doc.put(INT_COL2, INT_VALUE);
        doc.put(DOUBLE_COL2, DOUBLE_VALUE);
        doc.put(LONG_COL2, LONG_VALUE);
        doc.put(BOOLEAN_COL2, false);
        doc.put(FLOAT_COL2, FLOAT_VALUE2);
        doc.put(DATE_COL2, new Date());
        doc.put(TIMESTAMP_COL2, new BsonTimestamp(System.currentTimeMillis()));
        doc.put(OBJECT_ID_COL2, ObjectId.get());
        return doc;
    }

    /**
     * Helper method to create a document with the third set of test data (doc3 pattern)
     */
    private Document createDocument3()
    {
        Document doc = new Document();
        doc.put(STRING_COL, STRING_VALUE);
        doc.put(INT_COL2, INT_VALUE);
        doc.put(DOUBLE_COL, DOUBLE_VALUE);
        doc.put(LONG_COL2, LONG_VALUE);
        doc.put(BOOLEAN_COL, false);
        doc.put(FLOAT_COL, FLOAT_VALUE3);
        doc.put(DATE_COL, new Date());
        doc.put(TIMESTAMP_COL, new BsonTimestamp(System.currentTimeMillis()));
        doc.put(OBJECT_ID_COL, ObjectId.get());
        return doc;
    }

    /**
     * Helper method to create a list of test documents
     */
    private List<Document> createTestDocuments()
    {
        List<Document> documents = new ArrayList<>();
        documents.add(createDocument1());
        documents.add(createDocument2());
        documents.add(createDocument3());
        return documents;
    }
}
