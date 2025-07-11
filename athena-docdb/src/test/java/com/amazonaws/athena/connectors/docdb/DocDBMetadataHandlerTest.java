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
import java.util.List;
import java.util.Map;

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
    public void testDoListTables_withPagination()
    {
        try {
            Document tableNamesDocument = new Document("cursor",
                    new Document("firstBatch",
                            Arrays.asList(new Document("name", "table1"),
                                    new Document("name", "table2"),
                                    new Document("name", "table3"),
                                    new Document("name", "table4"),
                                    new Document("name", "table5"))));

            MongoDatabase mockDatabase = mock(MongoDatabase.class);
            when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
            when(mockDatabase.runCommand(any())).thenReturn(tableNamesDocument);

            // First request - page size 2, no token
            ListTablesRequest firstRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                    null, 2);
            ListTablesResponse firstResponse = handler.doListTables(allocator, firstRequest);

            // Verify first page
            assertEquals("Should return 2 tables for first page", 2, firstResponse.getTables().size());
            List<TableName> firstPageTables = new ArrayList<>(firstResponse.getTables());
            assertEquals(new TableName(DEFAULT_SCHEMA, "table1"), firstPageTables.get(0));
            assertEquals(new TableName(DEFAULT_SCHEMA, "table2"), firstPageTables.get(1));
            assertEquals("2", firstResponse.getNextToken());

            // Second request - page size 2, token "2"
            ListTablesRequest secondRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                    firstResponse.getNextToken(), 2);
            ListTablesResponse secondResponse = handler.doListTables(allocator, secondRequest);

            // Verify second page
            assertEquals("Should return 2 tables for second page", 2, secondResponse.getTables().size());
            List<TableName> secondPageTables = new ArrayList<>(secondResponse.getTables());
            assertEquals(new TableName(DEFAULT_SCHEMA, "table3"), secondPageTables.get(0));
            assertEquals(new TableName(DEFAULT_SCHEMA, "table4"), secondPageTables.get(1));
            assertEquals("4", secondResponse.getNextToken());

            // Final request - page size 2, token "4"
            ListTablesRequest finalRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                    secondResponse.getNextToken(), 2);
            ListTablesResponse finalResponse = handler.doListTables(allocator, finalRequest);

            // Verify final page
            assertEquals("Should return 1 table for final page", 1, finalResponse.getTables().size());
            List<TableName> finalPageTables = new ArrayList<>(finalResponse.getTables());
            assertEquals(new TableName(DEFAULT_SCHEMA, "table5"), finalPageTables.get(0));
            assertEquals("6", finalResponse.getNextToken());  // Token is still generated even for last page

            // Verify empty page after all data is consumed
            ListTablesRequest emptyRequest = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, DEFAULT_SCHEMA,
                    finalResponse.getNextToken(), 2);
            ListTablesResponse emptyResponse = handler.doListTables(allocator, emptyRequest);
            assertEquals("Should return 0 tables when all data is consumed", 0, emptyResponse.getTables().size());
            assertEquals("8", emptyResponse.getNextToken());  // Token continues to increment
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test
    public void doGetTable()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put("stringCol", "stringVal");
        doc1.put("intCol", 1);
        doc1.put("doubleCol", 2.2D);
        doc1.put("longCol", 100L);
        doc1.put("booleanCol", true);
        doc1.put("floatCol", 1.5F);
        doc1.put("dateCol", new Date());
        doc1.put("timestampCol", new BsonTimestamp(System.currentTimeMillis()));
        doc1.put("objectIdCol", ObjectId.get());
        doc1.put("unsupported", new UnsupportedType());

        Document doc2 = new Document();
        documents.add(doc2);
        doc2.put("stringCol2", "stringVal");
        doc2.put("intCol2", 1);
        doc2.put("doubleCol2", 2.2D);
        doc2.put("longCol2", 100L);
        doc2.put("booleanCol2", false);
        doc2.put("floatCol2", 2.5F);
        doc2.put("dateCol2", new Date());
        doc2.put("timestampCol2", new BsonTimestamp(System.currentTimeMillis()));
        doc2.put("objectIdCol2", ObjectId.get());

        Document doc3 = new Document();
        documents.add(doc3);
        doc3.put("stringCol", "stringVal");
        doc3.put("intCol2", 1);
        doc3.put("doubleCol", 2.2D);
        doc3.put("longCol2", 100L);
        doc3.put("booleanCol", false);
        doc3.put("floatCol", 3.5F);
        doc3.put("dateCol", new Date());
        doc3.put("timestampCol", new BsonTimestamp(System.currentTimeMillis()));
        doc3.put("objectIdCol", ObjectId.get());

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

        Field stringCol = res.getSchema().findField("stringCol");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol.getType()));

        Field stringCol2 = res.getSchema().findField("stringCol2");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol2.getType()));

        Field intCol = res.getSchema().findField("intCol");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol.getType()));

        Field intCol2 = res.getSchema().findField("intCol2");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol2.getType()));

        Field doubleCol = res.getSchema().findField("doubleCol");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol.getType()));

        Field doubleCol2 = res.getSchema().findField("doubleCol2");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol2.getType()));

        Field longCol = res.getSchema().findField("longCol");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol.getType()));

        Field longCol2 = res.getSchema().findField("longCol2");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol2.getType()));

        Field booleanCol = res.getSchema().findField("booleanCol");
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol.getType()));

        Field booleanCol2 = res.getSchema().findField("booleanCol2");
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol2.getType()));

        Field floatCol = res.getSchema().findField("floatCol");
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol.getType()));

        Field floatCol2 = res.getSchema().findField("floatCol2");
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol2.getType()));

        Field dateCol = res.getSchema().findField("dateCol");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol.getType()));

        Field dateCol2 = res.getSchema().findField("dateCol2");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol2.getType()));

        Field timestampCol = res.getSchema().findField("timestampCol");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol.getType()));

        Field timestampCol2 = res.getSchema().findField("timestampCol2");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol2.getType()));

        Field objectIdCol = res.getSchema().findField("objectIdCol");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(objectIdCol.getType()));

        Field objectIdCol2 = res.getSchema().findField("objectIdCol2");
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
        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put("stringCol", "stringVal");
        doc1.put("intCol", 1);
        doc1.put("doubleCol", 2.2D);
        doc1.put("longCol", 100L);
        doc1.put("booleanCol", true);
        doc1.put("floatCol", 1.5F);
        doc1.put("dateCol", new Date());
        doc1.put("timestampCol", new BsonTimestamp(System.currentTimeMillis()));
        doc1.put("objectIdCol", ObjectId.get());
        doc1.put("unsupported", new UnsupportedType());

        Document doc2 = new Document();
        documents.add(doc2);
        doc2.put("stringCol2", "stringVal");
        doc2.put("intCol2", 1);
        doc2.put("doubleCol2", 2.2D);
        doc2.put("longCol2", 100L);
        doc2.put("booleanCol2", false);
        doc2.put("floatCol2", 2.5F);
        doc2.put("dateCol2", new Date());
        doc2.put("timestampCol2", new BsonTimestamp(System.currentTimeMillis()));
        doc2.put("objectIdCol2", ObjectId.get());

        Document doc3 = new Document();
        documents.add(doc3);
        doc3.put("stringCol", "stringVal");
        doc3.put("intCol2", 1);
        doc3.put("doubleCol", 2.2D);
        doc3.put("longCol2", 100L);
        doc3.put("booleanCol", false);
        doc3.put("floatCol", 3.5F);
        doc3.put("dateCol", new Date());
        doc3.put("timestampCol", new BsonTimestamp(System.currentTimeMillis()));
        doc3.put("objectIdCol", ObjectId.get());

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
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        TableName tableNameInput = new TableName("DEfault", TEST_TABLE.toUpperCase());
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameInput, Collections.emptyMap());
        GetTableResponse res = caseInsensitiveHandler.doGetTable(allocator, req);

        assertEquals(DEFAULT_SCHEMA, res.getTableName().getSchemaName());
        assertEquals(TEST_TABLE, res.getTableName().getTableName());
        logger.info("doGetTable - {}", res);

        assertEquals(19, res.getSchema().getFields().size());

        Field stringCol = res.getSchema().findField("stringCol");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol.getType()));

        Field stringCol2 = res.getSchema().findField("stringCol2");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(stringCol2.getType()));

        Field intCol = res.getSchema().findField("intCol");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol.getType()));

        Field intCol2 = res.getSchema().findField("intCol2");
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(intCol2.getType()));

        Field doubleCol = res.getSchema().findField("doubleCol");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol.getType()));

        Field doubleCol2 = res.getSchema().findField("doubleCol2");
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(doubleCol2.getType()));

        Field longCol = res.getSchema().findField("longCol");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol.getType()));

        Field longCol2 = res.getSchema().findField("longCol2");
        assertEquals(Types.MinorType.BIGINT, Types.getMinorTypeForArrowType(longCol2.getType()));

        Field booleanCol = res.getSchema().findField("booleanCol");
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol.getType()));

        Field booleanCol2 = res.getSchema().findField("booleanCol2");
        assertEquals(Types.MinorType.BIT, Types.getMinorTypeForArrowType(booleanCol2.getType()));

        Field floatCol = res.getSchema().findField("floatCol");
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol.getType()));

        Field floatCol2 = res.getSchema().findField("floatCol2");
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(floatCol2.getType()));

        Field dateCol = res.getSchema().findField("dateCol");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol.getType()));

        Field dateCol2 = res.getSchema().findField("dateCol2");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(dateCol2.getType()));

        Field timestampCol = res.getSchema().findField("timestampCol");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol.getType()));

        Field timestampCol2 = res.getSchema().findField("timestampCol2");
        assertEquals(Types.MinorType.DATEMILLI, Types.getMinorTypeForArrowType(timestampCol2.getType()));

        Field objectIdCol = res.getSchema().findField("objectIdCol");
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(objectIdCol.getType()));

        Field objectIdCol2 = res.getSchema().findField("objectIdCol2");
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
        }
        catch (IllegalArgumentException ex) {
            assertEquals("Schema name is empty or more than 1 for case insensitive match. schemaName: deFAULT, size: 2", ex.getMessage());
        }
    }

    @Test
    public void doGetTableCaseInsensitiveMatchNotEnable()
            throws Exception
    {
        String mixedCaseSchemaName = "deFAULT";
        String mixedCaseTableName = "tesT_Table";
        List<Document> documents = new ArrayList<>();

        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put("stringCol", "stringVal");
        doc1.put("intCol", 1);
        doc1.put("doubleCol", 2.2D);
        doc1.put("longCol", 100L);
        doc1.put("booleanCol", true);
        doc1.put("floatCol", 1.5F);
        doc1.put("dateCol", new Date());
        doc1.put("timestampCol", new BsonTimestamp(System.currentTimeMillis()));
        doc1.put("objectIdCol", ObjectId.get());
        doc1.put("unsupported", new UnsupportedType());

        Document doc2 = new Document();
        documents.add(doc2);
        doc2.put("stringCol2", "stringVal");
        doc2.put("intCol2", 1);
        doc2.put("doubleCol2", 2.2D);
        doc2.put("longCol2", 100L);
        doc2.put("booleanCol2", false);
        doc2.put("floatCol2", 2.5F);
        doc2.put("dateCol2", new Date());
        doc2.put("timestampCol2", new BsonTimestamp(System.currentTimeMillis()));
        doc2.put("objectIdCol2", ObjectId.get());

        Document doc3 = new Document();
        documents.add(doc3);
        doc3.put("stringCol", "stringVal");
        doc3.put("intCol2", 1);
        doc3.put("doubleCol", 2.2D);
        doc3.put("longCol2", 100L);
        doc3.put("booleanCol", false);
        doc3.put("floatCol", 3.5F);
        doc3.put("dateCol", new Date());
        doc3.put("timestampCol", new BsonTimestamp(System.currentTimeMillis()));
        doc3.put("objectIdCol", ObjectId.get());

        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockClient.getDatabase(eq(mixedCaseSchemaName))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(mixedCaseTableName))).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
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
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
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
                new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
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
    public void testDoGetDataSourceCapabilities_whenQueryPassthroughEnabled()
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("enable_query_passthrough", "true");

        DocDBMetadataHandler handlerWithQPT = new DocDBMetadataHandler(awsGlue,
                connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena,
                SPILL_BUCKET, SPILL_PREFIX, configOptions);

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        GetDataSourceCapabilitiesResponse response = handlerWithQPT.doGetDataSourceCapabilities(allocator, request);

        assertNotNull("Response should not be null", response);
        assertEquals(DEFAULT_CATALOG, response.getCatalogName());
        assertFalse("Capabilities should not be empty when query passthrough is enabled", response.getCapabilities().isEmpty());
    }

    @Test
    public void testDoGetQueryPassthroughSchema()
    {
        try {
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
            queryPassthroughParameters.put("schemaFunctionName", "system.query");
            queryPassthroughParameters.put("DATABASE", "example");
            queryPassthroughParameters.put("COLLECTION", "tpcds");
            queryPassthroughParameters.put("FILTER", "{\"title\": \"Bill of Rights\"}");
            queryPassthroughParameters.put("enable_query_passthrough", "true");

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
            assertNotNull("title field should exist", titleField);
            assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(titleField.getType()));

            Field yearField = schema.findField("year");
            assertNotNull("year field should exist", yearField);
            assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(yearField.getType()));

            Field typeField = schema.findField("type");
            assertNotNull("type field should exist", typeField);
            assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(typeField.getType()));
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test(expected = AthenaConnectorException.class)
    public void testDoGetQueryPassthroughSchema_InvalidParameters() throws Exception
    {
        // Test with missing required parameters
        Map<String, String> invalidParams = new HashMap<>();
        invalidParams.put("schemaFunctionName", "system.query");
        invalidParams.put("enable_query_passthrough", "true");
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
}
