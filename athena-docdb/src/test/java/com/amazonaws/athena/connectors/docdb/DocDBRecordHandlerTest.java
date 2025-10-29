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
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.docdb.DocDBMetadataHandler.DOCDB_CONN_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DocDBRecordHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(DocDBRecordHandlerTest.class);
    private static final String EXAMPLE_DATABASE = "example";
    private static final String TPCDS_COLLECTION = "tpcds";
    private static final String SYSTEM_QUERY = "system.query";
    private static final String ENABLE_QUERY_PASSTHROUGH = "enable_query_passthrough";
    private static final String DISABLE_PROJECTION_AND_CASING = "disable_projection_and_casing";

    private DocDBRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private S3Client amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private DocDBMetadataHandler mdHandler;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private DocDBConnectionFactory connectionFactory;

    @Mock
    private MongoClient mockClient;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Mock
    private GlueClient awsGlue;

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    MongoDatabase mockDatabase;

    @Mock
    MongoCollection mockCollection;

    @Mock
    FindIterable mockIterable;

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .addField("col3", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                .addField("int", Types.MinorType.INT.getType())
                .addField("tinyint", Types.MinorType.TINYINT.getType())
                .addField("smallint", Types.MinorType.SMALLINT.getType())
                .addField("bigint", Types.MinorType.BIGINT.getType())
                .addField("uint1", Types.MinorType.UINT1.getType())
                .addField("uint2", Types.MinorType.UINT2.getType())
                .addField("uint4", Types.MinorType.UINT4.getType())
                .addField("uint8", Types.MinorType.UINT8.getType())
                .addField("float4", Types.MinorType.FLOAT4.getType())
                .addField("float8", Types.MinorType.FLOAT8.getType())
                .addField("bit", Types.MinorType.BIT.getType())
                .addField("varchar", Types.MinorType.VARCHAR.getType())
                .addField("varbinary", Types.MinorType.VARBINARY.getType())
                .addField("decimal", new ArrowType.Decimal(10, 2))
                .addField("decimalLong", new ArrowType.Decimal(36, 2))
                .addField("unsupported", Types.MinorType.VARCHAR.getType())
                .addStructField("struct")
                .addChildField("struct", "struct_string", Types.MinorType.VARCHAR.getType())
                .addChildField("struct", "struct_int", Types.MinorType.INT.getType())
                .addListField("list", Types.MinorType.VARCHAR.getType())
                .build();

        when(connectionFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(S3Client.class);
        mockDatabase = mock(MongoDatabase.class);
        mockCollection = mock(MongoCollection.class);
        mockIterable = mock(FindIterable.class);

        when(mockClient.getDatabase(eq(DEFAULT_SCHEMA))).thenReturn(mockDatabase);
        when(mockDatabase.getCollection(eq(TEST_TABLE))).thenReturn(mockCollection);

        when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        when(amazonS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });

        handler = new DocDBRecordHandler(amazonS3, mockSecretsManager, mockAthena, connectionFactory, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
        mdHandler = new DocDBMetadataHandler(awsGlue, connectionFactory, new LocalKeyFactory(), secretsManager, mockAthena, "spillBucket", "spillPrefix", com.google.common.collect.ImmutableMap.of());
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        int docNum = 11;
        Document doc1 = DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum++);
        documents.add(doc1);
        doc1.put("col3", 22.0D);

        Document doc2 = DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum++);
        documents.add(doc2);
        doc2.put("col3", 22.0D);

        Document doc3 = DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum++);
        documents.add(doc3);
        doc3.put("col3", 21.0D);
        doc3.put("unsupported",new UnsupportedType());

        when(mockCollection.find(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: query[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.projection(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: projection[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.FLOAT8.getType(), 22.0D)), false));

        S3SpillLocation splitLoc = createTestSpillLocation();
        Split split = createTestSplit(splitLoc);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schemaForRead,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() == 2);
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();

        for (int docNum = 0; docNum < 20_000; docNum++) {
            documents.add(DocumentGenerator.makeRandomRow(schemaForRead.getFields(), docNum));
        }

        when(mockCollection.find(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: query[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.projection(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: projection[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("col3", SortedRangeSet.copyOf(Types.MinorType.FLOAT8.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.FLOAT8.getType(), -10000D)), false));

        S3SpillLocation splitLoc = createTestSpillLocation();
        Split split = createTestSplit(splitLoc);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schemaForRead,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                1_500_000L, //~1.5MB so we should see some spill
                0L
        );
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                    logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }
    }

    @Test
    public void nestedStructTest()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();
        Document result = new Document();
        documents.add(result);

        Document listStruct1 = new Document();
        listStruct1.put("SomeSubStruct", "someSubStruct1");
        List<Document> subList = new ArrayList<>();
        Document listSubStruct1 = new Document();
        listSubStruct1.put("SomeSubSubStruct", "someSubSubStruct");
        subList.add(listSubStruct1);
        listStruct1.put("SomeSubList", subList);
        Document listStruct2 = new Document();
        listStruct2.put("SomeSubStruct1", "someSubStruct2");
        List<Document> list = new ArrayList<>();
        list.add(listStruct1);
        list.add(listStruct1);
        Document structWithList = new Document();
        structWithList.put("SomeList", list);
        Document structWithNullList = new Document();
        structWithNullList.put("SomeNullList", null);

        Document simpleSubStruct = new Document();
        simpleSubStruct.put("SomeSimpleSubStruct", "someSimpleSubStruct");
        structWithList.put("SimpleSubStruct", simpleSubStruct);
        structWithList.put("SimpleSubStructNullList", structWithNullList);

        result.put("ComplexStruct", structWithList);

        Document simpleStruct = new Document();
        simpleStruct.put("SomeSimpleStruct", "someSimpleStruct");
        result.put("SimpleStruct", simpleStruct);

        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        Mockito.lenient().when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = mdHandler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        when(mockCollection.find(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: query[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.projection(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: projection[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));


        Map<String, ValueSet> constraintsMap = new HashMap<>();
        S3SpillLocation splitLoc = createTestSpillLocation();
        Split split = createTestSplit(splitLoc);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                res.getSchema(),
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        assertTrue(response.getRecordCount() == 1);
        String expectedString = "[ComplexStruct : {[SomeList : {{[SomeSubStruct : someSubStruct1]," +
                "[SomeSubList : {{[SomeSubSubStruct : someSubSubStruct]}}]}," +
                "{[SomeSubStruct : someSubStruct1],[SomeSubList : {{[SomeSubSubStruct : someSubSubStruct]}}]}}]," +
                "[SimpleSubStruct : {[SomeSimpleSubStruct : someSimpleSubStruct]}]," +
                "[SimpleSubStructNullList : {[SomeNullList : null]}]}], [SimpleStruct : {[SomeSimpleStruct : someSimpleStruct]}]";
        assertEquals(expectedString, BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void dbRefTest()
            throws Exception
    {
        ObjectId id = ObjectId.get();

        List<Document> documents = new ArrayList<>();
        Document result = new Document();
        documents.add(result);
        result.put("DbRef", new DBRef("otherDb", "otherColl", id));

        Document simpleStruct = new Document();
        simpleStruct.put("SomeSimpleStruct", "someSimpleStruct");
        result.put("SimpleStruct", simpleStruct);

        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        Mockito.lenient().when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME,Collections.emptyMap());
        GetTableResponse res = mdHandler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        when(mockCollection.find(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: query[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.projection(nullable(Document.class))).thenAnswer((InvocationOnMock invocationOnMock) -> {
            logger.info("doReadRecordsNoSpill: projection[{}]", invocationOnMock.getArguments()[0]);
            return mockIterable;
        });
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));


        Map<String, ValueSet> constraintsMap = new HashMap<>();
        S3SpillLocation splitLoc = createTestSpillLocation();
        Split split = createTestSplit(splitLoc);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                res.getSchema(),
                split,
                new Constraints(constraintsMap,Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        assertTrue(response.getRecordCount() == 1);
        String expectedString = "[DbRef : {[_db : otherDb],[_ref : otherColl],[_id : " + id.toHexString() + "]}], [SimpleStruct : {[SomeSimpleStruct : someSimpleStruct]}]";
        assertEquals(expectedString, BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_whenCalledWithQueryPassthrough_usesQueryPassthroughPath()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();
        Document doc1 = new Document();
        documents.add(doc1);
        doc1.put("title", "Bill of Rights");
        doc1.put("year", 1791);
        doc1.put("type", "document");

        // Mock setup for database and collection
        MongoDatabase mockQptDatabase = mock(MongoDatabase.class);
        MongoCollection mockQptCollection = mock(MongoCollection.class);
        FindIterable mockQptIterable = mock(FindIterable.class);

        // Setup mocks for query passthrough
        when(mockClient.getDatabase(eq(EXAMPLE_DATABASE))).thenReturn(mockQptDatabase);
        when(mockQptDatabase.getCollection(eq(TPCDS_COLLECTION))).thenReturn(mockQptCollection);
        when(mockQptCollection.find(any(Document.class))).thenReturn(mockQptIterable);
        when(mockQptIterable.projection(any(Document.class))).thenReturn(mockQptIterable);
        when(mockQptIterable.batchSize(anyInt())).thenReturn(mockQptIterable);
        when(mockQptIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));

        // Create schema for the test
        Schema qptSchema = SchemaBuilder.newBuilder()
                .addField("title", Types.MinorType.VARCHAR.getType())
                .addField("year", Types.MinorType.INT.getType())
                .addField("type", Types.MinorType.VARCHAR.getType())
                .build();

        // Setup query passthrough parameters
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Map<String, String> qptParams = new HashMap<>();
        qptParams.put("schemaFunctionName", SYSTEM_QUERY);
        qptParams.put("DATABASE", EXAMPLE_DATABASE);
        qptParams.put("COLLECTION", TPCDS_COLLECTION);
        qptParams.put("FILTER", "{\"title\": \"Bill of Rights\"}");
        qptParams.put(ENABLE_QUERY_PASSTHROUGH, "true");

        S3SpillLocation splitLoc = createTestSpillLocation();
        Split split = createTestSplit(splitLoc);

        // Create read request with query passthrough
        ReadRecordsRequest request = new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(EXAMPLE_DATABASE, TPCDS_COLLECTION),
                qptSchema,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptParams, null),
                100_000_000_000L,
                100_000_000_000L
        );

        // Execute the read
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        // Verify the response
        assertTrue("Response should be of type ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;

        // Verify record count
        assertEquals(1, response.getRecordCount());

        // Verify record content
        Block records = response.getRecords();
        String rowAsString = BlockUtils.rowToString(records, 0);
        assertTrue("Row should contain title", rowAsString.contains("Bill of Rights"));
        assertTrue("Row should contain year", rowAsString.contains("1791"));
        assertTrue("Row should contain type", rowAsString.contains("document"));

        // Verify that the correct database and collection were queried
        verify(mockClient).getDatabase(EXAMPLE_DATABASE);
        verify(mockQptDatabase).getCollection(TPCDS_COLLECTION);

        // Verify that the filter was applied
        verify(mockQptCollection).find(eq(Document.parse("{\"title\": \"Bill of Rights\"}")));
    }

    @Test
    public void doReadRecords_whenPassthroughDisabled_usesNormalPath()
            throws Exception
    {
        // Create handler with query passthrough disabled
        DocDBRecordHandler passthroughDisabledHandler = new DocDBRecordHandler(
                amazonS3,
                mockSecretsManager,
                mockAthena,
                connectionFactory,
                com.google.common.collect.ImmutableMap.of(ENABLE_QUERY_PASSTHROUGH, "false")
        );

        List<Document> documents = new ArrayList<>();
        Document doc = new Document();
        documents.add(doc);
        doc.put("col1", 42);
        doc.put("col2", "testValue");

        Schema schema = SchemaBuilder.newBuilder()
                .addField("col1", Types.MinorType.INT.getType())
                .addField("col2", Types.MinorType.VARCHAR.getType())
                .build();

        setupMockCollection(documents);

        // Create constraints WITHOUT query passthrough arguments
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        ReadRecordsRequest request = createReadRecordsRequest(schema, constraints);

        RecordResponse rawResponse = passthroughDisabledHandler.doReadRecords(allocator, request);

        assertTrue("Response should be of type ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertEquals(1, response.getRecordCount());

        // Verify that the normal (non-passthrough) path was used
        verify(mockClient).getDatabase(DEFAULT_SCHEMA);
        verify(mockDatabase).getCollection(TEST_TABLE);
    }

    @Test
    public void doReadRecords_whenMissingQueryPassthroughArgs_throwsRuntimeException()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("title", Types.MinorType.VARCHAR.getType())
                .build();

        // Setup query passthrough parameters with missing required arguments
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Map<String, String> qptParams = new HashMap<>();
        qptParams.put("schemaFunctionName", SYSTEM_QUERY);
        qptParams.put(ENABLE_QUERY_PASSTHROUGH, "true");
        // Missing DATABASE, COLLECTION, and FILTER parameters
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptParams, null);

        ReadRecordsRequest request = createReadRecordsRequest(schema, constraints);

        // Missing query passthrough arguments should throw RuntimeException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should contain Missing Query Passthrough Argument",
                exception.getMessage().contains("Missing Query Passthrough Argument"));
    }

    @Test
    public void doReadRecords_whenWrongSchemaFunctionName_throwsRuntimeException()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addField("title", Types.MinorType.VARCHAR.getType())
                .build();

        // Setup query passthrough parameters with wrong schema function name
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Map<String, String> qptParams = new HashMap<>();
        qptParams.put("schemaFunctionName", "wrong.function");  // Wrong schema function name
        qptParams.put(ENABLE_QUERY_PASSTHROUGH, "true");
        qptParams.put("DATABASE", EXAMPLE_DATABASE);
        qptParams.put("COLLECTION", TPCDS_COLLECTION);
        qptParams.put("FILTER", "{\"title\": \"Bill of Rights\"}");
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptParams, null);

        ReadRecordsRequest request = createReadRecordsRequest(schema, constraints);

        // Wrong schema function name should throw RuntimeException
        RuntimeException exception = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should contain Function Signature doesn't match implementation's",
                exception.getMessage().contains("Function Signature doesn't match implementation's"));
    }

    @Test
    public void doReadRecords_whenInvalidPassthroughFlag_usesNormalPath()
            throws Exception
    {
        List<Document> documents = new ArrayList<>();
        Document doc = new Document();
        documents.add(doc);
        doc.put("title", "Bill of Rights");

        Schema schema = SchemaBuilder.newBuilder()
                .addField("title", Types.MinorType.VARCHAR.getType())
                .build();

        setupMockCollection(documents);

        // Setup constraints WITHOUT query passthrough arguments (empty map)
        // This simulates the case where passthrough is disabled at the engine level
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Map<String, String> emptyQptParams = new HashMap<>();  // Empty passthrough arguments
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, emptyQptParams, null);

        ReadRecordsRequest request = createReadRecordsRequest(schema, constraints);

        // With empty passthrough arguments, should use normal path (not passthrough)
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue("Response should be of type ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertEquals(1, response.getRecordCount());

        // Verify that the normal (non-passthrough) path was used
        verify(mockClient).getDatabase(DEFAULT_SCHEMA);
        verify(mockDatabase).getCollection(TEST_TABLE);
    }

    @Test
    public void doReadRecords_whenCaseInsensitivityEnabled_readsCaseInsensitively()
            throws Exception
    {
        // Create handler with disable_projection_and_casing enabled
        DocDBRecordHandler caseInsensitiveHandler = new DocDBRecordHandler(
                amazonS3, 
                mockSecretsManager, 
                mockAthena, 
                connectionFactory, 
                com.google.common.collect.ImmutableMap.of(DISABLE_PROJECTION_AND_CASING, "true")
        );

        List<Document> documents = new ArrayList<>();
        Document doc = new Document();
        // Add fields with mixed case
        doc.put("MixedCaseField", "testValue");
        doc.put("UPPERCASE_FIELD", 123);
        doc.put("lowercase_field", 456.78);
        documents.add(doc);

        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("mixedcasefield")  // lowercase schema field name
                .addIntField("uppercase_field")
                .addFloat8Field("LOWERCASE_FIELD")  // uppercase schema field name
                .build();

        setupMockCollection(documents);

        ReadRecordsRequest request = createReadRecordsRequest(schema, createEmptyConstraints());

        RecordResponse rawResponse = caseInsensitiveHandler.doReadRecords(allocator, request);

        assertTrue("Response should be of type ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("testReadWithCaseInsensitivity: rows[{}]", response.getRecordCount());

        assertEquals(1, response.getRecords().getRowCount());
        logger.info("testReadWithCaseInsensitivity: {}", BlockUtils.rowToString(response.getRecords(), 0));
        
        // Verify that fields are accessible despite case mismatch
        assertNotNull("Records should not be null", response.getRecords());
    }

    @Test
    public void doReadRecords_whenDataHasProcessingError_throwsRuntimeException()
    {
        List<Document> documents = new ArrayList<>();
        Document doc = new Document();
        
        // Create a document with a struct field that has problematic data
        // This will cause an exception during offerComplexValue processing
        Document nestedDoc = new Document();
        nestedDoc.put("nested_field", new Object() {
            @Override
            public String toString()
            {
                throw new RuntimeException("Simulated field processing error");
            }
        });
        doc.put("struct_field", nestedDoc);
        documents.add(doc);

        // Create schema with a STRUCT field (complex type)
        Schema schema = SchemaBuilder.newBuilder()
                .addStructField("struct_field")
                .addChildField("struct_field", "nested_field", Types.MinorType.VARCHAR.getType())
                .build();

        setupMockCollection(documents);

        ReadRecordsRequest request = createReadRecordsRequest(schema, createEmptyConstraints());

        // Test that processing a field with an error throws RuntimeException with the expected message
        RuntimeException exception = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should contain error description", 
                   exception.getMessage().contains("Error while processing field"));
    }

    @Test
    public void doReadRecords_whenMissingConnectionString_throwsRuntimeException()
    {
        Schema schema = SchemaBuilder.newBuilder()
                .addStringField("field1")
                .build();

        S3SpillLocation splitLoc = createTestSpillLocation();

        // Create a split WITHOUT the DOCDB_CONN_STR property
        Split splitWithoutConnStr = Split.newBuilder(splitLoc, keyFactory.create()).build();

        ReadRecordsRequest request = new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schema,
                splitWithoutConnStr,
                createEmptyConstraints(),
                100_000_000_000L,
                100_000_000_000L
        );

        // Missing connection string throws RuntimeException with the expected message
        RuntimeException exception = assertThrows(RuntimeException.class, () -> handler.doReadRecords(allocator, request));
        assertTrue("Exception message should contain Unable to create connection",
                   exception.getMessage().contains("Split property is null! Unable to create connection."));
    }
    /**
     * Helper method to create a standard S3SpillLocation for tests
     */
    private S3SpillLocation createTestSpillLocation()
    {
        return S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();
    }

    /**
     * Helper method to create a standard Split for tests
     */
    private Split createTestSplit(S3SpillLocation splitLoc)
    {
        return Split.newBuilder(splitLoc, keyFactory.create())
                .add(DOCDB_CONN_STR, CONNECTION_STRING)
                .build();
    }

    /**
     * Helper method to set up standard mock collection behavior
     */
    private void setupMockCollection(List<Document> documents)
    {
        when(mockCollection.find(nullable(Document.class))).thenReturn(mockIterable);
        when(mockIterable.projection(nullable(Document.class))).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(documents.iterator()));
    }

    /**
     * Helper method to create a standard ReadRecordsRequest
     */
    private ReadRecordsRequest createReadRecordsRequest(Schema schema, Constraints constraints)
    {
        S3SpillLocation splitLoc = createTestSpillLocation();
        Split split = createTestSplit(splitLoc);

        return new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                TABLE_NAME,
                schema,
                split,
                constraints,
                100_000_000_000L,
                100_000_000_000L
        );
    }

    /**
     * Helper method to create standard constraints with empty maps
     */
    private Constraints createEmptyConstraints()
    {
        return new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
