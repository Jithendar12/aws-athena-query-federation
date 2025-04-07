/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.handlers;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
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
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.request.FederationRequest;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.request.PingRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.serde.VersionedObjectMapperFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MetadataHandlerTest
{
    private MetadataHandler metadataHandler;
    private BlockAllocator blockAllocator;
    private final FederatedIdentity identity = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    String catalog = "catalog";
    String queryId = "queryId";
    @Mock
    private Constraints mockConstraints;

    @Before
    public void setUp()
    {
        Map<String, String> configOptions = new HashMap<>();
        String bucket = "bucket";
        configOptions.put("spill_bucket", bucket);
        String prefix = "prefix";
        configOptions.put("spill_prefix", prefix);

        // Create a mock implementation of MetadataHandler for testing
        metadataHandler = new MetadataHandler(
                new LocalKeyFactory(),
                mock(SecretsManagerClient.class),
                mock(AthenaClient.class),
                "test",
                bucket,
                prefix,
                configOptions
        ) {
            @Override
            public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker)
            {
                try {
                    // Writing one partition for testing doGetTableLayout
                    blockWriter.writeRows((Block block, int rowNum) -> {
                        block.setValue("partition_col", rowNum, "*");
                        return 1;
                    });
                }
                catch (Exception e) {
                    throw new RuntimeException("Failed to write partitions", e);
                }
            }

            @Override
            public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
            {
                throw new UnsupportedOperationException();
            }
        };

        blockAllocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
            throws Exception
    {
        blockAllocator.close();
    }

    @Test
    public void doGetTableLayout() throws Exception
    {
        GetTableLayoutRequest request = new GetTableLayoutRequest(identity, queryId, catalog,
                new TableName("testSchema", "testTable"),
                mockConstraints,
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse response = metadataHandler.doGetTableLayout(blockAllocator, request);

        assertNotNull(response);
        assertEquals(1, response.getPartitions().getRowCount());
    }

    @Test
    public void doGetTableLayoutWithConstraints() throws Exception
    {
        GetTableLayoutRequest request = new GetTableLayoutRequest(identity, queryId, catalog,
                new TableName("testSchema", "testTable"),
                mockConstraints,
                SchemaBuilder.newBuilder().addStringField("partition_col").build(),
                Collections.singleton("partition_col"));

        Schema partitionSchema = SchemaBuilder.newBuilder().addStringField("partition_col").build();

        BlockAllocator spyAllocator = spy(blockAllocator);

        GetTableLayoutResponse response = metadataHandler.doGetTableLayout(spyAllocator, request);
        assertNotNull(response);
        Block partitions = response.getPartitions();
        assertEquals(1, partitions.getRowCount());

        verify(spyAllocator).createBlock(partitionSchema);
    }

    @Test
    public void handleRequest()
            throws Exception
    {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema schema = SchemaBuilder.newBuilder().build();
        Split split = mock(Split.class);
        FederationRequest invalidRequest = new ReadRecordsRequest(null, catalog, queryId, tableName, schema, split, mockConstraints, 1024, 512);

        ByteArrayOutputStream invalidOutputStream = new ByteArrayOutputStream();
        ObjectMapper objectMapper = VersionedObjectMapperFactory.create(blockAllocator);
        objectMapper.writeValue(invalidOutputStream, invalidRequest);
        ByteArrayInputStream invalidInputStream = new ByteArrayInputStream(invalidOutputStream.toByteArray());
        ByteArrayOutputStream invalidTestOutputStream = new ByteArrayOutputStream();

        try {
            metadataHandler.handleRequest(invalidInputStream, invalidTestOutputStream, mock(Context.class));
            fail("Expected AthenaConnectorException for invalid request type");
        }
        catch (AthenaConnectorException e) {
            assertTrue(e.getMessage().contains("Expected a MetadataRequest but found"));
        }

        FederationRequest validRequest = new GetDataSourceCapabilitiesRequest(identity, queryId, catalog);

        ByteArrayOutputStream validOutputStream = new ByteArrayOutputStream();
        objectMapper.writeValue(validOutputStream, validRequest);
        ByteArrayInputStream validInputStream = new ByteArrayInputStream(validOutputStream.toByteArray());
        ByteArrayOutputStream validTestOutputStream = new ByteArrayOutputStream();

        metadataHandler.handleRequest(validInputStream, validTestOutputStream, mock(Context.class));

        FederationResponse response = objectMapper.readValue(validTestOutputStream.toByteArray(), FederationResponse.class);
        assertNotNull(response);
    }

    @Test
    public void pingHandleRequest() throws IOException
    {
        FederationRequest pingRequest = new PingRequest(identity, catalog, queryId);
        ByteArrayOutputStream pingOutputStream = new ByteArrayOutputStream();
        ObjectMapper objectMapper = VersionedObjectMapperFactory.create(blockAllocator);
        objectMapper.writeValue(pingOutputStream, pingRequest);
        ByteArrayInputStream pingInputStream = new ByteArrayInputStream(pingOutputStream.toByteArray());
        ByteArrayOutputStream pingTestOutputStream = new ByteArrayOutputStream();
        metadataHandler.handleRequest(pingInputStream, pingTestOutputStream, mock(Context.class));
        FederationResponse response = objectMapper.readValue(pingTestOutputStream.toByteArray(), FederationResponse.class);
        assertNotNull(response);
    }
}
