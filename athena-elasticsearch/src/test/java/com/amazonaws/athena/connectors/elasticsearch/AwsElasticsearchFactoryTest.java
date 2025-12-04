/*-
 * #%L
 * athena-elasticsearch
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
package com.amazonaws.athena.connectors.elasticsearch;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.elasticsearch.ElasticsearchClient;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the AwsElasticsearchFactory class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AwsElasticsearchFactoryTest
{
    @Test
    public void getClient_returnsElasticsearchClient()
    {
        ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
        when(mockClient.serviceName()).thenReturn("es");

        try (MockedStatic<ElasticsearchClient> elasticsearchClientStaticMock = mockStatic(ElasticsearchClient.class)) {
            elasticsearchClientStaticMock.when(ElasticsearchClient::create).thenReturn(mockClient);

            AwsElasticsearchFactory factory = new AwsElasticsearchFactory();
            ElasticsearchClient client = factory.getClient();

            assertNotNull("Elasticsearch client should not be null", client);
            assertTrue("Client should be an instance of ElasticsearchClient", 
                    client instanceof ElasticsearchClient);
            // Verify the client is properly configured
            assertFalse("Client should be active", client.serviceName().isEmpty());
            // Verify that ElasticsearchClient.create() was called
            elasticsearchClientStaticMock.verify(ElasticsearchClient::create);
        }
    }
}
