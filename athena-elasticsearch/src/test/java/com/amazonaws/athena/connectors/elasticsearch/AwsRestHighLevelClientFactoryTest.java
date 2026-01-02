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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the AwsRestHighLevelClientFactory class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AwsRestHighLevelClientFactoryTest
{
    private static final String TEST_ENDPOINT = "https://search-test.us-east-1.es.amazonaws.com";
    private static final String TEST_ENDPOINT_WITH_CREDENTIALS = "https://myusername@mypassword:www.example.com";
    private static final String TEST_USERNAME = "testuser";
    private static final String TEST_PASSWORD = "testpass";

    @Mock
    private AwsRestHighLevelClient mockClient;

    private AwsRestHighLevelClientFactory factoryWithAwsCredentials;
    private AwsRestHighLevelClientFactory factoryWithoutAwsCredentials;

    @Before
    public void setUp()
    {
        factoryWithAwsCredentials = new AwsRestHighLevelClientFactory(true);
        factoryWithoutAwsCredentials = new AwsRestHighLevelClientFactory(false);
    }

    @Test
    public void getOrCreateClient_withAwsCredentials_returnsNonNullClient()
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> ignored = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> {
                    when(mock.withCredentials(org.mockito.ArgumentMatchers.any())).thenReturn(mock);
                    when(mock.build()).thenReturn(mockClient);
                })) {
            AwsRestHighLevelClient client = factoryWithAwsCredentials.getOrCreateClient(TEST_ENDPOINT);

            assertNotNull("Client with AWS credentials should not be null", client);
        }
    }

    @Test
    public void getOrCreateClient_withoutAwsCredentials_returnsValidClient()
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> ignored = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> when(mock.build()).thenReturn(mockClient))) {
            AwsRestHighLevelClient client = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT);

            assertNotNull("Client without AWS credentials should not be null", client);
        }
    }

    @Test
    public void getOrCreateClient_withEmbeddedCredentials_returnsClientWithCredentials()
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> ignored = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> {
                    when(mock.withCredentials(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString())).thenReturn(mock);
                    when(mock.build()).thenReturn(mockClient);
                })) {
            AwsRestHighLevelClient client = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT_WITH_CREDENTIALS);

            assertNotNull("Client with embedded credentials should not be null", client);
        }
    }

    @Test
    public void getOrCreateClient_withUsernameAndPassword_returnsNewClient()
    {
        try (MockedConstruction<AwsRestHighLevelClient.Builder> ignored = mockConstruction(AwsRestHighLevelClient.Builder.class,
                (mock, context) -> {
                    when(mock.withCredentials(TEST_USERNAME, TEST_PASSWORD)).thenReturn(mock);
                    when(mock.build()).thenReturn(mockClient);
                })) {
            AwsRestHighLevelClient client = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT, TEST_USERNAME, TEST_PASSWORD);

            assertNotNull("Client with username and password should not be null", client);
        }
    }
}
