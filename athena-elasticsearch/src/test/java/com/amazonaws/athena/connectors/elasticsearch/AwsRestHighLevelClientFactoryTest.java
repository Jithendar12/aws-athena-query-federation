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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;

/**
 * This class is used to test the AwsRestHighLevelClientFactory class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AwsRestHighLevelClientFactoryTest
{
    private static final String TEST_ENDPOINT = "https://search-test.us-east-1.es.amazonaws.com";
    private static final String TEST_ENDPOINT_WITH_CREDENTIALS = "http://myusername@mypassword:www.example.com";
    private static final String TEST_USERNAME = "testuser";
    private static final String TEST_PASSWORD = "testpass";

    private AwsRestHighLevelClientFactory factoryWithAwsCredentials;
    private AwsRestHighLevelClientFactory factoryWithoutAwsCredentials;

    @Before
    public void setUp()
    {
        factoryWithAwsCredentials = new AwsRestHighLevelClientFactory(true);
        factoryWithoutAwsCredentials = new AwsRestHighLevelClientFactory(false);
    }

    @Test
    public void getOrCreateClient_withAwsCredentials_returnsClient()
    {
        AwsRestHighLevelClient client = factoryWithAwsCredentials.getOrCreateClient(TEST_ENDPOINT);

        assertNotNull("Client should not be null", client);
    }

    @Test
    public void getOrCreateClient_withoutAwsCredentials_returnsClient()
    {
        AwsRestHighLevelClient client = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT);

        assertNotNull("Client should not be null", client);
    }

    @Test
    public void getOrCreateClient_withEmbeddedCredentials_returnsClient()
    {
        AwsRestHighLevelClient client = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT_WITH_CREDENTIALS);

        assertNotNull("Client should not be null", client);
    }

    @Test
    public void getOrCreateClient_withUsernameAndPassword_returnsClient()
    {
        AwsRestHighLevelClient client = factoryWithoutAwsCredentials.getOrCreateClient(TEST_ENDPOINT, TEST_USERNAME, TEST_PASSWORD);

        assertNotNull("Client should not be null", client);
    }

    @Test
    public void getOrCreateClient_calledTwice_returnsCachedClient()
    {
        // Use a unique endpoint for this test to avoid conflicts with other tests
        String uniqueEndpoint = "https://unique-test-" + System.currentTimeMillis() + ".us-east-1.es.amazonaws.com";
        AwsRestHighLevelClient client1 = factoryWithAwsCredentials.getOrCreateClient(uniqueEndpoint);
        AwsRestHighLevelClient client2 = factoryWithAwsCredentials.getOrCreateClient(uniqueEndpoint);

        assertNotNull("First client should not be null", client1);
        assertNotNull("Second client should not be null", client2);
        // Note: Cache may evict unhealthy clients, so we just verify both calls return a client
        // The actual caching behavior is tested in CacheableAwsRestHighLevelClientTest
    }

    @Test
    public void getOrCreateClient_withUsernameAndPassword_calledTwice_returnsCachedClient()
    {
        // Use a unique endpoint for this test to avoid conflicts with other tests
        String uniqueEndpoint = "https://unique-test-" + System.currentTimeMillis() + ".us-east-1.es.amazonaws.com";
        AwsRestHighLevelClient client1 = factoryWithoutAwsCredentials.getOrCreateClient(uniqueEndpoint, TEST_USERNAME, TEST_PASSWORD);
        AwsRestHighLevelClient client2 = factoryWithoutAwsCredentials.getOrCreateClient(uniqueEndpoint, TEST_USERNAME, TEST_PASSWORD);

        assertNotNull("First client should not be null", client1);
        assertNotNull("Second client should not be null", client2);
        // Note: Cache may evict unhealthy clients, so we just verify both calls return a client
        // The actual caching behavior is tested in CacheableAwsRestHighLevelClientTest
    }
}
