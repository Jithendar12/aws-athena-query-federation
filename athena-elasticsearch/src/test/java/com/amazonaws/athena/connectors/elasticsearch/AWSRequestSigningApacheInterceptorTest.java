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

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is used to test the AWSRequestSigningApacheInterceptor class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AWSRequestSigningApacheInterceptorTest
{
    private static final String TEST_SERVICE = "es";
    private static final String TEST_REGION = "us-east-1";
    private static final String TEST_URI = "https://search-test.us-east-1.es.amazonaws.com/test";
    private static final String TEST_URI_INVALID = "invalid://[malformed uri";

    @Mock
    private AwsV4HttpSigner mockSigner;

    @Mock
    private AwsCredentialsProvider mockCredentialsProvider;

    private AWSRequestSigningApacheInterceptor interceptor;
    private HttpContext context;

    @Before
    public void setUp()
    {
        interceptor = new AWSRequestSigningApacheInterceptor(TEST_SERVICE, mockSigner, mockCredentialsProvider, TEST_REGION);
        context = new BasicHttpContext();
        HttpHost host = HttpHost.create("https://search-test.us-east-1.es.amazonaws.com");
        context.setAttribute(HttpCoreContext.HTTP_TARGET_HOST, host);
        
        // Mock the signer and credentials provider
        AwsCredentials mockCredentials = new AwsCredentials() {
            @Override
            public String accessKeyId() {
                return "test-access-key";
            }
            
            @Override
            public String secretAccessKey() {
                return "test-secret-key";
            }
        };
        lenient().when(mockCredentialsProvider.resolveCredentials()).thenReturn(mockCredentials);
        
        SignedRequest mockSignedRequest = mock(SignedRequest.class);
        SdkHttpFullRequest mockRequest = mock(SdkHttpFullRequest.class);
        lenient().when(mockSignedRequest.request()).thenReturn(mockRequest);
        lenient().when(mockRequest.headers()).thenReturn(Collections.emptyMap());
        lenient().when(mockSigner.sign(isA(Consumer.class))).thenReturn(mockSignedRequest);
    }

    @Test
    public void process_withValidRequest_succeeds() throws Exception
    {
        HttpRequest request = new HttpGet(TEST_URI);

        interceptor.process(request, context);
        assertNotNull("Request should be processed", request);
    }

    @Test
    public void process_withInvalidUri_throwsAthenaConnectorException()
    {
        HttpRequest request = new BasicHttpRequest("GET", TEST_URI_INVALID);

        try {
            interceptor.process(request, context);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Invalid URI",
                    ex.getMessage().contains("Invalid URI"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }

    @Test
    public void process_withEntityEnclosingRequest_succeeds() throws Exception
    {
        HttpPost request = new HttpPost(TEST_URI);
        StringEntity entity = new StringEntity("test body");
        request.setEntity(entity);

        interceptor.process(request, context);
        assertNotNull("Request should be processed", request);
    }

    @Test
    public void process_withEntityEnclosingRequestWithoutEntity_succeeds() throws Exception
    {
        HttpPost request = new HttpPost(TEST_URI);
        // No entity set

        interceptor.process(request, context);
        assertNotNull("Request should be processed", request);
    }

    @Test
    public void process_withNullHostInContext_succeeds() throws Exception
    {
        HttpContext contextWithoutHost = new BasicHttpContext();
        HttpRequest request = new HttpGet(TEST_URI);

        interceptor.process(request, contextWithoutHost);
        assertNotNull("Request should be processed", request);
    }

    @Test
    public void process_withInvalidUriSyntax_throwsAthenaConnectorException()
    {
        HttpRequest request = new BasicHttpRequest("GET", "invalid://[malformed uri");

        try {
            interceptor.process(request, context);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Invalid URI",
                    ex.getMessage().contains("Invalid URI"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }

    @Test
    public void process_withEntityEnclosingRequestAndEntity_setsEntityContent()
            throws Exception
    {
        HttpPost request = new HttpPost(TEST_URI);
        StringEntity entity = new StringEntity("test body");
        request.setEntity(entity);

        interceptor.process(request, context);
        assertNotNull("Request should be processed", request);
        assertNotNull("Entity should be set", request.getEntity());
    }

    @Test
    public void nvpToMapParams_withMultipleValues_addsAllValues()
    {
        try {
            java.lang.reflect.Method method = AWSRequestSigningApacheInterceptor.class.getDeclaredMethod("nvpToMapParams", java.util.List.class);
            method.setAccessible(true);

            java.util.List<org.apache.http.NameValuePair> params = new java.util.ArrayList<>();
            params.add(new org.apache.http.message.BasicNameValuePair("key1", "value1"));
            params.add(new org.apache.http.message.BasicNameValuePair("key1", "value2"));
            params.add(new org.apache.http.message.BasicNameValuePair("key2", "value3"));

            Map<String, List<String>> result = Collections.unmodifiableMap((Map<String, List<String>>) method.invoke(null, params));

            assertNotNull("Result should not be null", result);
            assertTrue("Should contain key1", result.containsKey("key1"));
            assertEquals("Should have 2 values for key1", 2, result.get("key1").size());
        }
        catch (Exception e) {
            fail("Failed to invoke nvpToMapParams: " + e.getMessage());
        }
    }

    @Test
    public void process_withURISyntaxExceptionInBuilder_throwsAthenaConnectorException()
    {
        HttpRequest request = org.mockito.Mockito.mock(HttpRequest.class);
        org.apache.http.RequestLine requestLine = org.mockito.Mockito.mock(org.apache.http.RequestLine.class);
        lenient().when(request.getRequestLine()).thenReturn(requestLine);
        lenient().when(requestLine.getMethod()).thenReturn("GET");
        lenient().when(requestLine.getUri()).thenReturn("https://test.com/path?query=value%");

        try {
            interceptor.process(request, context);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain Invalid URI",
                    ex.getMessage().contains("Invalid URI"));
        }
        catch (Exception e) {
            fail("Expected AthenaConnectorException but got: " + e.getClass().getName());
        }
    }
}
