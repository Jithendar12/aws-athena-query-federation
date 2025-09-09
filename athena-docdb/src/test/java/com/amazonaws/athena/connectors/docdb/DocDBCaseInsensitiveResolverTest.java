/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DocDBCaseInsensitiveResolverTest
{
    private static final String ENABLE_CASE_INSENSITIVE_MATCH = "enable_case_insensitive_match";
    private static final String TEST_DB = "TestDB";
    private static final String TEST_DB_LOWERCASE = "testdb";
    private static final String TEST_TABLE = "TestTable";
    private static final String TEST_TABLE_LOWERCASE = "testtable";
    private static final String NONEXISTENT = "nonexistent";

    @Mock
    private MongoClient mockClient;

    @Mock
    private MongoDatabase mockDatabase;

    @Mock
    private MongoIterable<String> mockDatabaseNames;

    @Mock
    private MongoIterable<String> mockCollectionNames;

    private Map<String, String> configOptions;

    @Before
    public void setup()
    {
        MockitoAnnotations.openMocks(this);
        configOptions = new HashMap<>();
        when(mockClient.listDatabaseNames()).thenReturn(mockDatabaseNames);
        when(mockDatabase.listCollectionNames()).thenReturn(mockCollectionNames);
    }

    @Test
    public void testGetSchemaNameMatch_whenCaseInsensitiveMatchDisabled()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "false");
        
        String result = DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
            configOptions, mockClient, TEST_DB);
        
        assertEquals(TEST_DB, result);
        verify(mockClient, never()).listDatabaseNames();
    }

    @Test
    public void testGetSchemaNameMatch_whenCaseInsensitiveMatchEnabled()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockDatabaseNames.spliterator()).thenReturn(
            Arrays.asList(TEST_DB, "otherDB").spliterator());

        String result = DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
            configOptions, mockClient, TEST_DB_LOWERCASE);

        assertEquals(TEST_DB, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSchemaNameMatch_whenNoMatchFound()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockDatabaseNames.spliterator()).thenReturn(
            Arrays.asList("DB1", "DB2").spliterator());

        DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
            configOptions, mockClient, NONEXISTENT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSchemaNameMatch_whenMultipleMatchesFound()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockDatabaseNames.spliterator()).thenReturn(
            Arrays.asList(TEST_DB, "TESTDB", TEST_DB_LOWERCASE).spliterator());

        DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
            configOptions, mockClient, TEST_DB_LOWERCASE);
    }

    @Test
    public void testGetTableNameMatch_whenCaseInsensitiveMatchDisabled()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "false");
        
        String result = DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
            configOptions, mockDatabase, TEST_TABLE);
        
        assertEquals(TEST_TABLE, result);
        verify(mockDatabase, never()).listCollectionNames();
    }

    @Test
    public void testGetTableNameMatch_whenCaseInsensitiveMatchEnabled()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockCollectionNames.spliterator()).thenReturn(
            Arrays.asList(TEST_TABLE, "otherTable").spliterator());

        String result = DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
            configOptions, mockDatabase, TEST_TABLE_LOWERCASE);

        assertEquals(TEST_TABLE, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableNameMatch_whenNoMatchFound()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockCollectionNames.spliterator()).thenReturn(
            Arrays.asList("Table1", "Table2").spliterator());

        DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
            configOptions, mockDatabase, NONEXISTENT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTableNameMatch_whenMultipleMatchesFound()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockCollectionNames.spliterator()).thenReturn(
            Arrays.asList(TEST_TABLE, "TESTTABLE", TEST_TABLE_LOWERCASE).spliterator());

        DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
            configOptions, mockDatabase, TEST_TABLE_LOWERCASE);
    }

    @Test
    public void testGetSchemaNameMatch_whenEmptyList()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockDatabaseNames.spliterator()).thenReturn(
            Collections.<String>emptyList().spliterator());

        try {
            DocDBCaseInsensitiveResolver.getSchemaNameCaseInsensitiveMatch(
                configOptions, mockClient, TEST_DB_LOWERCASE);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Schema name is empty"));
        }
    }

    @Test
    public void testGetTableNameMatch_whenEmptyList()
    {
        configOptions.put(ENABLE_CASE_INSENSITIVE_MATCH, "true");
        when(mockCollectionNames.spliterator()).thenReturn(
            Collections.<String>emptyList().spliterator());

        try {
            DocDBCaseInsensitiveResolver.getTableNameCaseInsensitiveMatch(
                configOptions, mockDatabase, TEST_TABLE_LOWERCASE);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Table name is empty"));
        }
    }
}
