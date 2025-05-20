/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.credentials.StaticCredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlServerJdbcConnectionFactoryTest
{
    @Test
    public void testGetConnection_withCredentials_replacesSecret()
    {
        try {
            DefaultCredentials credentials = new DefaultCredentials("user1", "pass1");
            CredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);

            String originalJdbcUrl = "jdbc:sqlserver://localhost:1433;databaseName=testdb;${test}";
            DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", "sqlserver", originalJdbcUrl, "test");

            DatabaseConnectionInfo info = new DatabaseConnectionInfo("com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433);

            Driver mockDriver = mock(Driver.class);
            Connection mockConnection = mock(Connection.class);
            AtomicReference<String> actualUrl = new AtomicReference<>();

            when(mockDriver.acceptsURL(any())).thenReturn(true);
            when(mockDriver.connect(any(), any(Properties.class))).thenAnswer(invocation -> {
                actualUrl.set(invocation.getArgument(0));
                return mockConnection;
            });

            DriverManager.registerDriver(mockDriver);

            SqlServerJdbcConnectionFactory factory = new SqlServerJdbcConnectionFactory(config, new HashMap<>(), info);
            Connection connection = factory.getConnection(credentialsProvider);

            String expectedUrl = "jdbc:sqlserver://localhost:1433;databaseName=testdb;user=user1;password=pass1";
            assertEquals(expectedUrl, actualUrl.get());
            assertEquals(mockConnection, connection);

            DriverManager.deregisterDriver(mockDriver);
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test
    public void testGetConnection_withNullCredentials_doesNotReplaceSecret()
    {
        try {
            String jdbcUrl = "jdbc:sqlserver://localhost:1433;databaseName=testdb;user=admin;password=admin";
            DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", "sqlserver", jdbcUrl, "test");

            DatabaseConnectionInfo info = new DatabaseConnectionInfo("com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433);

            Driver mockDriver = mock(Driver.class);
            Connection mockConnection = mock(Connection.class);
            AtomicReference<String> actualUrl = new AtomicReference<>();

            when(mockDriver.acceptsURL(any())).thenReturn(true);
            when(mockDriver.connect(any(), any(Properties.class))).thenAnswer(invocation -> {
                actualUrl.set(invocation.getArgument(0));
                return mockConnection;
            });

            DriverManager.registerDriver(mockDriver);

            SqlServerJdbcConnectionFactory factory = new SqlServerJdbcConnectionFactory(config, new HashMap<>(), info);
            Connection connection = factory.getConnection(null);

            assertEquals(jdbcUrl, actualUrl.get());
            assertEquals(mockConnection, connection);

            DriverManager.deregisterDriver(mockDriver);
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnection_whenDriverClassNotFound_throwsRuntimeException()
    {
        String jdbcUrl = "jdbc:sqlserver://localhost:1433;databaseName=testdb";
        DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", "sqlserver", jdbcUrl, "test");

        // Intentionally provide invalid driver class
        DatabaseConnectionInfo info = new DatabaseConnectionInfo("invalid.DriverClass", 1433);

        SqlServerJdbcConnectionFactory factory = new SqlServerJdbcConnectionFactory(config, new HashMap<>(), info);
        factory.getConnection(null);
    }

    @Test(expected = RuntimeException.class)
    public void testGetConnection_whenDriverFailsToConnect_throwsRuntimeException() throws Exception
    {
        DefaultCredentials credentials = new DefaultCredentials("user", "password");
        CredentialsProvider credentialsProvider = new StaticCredentialsProvider(credentials);

        String jdbcUrl = "jdbc:sqlserver://localhost:1433;databaseName=testdb";
        DatabaseConnectionConfig config = new DatabaseConnectionConfig("catalog", "sqlserver", jdbcUrl, "test");

        DatabaseConnectionInfo info = new DatabaseConnectionInfo("com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433);

        Driver mockDriver = mock(Driver.class);
        when(mockDriver.acceptsURL(anyString())).thenReturn(true);
        when(mockDriver.connect(anyString(), any(Properties.class)))
                .thenThrow(new SQLException("DB error", "08001", 999));

        DriverManager.registerDriver(mockDriver);

        try {
            SqlServerJdbcConnectionFactory factory = new SqlServerJdbcConnectionFactory(config, new HashMap<>(), info);
            factory.getConnection(credentialsProvider);
        }
        finally {
            DriverManager.deregisterDriver(mockDriver);
        }
    }
}
