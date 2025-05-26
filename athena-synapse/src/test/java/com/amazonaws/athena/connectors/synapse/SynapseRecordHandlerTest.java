/*-
 * #%L
 * athena-synapse
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;

import static com.amazonaws.athena.connectors.synapse.SynapseConstants.QUOTE_CHARACTER;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SynapseRecordHandlerTest
{
    private SynapseRecordHandler synapseRecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup()
            throws Exception
    {
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new SynapseQueryStringBuilder(QUOTE_CHARACTER, new SynapseFederationExpressionParser(QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SynapseConstants.NAME,
                "synapse://jdbc:sqlserver://hostname;databaseName=fakedatabase");

        this.synapseRecordHandler = new SynapseRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    private ValueSet getSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        when(range.isSingleValue()).thenReturn(true);
        when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void buildSplitSql()
            throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        when(split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn("id");
        when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("100000");
        when(split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("300000");

        ValueSet valueSet = getSingleValueSet("varcharTest");
        Constraints constraints = Mockito.mock(Constraints.class);
        when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put("testCol4", valueSet)
                .build());

        when(constraints.getLimit()).thenReturn(5L);

        String expectedSql = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\" FROM \"testSchema\".\"testTable\"  WHERE (\"testCol4\" = ?) AND id > 100000 and id <= 300000";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        when(this.connection.prepareStatement(eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        verify(preparedStatement, Mockito.times(1)).setString(1, "varcharTest");
    }

    @Test
    public void buildSplitSqlWithPartition()
            throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol4", Types.MinorType.VARBINARY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        when(split.getProperties()).thenReturn(com.google.common.collect.ImmutableMap.of("PARTITION_BOUNDARY_FROM", "0", "PARTITION_NUMBER", "1", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", "100000"));
        when(split.getProperty(eq("PARTITION_BOUNDARY_FROM"))).thenReturn("0");
        when(split.getProperty(eq("PARTITION_NUMBER"))).thenReturn("1");
        when(split.getProperty(eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        when(split.getProperty(eq("PARTITION_BOUNDARY_TO"))).thenReturn("100000");

        Constraints constraints = Mockito.mock(Constraints.class);
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        when(split.getProperties()).thenReturn(com.google.common.collect.ImmutableMap.of("PARTITION_BOUNDARY_FROM", " ", "PARTITION_NUMBER", "1", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", "100000"));
        when(split.getProperty(eq("PARTITION_BOUNDARY_FROM"))).thenReturn(" ");
        when(split.getProperty(eq("PARTITION_NUMBER"))).thenReturn("1");
        when(split.getProperty(eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        when(split.getProperty(eq("PARTITION_BOUNDARY_TO"))).thenReturn("100000");
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        when(split.getProperties()).thenReturn(com.google.common.collect.ImmutableMap.of("PARTITION_BOUNDARY_FROM", "300000", "PARTITION_NUMBER", "2", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", " "));
        when(split.getProperty(eq("PARTITION_BOUNDARY_FROM"))).thenReturn("300000");
        when(split.getProperty(eq("PARTITION_NUMBER"))).thenReturn("1");
        when(split.getProperty(eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        when(split.getProperty(eq("PARTITION_BOUNDARY_TO"))).thenReturn(" ");
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        when(split.getProperties()).thenReturn(com.google.common.collect.ImmutableMap.of("PARTITION_BOUNDARY_FROM", " ", "PARTITION_NUMBER", "2", "PARTITION_COLUMN", "testCol1", "PARTITION_BOUNDARY_TO", " "));
        when(split.getProperty(eq("PARTITION_BOUNDARY_FROM"))).thenReturn(" ");
        when(split.getProperty(eq("PARTITION_NUMBER"))).thenReturn("1");
        when(split.getProperty(eq("PARTITION_COLUMN"))).thenReturn("testCol1");
        when(split.getProperty(eq("PARTITION_BOUNDARY_TO"))).thenReturn(" ");
        this.synapseRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
    }

    @Test
    public void testReadWithConstraint()
    {
        try {
            TableName tableName = new TableName("testSchema", "testTable");

            Schema schema = SchemaBuilder.newBuilder()
                    .addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build())
                    .addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build())
                    .addField(FieldBuilder.newBuilder("created_at", Types.MinorType.DATEMILLI.getType()).build())
                    .build();

            Split split = Mockito.mock(Split.class);
            when(split.getProperties()).thenReturn(Collections.emptyMap());

            // Setup mock result set with actual test data
            ResultSet resultSet = Mockito.mock(ResultSet.class);
            when(resultSet.next()).thenReturn(true, true, false); // Return true twice for two rows, then false
            when(resultSet.getInt("id")).thenReturn(123, 124);
            when(resultSet.getString("name")).thenReturn("test1", "test2");
            when(resultSet.getTimestamp("created_at")).thenReturn(new Timestamp(System.currentTimeMillis()));

            PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
            when(connection.prepareStatement(Mockito.anyString())).thenReturn(preparedStatement);
            when(preparedStatement.executeQuery()).thenReturn(resultSet);

            DatabaseMetaData metaData = Mockito.mock(DatabaseMetaData.class);
            when(connection.getMetaData()).thenReturn(metaData);
            when(metaData.getURL()).thenReturn("jdbc:sqlserver://test.sql.azuresynapse.net:1433;databaseName=testdb;");

            FederatedIdentity identity = mock(FederatedIdentity.class);
            ReadRecordsRequest request = new ReadRecordsRequest(
                    identity,
                    "testCatalog",
                    "queryId",
                    tableName,
                    schema,
                    split,
                    new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 1000, Collections.emptyMap()),
                    0,
                    0
            );

            BlockSpiller spiller = Mockito.mock(BlockSpiller.class);
            QueryStatusChecker queryStatusChecker = mock(QueryStatusChecker.class);
            when(queryStatusChecker.isQueryRunning()).thenReturn(true);

            // Execute the test
            synapseRecordHandler.readWithConstraint(spiller, request, queryStatusChecker);

            // Verify that writeRows was called twice (once for each row)
            verify(spiller, Mockito.times(2)).writeRows(Mockito.any());
            verify(resultSet, Mockito.times(3)).next(); // Called 3 times (2 true, 1 false)
        }
        catch (Exception e) {
            fail("Unexpected exception in testReadWithConstraint: " + e.getMessage());
        }
    }
}
