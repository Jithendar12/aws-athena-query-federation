/*-
 * #%L
 * athena-jdbc
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

package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.ENABLE_QUERY_PASSTHROUGH;
import static com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough.QUERY;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.PARTITION_NUMBER;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;
import static org.mockito.ArgumentMatchers.nullable;

public class SqlServerRecordHandlerTest
{
    private SqlServerRecordHandler sqlServerRecordHandler;
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
        System.setProperty("aws.region", "us-east-1");
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER ,new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", SqlServerConstants.NAME,
                "sqlserver://jdbc:sqlserver://hostname;databaseName=fakedatabase");

        this.sqlServerRecordHandler = new SqlServerRecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    @Test
    public void buildSplitSqlNew()
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
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)).thenReturn("pf");
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN)).thenReturn("testCol1");
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("1");

        ValueSet valueSet = createSingleValueSet("varcharTest");
        Constraints constraints = new Constraints(
                ImmutableMap.of("testCol4", valueSet),
                Collections.emptyList(),
                Collections.emptyList(),
                5L,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"testCol1\", \"testCol2\", \"testCol3\", \"testCol4\" FROM \"testSchema\".\"testTable\"  WHERE (\"testCol4\" = ?) AND  $PARTITION.pf(testCol1) = 1";
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "varcharTest");
    }

    @Test
    public void buildSplitSql_ComplexMixedPredicates_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("age", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("department", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("is_active", Types.MinorType.BIT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("amount", Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Create complex constraints with multiple predicate types
        ValueSet idSet = createMultiValueSet(1, 2, 3, 4, 5);
        ValueSet ageSet = createRangeSet(Marker.Bound.EXACTLY, 25, Marker.Bound.EXACTLY, 65);
        ValueSet salarySet = createRangeSet(Marker.Bound.ABOVE, 50000.0, Marker.Bound.BELOW, Double.MAX_VALUE);
        ValueSet deptSet = createSingleValueSet("IT");
        ValueSet activeSet = createSingleValueSet(true);
        ValueSet amountSet = createSingleValueSet(1234.56);

        Constraints constraints = new Constraints(
                ImmutableMap.of(
                        "id", idSet,
                        "age", ageSet,
                        "salary", salarySet,
                        "department", deptSet,
                        "is_active", activeSet,
                        "amount", amountSet
                ),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"name\", \"age\", \"salary\", \"department\", \"is_active\", \"amount\" FROM \"testSchema\".\"testTable\"  WHERE (\"id\" IN (?,?,?,?,?)) AND ((\"age\" >= ? AND \"age\" <= ?)) AND ((\"salary\" > ? AND \"salary\" < ?)) AND (\"department\" = ?) AND (\"is_active\" = ?) AND (\"amount\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(2, 2);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(3, 3);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(4, 4);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(5, 5);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(6, 25);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(7, 65);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(8, 50000.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(9, Double.MAX_VALUE);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(10, "IT");
        Mockito.verify(preparedStatement, Mockito.times(1)).setBoolean(11, true);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(12, 1234.56);
    }

    @Test
    public void buildSplitSql_RangePredicates_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("price", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("quantity", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("discount", Types.MinorType.FLOAT4.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Complex range predicates with valid bounds
        ValueSet priceSet = createRangeSet(Marker.Bound.ABOVE, 100.0, Marker.Bound.EXACTLY, 1000.0);
        ValueSet quantitySet = createRangeSet(Marker.Bound.EXACTLY, 10, Marker.Bound.BELOW, 100);
        ValueSet discountSet = createRangeSet(Marker.Bound.ABOVE, 0.05f, Marker.Bound.BELOW, 0.5f);

        Constraints constraints = new Constraints(
                ImmutableMap.of(
                        "price", priceSet,
                        "quantity", quantitySet,
                        "discount", discountSet
                ),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"price\", \"quantity\", \"discount\" FROM \"testSchema\".\"testTable\"  WHERE ((\"price\" > ? AND \"price\" <= ?)) AND ((\"quantity\" >= ? AND \"quantity\" < ?)) AND ((\"discount\" > ? AND \"discount\" < ?))";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameter setting for range constraints
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(1, 100.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setDouble(2, 1000.0);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(3, 10);
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(4, 100);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(5, 0.05f);
        Mockito.verify(preparedStatement, Mockito.times(1)).setFloat(6, 0.5f);
    }

    @Test
    public void buildSplitSql_OrderBy_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", Types.MinorType.FLOAT8.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        OrderByField orderByField = new OrderByField("salary", OrderByField.Direction.DESC_NULLS_LAST);
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                ImmutableList.of(orderByField),
                10, // limit
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\", \"name\", \"salary\" FROM \"testSchema\".\"testTable\"  ORDER BY \"salary\" DESC NULLS LAST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_MultipleOrderByFields_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("department", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("salary", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Multiple ORDER BY fields with different directions
        OrderByField orderByField1 = new OrderByField("department", OrderByField.Direction.ASC_NULLS_LAST);
        OrderByField orderByField2 = new OrderByField("salary", OrderByField.Direction.DESC_NULLS_FIRST);
        OrderByField orderByField3 = new OrderByField("name", OrderByField.Direction.ASC_NULLS_LAST);

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                ImmutableList.of(orderByField1, orderByField2, orderByField3),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"department\", \"salary\", \"name\" FROM \"testSchema\".\"testTable\"  ORDER BY \"department\" ASC NULLS LAST, \"salary\" DESC NULLS FIRST, \"name\" ASC NULLS LAST";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_EmptyConstraints_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Empty constraints
        Constraints constraints = createEmptyConstraint();

        String expectedSql = "SELECT \"id\" FROM \"testSchema\".\"testTable\" ";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_SingleValueInClause_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Single value should use equality, not IN clause
        ValueSet singleValueSet = createSingleValueSet(1);
        Constraints constraints = new Constraints(
                ImmutableMap.of("id", singleValueSet),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\" FROM \"testSchema\".\"testTable\"  WHERE (\"id\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);

        // Verify parameter setting for single value
        Mockito.verify(preparedStatement, Mockito.times(1)).setInt(1, 1);
    }

    @Test
    public void buildSplitSql_LargeInClause_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Large IN clause with many values
        ValueSet largeInSet = createMultiValueSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        Constraints constraints = new Constraints(
                ImmutableMap.of("id", largeInSet),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );

        String expectedSql = "SELECT \"id\" FROM \"testSchema\".\"testTable\"  WHERE (\"id\" IN (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?))";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        for (int i = 1; i <= 15; i++) {
            Mockito.verify(preparedStatement, Mockito.times(1)).setInt(i, i);
        }
    }

    @Test(expected = SQLException.class)
    public void buildSplitSql_InvalidConnection_ThrowsSQLException() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build())
                .build();
        Split split = createTestSplit();
        Constraints constraints = createEmptyConstraint();
        Connection invalidConnection = Mockito.mock(Connection.class);
        Mockito.when(invalidConnection.prepareStatement(Mockito.anyString())).thenThrow(new SQLException("Connection error"));

        this.sqlServerRecordHandler.buildSplitSql(invalidConnection, "testCatalog", tableName, schema, constraints, split);
    }
    
    @Test
    public void buildSplitSql_LimitConstraint_IgnoresLimit() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("id", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("name", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = createTestSplit();

        // Test LIMIT constraint (should be ignored by SQL Server)
        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                100L, // limit
                Collections.emptyMap(),
                null
        );

        // SQL Server doesn't support LIMIT, so it should be ignored
        String expectedSql = "SELECT \"id\", \"name\" FROM \"testSchema\".\"testTable\" ";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
    }

    @Test
    public void buildSplitSql_QueryPassthrough_ReturnsPassthroughQuery() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol1", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("partition", Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");

        String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", "testSchema", "testTable", "testCol1");
        Map<String, String> queryPassthroughArgs = new com.google.common.collect.ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(QUERY, testQuery)
                .put(SCHEMA_FUNCTION_NAME, "system.query")
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", SqlServerConstants.NAME)
                .put("schema", "testSchema")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(testQuery);
        PreparedStatement preparedStatement = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);

        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(this.connection).prepareStatement(testQuery);
    }

    @Test
    public void buildSplitSql_PassthroughDisabled_ReturnsPreparedStatement() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema schema = createTestSchema();
        Split split = createTestSplit();

        ValueSet intValueSet = createSingleValueSet(123);
        ValueSet varcharValueSet = createSingleValueSet("abc");

        Constraints constraints = new Constraints(
                com.google.common.collect.ImmutableMap.of("intCol", intValueSet, "varcharCol", varcharValueSet),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null);

        Assert.assertFalse("Expected isQueryPassThrough to return false", constraints.isQueryPassThrough());

        String expectedSql = "SELECT \"intCol\", \"varcharCol\", \"bigintCol\", \"floatCol\", \"doubleCol\", \"dateCol\", \"timestampCol\", \"boolCol\" FROM \"testSchema\".\"testTable\"  WHERE (\"intCol\" = ?) AND (\"varcharCol\" = ?)";
        PreparedStatement expectedPreparedStatement = createMockPreparedStatement(expectedSql);

        PreparedStatement result = this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, result);
        Mockito.verify(result, Mockito.times(1)).setInt(1, 123);
        Mockito.verify(result, Mockito.times(1)).setString(2, "abc");
        Mockito.verify(this.connection).prepareStatement(expectedSql);
    }

    @Test
    public void buildSplitSql_PassthroughEnabledMissingQuery_ThrowsException() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema schema = createTestSchema();
        Split split = createTestSplit();

        Map<String, String> queryPassthroughArgs = new com.google.common.collect.ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(SCHEMA_FUNCTION_NAME, "system.query")
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", SqlServerConstants.NAME)
                .put("schema", "testSchema")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        Assert.assertTrue("Expected isQueryPassThrough to return true", constraints.isQueryPassThrough());

        try {
            this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
            Assert.fail("Expected exception to be thrown");
        }
        catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Missing Query Passthrough Argument"));
        }
    }

    @Test
    public void buildSplitSql_PassthroughWrongSchema_ThrowsException() throws SQLException
    {
        TableName tableName = new TableName("testSchema", "testTable");
        Schema schema = createTestSchema();
        Split split = createTestSplit();

        String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", "testSchema", "testTable", "testCol1");
        Map<String, String> queryPassthroughArgs = new com.google.common.collect.ImmutableMap.Builder<@NotNull String, @NotNull String>()
                .put(QUERY, testQuery)
                .put(SCHEMA_FUNCTION_NAME, "wrong.function")
                .put(ENABLE_QUERY_PASSTHROUGH, "true")
                .put("name", SqlServerConstants.NAME)
                .put("schema", "testSchema")
                .build();

        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, queryPassthroughArgs, null);

        Assert.assertTrue("Expected isQueryPassThrough to return true", constraints.isQueryPassThrough());

        try {
            this.sqlServerRecordHandler.buildSplitSql(this.connection, "testCatalogName", tableName, schema, constraints, split);
            Assert.fail("Expected exception to be thrown");
        }
        catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Function Signature doesn't match implementation's"));
        }
    }

    private Schema createTestSchema()
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder("intCol", Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("varcharCol", Types.MinorType.VARCHAR.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("bigintCol", Types.MinorType.BIGINT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("floatCol", Types.MinorType.FLOAT4.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("doubleCol", Types.MinorType.FLOAT8.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("dateCol", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("timestampCol", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("boolCol", Types.MinorType.BIT.getType()).build());
        return schemaBuilder.build();
    }
    
    private ValueSet createSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
    
    private ValueSet createMultiValueSet(Object... values)
    {
        java.util.List<Range> ranges = new java.util.ArrayList<>();
        for (Object value : values) {
            Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(range.isSingleValue()).thenReturn(true);
            Mockito.when(range.getLow().getValue()).thenReturn(value);
            ranges.add(range);
        }
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(ranges);
        return valueSet;
    }
    
    private ValueSet createRangeSet(Marker.Bound lowerBound, Object lowerValue, Marker.Bound upperBound, Object upperValue)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(false);
        Mockito.when(range.getLow().getValue()).thenReturn(lowerValue);
        Mockito.when(range.getHigh().getValue()).thenReturn(upperValue);
        Mockito.when(range.getLow().getBound()).thenReturn(lowerBound);
        Mockito.when(range.getHigh().getBound()).thenReturn(upperBound);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }
    
    private Split createTestSplit()
    {
        Split split = Mockito.mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(PARTITION_NUMBER, "0");
        Mockito.when(split.getProperties()).thenReturn(properties);
        Mockito.when(split.getProperty(Mockito.eq(PARTITION_NUMBER))).thenReturn("0");
        Mockito.when(split.getProperty(Mockito.anyString())).thenReturn("0");
        return split;
    }
    
    private Constraints createEmptyConstraint()
    {
        return new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT,
                Collections.emptyMap(),
                null
        );
    }
    
    private PreparedStatement createMockPreparedStatement(String expectedSql) throws SQLException
    {
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql)))
                .thenReturn(preparedStatement);
        Mockito.when(preparedStatement.getConnection()).thenReturn(this.connection);
        return preparedStatement;
    }
}
