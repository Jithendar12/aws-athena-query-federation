/*-
 * #%L
 * athena-db2
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
package com.amazonaws.athena.connectors.db2;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import static com.amazonaws.athena.connectors.db2.Db2Constants.PARTITION_NUMBER;
import static com.amazonaws.athena.connectors.db2.Db2Constants.QUOTE_CHARACTER;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;

public class Db2RecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String TEST_COL1 = "testCol1";
    private static final String TEST_PARTITION = "partition_name";
    private static final String TEST_PARTITION_VALUE = "partition_value";

    private Db2RecordHandler db2RecordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;
    private JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private S3Client amazonS3;
    private SecretsManagerClient secretsManager;
    private AthenaClient athena;

    @Before
    public void setup() throws Exception
    {
        System.setProperty("aws.region", "us-east-1");
        this.amazonS3 = Mockito.mock(S3Client.class);
        this.secretsManager = Mockito.mock(SecretsManagerClient.class);
        this.athena = Mockito.mock(AthenaClient.class);
        this.connection = Mockito.mock(Connection.class);
        this.jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(this.jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(this.connection);
        jdbcSplitQueryBuilder = new Db2QueryStringBuilder(QUOTE_CHARACTER, new Db2FederationExpressionParser(QUOTE_CHARACTER));
        final DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog", Db2Constants.NAME,
                "dbtwo://jdbc:db2://hostname/fakedatabase:${testsecret}");
        this.db2RecordHandler = new Db2RecordHandler(databaseConnectionConfig, amazonS3, secretsManager, athena, jdbcConnectionFactory, jdbcSplitQueryBuilder, com.google.common.collect.ImmutableMap.of());
    }

    private ValueSet getSingleValueSet(Object value)
    {
        Range range = Mockito.mock(Range.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(range.isSingleValue()).thenReturn(true);
        Mockito.when(range.getLow().getValue()).thenReturn(value);
        ValueSet valueSet = Mockito.mock(SortedRangeSet.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(valueSet.getRanges().getOrderedRanges()).thenReturn(Collections.singletonList(range));
        return valueSet;
    }

    @Test
    public void buildSplitSqlNew()
            throws SQLException
    {
        final String testCol4 = "testCol4";

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
        schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol2", Types.MinorType.DATEDAY.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder("testCol3", Types.MinorType.DATEMILLI.getType()).build());
        schemaBuilder.addField(FieldBuilder.newBuilder(testCol4, Types.MinorType.VARCHAR.getType()).build());
        Schema schema = schemaBuilder.build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("0");

        ValueSet valueSet = getSingleValueSet("varcharTest");
        Constraints constraints = Mockito.mock(Constraints.class);
        Mockito.when(constraints.getSummary()).thenReturn(new ImmutableMap.Builder<String, ValueSet>()
                .put(testCol4, valueSet)
                .build());

        String expectedSql = String.format("SELECT \"%s\", \"testCol2\", \"testCol3\", \"%s\" FROM \"%s\".\"%s\"  WHERE (\"%s\" = ?)", 
                TEST_COL1, testCol4, TEST_SCHEMA, TEST_TABLE, testCol4);
        PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(this.connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedPreparedStatement);
        PreparedStatement preparedStatement = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);
        Assert.assertEquals(expectedPreparedStatement, preparedStatement);
        Mockito.verify(preparedStatement, Mockito.times(1)).setString(1, "varcharTest");
    }

    @Test
    public void testBuildSplitSql_withQueryPassthrough()
    {
        try {
            TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);

            SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();
            schemaBuilder.addField(FieldBuilder.newBuilder(TEST_COL1, Types.MinorType.INT.getType()).build());
            schemaBuilder.addField(FieldBuilder.newBuilder(TEST_PARTITION, Types.MinorType.VARCHAR.getType()).build());
            Schema schema = schemaBuilder.build();

            Split split = Mockito.mock(Split.class);
            Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap(TEST_PARTITION, TEST_PARTITION_VALUE));
            Mockito.when(split.getProperty(Mockito.eq(TEST_PARTITION))).thenReturn(TEST_PARTITION_VALUE);

            Constraints constraints = Mockito.mock(Constraints.class);
            Mockito.when(constraints.isQueryPassThrough()).thenReturn(true);

            String testQuery = String.format("SELECT * FROM %s.%s WHERE %s = 1", TEST_SCHEMA, TEST_TABLE, TEST_COL1);
            Map<String, String> queryPassthroughArgs = new ImmutableMap.Builder<String, String>()
                    .put(JdbcQueryPassthrough.QUERY, testQuery)
                    .put("schemaFunctionName", "system.query")
                    .put("enableQueryPassthrough", "true")
                    .put("name", "query")
                    .put("schema", "system")
                    .build();

            Mockito.when(constraints.getQueryPassthroughArguments()).thenReturn(queryPassthroughArgs);

            PreparedStatement expectedPreparedStatement = Mockito.mock(PreparedStatement.class);
            Mockito.when(this.connection.prepareStatement(nullable(String.class))).thenReturn(expectedPreparedStatement);

            PreparedStatement preparedStatement = this.db2RecordHandler.buildSplitSql(this.connection, TEST_CATALOG, tableName, schema, constraints, split);

            Assert.assertEquals(expectedPreparedStatement, preparedStatement);
            Mockito.verify(this.connection).prepareStatement(testQuery);
        }
        catch (Exception e) {
            fail("Unexpected exception:" + e.getMessage());
        }
    }
}
