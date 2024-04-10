/*-
 * #%L
 * athena-snowflake
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

package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.SdkClientException;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.SupportedTypes;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.functions.StandardFunctions;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.ComplexExpressionPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcArrowTypeConverter;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.amazonaws.athena.connectors.jdbc.manager.PreparedStatementBuilder;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;


/**
 * Handles metadata for Snowflake. User must have access to `schemata`, `tables`, `columns` in
 * information_schema.
 */
public class SnowflakeMetadataHandler extends JdbcMetadataHandler
{
    static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");
    static final String BLOCK_PARTITION_COLUMN_NAME = "partition";
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeMetadataHandler.class);
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String EXPORT_BUCKET_KEY = "export_bucket";
    private static final String EXTERNAL_STAGE_NAME = "externalS3Stage";
    private static final String EMPTY_STRING = StringUtils.EMPTY;
    SnowflakeQueryStringBuilder snowflakeQueryStringBuilder = new SnowflakeQueryStringBuilder(SNOWFLAKE_QUOTE_CHARACTER, new SnowflakeFederationExpressionParser(SNOWFLAKE_QUOTE_CHARACTER));

    private static final String CASE_UPPER = "upper";
    private static final String CASE_LOWER = "lower";

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link SnowflakeMuxCompositeHandler} instead.
     */
    public SnowflakeMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(SnowflakeConstants.SNOWFLAKE_NAME, configOptions), configOptions);
    }

    /**
     * Used by Mux.
     */
    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        this(databaseConnectionConfig, new GenericJdbcConnectionFactory(databaseConnectionConfig,
                JDBC_PROPERTIES, new DatabaseConnectionInfo(SnowflakeConstants.SNOWFLAKE_DRIVER_CLASS,
                SnowflakeConstants.SNOWFLAKE_DEFAULT_PORT)), configOptions);
    }

    @VisibleForTesting
    protected SnowflakeMetadataHandler(
        DatabaseConnectionConfig databaseConnectionConfig,
        AWSSecretsManager secretsManager,
        AmazonAthena athena,
        JdbcConnectionFactory jdbcConnectionFactory,
        java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, secretsManager, athena, jdbcConnectionFactory, configOptions);
    }

    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request)
    {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();

        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_COMPLEX_EXPRESSION_PUSHDOWN.withSupportedSubTypes(
                ComplexExpressionPushdownSubType.SUPPORTED_FUNCTION_EXPRESSION_TYPES
                        .withSubTypeProperties(Arrays.stream(StandardFunctions.values())
                                .map(standardFunctions -> standardFunctions.getFunctionName().getFunctionName())
                                .toArray(String[]::new))
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));

        jdbcQueryPassthrough.addQueryPassthroughCapabilityIfEnabled(capabilities, configOptions);
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    public SnowflakeMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, JdbcConnectionFactory jdbcConnectionFactory, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, jdbcConnectionFactory, configOptions);
    }

    @Override
    public Schema getPartitionSchema(final String catalogName)
    {
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType());
        return schemaBuilder.build();
    }

    /**
     * Here we inject the additional column to hold the Prepared SQL Statement.
     *
     * @param partitionSchemaBuilder The SchemaBuilder you can use to add additional columns and metadata to the
     * partitions response.
     * @param request The GetTableLayoutResquest that triggered this call.
     */
    @Override
    public void enhancePartitionSchema(SchemaBuilder partitionSchemaBuilder, GetTableLayoutRequest request)
    {
        LOGGER.info("{}: Catalog {}, table {}", request.getQueryId(), request.getTableName().getSchemaName(), request.getTableName());
        partitionSchemaBuilder.addField("copySql", new ArrowType.Utf8());
        partitionSchemaBuilder.addField("queryId", new ArrowType.Utf8());
        partitionSchemaBuilder.addField("createStageSql", new ArrowType.Utf8());
    }

    /**
     * Used to get the partitions that must be read from the request table in order to satisfy the requested predicate.
     * Here generating the SQL from the request and attaching it as a additional column
     *
     * @param blockWriter Used to write rows (partitions) into the Apache Arrow response.
     * @param request Provides details of the catalog, database, and table being queried as well as any filter predicate.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception
    {
        LOGGER.info("in getPartitions: ", request);
        AWSCredentials credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
        String catalog = request.getCatalogName();
        Schema schemaName = request.getSchema();
        TableName tableName = request.getTableName();
        Constraints constraints  = request.getConstraints();
        LOGGER.info("catlogName, schemaName, tableName: ", request.getCatalogName(), tableName.getSchemaName(), tableName.getTableName());
        //get the bucket where export results wll be uploaded
        String s3ExportBucket = getS3ExportBucket();
        //Appending a random int to the query id to support multiple federated queries within a single query

        String randomStr = UUID.randomUUID().toString();
        String queryID = request.getQueryId().replace("-", "").concat(randomStr);

        //Build the SQL query
        Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
        String createStageSql = "CREATE OR REPLACE STAGE ATHENA_DEV.ATHENA.AFQS3STAGE" +
        " URL = 's3://" + s3ExportBucket + "/" + queryID + "' " +
                "CREDENTIALS = (AWS_KEY_ID = 'xxxx' , " +
                "AWS_SECRET_KEY = 'yyyy') " +
                "FILE_FORMAT = (TYPE = 'PARQUET')";

        String generatedSql = snowflakeQueryStringBuilder.buildSqlString(connection, catalog, tableName.getSchemaName(), tableName.getTableName(), schemaName, constraints, null);
        String copySqlBuilder = "COPY INTO @ATHENA_DEV.ATHENA.AFQS3STAGE" + " FROM (" + generatedSql + " ) overwrite = true FILE_FORMAT = (" + "TYPE = 'PARQUET'," + " COMPRESSION = 'NONE') HEADER = TRUE MAX_FILE_SIZE = 15000000 ";

        LOGGER.info("Snowflake CreateStageSql Statement: {}", createStageSql);
        LOGGER.info("Snowflake Copy Statement: {}", copySqlBuilder);
        LOGGER.info("queryID: {}", queryID);


        // write the prepared SQL statement to the partition column created in enhancePartitionSchema
        blockWriter.writeRows((Block block, int rowNum) -> {
            boolean matched;
            matched = block.setValue("copySql", rowNum, copySqlBuilder);
            matched &= block.setValue("queryId", rowNum, queryID);
            matched &= block.setValue("createStageSql", rowNum, createStageSql);
            //If all fields matches then we wrote 1 row during this call so we return 1
            return matched ? 1 : 0;
        });

    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        Set<Split> splits = new HashSet<>();
        String exportBucket = getS3ExportBucket();
        String queryId = request.getQueryId().replace("-", "");

        // Get the SQL statement which was created in getPartitions
        FieldReader fieldReaderPS = request.getPartitions().getFieldReader("copySql");
        String copySql = fieldReaderPS.readText().toString();
        String catalogName = request.getCatalogName();

        FieldReader fieldReaderQid = request.getPartitions().getFieldReader("queryId");
        String queryID = fieldReaderQid.readText().toString();

        FieldReader fieldReaderAwsRegion = request.getPartitions().getFieldReader("createStageSql");
        String createStageSql = fieldReaderAwsRegion.readText().toString();

        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement preparedStatement = new PreparedStatementBuilder()
                     .withConnection(connection)
                     .withQuery(createStageSql)
                     .withParameters(Arrays.asList(request.getTableName().getSchemaName() + "." +
                             request.getTableName().getTableName()))
                     .build();
             PreparedStatement preparedStatement2 = new PreparedStatementBuilder()
                     .withConnection(connection)
                     .withQuery(copySql)
                     .withParameters(Arrays.asList(request.getTableName().getSchemaName() + "." +
                             request.getTableName().getTableName()))
                     .build()) {

            preparedStatement.executeUpdate();
            Thread.sleep(1000);
            preparedStatement2.execute();
            String recentSfQueryId = null;
            try (PreparedStatement preparedStatement3 = connection.prepareStatement("SELECT LAST_QUERY_ID()")) {
                try (ResultSet resultSet = preparedStatement3.executeQuery()) {
                    if (resultSet.next()) {
                        recentSfQueryId = resultSet.getString(1);
                    }
                }
            }
            if (recentSfQueryId != null) {
                int maxSleep = 3600000; // sleep maximum of 10 minutes for unload data from snowflake to s3

                // Wait until the query is finished
                while (maxSleep > 0) {
                    Thread.sleep(5000); // Wait for 5 seconds

                    // Check the status of the query using its ID
                    String queryStatus = getQueryStatus(connection, recentSfQueryId);
                    if (queryStatus.equalsIgnoreCase("success")) {
                        break;
                    }
                    maxSleep -= 5000;
                }
            }

            /*
             * For each generated S3 object, create a split and add data to the split.
             */
            List<S3ObjectSummary> s3ObjectSummaries = getlistExportedObjects(exportBucket, queryId);

            if (!s3ObjectSummaries.isEmpty()) {
                for (S3ObjectSummary objectSummary : s3ObjectSummaries) {
                    Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                            .add("query_id", queryID)
                            .add("exportBucket", exportBucket)
                            .add("s3ObjectKey", objectSummary.getKey())
                            .build();
                    splits.add(split);
                }
                LOGGER.info("doGetSplits: exit - ", splits.size());
                return new GetSplitsResponse(catalogName, splits);
            }
            else {
                // No records were exported by Snowflake for the issued query, creating an "empty" split
                LOGGER.info("No records were exported by Snowflake");
                Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                        .add("query_id", queryID)
                        .add("exportBucket", exportBucket)
                        .add("s3ObjectKey", EMPTY_STRING)
                        .build();
                splits.add(split);
                LOGGER.info("doGetSplits: exit - ", splits.size());
                return new GetSplitsResponse(catalogName, split);
            }

        }
        catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getQueryStatus(Connection connection, String queryId) throws SQLException
    {
        String queryStatus = null;
        String query = "SELECT EXECUTION_STATUS FROM table(information_schema.query_history()) WHERE query_id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, queryId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    queryStatus = resultSet.getString(1);
                }
            }
        }
        return queryStatus;
    }


    /*
     * Get the list of all the exported S3 objects
     */
    private List<S3ObjectSummary> getlistExportedObjects(String s3ExportBucket, String queryId)
    {
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        ObjectListing objectListing;
        try {
            objectListing = amazonS3.listObjects(new ListObjectsRequest().withBucketName(s3ExportBucket).withPrefix(queryId));
        }
        catch (SdkClientException e) {
            throw new RuntimeException("Exception listing the exported objects : " + e.getMessage(), e);
        }
        return objectListing.getObjectSummaries();
    }

    @Override
    public GetTableResponse doGetTable(final BlockAllocator blockAllocator, final GetTableRequest getTableRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            Schema partitionSchema = getPartitionSchema(getTableRequest.getCatalogName());
            TableName tableName = getTableFromMetadata(connection.getCatalog(), getTableRequest.getTableName(), connection.getMetaData());
            GetTableResponse getTableResponse = new GetTableResponse(getTableRequest.getCatalogName(), tableName, getSchema(connection, tableName, partitionSchema),
                    partitionSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet()));
            return getTableResponse;
        }
    }

    /**
     *
     * @param jdbcConnection
     * @param tableName
     * @param partitionSchema
     * @return
     * @throws Exception
     */
    public Schema getSchema(Connection jdbcConnection, TableName tableName, Schema partitionSchema)
            throws Exception
    {
        /**
         * query to fetch column data type to handle appropriate datatype to arrowtype conversions.
         */
        String dataTypeQuery = "select COLUMN_NAME, DATA_TYPE from \"INFORMATION_SCHEMA\".\"COLUMNS\" WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
        SchemaBuilder schemaBuilder = SchemaBuilder.newBuilder();

        try (ResultSet resultSet = getColumns(jdbcConnection.getCatalog(), tableName, jdbcConnection.getMetaData());
             Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider());
             PreparedStatement stmt = connection.prepareStatement(dataTypeQuery)) {
            stmt.setString(1, tableName.getSchemaName().toUpperCase());
            stmt.setString(2, tableName.getTableName().toUpperCase());

            HashMap<String, String> hashMap = new HashMap<String, String>();
            ResultSet dataTypeResultSet = stmt.executeQuery();

            String type = "";
            String name = "";

            while (dataTypeResultSet.next()) {
                type = dataTypeResultSet.getString("DATA_TYPE");
                name = dataTypeResultSet.getString(COLUMN_NAME);
                hashMap.put(name.trim(), type.trim());
            }
            if (hashMap.isEmpty() == true) {
                LOGGER.debug("No data type  available for TABLE in hashmap : " + tableName.getTableName());
            }
            boolean found = false;
            while (resultSet.next()) {
                ArrowType columnType = JdbcArrowTypeConverter.toArrowType(
                        resultSet.getInt("DATA_TYPE"),
                        resultSet.getInt("COLUMN_SIZE"),
                        resultSet.getInt("DECIMAL_DIGITS"),
                        configOptions);
                String columnName = resultSet.getString(COLUMN_NAME);
                String dataType = hashMap.get(columnName);
                LOGGER.debug("columnName: " + columnName);
                LOGGER.debug("dataType: " + dataType);
                final Map<String, ArrowType> stringArrowTypeMap = com.google.common.collect.ImmutableMap.of(
                    "INTEGER", (ArrowType) Types.MinorType.INT.getType(),
                    "DATE", (ArrowType) Types.MinorType.DATEDAY.getType(),
                    "TIMESTAMP", (ArrowType) Types.MinorType.DATEMILLI.getType(),
                    "TIMESTAMP_LTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
                    "TIMESTAMP_NTZ", (ArrowType) Types.MinorType.DATEMILLI.getType(),
                    "TIMESTAMP_TZ", (ArrowType) Types.MinorType.DATEMILLI.getType()
                );
                if (dataType != null && stringArrowTypeMap.containsKey(dataType.toUpperCase())) {
                    columnType = stringArrowTypeMap.get(dataType.toUpperCase());
                }
                /**
                 * converting into VARCHAR for not supported data types.
                 */
                if (columnType == null) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }
                if (columnType != null && !SupportedTypes.isSupported(columnType)) {
                    columnType = Types.MinorType.VARCHAR.getType();
                }

                if (columnType != null && SupportedTypes.isSupported(columnType)) {
                    LOGGER.debug(" AddField Schema Building...()  ");
                    schemaBuilder.addField(FieldBuilder.newBuilder(columnName, columnType).build());
                    found = true;
                }
                else {
                    LOGGER.error("getSchema: Unable to map type for column[" + columnName + "] to a supported type, attempted " + columnType);
                }
            }
            if (!found) {
                throw new RuntimeException("Could not find table in " + tableName.getSchemaName());
            }
            partitionSchema.getFields().forEach(schemaBuilder::addField);
        }
        LOGGER.debug(schemaBuilder.toString());
        return schemaBuilder.build();
    }

    /**
     *
     * @param catalogName
     * @param tableHandle
     * @param metadata
     * @return
     * @throws SQLException
     */
    private ResultSet getColumns(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                catalogName,
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    /**
     * Finding table name from query hint
     * In sap hana schemas and tables can be case sensitive, but executed query from athena sends table and schema names
     * in lower case, this has been handled by appending query hint to the table name as below
     * "lambda:lambdaname".SCHEMA_NAME."TABLE_NAME@schemacase=upper&tablecase=upper"
     * @param table
     * @return
     */
    protected  TableName findTableNameFromQueryHint(TableName table)
    {
        //if no query hints has been passed then return input table name
        if (!table.getTableName().contains("@")) {
            return new TableName(table.getSchemaName().toUpperCase(), table.getTableName().toUpperCase());
        }
        //analyze the hint to find table and schema case
        String[] tbNameWithQueryHint = table.getTableName().split("@");
        String[] hintDetails = tbNameWithQueryHint[1].split("&");
        String schemaCase = CASE_UPPER;
        String tableCase = CASE_UPPER;
        String tableName = tbNameWithQueryHint[0];
        for (String str : hintDetails) {
            String[] hintDetail = str.split("=");
            if (hintDetail[0].contains("schema")) {
                schemaCase = hintDetail[1];
            }
            else if (hintDetail[0].contains("table")) {
                tableCase = hintDetail[1];
            }
        }
        if (schemaCase.equalsIgnoreCase(CASE_UPPER) && tableCase.equalsIgnoreCase(CASE_UPPER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(CASE_LOWER) && tableCase.equalsIgnoreCase(CASE_LOWER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toLowerCase());
        }
        else if (schemaCase.equalsIgnoreCase(CASE_LOWER) && tableCase.equalsIgnoreCase(CASE_UPPER)) {
            return new TableName(table.getSchemaName().toLowerCase(), tableName.toUpperCase());
        }
        else if (schemaCase.equalsIgnoreCase(CASE_UPPER) && tableCase.equalsIgnoreCase(CASE_LOWER)) {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toLowerCase());
        }
        else {
            return new TableName(table.getSchemaName().toUpperCase(), tableName.toUpperCase());
        }
    }

    /**
     * Logic to handle case sensitivity of table name and schema name
     * @param catalogName
     * @param tableHandle
     * @param metadata
     * @return
     * @throws SQLException
     */
    protected TableName getTableFromMetadata(final String catalogName, final TableName tableHandle, final DatabaseMetaData metadata)
            throws SQLException
    {
        TableName tableName = findTableNameFromQueryHint(tableHandle);
        //check for presence exact table and schema name returned by findTableNameFromQueryHint method by invoking metadata.getTables method
        ResultSet resultSet = metadata.getTables(catalogName, tableName.getSchemaName(), tableName.getTableName(), null);
        while (resultSet.next()) {
            if (tableName.getTableName().equals(resultSet.getString(3))) {
                tableName = new TableName(tableName.getSchemaName(), resultSet.getString(3));
                return tableName;
            }
        }
        // if table not found in above step, check for presence of input table by doing pattern search
        ResultSet rs = metadata.getTables(catalogName, tableName.getSchemaName().toUpperCase(), "%", null);
        while (rs.next()) {
            if (tableName.getTableName().equalsIgnoreCase(rs.getString(3))) {
                tableName = new TableName(tableName.getSchemaName().toUpperCase(), rs.getString(3));
                return tableName;
            }
        }
        return tableName;
    }
    @Override
    public ListSchemasResponse doListSchemaNames(final BlockAllocator blockAllocator, final ListSchemasRequest listSchemasRequest)
            throws Exception
    {
        try (Connection connection = getJdbcConnectionFactory().getConnection(getCredentialProvider())) {
            LOGGER.info("{}: List schema names for Catalog {}", listSchemasRequest.getQueryId(), listSchemasRequest.getCatalogName());
            return new ListSchemasResponse(listSchemasRequest.getCatalogName(), listDatabaseNames(connection));
        }
    }
    protected static Set<String> listDatabaseNames(final Connection jdbcConnection)
            throws Exception
    {
        try (ResultSet resultSet = jdbcConnection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            String inputCatalogName = jdbcConnection.getCatalog();
            String inputSchemaName = jdbcConnection.getSchema();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                String catalogName = resultSet.getString("TABLE_CATALOG");
                // skip internal schemas
                boolean shouldAddSchema =
                        ((inputSchemaName == null) || schemaName.equals(inputSchemaName)) &&
                                (!schemaName.equals("information_schema") && catalogName.equals(inputCatalogName));

                if (shouldAddSchema) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
    }

    public String getS3ExportBucket()
    {
        return configOptions.get(EXPORT_BUCKET_KEY);
    }
}
