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
package com.amazonaws.athena.connectors.elasticsearch.qpt;

import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * This class is used to test the ElasticsearchQueryPassthrough class.
 */
public class ElasticsearchQueryPassthroughTest
{
    private static final String EXPECTED_SCHEMA_NAME = "system";
    private static final String EXPECTED_FUNCTION_NAME = "query";
    private static final String SCHEMA_ARG = "SCHEMA";
    private static final String INDEX_ARG = "INDEX";
    private static final String QUERY_ARG = "QUERY";

    private ElasticsearchQueryPassthrough queryPassthrough;

    @Test
    public void getFunctionSchema_returnsSystemSchema()
    {
        queryPassthrough = new ElasticsearchQueryPassthrough();
        String schema = queryPassthrough.getFunctionSchema();

        assertEquals("Schema should be 'system'", EXPECTED_SCHEMA_NAME, schema);
    }

    @Test
    public void getFunctionName_returnsQueryName()
    {
        queryPassthrough = new ElasticsearchQueryPassthrough();
        String functionName = queryPassthrough.getFunctionName();

        assertEquals("Function name should be 'query'", EXPECTED_FUNCTION_NAME, functionName);
    }

    @Test
    public void getFunctionArguments_returnsExpectedArguments()
    {
        queryPassthrough = new ElasticsearchQueryPassthrough();
        List<String> arguments = queryPassthrough.getFunctionArguments();

        assertNotNull("Arguments should not be null", arguments);
        assertEquals("Should have 3 arguments", 3, arguments.size());
        assertEquals("First argument should be SCHEMA", SCHEMA_ARG, arguments.get(0));
        assertEquals("Second argument should be INDEX", INDEX_ARG, arguments.get(1));
        assertEquals("Third argument should be QUERY", QUERY_ARG, arguments.get(2));
    }

    @Test
    public void getLogger_returnsLogger()
    {
        queryPassthrough = new ElasticsearchQueryPassthrough();
        Logger logger = queryPassthrough.getLogger();

        assertNotNull("Logger should not be null", logger);
    }
}
