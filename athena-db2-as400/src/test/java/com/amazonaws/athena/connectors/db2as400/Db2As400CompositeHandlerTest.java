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
package com.amazonaws.athena.connectors.db2as400;

import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JDBCUtil.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*","org.w3c.*","javax.net.ssl.*","sun.security.*","jdk.internal.reflect.*"})
public class Db2As400CompositeHandlerTest
{
    private Db2As400CompositeHandler db2As400CompositeHandler;
    private static final Logger logger = LoggerFactory.getLogger(Db2As400CompositeHandler.class);

    static {
        System.setProperty("aws.region", "us-east-1");
    }

    @Test
    public void Db2CompositeHandlerTest() {
        DatabaseConnectionConfig databaseConnectionConfig = new DatabaseConnectionConfig("testCatalog1", Db2As400Constants.NAME,
                "dbtwo_as400://jdbc:as400://samplehost:prompt=false;user=dummy;password=dummy;");
        PowerMockito.mockStatic(JDBCUtil.class);
        PowerMockito.when(JDBCUtil.getSingleDatabaseConfigFromEnv(Db2As400Constants.NAME, System.getenv())).thenReturn(databaseConnectionConfig);
        db2As400CompositeHandler = new Db2As400CompositeHandler();
        logger.info("Db2CompositeHandler: {}", db2As400CompositeHandler);
        Assert.assertTrue(db2As400CompositeHandler instanceof Db2As400CompositeHandler);
    }
}
