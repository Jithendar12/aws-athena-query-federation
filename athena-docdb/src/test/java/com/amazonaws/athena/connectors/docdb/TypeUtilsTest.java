/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TypeUtilsTest
{
    @Test
    public void unsupportedCoerce()
    {
        Object result = TypeUtils.coerce(FieldBuilder.newBuilder("unsupported", Types.MinorType.VARCHAR.getType()).build(), new UnsupportedType());
        assertEquals("UnsupportedType{}", result);
        assertTrue(result instanceof String);
    }

    @Test
    public void testCoerce_withFloat8Field()
    {
        Field float8Field = FieldBuilder.newBuilder("float8", Types.MinorType.FLOAT8.getType()).build();
        
        // Test Integer to Double conversion
        Object intResult = TypeUtils.coerce(float8Field, 42);
        assertTrue("Integer result should be Double type", intResult instanceof Double);
        assertEquals(42.0, (Double) intResult, 0.001);

        // Test Float to Double conversion
        Object floatResult = TypeUtils.coerce(float8Field, 42.5f);
        assertTrue("Float result should be Double type", floatResult instanceof Double);
        assertEquals(42.5, (Double) floatResult, 0.001);

        // Test passing Double directly
        Object doubleResult = TypeUtils.coerce(float8Field, 42.5d);
        assertTrue("Double result should be Double type", doubleResult instanceof Double);
        assertEquals(42.5, (Double) doubleResult, 0.001);
    }

    @Test
    public void testCoerce_withFloat4Field()
    {
        Field float4Field = FieldBuilder.newBuilder("float4", Types.MinorType.FLOAT4.getType()).build();
        
        // Test Integer to Float conversion
        Object intResult = TypeUtils.coerce(float4Field, 42);
        assertTrue("Integer result should be Float type", intResult instanceof Float);
        assertEquals(42.0f, (Float) intResult, 0.001);

        // Test Double to Float conversion
        Object doubleResult = TypeUtils.coerce(float4Field, 42.5d);
        assertTrue("Double result should be Float type", doubleResult instanceof Float);
        assertEquals(42.5f, (Float) doubleResult, 0.001);

        // Test passing Float directly
        Object floatResult = TypeUtils.coerce(float4Field, 42.5f);
        assertTrue("Float result should be Float type", floatResult instanceof Float);
        assertEquals(42.5f, (Float) floatResult, 0.001);
    }

    @Test
    public void testCoerce_withIntField()
    {
        Field intField = FieldBuilder.newBuilder("int", Types.MinorType.INT.getType()).build();
        
        // Test Float to Int conversion
        Object floatResult = TypeUtils.coerce(intField, 42.7f);
        assertTrue("Float result should be Integer type", floatResult instanceof Integer);
        assertEquals(42, floatResult);

        // Test Double to Int conversion
        Object doubleResult = TypeUtils.coerce(intField, 42.7d);
        assertTrue("Double result should be Integer type", doubleResult instanceof Integer);
        assertEquals(42, doubleResult);

        // Test passing Integer directly
        Object intResult = TypeUtils.coerce(intField, 42);
        assertTrue("Integer result should be Integer type", intResult instanceof Integer);
        assertEquals(42, intResult);
    }

    @Test
    public void testCoerce_withDateMilliField()
    {
        Field dateField = FieldBuilder.newBuilder("date", Types.MinorType.DATEMILLI.getType()).build();
        
        // Create a BsonTimestamp with a known time value
        int timestampSeconds = 1000;  // Use a simple timestamp value
        BsonTimestamp bsonTimestamp = new BsonTimestamp(timestampSeconds, 0);
        
        // Test BsonTimestamp to Date conversion
        Object result = TypeUtils.coerce(dateField, bsonTimestamp);
        assertTrue("BsonTimestamp result should be Date type", result instanceof Date);
        assertEquals(1000000L, ((Date) result).getTime());  // Should be 1000 seconds * 1000 milliseconds/second

        // Test passing Date directly
        Date date = new Date(1000000L);  // Same timestamp in milliseconds
        Object dateResult = TypeUtils.coerce(dateField, date);
        assertTrue("Date result should be Date type", dateResult instanceof Date);
        assertEquals(date, dateResult);
    }

    @Test
    public void testCoerce_withObjectId()
    {
        Field varcharField = FieldBuilder.newBuilder("objectId", Types.MinorType.VARCHAR.getType()).build();
        
        // Create an ObjectId with a known value
        ObjectId objectId = new ObjectId("507f1f77bcf86cd799439011");
        
        // Test ObjectId to String conversion
        Object result = TypeUtils.coerce(varcharField, objectId);
        assertTrue("ObjectId result should be String type", result instanceof String);
        assertEquals("507f1f77bcf86cd799439011", result);
        
        // Test with a different field type to ensure ObjectId is always converted to String
        Field intField = FieldBuilder.newBuilder("int", Types.MinorType.INT.getType()).build();
        Object result2 = TypeUtils.coerce(intField, objectId);
        assertTrue("ObjectId result should be String type regardless of field type", result2 instanceof String);
        assertEquals("507f1f77bcf86cd799439011", result2);
    }
}
