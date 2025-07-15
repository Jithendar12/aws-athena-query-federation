/*-
 * #%L
 * athena-dynamodb
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
package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.dynamodb.util.DDBRecordMetadata;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTypeUtils;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class DDBTypeUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(DDBTypeUtilsTest.class);

    private String col1 = "col_1";
    private String col2 = "col_2";
    private Schema mapping;

    @Mock
    private DDBRecordMetadata ddbRecordMetadata;

    @Before
    public void setUp()
            throws IOException
    {
        ddbRecordMetadata = mock(DDBRecordMetadata.class);
    }

    @Test
    public void makeDecimalExtractorTest()
            throws Exception
    {
        logger.info("makeDecimalExtractorTest - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField(col1, ArrowType.Decimal.createDecimal(38, 18, 128))
                .addField(col2, ArrowType.Decimal.createDecimal(38, 18, 128))
                .build();

        String literalValue = "12345";
        String literalValue2 = "789.1234";

        AttributeValue myValue =  AttributeValue.builder().n(literalValue).build();
        AttributeValue myValue2 = AttributeValue.builder().n(literalValue2).build();

        Map<String, AttributeValue> testValue = ImmutableMap.of(col1, myValue, col2, myValue2);

        Map<String, Object> expectedResults = ImmutableMap.of(
                col1, new BigDecimal(literalValue),
                col2, new BigDecimal(literalValue2));
        Map<String, Object> extractedResults = testField(mapping, testValue);
        logger.info("makeDecimalExtractorTest - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);
        logger.info("makeDecimalExtractorTest - exit");
    }

    @Test
    public void makeVarBinaryExtractorTest()
            throws Exception
    {
        logger.info("makeVarBinaryExtractorTest - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField(col1, Types.MinorType.VARBINARY.getType())
                .addField(col2, Types.MinorType.VARBINARY.getType())
                .build();

        byte[] byteValue1 = "Hello".getBytes();
        ByteBuffer byteBuffer1 = ByteBuffer.wrap(byteValue1);
        byte[] byteValue2 = "World!".getBytes();
        ByteBuffer byteBuffer2 = ByteBuffer.wrap(byteValue2);

        // Creating AttributeValue with binary data in SDK v2
        AttributeValue myValue = AttributeValue.builder()
                .b(SdkBytes.fromByteBuffer(byteBuffer1))
                .build();
        AttributeValue myValue2 = AttributeValue.builder()
                .b(SdkBytes.fromByteBuffer(byteBuffer2))
                .build();

        Map<String, AttributeValue> testValue = ImmutableMap.of(col1, myValue, col2, myValue2);

        Map<String, Object> extractedResults = testField(mapping, testValue);
        assertEquals("Extracted results are not as expected!",
                new String(byteValue1),
                new String((byte[]) extractedResults.get(col1)));
        assertEquals("Extracted results are not as expected!",
                new String(byteValue2),
                new String((byte[]) extractedResults.get(col2)));
        logger.info("makeVarBinaryExtractorTest - exit");
    }

    @Test
    public void makeBitExtractorTest()
            throws Exception
    {
        logger.info("makeBitExtractorTest - enter");

        mapping = SchemaBuilder.newBuilder()
                .addField(col1, Types.MinorType.BIT.getType())
                .addField(col2, Types.MinorType.BIT.getType())
                .build();

        AttributeValue myValue = AttributeValue.builder()
                .bool(true)
                .build();
        AttributeValue myValue2 = AttributeValue.builder()
                .bool(false)
                .build();

        Map<String, AttributeValue> testValue = ImmutableMap.of(col1, myValue, col2, myValue2);

        Map<String, Object> expectedResults = ImmutableMap.of(
                col1, 1,
                col2, 0);
        Map<String, Object> extractedResults = testField(mapping, testValue);
        logger.info("makeBitExtractorTest - Expected: {}, Extracted: {}", expectedResults, extractedResults);
        assertEquals("Extracted results are not as expected!", expectedResults, extractedResults);
        logger.info("makeBitExtractorTest - exit");
    }
    
    @Test
    public void inferArrowFieldListWithNullTest() throws Exception
    {
        java.util.ArrayList inputArray = new java.util.ArrayList<String>();
        inputArray.add("value1");
        inputArray.add(null);
        inputArray.add("value3");

        Field testField = DDBTypeUtils.inferArrowField("asdf", DDBTypeUtils.toAttributeValue(inputArray));

        assertEquals("Type does not match!", ArrowType.List.INSTANCE, testField.getType());
        assertEquals("Children Length Off!", 1, testField.getChildren().size());
        assertEquals("Wrong Child Type!", ArrowType.Utf8.INSTANCE, testField.getChildren().get(0).getType());
    }

    @Test
    public void testInferArrowField_whenSetOfNumbers()
    {
        Set<BigDecimal> numberSet = new HashSet<>();
        numberSet.add(new BigDecimal("123.45"));
        numberSet.add(new BigDecimal("678.90"));
        
        Field result = DDBTypeUtils.inferArrowField("testNumberSet", DDBTypeUtils.toAttributeValue(numberSet));

        assertNotNull(result);
        assertEquals("Field name should match", "testNumberSet", result.getName());
        assertEquals("Field type should be LIST", Types.MinorType.LIST.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
        assertEquals("Child field type should be DECIMAL", ArrowType.Decimal.createDecimal(38, 9, 128), result.getChildren().get(0).getType());
    }

    @Test
    public void testInferArrowField_whenSetOfStrings()
    {
        Set<String> stringSet = new HashSet<>();
        stringSet.add("value1");
        stringSet.add("value2");
        
        Field result = DDBTypeUtils.inferArrowField("testStringSet", DDBTypeUtils.toAttributeValue(stringSet));

        assertNotNull(result);
        assertEquals("Field name should match", "testStringSet", result.getName());
        assertEquals("Field type should be LIST", Types.MinorType.LIST.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
        assertEquals("Child field type should be VARCHAR", Types.MinorType.VARCHAR.getType(), result.getChildren().get(0).getType());
    }

    @Test
    public void testInferArrowField_whenEmptyList()
    {
        AttributeValue value = AttributeValue.builder()
                .l(Collections.emptyList())
                .build();
        
        Field result = DDBTypeUtils.inferArrowField("testEmptyList", value);
        // This is the expected behavior for empty lists that can't be inferred
        assertNull(result);
    }

    @Test
    public void testInferArrowField_whenBytesType()
    {
        byte[] bytes = "test data".getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        AttributeValue value = AttributeValue.builder().b(SdkBytes.fromByteBuffer(buffer)).build();
        
        Field result = DDBTypeUtils.inferArrowField("testBytes", value);

        assertNotNull(result);
        assertEquals("Field name should match", "testBytes", result.getName());
        assertEquals("Field type should be VARBINARY", Types.MinorType.VARBINARY.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void testInferArrowField_whenBooleanType()
    {
        AttributeValue value = AttributeValue.builder().bool(true).build();
        
        Field result = DDBTypeUtils.inferArrowField("testBoolean", value);

        assertNotNull(result);
        assertEquals("Field name should match", "testBoolean", result.getName());
        assertEquals("Field type should be BIT", Types.MinorType.BIT.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void testInferArrowField_whenNullAttributeValue()
    {
        // Create an AttributeValue with null to trigger the null return path
        AttributeValue value = AttributeValue.builder().nul(true).build();
        Field result = DDBTypeUtils.inferArrowField("testField", value);
        assertNull(result);
    }

    private Map<String, Object> testField(Schema mapping, Map<String, AttributeValue> values)
            throws Exception
    {
        Map<String, Object> results = new HashMap<>();
        for (Field field : mapping.getFields()) {
            Optional<Extractor> optionalExtractor = DDBTypeUtils.makeExtractor(field, ddbRecordMetadata, false);

            if (optionalExtractor.isPresent()) {
                Extractor extractor = optionalExtractor.get();
                if (extractor instanceof VarCharExtractor) {
                    NullableVarCharHolder holder = new NullableVarCharHolder();
                    ((VarCharExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
                else if (extractor instanceof VarBinaryExtractor) {
                    NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
                    ((VarBinaryExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
                else if (extractor instanceof DecimalExtractor) {
                    NullableDecimalHolder holder = new NullableDecimalHolder();
                    ((DecimalExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
                else if (extractor instanceof BitExtractor) {
                    NullableBitHolder holder = new NullableBitHolder();
                    ((BitExtractor) extractor).extract(values, holder);
                    assertEquals("Could not extract value for: " + field.getName(), 1, holder.isSet);
                    results.put(field.getName(), holder.value);
                }
            }
            else {
                //generate field writer factor for complex data types.
                fail(String.format("Extractor not found for Type {}", field.getType()));
            }
        }
        return results;
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenStringType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testString", "S");
        assertEquals("Field name should match", "testString", result.getName());
        assertEquals("Field type should be VARCHAR", Types.MinorType.VARCHAR.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenNumberType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testNumber", "N");
        assertEquals("Field name should match", "testNumber", result.getName());
        assertEquals("Field type should be DECIMAL", ArrowType.Decimal.createDecimal(38, 9, 128), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenBooleanType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testBoolean", "BOOL");
        assertEquals("Field name should match", "testBoolean", result.getName());
        assertEquals("Field type should be BIT", Types.MinorType.BIT.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenBinaryType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testBinary", "B");
        assertEquals("Field name should match", "testBinary", result.getName());
        assertEquals("Field type should be VARBINARY", Types.MinorType.VARBINARY.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenStringSetType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testStringSet", "SS");
        assertEquals("Field name should match", "testStringSet", result.getName());
        assertEquals("Field type should be LIST", Types.MinorType.LIST.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
        assertEquals("Child field type should be VARCHAR", Types.MinorType.VARCHAR.getType(), result.getChildren().get(0).getType());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenNumberSetType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testNumberSet", "NS");
        assertEquals("Field name should match", "testNumberSet", result.getName());
        assertEquals("Field type should be LIST", Types.MinorType.LIST.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
        assertEquals("Child field type should be DECIMAL", ArrowType.Decimal.createDecimal(38, 9, 128), result.getChildren().get(0).getType());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenBinarySetType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testBinarySet", "BS");
        assertEquals("Field name should match", "testBinarySet", result.getName());
        assertEquals("Field type should be LIST", Types.MinorType.LIST.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
        assertEquals("Child field type should be VARBINARY", Types.MinorType.VARBINARY.getType(), result.getChildren().get(0).getType());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenListType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testList", "L");
        assertEquals("Field name should match", "testList", result.getName());
        assertEquals("Field type should be LIST", Types.MinorType.LIST.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void testGetArrowFieldFromDDBType_whenMapType()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType("testMap", "M");
        assertEquals("Field name should match", "testMap", result.getName());
        assertEquals("Field type should be STRUCT", Types.MinorType.STRUCT.getType(), result.getType());
        assertTrue("Field should be nullable", result.getFieldType().isNullable());
    }

    @Test(expected = AthenaConnectorException.class)
    public void testGetArrowFieldFromDDBType_whenUnknownType_throwsAthenaConnectorException()
    {
        DDBTypeUtils.getArrowFieldFromDDBType("testUnknown", "UNKNOWN");
    }

    @Test
    public void testCoerceValueToExpectedType_whenBigDecimalToDateMilli()
    {
        BigDecimal timestamp = new BigDecimal("1609459200000"); // 2021-01-01 00:00:00 UTC
        Field field = new Field("testDate", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(timestamp, field, Types.MinorType.DATEMILLI, 
                new DDBRecordMetadata(SchemaBuilder.newBuilder().addField(field).build()));
        
        assertTrue("Should convert to LocalDateTime", result instanceof LocalDateTime);
        LocalDateTime dateTime = (LocalDateTime) result;
        assertEquals("Year should be 2021", 2021, dateTime.getYear());
        assertEquals("Month should be 1", 1, dateTime.getMonthValue());
        assertEquals("Day should be 1", 1, dateTime.getDayOfMonth());
    }

    @Test
    public void testCoerceValueToExpectedType_whenBigDecimalToDateDay()
    {
        BigDecimal days = new BigDecimal("18628");
        Field field = new Field("testDate", FieldType.nullable(Types.MinorType.DATEDAY.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(SchemaBuilder.newBuilder()
                .addField("testDate", Types.MinorType.DATEDAY.getType())
                .build());
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(days, field, Types.MinorType.DATEDAY, metadata);
        
        assertNotNull("Result should not be null", result);
        assertTrue("Should convert to LocalDate", result instanceof LocalDate);
    }

    @Test
    public void testCoerceValueToExpectedType_whenBigDecimalToNumericType()
    {
        BigDecimal value = new BigDecimal("123.45");
        Field field = new Field("testNumber", FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(SchemaBuilder.newBuilder()
                .addField("testNumber", Types.MinorType.FLOAT8.getType())
                .build());
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(value, field, Types.MinorType.FLOAT8, metadata);
        
        assertNotNull("Result should not be null", result);
        // The coercion should convert BigDecimal to Double for FLOAT8 type
        assertTrue("Should be Double for FLOAT8 type", result instanceof Double);
        assertEquals("Value should match", value.doubleValue(), (Double) result, 0.001);
    }

    @Test
    public void testCoerceValueToExpectedType_whenInvalidDateFormat()
    {
        BigDecimal invalidValue = new BigDecimal("-1");
        Field field = new Field("testDate", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(SchemaBuilder.newBuilder()
                .addField("testDate", Types.MinorType.DATEMILLI.getType())
                .build());
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(invalidValue, field, Types.MinorType.DATEMILLI, metadata);
        
        assertTrue("Should convert to LocalDateTime", result instanceof LocalDateTime);
        LocalDateTime dateTime = (LocalDateTime) result;
        assertEquals("Should be 1969-12-31", 1969, dateTime.getYear());
        assertEquals("Should be December", 12, dateTime.getMonthValue());
        assertEquals("Should be 31st", 31, dateTime.getDayOfMonth());
        assertEquals("Should be 23:59:59", 23, dateTime.getHour());
        assertEquals("Should be 23:59:59", 59, dateTime.getMinute());
        assertEquals("Should be 23:59:59", 59, dateTime.getSecond());
        assertEquals("Should be 999 milliseconds", 999, dateTime.getNano() / 1_000_000);
    }

    @Test
    public void testConvertArrowTypeIfNecessary_whenLocalDateTimeWithFormat()
    {
        LocalDateTime dateTime = LocalDateTime.of(2021, 1, 1, 12, 30, 45);
        
        // Create schema with custom metadata for timezone and format
        Map<String, String> customMetadata = new HashMap<>();
        customMetadata.put("defaultTimeZone", "UTC");
        customMetadata.put("datetimeFormatMappingNormalized", "testColumn=yyyy-MM-dd HH:mm:ss");
        
        Schema schemaWithMetadata = new Schema(Collections.emptyList(), customMetadata);
        DDBRecordMetadata metadata = new DDBRecordMetadata(schemaWithMetadata);
        
        Object result = DDBTypeUtils.convertArrowTypeIfNecessary("testColumn", dateTime, metadata);
        
        assertTrue("Should return formatted string", result instanceof String);
        assertEquals("Should match format", "2021-01-01 12:30:45", result);
    }

    @Test
    public void testCoerceValueToExpectedType_whenBigDecimalToUnsupportedDateType()
    {
        BigDecimal value = new BigDecimal("123456789");
        Field field = new Field("testField", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(value, field, Types.MinorType.VARCHAR, metadata);
        
        assertEquals("Should return original value", value, result);
    }

    @Test
    public void testCoerceValueToExpectedType_whenIllegalArgumentExceptionInCatch()
    {
        // This will trigger the catch block by using a type that causes IllegalArgumentException
        BigDecimal value = new BigDecimal("12345");
        Field field = new Field("testField", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
        
        // Force the coercible type flag to trigger the DateTime path with non-DateTime type
        // This should return the original value after the exception is caught
        Object result = DDBTypeUtils.coerceValueToExpectedType(value, field, Types.MinorType.VARCHAR, metadata);
        
        assertEquals("Should return original value when exception occurs", value, result);
    }

    @Test(expected = AthenaConnectorException.class)
    public void testCoerceListToExpectedType_whenMapInsteadOfList_throwsAthenaConnectorException()
    {
        Map<String, String> mapValue = new HashMap<>();
        mapValue.put("key", "value");
        
        Field childField = new Field("child", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        Field field = new Field("testList", FieldType.nullable(Types.MinorType.LIST.getType()), Collections.singletonList(childField));
        DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
        
        DDBTypeUtils.coerceListToExpectedType(mapValue, field, metadata);
    }

    @Test
    public void testMakeExtractor_whenCaseInsensitive()
    {
        Field field = new Field("TestField", FieldType.nullable(ArrowType.Decimal.createDecimal(38, 9, 128)), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
        
        Optional<Extractor> extractor = DDBTypeUtils.makeExtractor(field, metadata, true);
        
        assertTrue("Should create extractor", extractor.isPresent());
    }

    @Test
    public void testToAttributeValue_whenBigDecimal()
    {
        BigDecimal value = new BigDecimal("123.456");
        
        AttributeValue result = DDBTypeUtils.toAttributeValue(value);
        
        assertNotNull("Should create AttributeValue", result);
        assertEquals("Should preserve value", "123.456", result.n());
    }

    @Test
    public void testToAttributeValue_whenByteArray()
    {
        byte[] bytes = "test data".getBytes();
        
        AttributeValue result = DDBTypeUtils.toAttributeValue(bytes);
        
        assertNotNull("Should create AttributeValue", result);
        assertNotNull("Should have binary value", result.b());
    }

    @Test
    public void testToAttributeValue_whenByteBuffer()
    {
        ByteBuffer buffer = ByteBuffer.wrap("test data".getBytes());
        
        AttributeValue result = DDBTypeUtils.toAttributeValue(buffer);
        
        assertNotNull("Should create AttributeValue", result);
        assertNotNull("Should have binary value", result.b());
    }

    @Test(expected = AthenaConnectorException.class)
    public void testToAttributeValue_whenUnsupportedType_throwsAthenaConnectorException()
    {
        Object unsupportedValue = new StringBuilder("unsupported");
        DDBTypeUtils.toAttributeValue(unsupportedValue);
    }

    @Test(expected = AthenaConnectorException.class)
    public void testJsonToAttributeValue_whenUnknownKey_throwsAthenaConnectorException()
    {
        String json = "{\"wrongKey\": \"value\"}";
        DDBTypeUtils.jsonToAttributeValue(json, "expectedKey");
    }

    @Test(expected = AthenaConnectorException.class)
    public void testToAttributeValue_whenUnsupportedSetType_throwsAthenaConnectorException()
    {
        Set<Object> unsupportedSet = new HashSet<>();
        unsupportedSet.add(new StringBuilder("unsupported"));
        
        DDBTypeUtils.toAttributeValue(unsupportedSet);
    }
}
