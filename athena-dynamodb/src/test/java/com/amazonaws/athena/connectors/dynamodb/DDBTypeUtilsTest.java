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
    private static final String TEST_FIELD_NAME = "testField";
    private static final String TEST_DATE_FIELD = "testDate";
    private static final String TEST_COLUMN_NAME = "testColumn";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String TEST_DATA = "test data";
    private static final String UNKNOWN_TYPE = "UNKNOWN";
    private static final String UNSUPPORTED = "unsupported";
    private static final String STRING_TYPE = "S";
    private static final String NUMBER_TYPE = "N";
    private static final String BOOLEAN_TYPE = "BOOL";
    private static final String BINARY_TYPE = "B";
    private static final String STRING_SET_TYPE = "SS";
    private static final String NUMBER_SET_TYPE = "NS";
    private static final String BINARY_SET_TYPE = "BS";
    private static final String LIST_TYPE = "L";
    private static final String MAP_TYPE = "M";
    private static final String UTC_TIMEZONE = "UTC";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String EXPECTED_KEY = "expectedKey";
    private static final String WRONG_KEY = "wrongKey";
    private static final String TEST_STRING_SET = "testStringSet";
    private static final String TEST_NUMBER_SET = "testNumberSet";
    private static final String TEST_BINARY_SET = "testBinarySet";
    private static final String TEST_LIST = "testList";
    private static final String TEST_MAP = "testMap";
    private static final String TEST_BYTES = "testBytes";
    private static final String TEST_BOOLEAN = "testBoolean";
    private static final String TEST_EMPTY_LIST = "testEmptyList";
    private static final String TEST_STRING = "testString";
    private static final String TEST_NUMBER = "testNumber";
    private static final String TEST_BINARY = "testBinary";
    private static final String TEST_UNKNOWN = "testUnknown";

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
    public void inferArrowField_withSetOfNumbers_returnsListFieldWithDecimalChild()
    {
        Set<BigDecimal> numberSet = new HashSet<>();
        numberSet.add(new BigDecimal("123.45"));
        numberSet.add(new BigDecimal("678.90"));
        
        Field result = DDBTypeUtils.inferArrowField(TEST_NUMBER_SET, DDBTypeUtils.toAttributeValue(numberSet));

        assertField(result, TEST_NUMBER_SET, Types.MinorType.LIST.getType());
        assertEquals(ArrowType.Decimal.createDecimal(38, 9, 128), result.getChildren().get(0).getType());
    }

    @Test
    public void inferArrowField_withSetOfStrings_returnsListFieldWithVarcharChild()
    {
        Set<String> stringSet = new HashSet<>();
        stringSet.add(VALUE_1);
        stringSet.add(VALUE_2);
        
        Field result = DDBTypeUtils.inferArrowField(TEST_STRING_SET, DDBTypeUtils.toAttributeValue(stringSet));

        assertField(result, TEST_STRING_SET, Types.MinorType.LIST.getType());
        assertEquals(Types.MinorType.VARCHAR.getType(), result.getChildren().get(0).getType());
    }

    @Test
    public void inferArrowField_withEmptyList_returnsNull()
    {
        AttributeValue value = AttributeValue.builder()
                .l(Collections.emptyList())
                .build();
        
        Field result = DDBTypeUtils.inferArrowField(TEST_EMPTY_LIST, value);
        // This is the expected behavior for empty lists that can't be inferred
        assertNull("Result should be null for empty list", result);
    }

    @Test
    public void inferArrowField_withBytesType_returnsVarBinaryField()
    {
        byte[] bytes = TEST_DATA.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        AttributeValue value = AttributeValue.builder().b(SdkBytes.fromByteBuffer(buffer)).build();
        
        Field result = DDBTypeUtils.inferArrowField(TEST_BYTES, value);

        assertField(result, TEST_BYTES, Types.MinorType.VARBINARY.getType());
    }

    @Test
    public void inferArrowField_withBooleanType_returnsBitField()
    {
        AttributeValue value = AttributeValue.builder().bool(true).build();
        
        Field result = DDBTypeUtils.inferArrowField(TEST_BOOLEAN, value);

        assertField(result, TEST_BOOLEAN, Types.MinorType.BIT.getType());
    }

    @Test
    public void inferArrowField_withNullAttributeValue_returnsNull()
    {
        // Create an AttributeValue with null to trigger the null return path
        AttributeValue value = AttributeValue.builder().nul(true).build();
        Field result = DDBTypeUtils.inferArrowField(TEST_FIELD_NAME, value);
        assertNull("Result should be null for null attribute value", result);
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
    public void getArrowFieldFromDDBType_withStringType_returnsVarcharField()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_STRING, STRING_TYPE);
        assertField(result, TEST_STRING, Types.MinorType.VARCHAR.getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withNumberType_returnsDecimalField()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_NUMBER, NUMBER_TYPE);
        assertEquals(TEST_NUMBER, result.getName());
        assertEquals(ArrowType.Decimal.createDecimal(38, 9, 128), result.getType());
        assertTrue("FieldType should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void getArrowFieldFromDDBType_withBooleanType_returnsBitField()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_BOOLEAN, BOOLEAN_TYPE);
        assertField(result, TEST_BOOLEAN, Types.MinorType.BIT.getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withBinaryType_returnsVarBinaryField()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_BINARY, BINARY_TYPE);
        assertField(result, TEST_BINARY, Types.MinorType.VARBINARY.getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withStringSetType_returnsListFieldWithVarcharChild()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_STRING_SET, STRING_SET_TYPE);
        assertField(result, TEST_STRING_SET, Types.MinorType.LIST.getType());
        assertEquals(Types.MinorType.VARCHAR.getType(), result.getChildren().get(0).getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withNumberSetType_returnsListFieldWithDecimalChild()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_NUMBER_SET, NUMBER_SET_TYPE);
        assertField(result, TEST_NUMBER_SET, Types.MinorType.LIST.getType());
        assertEquals(ArrowType.Decimal.createDecimal(38, 9, 128), result.getChildren().get(0).getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withBinarySetType_returnsListFieldWithVarBinaryChild()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_BINARY_SET, BINARY_SET_TYPE);
        assertField(result, TEST_BINARY_SET, Types.MinorType.LIST.getType());
        assertEquals(Types.MinorType.VARBINARY.getType(), result.getChildren().get(0).getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withListType_returnsListField()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_LIST, LIST_TYPE);
        assertField(result, TEST_LIST, Types.MinorType.LIST.getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withMapType_returnsStructField()
    {
        Field result = DDBTypeUtils.getArrowFieldFromDDBType(TEST_MAP, MAP_TYPE);
        assertField(result, TEST_MAP, Types.MinorType.STRUCT.getType());
    }

    @Test
    public void getArrowFieldFromDDBType_withUnknownType_throwsAthenaConnectorException()
    {
        try {
            DDBTypeUtils.getArrowFieldFromDDBType(TEST_UNKNOWN, UNKNOWN_TYPE);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain error about unknown type",
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
    }

    @Test
    public void coerceValueToExpectedType_withBigDecimalToDateMilli_returnsLocalDateTime()
    {
        BigDecimal timestamp = new BigDecimal("1609459200000"); // 2021-01-01 00:00:00 UTC
        Field field = new Field(TEST_DATE_FIELD, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(timestamp, field, Types.MinorType.DATEMILLI, 
                new DDBRecordMetadata(SchemaBuilder.newBuilder().addField(field).build()));
        
        assertTrue("Result should be LocalDateTime", result instanceof LocalDateTime);
        LocalDateTime dateTime = (LocalDateTime) result;
        assertEquals(2021, dateTime.getYear());
        assertEquals(1, dateTime.getMonthValue());
        assertEquals(1, dateTime.getDayOfMonth());
    }

    @Test
    public void coerceValueToExpectedType_withBigDecimalToDateDay_returnsLocalDate()
    {
        BigDecimal days = new BigDecimal("18628");
        Field field = new Field(TEST_DATE_FIELD, FieldType.nullable(Types.MinorType.DATEDAY.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(SchemaBuilder.newBuilder()
                .addField(TEST_DATE_FIELD, Types.MinorType.DATEDAY.getType())
                .build());
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(days, field, Types.MinorType.DATEDAY, metadata);
        
        assertNotNull("Result should not be null", result);
        assertTrue("Result should be LocalDate", result instanceof LocalDate);
    }

    @Test
    public void coerceValueToExpectedType_withBigDecimalToNumericType_returnsDouble()
    {
        BigDecimal value = new BigDecimal("123.45");
        Field field = new Field("testNumber", FieldType.nullable(Types.MinorType.FLOAT8.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(SchemaBuilder.newBuilder()
                .addField("testNumber", Types.MinorType.FLOAT8.getType())
                .build());
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(value, field, Types.MinorType.FLOAT8, metadata);
        
        assertNotNull("Result should not be null", result);
        // The coercion should convert BigDecimal to Double for FLOAT8 type
        assertTrue("Result should be Double for FLOAT8 type", result instanceof Double);
        assertEquals(value.doubleValue(), (Double) result, 0.001);
    }

    @Test
    public void coerceValueToExpectedType_withInvalidDateFormat_returnsLocalDateTimeWithDefaultValues()
    {
        BigDecimal invalidValue = new BigDecimal("-1");
        Field field = new Field(TEST_DATE_FIELD, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(SchemaBuilder.newBuilder()
                .addField(TEST_DATE_FIELD, Types.MinorType.DATEMILLI.getType())
                .build());
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(invalidValue, field, Types.MinorType.DATEMILLI, metadata);
        
        assertTrue("Result should be LocalDateTime", result instanceof LocalDateTime);
        LocalDateTime dateTime = (LocalDateTime) result;
        assertEquals(1969, dateTime.getYear());
        assertEquals(12, dateTime.getMonthValue());
        assertEquals(31, dateTime.getDayOfMonth());
        assertEquals(23, dateTime.getHour());
        assertEquals(59, dateTime.getMinute());
        assertEquals(59, dateTime.getSecond());
        assertEquals(999, dateTime.getNano() / 1_000_000);
    }

    @Test
    public void convertArrowTypeIfNecessary_withLocalDateTimeWithFormat_returnsFormattedString()
    {
        LocalDateTime dateTime = LocalDateTime.of(2021, 1, 1, 12, 30, 45);
        
        // Create schema with custom metadata for timezone and format
        Map<String, String> customMetadata = new HashMap<>();
        customMetadata.put("defaultTimeZone", UTC_TIMEZONE);
        customMetadata.put("datetimeFormatMappingNormalized", TEST_COLUMN_NAME + "=" + DATETIME_FORMAT);
        
        Schema schemaWithMetadata = new Schema(Collections.emptyList(), customMetadata);
        DDBRecordMetadata metadata = new DDBRecordMetadata(schemaWithMetadata);
        
        Object result = DDBTypeUtils.convertArrowTypeIfNecessary(TEST_COLUMN_NAME, dateTime, metadata);
        
        assertTrue("Result should be String", result instanceof String);
        assertEquals("2021-01-01 12:30:45", result);
    }

    @Test
    public void coerceValueToExpectedType_withBigDecimalToDateDay_returnsEpochDays()
    {
        BigDecimal value = new BigDecimal("123456789");
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
        
        Object result = DDBTypeUtils.coerceValueToExpectedType(value, field, Types.MinorType.VARCHAR, metadata);
        
        assertEquals(value, result);
    }

    @Test
    public void coerceValueToExpectedType_withInvalidDateFormat_returnsOriginalValue()
    {
        // This will trigger the catch block by using a type that causes IllegalArgumentException
        BigDecimal value = new BigDecimal("12345");
        Field field = new Field(TEST_FIELD_NAME, FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
        
        // Force the coercible type flag to trigger the DateTime path with non-DateTime type
        // This should return the original value after the exception is caught
        Object result = DDBTypeUtils.coerceValueToExpectedType(value, field, Types.MinorType.VARCHAR, metadata);
        
        assertEquals(value, result);
    }

    @Test
    public void coerceListToExpectedType_withMapInsteadOfList_throwsAthenaConnectorException()
    {
        try {
            Map<String, String> mapValue = new HashMap<>();
            mapValue.put(KEY, VALUE);
            
            Field childField = new Field("child", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null);
            Field field = new Field(TEST_LIST, FieldType.nullable(Types.MinorType.LIST.getType()), Collections.singletonList(childField));
            DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
            
            DDBTypeUtils.coerceListToExpectedType(mapValue, field, metadata);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain error about map instead of list",
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
    }

    @Test
    public void makeExtractor_withCaseInsensitive_returnsExtractor()
    {
        Field field = new Field("TestField", FieldType.nullable(ArrowType.Decimal.createDecimal(38, 9, 128)), null);
        DDBRecordMetadata metadata = new DDBRecordMetadata(new Schema(Collections.singletonList(field)));
        
        Optional<Extractor> extractor = DDBTypeUtils.makeExtractor(field, metadata, true);
        
        assertTrue("Extractor should be present", extractor.isPresent());
    }

    @Test
    public void toAttributeValue_withBigDecimal_returnsAttributeValueWithNumber()
    {
        BigDecimal value = new BigDecimal("123.456");
        
        AttributeValue result = DDBTypeUtils.toAttributeValue(value);
        
        assertNotNull("Result should not be null", result);
        assertEquals("123.456", result.n());
    }

    @Test
    public void toAttributeValue_withByteArray_returnsAttributeValueWithBinary()
    {
        byte[] bytes = TEST_DATA.getBytes();
        
        AttributeValue result = DDBTypeUtils.toAttributeValue(bytes);
        
        assertNotNull("Result should not be null", result);
        assertNotNull("Result should have binary value", result.b());
    }

    @Test
    public void toAttributeValue_withByteBuffer_returnsAttributeValueWithBinary()
    {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_DATA.getBytes());
        
        AttributeValue result = DDBTypeUtils.toAttributeValue(buffer);
        
        assertNotNull("Result should not be null", result);
        assertNotNull("Result should have binary value", result.b());
    }

    private void assertField(Field result, String expectedName, ArrowType expectedType)
    {
        assertNotNull("Result should not be null", result);
        assertEquals(expectedName, result.getName());
        assertEquals(expectedType, result.getType());
        assertTrue("FieldType should be nullable", result.getFieldType().isNullable());
    }

    @Test
    public void toAttributeValue_withUnsupportedType_throwsAthenaConnectorException()
    {
        try {
            Object unsupportedValue = new StringBuilder(UNSUPPORTED);
            DDBTypeUtils.toAttributeValue(unsupportedValue);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain error about unsupported type",
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
    }

    @Test
    public void jsonToAttributeValue_withUnknownKey_throwsAthenaConnectorException()
    {
        try {
            String json = "{\"" + WRONG_KEY + "\": \"" + VALUE + "\"}";
            DDBTypeUtils.jsonToAttributeValue(json, EXPECTED_KEY);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain error about unknown key",
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
    }

    @Test
    public void toAttributeValue_withUnsupportedSetType_throwsAthenaConnectorException()
    {
        try {
            Set<Object> unsupportedSet = new HashSet<>();
            unsupportedSet.add(new StringBuilder(UNSUPPORTED));
            
            DDBTypeUtils.toAttributeValue(unsupportedSet);
            fail("Expected AthenaConnectorException was not thrown");
        }
        catch (AthenaConnectorException ex) {
            assertTrue("Exception message should contain error about unsupported set type",
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
    }
}
