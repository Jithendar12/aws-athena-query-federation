/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.mongodb.DBRef;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocDBFieldResolverTest
{
    private static final String TEST_LIST_FIELD = "myList";
    private static final String TEST_STRUCT_FIELD = "myStruct";
    private static final String TEST_NESTED_FIELD = "nestedField";
    private static final String TEST_COMPLEX_LIST = "complexList";
    private static final String TEST_FIELD1 = "field1";
    private static final String TEST_ID = "123";
    private static final String TEST_DB = "testDB";
    private static final String TEST_COLLECTION = "testCollection";

    private final FieldResolver resolver = DocDBFieldResolver.DEFAULT_FIELD_RESOLVER;

    @Test
    public void testGetFieldValue_whenListField()
    {
        Field listField = new Field(TEST_LIST_FIELD, 
            FieldType.nullable(Types.MinorType.LIST.getType()),
                List.of(new Field("item",
                        FieldType.nullable(Types.MinorType.VARCHAR.getType()),
                        null)));

        Document doc = new Document();
        List<String> list = Arrays.asList("item1", "item2", "item3");
        doc.put(TEST_LIST_FIELD, list);

        Object result = resolver.getFieldValue(listField, doc);
        assertEquals(list, result);
    }

    @Test
    public void testGetFieldValue_whenSimpleDocument()
    {
        Field structField = new Field(TEST_STRUCT_FIELD, 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);

        Document outerDoc = new Document();
        outerDoc.put(TEST_STRUCT_FIELD, "value");

        Object result = resolver.getFieldValue(structField, outerDoc);
        assertEquals("value", result);
    }

    @Test
    public void testGetFieldValue_whenDBRef()
    {
        Field idField = new Field("_id", 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);
        Field dbField = new Field("_db", 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);
        Field refField = new Field("_ref", 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);

        DBRef dbRef = mock(DBRef.class);
        when(dbRef.getId()).thenReturn(TEST_ID);
        when(dbRef.getDatabaseName()).thenReturn(TEST_DB);
        when(dbRef.getCollectionName()).thenReturn(TEST_COLLECTION);

        assertEquals(TEST_ID, resolver.getFieldValue(idField, dbRef));
        assertEquals(TEST_DB, resolver.getFieldValue(dbField, dbRef));
        assertEquals(TEST_COLLECTION, resolver.getFieldValue(refField, dbRef));
    }

    @Test
    public void testGetFieldValue_whenNestedDocument()
    {
        List<Field> nestedFields = new ArrayList<>();
        nestedFields.add(new Field(TEST_NESTED_FIELD, 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null));
        Field structField = new Field(TEST_STRUCT_FIELD, 
            FieldType.nullable(Types.MinorType.STRUCT.getType()), 
            nestedFields);

        Document innerDoc = new Document();
        innerDoc.put(TEST_NESTED_FIELD, "nestedValue");
        Document outerDoc = new Document();
        outerDoc.put(TEST_STRUCT_FIELD, innerDoc);

        Object result = resolver.getFieldValue(structField, outerDoc);
        assertEquals(innerDoc, result);
    }

    @Test
    public void testGetFieldValue_whenComplexList()
    {
        List<Field> structFields = new ArrayList<>();
        structFields.add(new Field(TEST_FIELD1, 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null));
        Field itemField = new Field("item", 
            FieldType.nullable(Types.MinorType.STRUCT.getType()), 
            structFields);
        Field listField = new Field(TEST_COMPLEX_LIST, 
            FieldType.nullable(Types.MinorType.LIST.getType()),
                List.of(itemField));

        Document doc1 = new Document(TEST_FIELD1, "value1");
        Document doc2 = new Document(TEST_FIELD1, "value2");
        List<Document> list = Arrays.asList(doc1, doc2);
        
        Document mainDoc = new Document();
        mainDoc.put(TEST_COMPLEX_LIST, list);

        Object result = resolver.getFieldValue(listField, mainDoc);
        assertEquals(list, result);
    }

    @Test(expected = RuntimeException.class)
    public void testGetFieldValue_whenInvalidValue()
    {
        Field field = new Field("test", 
            FieldType.nullable(Types.MinorType.VARCHAR.getType()), 
            null);

        resolver.getFieldValue(field, 123);
    }
} 
