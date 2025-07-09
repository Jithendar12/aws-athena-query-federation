/*-
 * #%L
 * athena-docdb
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
package com.amazonaws.athena.connectors.docdb;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaUtilsTest
{

    @Test
    public void UnsupportedTypeTest()
    {
        List<Document> docs = new ArrayList<>();
        Document unsupported = new Document();
        unsupported.put("unsupported_col1", new UnsupportedType());
        docs.add(unsupported);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(1, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().stream().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("unsupported_col1").getType()));
    }

    @Test
    public void basicMergeTest()
    {
        List<String> list = new ArrayList<>();
        list.add("test");
        list.add("test");
        list.add("test");

        Document struct1 = new Document();
        struct1.put("struct_col1", 1);
        struct1.put("struct_col2", "string");
        struct1.put("struct_col3", 1.0D);

        Document struct2 = new Document();
        struct2.put("struct_col1", 1);
        struct2.put("struct_col2", "string");
        struct2.put("struct_col3", 1);
        struct2.put("struct_col4", 2.0F);

        List<Document> docs = new ArrayList<>();
        Document doc1 = new Document();
        doc1.put("col1", 1);
        doc1.put("col2", "string");
        doc1.put("col3", 1.0D);
        doc1.put("col5", list);
        doc1.put("col6", struct1);
        docs.add(doc1);

        Document doc2 = new Document();
        doc2.put("col1", 1);
        doc2.put("col2", "string");
        doc2.put("col4", 1.0F);
        doc2.put("col6", struct2);
        docs.add(doc2);

        Document doc3 = new Document();
        doc3.put("col1", 1);
        doc3.put("col2", "string");
        doc3.put("col4", 1);
        doc3.put("col5", list);
        docs.add(doc3);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(6, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().stream().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col1").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("col3").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col4").getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(fields.get("col5").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col5").getChildren().get(0).getType()));
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(fields.get("col6").getType()));
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(0).getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(1).getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(2).getType()));
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(3).getType()));
    }

    @Test
    public void emptyListTest()
    {
        List<Document> docs = new ArrayList<>();
        Document doc1 = new Document();
        List<String> emptyList = new ArrayList<>();
        doc1.put("col1", 1);
        doc1.put("col2", "string");
        doc1.put("col3", 1.0D);
        doc1.put("col4", emptyList);
        docs.add(doc1);

        Document doc2 = new Document();
        List<Integer> list2 = new ArrayList<>();
        list2.add(100);
        doc2.put("col1", 1);
        doc2.put("col2", "string");
        doc2.put("col3", 1.0D);
        doc2.put("col4", list2);
        docs.add(doc2);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(4, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().stream().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col1").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("col3").getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(fields.get("col4").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col4").getChildren().get(0).getType()));
    }

    @Test
    public void dbRefTet()
    {
        List<Document> docs = new ArrayList<>();
        Document doc1 = new Document();
        doc1.put("col1", 1);
        doc1.put("col2", new DBRef("otherColl", ObjectId.get()));
        docs.add(doc1);

        Document doc2 = new Document();
        doc2.put("col1", 1);
        doc2.put("col2", new DBRef("otherDb", "otherColl", ObjectId.get()));
        docs.add(doc2);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(2, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col1").getType()));
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(fields.get("col2").getType()));
        assertEquals("_db", fields.get("col2").getChildren().get(0).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getChildren().get(0).getType()));
        assertEquals("_ref", fields.get("col2").getChildren().get(1).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getChildren().get(1).getType()));
        assertEquals("_id", fields.get("col2").getChildren().get(2).getName());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getChildren().get(2).getType()));
    }

    @Test
    public void inferSchema_whenCollectionIsEmpty_returnsEmptySchema()
    {
        List<Document> docs = new ArrayList<>();
        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(0, schema.getFields().size());
    }

    @Test
    public void inferSchema_whenNoColumnsExist_throwsRuntimeException()
    {
        List<Document> docs = new ArrayList<>();
        // Add 100 empty documents to exceed the fieldCount threshold
        for (int i = 0; i < 100; i++) {
            docs.add(new Document());
        }

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        assertThrows(RuntimeException.class, () -> SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10));
    }

    @Test
    public void inferSchema_whenDocumentsHaveListFields_mergesListFieldsCorrectly()
    {
        List<Document> docs = new ArrayList<>();
        
        // First document with a list of strings
        Document doc1 = new Document();
        List<String> list1 = new ArrayList<>();
        list1.add("value1");
        list1.add("value2");
        doc1.put("myList", list1);
        docs.add(doc1);

        // Second document with a list of strings
        Document doc2 = new Document();
        List<String> list2 = new ArrayList<>();
        list2.add("value3");
        list2.add("value4");
        doc2.put("myList", list2);
        docs.add(doc2);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(1, schema.getFields().size());

        Field listField = schema.getFields().get(0);
        assertEquals("myList", listField.getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(listField.getType()));
        assertEquals(1, listField.getChildren().size());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(listField.getChildren().get(0).getType()));
    }

    @Test
    public void inferSchema_whenDocumentsHaveStructFields_mergesStructFieldsCorrectly()
    {
        List<Document> docs = new ArrayList<>();
        
        // First document with a struct having two fields
        Document doc1 = new Document();
        Document struct1 = new Document();
        struct1.put("field1", "value1");
        struct1.put("field2", 42);
        doc1.put("myStruct", struct1);
        docs.add(doc1);

        // Second document with a struct having overlapping and new fields
        Document doc2 = new Document();
        Document struct2 = new Document();
        struct2.put("field1", "value2");
        struct2.put("field3", 3.14);
        doc2.put("myStruct", struct2);
        docs.add(doc2);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(1, schema.getFields().size());

        Field structField = schema.getFields().get(0);
        assertEquals("myStruct", structField.getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(structField.getType()));
        assertEquals(3, structField.getChildren().size());

        Map<String, Field> fields = new HashMap<>();
        structField.getChildren().forEach(f -> fields.put(f.getName(), f));

        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("field1").getType()));
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("field2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("field3").getType()));
    }

    @Test
    public void inferSchema_whenDocumentsHaveNestedListOfStructs_mergesNestedStructuresCorrectly()
    {
        List<Document> docs = new ArrayList<>();
        
        // First document with list of documents - each document will become a STRUCT
        Document doc1 = new Document();
        List<Document> nestedList1 = new ArrayList<>();
        Document innerDoc1a = new Document();
        innerDoc1a.put("innerField", "a");
        nestedList1.add(innerDoc1a);
        doc1.put("nestedListField", nestedList1);
        docs.add(doc1);

        // Second document with list of documents with more fields
        Document doc2 = new Document();
        List<Document> nestedList2 = new ArrayList<>();
        Document innerDoc2a = new Document();
        innerDoc2a.put("innerField", "b");
        nestedList2.add(innerDoc2a);
        Document innerDoc2b = new Document();
        innerDoc2b.put("innerField2", 42);
        nestedList2.add(innerDoc2b);
        doc2.put("nestedListField", nestedList2);
        docs.add(doc2);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(1, schema.getFields().size());

        Field nestedListField = schema.getFields().get(0);
        assertEquals("nestedListField", nestedListField.getName());
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(nestedListField.getType()));
        assertEquals(1, nestedListField.getChildren().size());
        
        // The inner field is a STRUCT (Document objects are inferred as STRUCTs)
        Field innerField = nestedListField.getChildren().get(0);
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(innerField.getType()));
        
        // Verify the struct has at least one field
        assertFalse("Inner field children should not be empty", innerField.getChildren().isEmpty());
    }

    @Test
    public void inferSchema_whenDocumentsHaveStructWithListAndStructFields_mergesComplexStructuresCorrectly()
    {
        List<Document> docs = new ArrayList<>();
        
        // First document with struct containing a list and a nested struct
        Document doc1 = new Document();
        Document struct1 = new Document();
        List<String> list1 = new ArrayList<>();
        list1.add("item1");
        list1.add("item2");
        struct1.put("listField", list1);
        
        Document nestedStruct1 = new Document();
        nestedStruct1.put("nestedField1", "value1");
        struct1.put("structField", nestedStruct1);
        
        doc1.put("complexStruct", struct1);
        docs.add(doc1);

        // Second document with struct containing same list and struct fields with different values
        Document doc2 = new Document();
        Document struct2 = new Document();
        List<String> list2 = new ArrayList<>();
        list2.add("item3");
        list2.add("item4");
        struct2.put("listField", list2);
        
        Document nestedStruct2 = new Document();
        nestedStruct2.put("nestedField1", "value2");
        nestedStruct2.put("nestedField2", 42);
        struct2.put("structField", nestedStruct2);
        
        doc2.put("complexStruct", struct2);
        docs.add(doc2);

        MongoDatabase mockDatabase = setupMockDatabase(docs);

        Schema schema = SchemaUtils.inferSchema(mockDatabase, new TableName("test", "test"), 10);
        assertEquals(1, schema.getFields().size());

        Field complexStructField = schema.getFields().get(0);
        assertEquals("complexStruct", complexStructField.getName());
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(complexStructField.getType()));
        assertEquals(2, complexStructField.getChildren().size());

        Map<String, Field> fields = new HashMap<>();
        complexStructField.getChildren().forEach(f -> fields.put(f.getName(), f));

        // Verify listField was merged correctly
        Field listField = fields.get("listField");
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(listField.getType()));
        assertEquals(1, listField.getChildren().size());
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(listField.getChildren().get(0).getType()));

        // Verify structField was merged correctly
        Field structField = fields.get("structField");
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(structField.getType()));
        assertEquals(2, structField.getChildren().size());

        Map<String, Field> nestedFields = new HashMap<>();
        structField.getChildren().forEach(f -> nestedFields.put(f.getName(), f));
        
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(nestedFields.get("nestedField1").getType()));
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(nestedFields.get("nestedField2").getType()));
    }

    private MongoDatabase setupMockDatabase(List<Document> docs)
    {
        MongoDatabase mockDatabase = mock(MongoDatabase.class);
        MongoCollection mockCollection = mock(MongoCollection.class);
        FindIterable mockIterable = mock(FindIterable.class);
        when(mockDatabase.getCollection(any())).thenReturn(mockCollection);
        when(mockCollection.find()).thenReturn(mockIterable);
        when(mockIterable.limit(anyInt())).thenReturn(mockIterable);
        when(mockIterable.maxScan(anyInt())).thenReturn(mockIterable);
        when(mockIterable.batchSize(anyInt())).thenReturn(mockIterable);
        when(mockIterable.iterator()).thenReturn(new StubbingCursor(docs.iterator()));
        return mockDatabase;
    }
}
