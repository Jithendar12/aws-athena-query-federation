/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryUtilsTest
{
    private final BlockAllocatorImpl allocator = new BlockAllocatorImpl();

    @Test
    public void testMakePredicateWithSortedRangeSet()
    {
        Field field = new Field("year", FieldType.nullable(new ArrowType.Int(32, true)), null);

        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.INT.getType(),
                ImmutableList.of(
                        Range.lessThan(allocator, Types.MinorType.INT.getType(), 1950),
                        Range.equal(allocator, Types.MinorType.INT.getType(), 1952),
                        Range.range(allocator, Types.MinorType.INT.getType(), 1955, false, 1972, true),
                        Range.greaterThanOrEqual(allocator, Types.MinorType.INT.getType(), 2010)),
                false
        );
        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);
        Document expected = new Document("$or", ImmutableList.of(
                new Document("year", new Document("$lt", 1950)),
                new Document("year", new Document("$gt", 1955).append("$lte", 1972)),
                new Document("year", new Document("$gte", 2010)),
                new Document("year", new Document("$eq", 1952))
        ));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void testMakePredicateWithId()
    {
        Field field = new Field("_id", FieldType.nullable(new ArrowType.Utf8()), null);

        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "4ecbe7f9e8c1c9092c000027")),
                false
        );
        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);
        Document expected =
                new Document("_id", new Document("$eq", new ObjectId("4ecbe7f9e8c1c9092c000027")));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void testMakePredicate_withEquatableValueSet()
    {
        Field field = new Field("category", FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a single value EquatableValueSet with nulls not allowed
        EquatableValueSet.Builder builder = EquatableValueSet.newBuilder(
                allocator, Types.MinorType.VARCHAR.getType(), false, false);
        builder.add("books");
        ValueSet equatableSet = builder.build();

        Document result = QueryUtils.makePredicate(field, equatableSet);
        assertNotNull(result);

        Document expected = new Document("category", new Document("$eq", "books"));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void testMakePredicate_withEquatableValueSetNumeric()
    {
        Field field = new Field("price", FieldType.nullable(new ArrowType.Int(32, true)), null);

        // Create a single value EquatableValueSet with nulls not allowed
        EquatableValueSet.Builder builder = EquatableValueSet.newBuilder(
                allocator, Types.MinorType.INT.getType(), false, false);
        builder.add(100);
        ValueSet equatableSet = builder.build();

        Document result = QueryUtils.makePredicate(field, equatableSet);
        assertNotNull(result);

        Document expected = new Document("price", new Document("$eq", 100));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void testMakePredicate_withMultipleObjectIds()
    {
        Field field = new Field("_id", FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a SortedRangeSet with multiple single values to trigger the IN operator case
        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "4ecbe7f9e8c1c9092c000027"),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "4ecbe7f9e8c1c9092c000028")),
                false
        );

        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);

        // The implementation should convert strings to ObjectIds and use $in operator
        Document expected = new Document("_id", 
            new Document("$in", Arrays.asList(
                new ObjectId("4ecbe7f9e8c1c9092c000027"),
                new ObjectId("4ecbe7f9e8c1c9092c000028")
            ))
        );
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void testMakePredicate_withMultipleValues()
    {
        Field field = new Field("category", FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a SortedRangeSet with multiple single values to trigger the IN operator case
        ValueSet rangeSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "books"),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "electronics"),
                        Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "clothing")),
                false
        );

        Document result = QueryUtils.makePredicate(field, rangeSet);
        assertNotNull(result);

        // For non-_id fields, the implementation should use $in with the original values
        Document expected = new Document("category", 
            new Document("$in", Arrays.asList("books", "electronics", "clothing"))
        );
        
        // Compare the documents directly instead of their JSON strings
        Document categoryDoc = (Document) result.get("category");
        Document expectedCategoryDoc = (Document) expected.get("category");
        
        assertEquals("$in", expectedCategoryDoc.keySet().iterator().next());
        assertEquals("$in", categoryDoc.keySet().iterator().next());
        
        // Compare arrays ignoring order
        assertEquals(
            new java.util.HashSet<>((java.util.List<?>) expectedCategoryDoc.get("$in")),
            new java.util.HashSet<>((java.util.List<?>) categoryDoc.get("$in"))
        );
    }

    @Test
    public void testMakePredicate_withNoneConstraint()
    {
        Field field = new Field("category", FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a ValueSet that represents no values (isNone)
        ValueSet noneSet = SortedRangeSet.none(Types.MinorType.VARCHAR.getType());

        Document result = QueryUtils.makePredicate(field, noneSet);
        assertNotNull(result);

        // Should generate a predicate that matches null values
        // Using both $exists and $eq for more precise null handling in MongoDB
        Document expected = new Document("category", 
            new Document("$exists", true)
            .append("$eq", null));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void testMakePredicate_withAllConstraint()
    {
        Field field = new Field("category", FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a ValueSet that represents all values (isAll)
        ValueSet allSet = SortedRangeSet.notNull(allocator, Types.MinorType.VARCHAR.getType());

        Document result = QueryUtils.makePredicate(field, allSet);
        assertNotNull(result);

        // Should generate a predicate that matches non-null values
        Document expected = new Document("category", new Document("$ne", null));
        assertEquals(expected.toJson(), result.toJson());
    }

    @Test
    public void testMakePredicate_withNullAllowed()
    {
        Field field = new Field("category", FieldType.nullable(new ArrowType.Utf8()), null);

        // Create a ValueSet that allows null values
        ValueSet nullAllowedSet = SortedRangeSet.copyOf(
                Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), "books")),
                true
        );

        Document result = QueryUtils.makePredicate(field, nullAllowedSet);
        assertNull(result);
    }

    @Test
    public void testParseFilter()
    {
        String jsonFilter = "{ \"field\": { \"$eq\": \"value\" } }";

        Document result = QueryUtils.parseFilter(jsonFilter);
        assertNotNull(result);
        assertEquals("value", ((Document) result.get("field")).get("$eq"));
    }

    @Test
    public void testParseFilterInvalidJson()
    {
        String invalidJsonFilter = "{ field: { $eq: value } }";

        assertThrows(IllegalArgumentException.class, () -> {
            QueryUtils.parseFilter(invalidJsonFilter);
        });
    }
}
