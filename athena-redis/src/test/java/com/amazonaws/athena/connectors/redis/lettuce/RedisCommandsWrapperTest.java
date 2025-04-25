/*-
 * #%L
 * athena-redis
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
package com.amazonaws.athena.connectors.redis.lettuce;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.Range;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.ScoredValueScanCursor;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RedisCommandsWrapperTest
{
    private static final String TEST_KEY = "key";
    private static final String TEST_VALUE = "value";
    private static final String RESPONSE_OK = "OK";
    @Mock
    private RedisCommands<String, String> standaloneCommands;

    @Mock
    private RedisAdvancedClusterCommands<String, String> clusterCommands;

    @Mock
    private KeyScanCursor<String> keyScanCursor;

    @Mock
    private ScanCursor scanCursor;

    @Mock
    private ScanArgs scanArgs;

    @Mock
    private ScoredValueScanCursor<String> scoredValueScanCursor;

    private RedisCommandsWrapper<String, String> standaloneWrapper;
    private RedisCommandsWrapper<String, String> clusterWrapper;

    @Before
    public void setUp()
    {
        standaloneWrapper = new RedisCommandsWrapper<>(standaloneCommands, null, false);
        clusterWrapper = new RedisCommandsWrapper<>(null, clusterCommands, true);
    }

    @Test
    public void testScan_withStandaloneConnection_returnsKeyScanCursor()
    {
        when(standaloneCommands.scan(scanCursor, scanArgs)).thenReturn(keyScanCursor);

        KeyScanCursor<String> result = standaloneWrapper.scan(scanCursor, scanArgs);

        assertEquals(keyScanCursor, result);
        verify(standaloneCommands).scan(scanCursor, scanArgs);
    }

    @Test
    public void testScan_withClusterConnection_returnsKeyScanCursor()
    {
        when(clusterCommands.scan(scanCursor, scanArgs)).thenReturn(keyScanCursor);

        KeyScanCursor<String> result = clusterWrapper.scan(scanCursor, scanArgs);

        assertEquals(keyScanCursor, result);
        verify(clusterCommands).scan(scanCursor, scanArgs);
    }

    @Test
    public void testZcount_withStandaloneConnection_returnsCount()
    {
        Range<? extends Number> range = Range.create(1, 100);
        when(standaloneCommands.zcount(TEST_KEY, range)).thenReturn(10L);

        Long result = standaloneWrapper.zcount(TEST_KEY, range);

        assertEquals(Long.valueOf(10), result);
        verify(standaloneCommands).zcount(TEST_KEY, range);
    }

    @Test
    public void testZcount_withClusterConnection_returnsCount()
    {
        Range<? extends Number> range = Range.create(1, 100);
        when(clusterCommands.zcount(TEST_KEY, range)).thenReturn(20L);

        Long result = clusterWrapper.zcount(TEST_KEY, range);

        assertEquals(Long.valueOf(20), result);
        verify(clusterCommands).zcount(TEST_KEY, range);
    }

    @Test
    public void testZrange_withStandaloneConnection_returnsList()
    {
        List<String> expectedList = Collections.singletonList(TEST_VALUE);
        when(standaloneCommands.zrange(TEST_KEY, 0, 1)).thenReturn(expectedList);

        List<String> result = standaloneWrapper.zrange(TEST_KEY, 0, 1);

        assertEquals(expectedList, result);
        verify(standaloneCommands).zrange(TEST_KEY, 0, 1);
    }

    @Test
    public void testZrange_withClusterConnection_returnsList()
    {
        List<String> expectedList = Collections.singletonList(TEST_VALUE);
        when(clusterCommands.zrange(TEST_KEY, 0, 1)).thenReturn(expectedList);

        List<String> result = clusterWrapper.zrange(TEST_KEY, 0, 1);

        assertEquals(expectedList, result);
        verify(clusterCommands).zrange(TEST_KEY, 0, 1);
    }

    @Test
    public void testGet_withStandaloneConnection_returnsValue()
    {
        when(standaloneCommands.get(TEST_KEY)).thenReturn(TEST_VALUE);

        String result = standaloneWrapper.get(TEST_KEY);

        assertEquals(TEST_VALUE, result);
        verify(standaloneCommands).get(TEST_KEY);
    }

    @Test
    public void testGet_withClusterConnection_returnsValue()
    {
        when(clusterCommands.get(TEST_KEY)).thenReturn(TEST_VALUE);

        String result = clusterWrapper.get(TEST_KEY);

        assertEquals(TEST_VALUE, result);
        verify(clusterCommands).get(TEST_KEY);
    }

    @Test
    public void testHgetall_withStandaloneConnection_returnsMap()
    {
        Map<String, String> expectedMap = Collections.singletonMap("field", TEST_VALUE);
        when(standaloneCommands.hgetall(TEST_KEY)).thenReturn(expectedMap);

        Map<String, String> result = standaloneWrapper.hgetall(TEST_KEY);

        assertEquals(expectedMap, result);
        verify(standaloneCommands).hgetall(TEST_KEY);
    }

    @Test
    public void testHgetall_withClusterConnection_returnsMap()
    {
        Map<String, String> expectedMap = Collections.singletonMap("field", TEST_VALUE);
        when(clusterCommands.hgetall(TEST_KEY)).thenReturn(expectedMap);

        Map<String, String> result = clusterWrapper.hgetall(TEST_KEY);

        assertEquals(expectedMap, result);
        verify(clusterCommands).hgetall(TEST_KEY);
    }

    @Test
    public void testZscan_withStandaloneConnection_returnsCursor()
    {
        when(standaloneCommands.zscan(TEST_KEY, scanCursor)).thenReturn(scoredValueScanCursor);

        ScoredValueScanCursor<String> result = standaloneWrapper.zscan(TEST_KEY, scanCursor);

        assertEquals(scoredValueScanCursor, result);
        verify(standaloneCommands).zscan(TEST_KEY, scanCursor);
    }

    @Test
    public void testZscan_withClusterConnection_returnsCursor()
    {
        when(clusterCommands.zscan(TEST_KEY, scanCursor)).thenReturn(scoredValueScanCursor);

        ScoredValueScanCursor<String> result = clusterWrapper.zscan(TEST_KEY, scanCursor);

        assertEquals(scoredValueScanCursor, result);
        verify(clusterCommands).zscan(TEST_KEY, scanCursor);
    }

    @Test
    public void testEvalReadOnly_withStandaloneConnection_returnsResult()
    {
        byte[] script = "return redis.call('GET', KEYS[1])".getBytes();
        String[] keys = new String[]{"key"};
        String[] values = new String[]{"value"};

        when(standaloneCommands.evalReadOnly(script, ScriptOutputType.VALUE, keys, values)).thenReturn("result");

        String result = standaloneWrapper.evalReadOnly(script, ScriptOutputType.VALUE, keys, values);

        assertEquals("result", result);
        verify(standaloneCommands).evalReadOnly(script, ScriptOutputType.VALUE, keys, values);
    }

    @Test
    public void testEvalReadOnly_withClusterConnection_returnsResult()
    {
        byte[] script = "return redis.call('GET', KEYS[1])".getBytes();
        String[] keys = new String[]{"key"};
        String[] values = new String[]{"value"};

        when(clusterCommands.evalReadOnly(script, ScriptOutputType.VALUE, keys, values)).thenReturn("result");

        String result = clusterWrapper.evalReadOnly(script, ScriptOutputType.VALUE, keys, values);

        assertEquals("result", result);
        verify(clusterCommands).evalReadOnly(script, ScriptOutputType.VALUE, keys, values);
    }

    @Test
    public void testHmset_withStandaloneConnection_returnsOK()
    {
        Map<String, String> data = Collections.singletonMap("field", TEST_VALUE);

        when(standaloneCommands.hmset(TEST_KEY, data)).thenReturn(RESPONSE_OK);

        String result = standaloneWrapper.hmset(TEST_KEY, data);

        assertEquals(RESPONSE_OK, result);
        verify(standaloneCommands).hmset(TEST_KEY, data);
    }

    @Test
    public void testHmset_withClusterConnection_returnsOK()
    {
        Map<String, String> data = Collections.singletonMap("field", TEST_VALUE);

        when(clusterCommands.hmset(TEST_KEY, data)).thenReturn(RESPONSE_OK);

        String result = clusterWrapper.hmset(TEST_KEY, data);

        assertEquals(RESPONSE_OK, result);
        verify(clusterCommands).hmset(TEST_KEY, data);
    }

    @Test
    public void testZadd_withStandaloneConnection_returnsOne()
    {
        when(standaloneCommands.zadd("key", 1.0, "value")).thenReturn(1L);

        Long result = standaloneWrapper.zadd("key", 1.0, "value");

        assertEquals(Long.valueOf(1), result);
        verify(standaloneCommands).zadd("key", 1.0, "value");
    }

    @Test
    public void testZadd_withClusterConnection_returnsOne()
    {
        when(clusterCommands.zadd("key", 1.0, "value")).thenReturn(1L);

        Long result = clusterWrapper.zadd("key", 1.0, "value");

        assertEquals(Long.valueOf(1), result);
        verify(clusterCommands).zadd("key", 1.0, "value");
    }

    @Test
    public void testSet_withStandaloneConnection_returnsOK()
    {
        when(standaloneCommands.set(TEST_KEY, TEST_VALUE)).thenReturn(RESPONSE_OK);

        String result = standaloneWrapper.set(TEST_KEY, TEST_VALUE);

        assertEquals(RESPONSE_OK, result);
        verify(standaloneCommands).set(TEST_KEY, TEST_VALUE);
    }

    @Test
    public void testSet_withClusterConnection_returnsOK()
    {
        when(clusterCommands.set(TEST_KEY, TEST_VALUE)).thenReturn(RESPONSE_OK);

        String result = clusterWrapper.set(TEST_KEY, TEST_VALUE);

        assertEquals(RESPONSE_OK, result);
        verify(clusterCommands).set(TEST_KEY, TEST_VALUE);
    }
}
