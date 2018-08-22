/*
 * Copyright 2016 Shikhar Bhushan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dynamok.source;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RecordMapperTest {

    @Test
    public void conversions() {
        final String string = "test";
        final String number = "42";
        final ByteBuffer bytes = ByteBuffer.wrap(new byte[]{42});
        final boolean bool = true;
        final boolean nullValue = true;
        final Map<String, AttributeValue> attributeValueMap = ImmutableMap.<String, AttributeValue>builder()
                .put("thestring", new AttributeValue().withS(string))
                .put("thenumber", new AttributeValue().withN(number))
                .put("thebytes", new AttributeValue().withB(bytes))
                .put("thestrings", new AttributeValue().withSS(string))
                .put("thenumbers", new AttributeValue().withNS(number))
                .put("thebyteslist", new AttributeValue().withBS(bytes))
                .put("thenull", new AttributeValue().withNULL(true))
                .put("thebool", new AttributeValue().withBOOL(bool)).build();
        final Schema schema = RecordMapper.convertSchema("", attributeValueMap);
        final Struct record = RecordMapper.convertRecord(schema, attributeValueMap);

        assertEquals(string, record.get("thestring"));
        assertEquals(number, record.get("thenumber"));
        assertEquals(bytes, record.get("thebytes"));
        assertEquals(Collections.singletonList(string), record.get("thestrings"));
        assertEquals(Collections.singletonList(number), record.get("thenumbers"));
        assertEquals(Collections.singletonList(bytes), record.get("thebyteslist"));
        assertEquals(nullValue, record.get("thenull"));
        assertEquals(bool, record.get("thebool"));
    }
}
