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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public class RecordMapper {

    public static Schema convertSchema(String tableName, Map<String, AttributeValue> attributes) {
        final SchemaBuilder builder = SchemaBuilder.struct().name(tableName);
        for (Map.Entry<String, AttributeValue> attribute : attributes.entrySet()) {
            final String attributeName = attribute.getKey();
            final AttributeValue attributeValue = attribute.getValue();
            if (attributeValue.getS() != null) {
                builder.field(attributeName, Schema.OPTIONAL_STRING_SCHEMA);
            } else if (attributeValue.getN() != null) {
                builder.field(attributeName, Schema.OPTIONAL_STRING_SCHEMA);
            } else if (attributeValue.getB() != null) {
                builder.field(attributeName, Schema.OPTIONAL_BYTES_SCHEMA);
            } else if (attributeValue.getSS() != null) {
                builder.field(attributeName, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build());
            } else if (attributeValue.getNS() != null) {
                builder.field(attributeName, SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build());
            } else if (attributeValue.getBS() != null) {
                builder.field(attributeName, SchemaBuilder.array(Schema.BYTES_SCHEMA).optional().build());
            } else if (attributeValue.getNULL() != null) {
                builder.field(attributeName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            } else if (attributeValue.getBOOL() != null) {
                builder.field(attributeName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            }
        }
        return builder.build();
    }

    public static Struct convertRecord(Schema schema, Map<String, AttributeValue> attributes) {
        final Struct record = new Struct(schema);
        for (Map.Entry<String, AttributeValue> attribute : attributes.entrySet()) {
            final String attributeName = attribute.getKey();
            final AttributeValue attributeValue = attribute.getValue();
            if (attributeValue.getS() != null) {
                record.put(attributeName, attributeValue.getS());
            } else if (attributeValue.getN() != null) {
                record.put(attributeName, attributeValue.getN());
            } else if (attributeValue.getB() != null) {
                record.put(attributeName, attributeValue.getB());
            } else if (attributeValue.getSS() != null) {
                record.put(attributeName, attributeValue.getSS());
            } else if (attributeValue.getNS() != null) {
                record.put(attributeName, attributeValue.getNS());
            } else if (attributeValue.getBS() != null) {
                record.put(attributeName, attributeValue.getBS());
            } else if (attributeValue.getNULL() != null) {
                record.put(attributeName, attributeValue.getNULL());
            } else if (attributeValue.getBOOL() != null) {
                record.put(attributeName, attributeValue.getBOOL());
            }
        }
        return record;
    }

}
