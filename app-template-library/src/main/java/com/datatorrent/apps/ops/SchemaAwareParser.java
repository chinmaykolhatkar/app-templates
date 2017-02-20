package com.datatorrent.apps.ops;

import com.datatorrent.apps.lib.schema.api.Schema;
import com.datatorrent.apps.lib.schema.api.SchemaAware;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.contrib.parser.DelimitedSchema;

import java.util.Date;
import java.util.Map;

public class SchemaAwareParser extends CsvParser implements SchemaAware
{
  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    String schema = getSchema();
    DelimitedSchema delimitedParserSchema = new DelimitedSchema(schema);
    for (DelimitedSchema.Field field : delimitedParserSchema.getFields()) {
      outSchema.get(out).addField(field.getName(), getFieldType(field.getType()));
    }
  }

  private Class getFieldType(DelimitedSchema.FieldType type)
  {
    switch (type) {
      case BOOLEAN:
        return Boolean.class;
      case DOUBLE:
        return Double.class;
      case INTEGER:
        return Integer.class;
      case FLOAT:
        return Float.class;
      case LONG:
        return Long.class;
      case SHORT:
        return Short.class;
      case CHARACTER:
        return Character.class;
      case STRING:
        return String.class;
      case DATE:
        return Date.class;
      default:
        return Object.class;
    }
  }
}
