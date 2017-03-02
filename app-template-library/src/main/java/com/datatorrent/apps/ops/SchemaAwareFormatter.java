package com.datatorrent.apps.ops;

import com.datatorrent.apps.lib.schema.api.Schema;
import com.datatorrent.apps.lib.schema.api.SchemaAware;
import com.datatorrent.contrib.formatter.CsvFormatter;

import java.util.List;
import java.util.Map;

/**
 * Created by dtadmin on 20-Feb-17.
 */
public class SchemaAwareFormatter extends CsvFormatter implements SchemaAware
{
  private String seperator = ",";
  private String fieldOrder;

  private static final String schemaFormat = "{\n" +
          "      \"separator\": \"%s\",\n" +
          "      \"quoteChar\": \"\\\"\",\n" +
          "      \"fields\": [%s]\n" +
          "      }";
  private static final String fieldFormat = "{\n" +
          "      \"name\": \"%s\",\n" +
          "      \"type\": \"%s\"\n" +
          "      }";

  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    String fieldSchemaPart = "";

    boolean first = true;
    Map<String, Class> fieldList = inSchema.get(in).fieldList;

    for (String fieldName : fieldOrder.split(",")) {
      if (!fieldList.containsKey(fieldName)) {
        continue;
      }

      Class fieldType = fieldList.get(fieldName);
      if (first) {
        first = false;
      }
      else {
        fieldSchemaPart += ",";
      }
      fieldSchemaPart += String.format(fieldFormat, fieldName, fieldType.getSimpleName());
    }

    String schema = String.format(schemaFormat, seperator, fieldSchemaPart);

    setSchema(schema);
  }

  public String getSeperator()
  {
    return seperator;
  }

  public void setSeperator(String seperator)
  {
    this.seperator = seperator;
  }

  public String getFieldOrder() {
    return fieldOrder;
  }

  public void setFieldOrder(String fieldOrder) {
    this.fieldOrder = fieldOrder;
  }
}
