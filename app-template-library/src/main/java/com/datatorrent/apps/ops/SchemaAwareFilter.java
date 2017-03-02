package com.datatorrent.apps.ops;

import com.datatorrent.apps.lib.schema.api.Schema;
import com.datatorrent.apps.lib.schema.api.SchemaAware;
import com.datatorrent.lib.filter.FilterOperator;
import com.google.common.base.Preconditions;

import java.util.Map;

public class SchemaAwareFilter extends FilterOperator implements SchemaAware
{
  @Override
  public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
  {
    for (Map.Entry<String, Class> entry : inSchema.get(input).getFieldList().entrySet()) {
      if (outSchema.containsKey(truePort)) outSchema.get(truePort).addField(entry.getKey(), entry.getValue());
      if (outSchema.containsKey(falsePort)) outSchema.get(falsePort).addField(entry.getKey(), entry.getValue());
      if (outSchema.containsKey(error)) outSchema.get(error).addField(entry.getKey(), entry.getValue());
    }
  }
}
