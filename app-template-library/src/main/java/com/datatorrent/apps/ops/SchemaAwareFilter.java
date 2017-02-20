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
    Preconditions.checkArgument(inSchema.containsKey(input));
    if (truePort.isConnected()) {
      Preconditions.checkArgument(outSchema.containsKey(truePort));
    }
    if (falsePort.isConnected()) {
      Preconditions.checkArgument(outSchema.containsKey(falsePort));
    }
    if (error.isConnected()) {
      Preconditions.checkArgument(outSchema.containsKey(error));
    }

    for (Map.Entry<String, Class> entry : inSchema.get(input).getFieldList().entrySet()) {
      if (truePort.isConnected()) outSchema.get(truePort).addField(entry.getKey(), entry.getValue());
      if (falsePort.isConnected()) outSchema.get(falsePort).addField(entry.getKey(), entry.getValue());
      if (error.isConnected()) outSchema.get(error).addField(entry.getKey(), entry.getValue());
    }
  }
}
