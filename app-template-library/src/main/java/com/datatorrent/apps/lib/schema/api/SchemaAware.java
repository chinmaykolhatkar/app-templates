package com.datatorrent.apps.lib.schema.api;

import com.datatorrent.api.Operator;
import com.datatorrent.apps.lib.schema.api.Schema;

import java.util.Map;

public interface SchemaAware
{
  void registerSchema(Map<Operator.InputPort, Schema> inSchema, Map<Operator.OutputPort, Schema> outSchema);
}
