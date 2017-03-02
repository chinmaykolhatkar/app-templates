package com.datatorrent.apps.lib.schema.visitor;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Visitor;
import com.datatorrent.apps.lib.schema.api.Schema;
import com.datatorrent.apps.lib.schema.api.SchemaAware;
import com.datatorrent.apps.lib.schema.util.TupleSchemaRegistry;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class SchemaDAGVisitor implements Visitor
{
  private DAG dag;
  private transient Map<DAG.OperatorMeta, OperatorSchemaInfo> processedOperators = new HashMap<>();
  private transient TupleSchemaRegistry schemaRegistry = new TupleSchemaRegistry();
  private transient String schemaJarPath;

  public void preVisitDAG(DAG dag)
  {
    this.dag = dag;
  }

  public void visitOperator(DAG.OperatorMeta ometa)
  {
  }

  public void visitStream(DAG.StreamMeta smeta)
  {
  }

  public void postVisitDAG()
  {
    Stack<DAG.OperatorMeta> pendingOperators = new Stack<>();

    for (DAG.OperatorMeta o : dag.getAllOperatorsMeta()) {
      pendingOperators.push(o);
    }

    while (!pendingOperators.empty()) {
      DAG.OperatorMeta op = pendingOperators.pop();
      if (processedOperators.containsKey(op)) {
        // operator already processed.
        continue;
      }

      boolean allUpstreamsDone = true;

      for (Map.Entry<DAG.InputPortMeta, DAG.StreamMeta> entry : op.getInputStreams().entrySet()) {
        DAG.StreamMeta s = entry.getValue();

        if ((s.getSource() != null)  // There is a source for this stream
            && !processedOperators.containsKey(s.getSource().getOperatorMeta()) // Upstream operator is not done yet.
            ) {
          pendingOperators.push(op);
          pendingOperators.push(s.getSource().getOperatorMeta());
          allUpstreamsDone = false;
        }
      }

      if (allUpstreamsDone) {
        processOperator(op);
      }
    }

    try {
      schemaRegistry.finalizeSchemas();

      assignTupleClassAttr();

      schemaJarPath = schemaRegistry.generateCommonJar(true);
      String jars = dag.getAttributes().get(Context.DAGContext.LIBRARY_JARS);
      dag.setAttribute(Context.DAGContext.LIBRARY_JARS,
          ((jars != null) && (jars.length() != 0)) ? jars + "," + schemaJarPath : schemaJarPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void assignTupleClassAttr()
  {
    for (Map.Entry<DAG.OperatorMeta, OperatorSchemaInfo> entry : processedOperators.entrySet()) {
      OperatorSchemaInfo info = entry.getValue();
      for (Map.Entry<DAG.InputPortMeta, Schema> schemaEntry : info.inputPortSchema.entrySet()) {
        if (!schemaEntry.getKey().getAttributes().contains(Context.PortContext.TUPLE_CLASS) &&  // No TUPLE_CLASS is set.
            schemaEntry.getValue().beanClass != null) {
          schemaEntry.getKey().getAttributes().put(DAG.PortContext.TUPLE_CLASS, schemaEntry.getValue().beanClass);
        }
      }

      for (Map.Entry<DAG.OutputPortMeta, Schema> schemaEntry : info.outputPortSchema.entrySet()) {
        if (!schemaEntry.getKey().getAttributes().contains(Context.PortContext.TUPLE_CLASS) &&  // No TUPLE_CLASS is set.
            schemaEntry.getValue().beanClass != null) {
          schemaEntry.getKey().getAttributes().put(DAG.PortContext.TUPLE_CLASS, schemaEntry.getValue().beanClass);
        }
      }
    }
  }

  private void processOperator(DAG.OperatorMeta op)
  {
    OperatorSchemaInfo info = new OperatorSchemaInfo();
    processedOperators.put(op, info);

    Map<Operator.InputPort, Schema> inSchema = new HashMap<>();
    Map<Operator.OutputPort, Schema> outSchema = new HashMap<>();

    for (Map.Entry<DAG.InputPortMeta, DAG.StreamMeta> entry : op.getInputStreams().entrySet()) {
      // Get upstream operator schema
      DAG.OutputPortMeta upstreamOutPort = entry.getValue().getSource();
      OperatorSchemaInfo upstreamInfo = processedOperators.get(upstreamOutPort.getOperatorMeta());
      Schema upstreamSchema = upstreamInfo.outputPortSchema.get(upstreamOutPort);
      info.inputPortSchema.put(entry.getKey(), upstreamSchema);

      inSchema.put(entry.getKey().getPort(), upstreamSchema);
    }

    for (Map.Entry<DAG.OutputPortMeta, DAG.StreamMeta> entry : op.getOutputStreams().entrySet()) {
      String schemaName = entry.getValue().getName();

      Schema newSchema = schemaRegistry.createNewSchema(schemaName);
      info.outputPortSchema.put(entry.getKey(), newSchema);

      outSchema.put(entry.getKey().getPort(), newSchema);
    }

    Operator operator = op.getOperator();
    if (operator instanceof SchemaAware) {
      ((SchemaAware) operator).registerSchema(inSchema, outSchema);
    }
  }

  private class OperatorSchemaInfo
  {
    public transient Map<DAG.InputPortMeta, Schema> inputPortSchema = new HashMap<>();
    public transient Map<DAG.OutputPortMeta, Schema> outputPortSchema = new HashMap<>();
  }

  @VisibleForTesting
  public String getSchemaJarPath()
  {
    return schemaJarPath;
  }
}
