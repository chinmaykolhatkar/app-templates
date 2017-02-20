package com.datatorrent.apps.lib;


import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.apps.lib.schema.api.Schema;
import com.datatorrent.apps.lib.schema.api.SchemaAware;
import com.datatorrent.apps.lib.schema.visitor.SchemaDAGVisitor;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

public class SchemaDAGVisitorTest
{
  @Test
  public void testSchema()
  {
    LogicalPlan dag = new LogicalPlan();

    TestInputOperator input = dag.addOperator("input",
        new TestInputOperator(ImmutableMap.<String, Class>of("test1", Integer.class, "test2", String.class)));

    TestGenericOperator middle = dag.addOperator("middle",
        new TestGenericOperator("testAbc", Date.class));

    TestOutputOperator output = dag.addOperator("output",
        TestOutputOperator.class);

    ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);

    LogicalPlan.StreamMeta in_middle = dag.addStream("in_middle", input.out, middle.in);
    LogicalPlan.StreamMeta middle_out = dag.addStream("middle_out", middle.out, output.in);
    LogicalPlan.StreamMeta middle_console = dag.addStream("middle_console", middle.out1, console.input);

    SchemaDAGVisitor schemaDAGVisitor = new SchemaDAGVisitor();
    dag.setAttribute(Context.DAGContext.VISITORS, Arrays.asList(new DAG.Visitor[]{schemaDAGVisitor}));
    dag.visitDAG();
    dag.validate();

    // Validate in_middle
    Class<?> aClass = in_middle.getSource().getAttributes().get(Context.PortContext.TUPLE_CLASS);
    Class<?> bClass = in_middle.getSinks().iterator().next().getAttributes().get(Context.PortContext.TUPLE_CLASS);

    Assert.assertTrue(aClass == bClass);
    Assert.assertEquals(2, aClass.getDeclaredFields().length);
    assertClassContainsField(aClass, "test1", Integer.class);
    assertClassContainsField(aClass, "test2", String.class);

    // Validate middle_out
    aClass = middle_out.getSource().getAttributes().get(Context.PortContext.TUPLE_CLASS);
    bClass = middle_out.getSinks().iterator().next().getAttributes().get(Context.PortContext.TUPLE_CLASS);
    Assert.assertTrue(aClass == bClass);

    Assert.assertEquals(3, aClass.getDeclaredFields().length);
    assertClassContainsField(aClass, "test1", Integer.class);
    assertClassContainsField(aClass, "test2", String.class);
    assertClassContainsField(aClass, "testAbc", Date.class);

    // Validate middle_console
    Assert.assertFalse(middle_console.getSource().getAttributes().contains(Context.PortContext.TUPLE_CLASS));
    Assert.assertFalse(middle_console.getSinks().iterator().next().getAttributes().contains(Context.PortContext.TUPLE_CLASS));

    // Validate DAG attribute
    String schemaJarPath = schemaDAGVisitor.getSchemaJarPath();
    Assert.assertTrue(dag.getAttributes().get(Context.DAGContext.LIBRARY_JARS).contains(schemaJarPath));
  }

  private void assertClassContainsField(Class clazz, String fieldName, Class<?> type)
  {
    try {
      Field f = clazz.getDeclaredField(fieldName);
      Assert.assertTrue(f.getType() == type);
    } catch (NoSuchFieldException e) {
      Assert.fail("Not field: " + fieldName + " of type: " + type +" found in class: " + clazz);
    }
  }

  public static class TestInputOperator extends BaseOperator implements InputOperator, SchemaAware
  {
    private Map<String, Class> newFields;

    @OutputPortFieldAnnotation(schemaRequired = true)
    public transient DefaultOutputPort<Object> out = new DefaultOutputPort<>();

    public TestInputOperator()
    {
    }

    public TestInputOperator(Map<String, Class> newFields)
    {
      this.newFields = newFields;
    }

    @Override
    public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
    {
      Assert.assertTrue(inSchema.size() == 0);
      Assert.assertTrue(outSchema.containsKey(out));

      outSchema.get(out).addField("test1", Integer.class);
      outSchema.get(out).addField("test2", String.class);
    }

    @Override
    public void emitTuples()
    {

    }
  }

  public static class TestGenericOperator extends BaseOperator implements SchemaAware
  {
    private String additionalField;
    private Class<?> additionalFieldClazz;

    public TestGenericOperator()
    {
    }

    public TestGenericOperator(String additionalField, Class additionalFieldClazz)
    {
      this.additionalField = additionalField;
      this.additionalFieldClazz = additionalFieldClazz;
    }

    @InputPortFieldAnnotation(schemaRequired = true)
    public transient final DefaultInputPort<Object> in = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {

      }
    };

    @OutputPortFieldAnnotation(schemaRequired = true)
    public transient final DefaultOutputPort<Object> out = new DefaultOutputPort<>();

    public transient final DefaultOutputPort<String> out1 = new DefaultOutputPort<>();

    @Override
    public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
    {
      Assert.assertTrue(inSchema.containsKey(in));
      Assert.assertTrue(outSchema.containsKey(out));
      Assert.assertTrue(outSchema.containsKey(out1));

      Assert.assertTrue(outSchema.get(out).fieldList.size() == 0);
      Assert.assertTrue(outSchema.get(out1).fieldList.size() == 0);

      for (Map.Entry<String, Class> entry : inSchema.get(in).fieldList.entrySet()) {
        outSchema.get(out).addField(entry.getKey(), entry.getValue());
      }
      outSchema.get(out).addField(additionalField, additionalFieldClazz);
    }
  }

  public static class TestOutputOperator extends BaseOperator implements SchemaAware
  {
    @InputPortFieldAnnotation(schemaRequired = true)
    public transient final DefaultInputPort<Object> in = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {

      }
    };


    @Override
    public void registerSchema(Map<InputPort, Schema> inSchema, Map<OutputPort, Schema> outSchema)
    {
      Assert.assertTrue(inSchema.containsKey(in));
      Assert.assertTrue(outSchema.size() == 0);
    }
  }
}
