package com.datatorrent.apps.lib.schema.api;

import com.datatorrent.apps.lib.schema.util.BeanClassGenerator;
import org.apache.apex.malhar.lib.utils.ClassLoaderUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Schema
{
  public String name;
  public String fqcn;
  public Map<String, Class> fieldList = new HashMap<>();
  public Class beanClass;
  public byte[] beanClassBytes;

  public Schema addField(String name, Class type)
  {
    fieldList.put(name, type);
    return this;
  }

  public Class getField(String name)
  {
    return fieldList.get(name);
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public Map<String, Class> getFieldList()
  {
    return fieldList;
  }

  public void setFieldList(Map<String, Class> fieldList)
  {
    this.fieldList = fieldList;
  }

  public Schema generateBean() throws IOException
  {
    // Use Bean Class generator to generate the class
    if (fieldList.size() != 0) {
      this.beanClassBytes = BeanClassGenerator.createAndWriteBeanClass(this.fqcn, fieldList);
      this.beanClass = ClassLoaderUtils.readBeanClass(fqcn, beanClassBytes);
    }
    return this;
  }

  @Override
  public String toString()
  {
    return "Schema{" +
        "fqcn='" + fqcn + '\'' +
        ", fieldList=" + fieldList +
        '}';
  }
}
