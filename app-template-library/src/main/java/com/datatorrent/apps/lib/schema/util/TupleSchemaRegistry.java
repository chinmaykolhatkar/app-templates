package com.datatorrent.apps.lib.schema.util;

import com.datatorrent.apps.lib.schema.api.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

public class TupleSchemaRegistry
{
  public static final String FQCN_PACKAGE = "org.apache.apex.generated.schema.";
  private Map<String, Schema> schemas = new HashMap<>();

  public Schema createNewSchema(String name)
  {
    if (schemas.containsKey(name)) {
      return schemas.get(name);
    }

    Schema schema = new Schema();
    schema.name = name;
    schema.fqcn = FQCN_PACKAGE + name;
    schemas.put(name, schema);

    return schema;
  }

  public String generateCommonJar(boolean deleteOnExit) throws IOException
  {
    File file = File.createTempFile("tupleSchema", ".jar");
    if (deleteOnExit) {
      file.deleteOnExit();
    }

    FileSystem fs = FileSystem.newInstance(file.toURI(), new Configuration());
    FSDataOutputStream out = fs.create(new Path(file.getAbsolutePath()));
    JarOutputStream jout = new JarOutputStream(out);

    for (Schema schema : schemas.values()) {
      if ((schema.fieldList.size() != 0) && (schema.beanClassBytes != null)) {
        jout.putNextEntry(new ZipEntry(schema.fqcn.replace(".", "/") + ".class"));
        jout.write(schema.beanClassBytes);
        jout.closeEntry();
      }
    }

    jout.close();
    out.close();

    return file.getAbsolutePath();
  }

  public void finalizeSchemas() throws IOException
  {
    for (Schema schema : schemas.values()) {
      schema.generateBean();
    }
  }
}
