/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.apps;

import java.util.Map;


import com.datatorrent.apps.ops.SchemaAwareFilter;
import com.datatorrent.apps.ops.SchemaAwareFormatter;
import com.datatorrent.apps.ops.SchemaAwareParser;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.filter.FilterOperator;
import com.datatorrent.lib.transform.TransformOperator;

@ApplicationAnnotation(name = "Kafka-to-HDFS-Filter")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaInputOperator = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
    SchemaAwareParser csvParser = dag.addOperator("csvParser", SchemaAwareParser.class);
    SchemaAwareFilter filterOperator = dag.addOperator("filter", new SchemaAwareFilter());
    SchemaAwareFormatter formatter = dag.addOperator("formatter", new SchemaAwareFormatter());
    StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", new StringFileOutputOperator());
    ConsoleOutputOperator consoleOutputOperator = dag.addOperator("Console", ConsoleOutputOperator.class);

    dag.addStream("data", kafkaInputOperator.outputPort, csvParser.in);
    dag.addStream("pojo", csvParser.out, filterOperator.input);
    dag.addStream("filtered", filterOperator.truePort, formatter.in);
    dag.addStream("string", formatter.out, fileOutput.input, consoleOutputOperator.input);

    /*
     * To add custom logic to your DAG, add your custom operator here with
     * dag.addOperator api call and connect it in the dag using the dag.addStream
     * api call. 
     * 
     * For example: 
     * 
     * To transform the filtered records we can add transform operator in betweeen.
     * Effective pipeline would be as follows:
     * kafkaInput->csvParser->filterOperator->transform->formatter->fileOutput can be achieved as follows
     * 
     * Use following code block to add transform operator: 
     * 
     * TransformOperator transform = dag.addOperator("transform", new TransformOperator());
     * Map<String, String> expMap = Maps.newHashMap();
     * expMap.put("name", "{$.name}.toUpperCase()");
     * transform.setExpressionMap(expMap);
     * 
     * Connect these operators with appropriate streams
     * Replace the following line below:
     * dag.addStream("filtered", filterOperator.truePort, formatter.in);
     * 
     * with these lines:
     * dag.addStream("filtered", filterOperator.truePort, transform.input);
     * dag.addStream("transformed", transform.output, formatter.in);
     * 
     * In ApplicationTest.java
     * 
     * Replace following line in setup() method:
     * outputFilePath = outputDir + "/output.txt_5.0";
     * with this lines:
     * outputFilePath = outputDir + "/output.txt_6.0";
     * 
     * Replace following line in compare() method:
     * Assert.assertArrayEquals(lines_filtered, output.split("\\n"));
     * with this lines:
     * Assert.assertArrayEquals(lines_transformed, output.split("\\n"));
     * 
     */
  }

}
