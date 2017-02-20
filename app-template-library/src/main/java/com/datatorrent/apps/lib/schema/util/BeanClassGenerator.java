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
package com.datatorrent.apps.lib.schema.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.xbean.asm5.ClassWriter;
import org.apache.xbean.asm5.Opcodes;
import org.apache.xbean.asm5.tree.*;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Creates a bean class on fly.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class BeanClassGenerator
{
  public static final ImmutableMap<String, Character> PRIMITIVE_TYPES;

  public static final char typeIdentifierBoolean = 'Z';
  public static final char typeIdentifierChar = 'C';
  public static final char typeIdentifierByte = 'B';
  public static final char typeIdentifierShort = 'S';
  public static final char typeIdentifierInt = 'I';
  public static final char typeIdentifierFloat = 'F';
  public static final char typeIdentifierLong = 'J';
  public static final char typeIdentifierDouble = 'D';


  static {
    Map<String, Character> types = Maps.newHashMap();
    types.put("boolean", typeIdentifierBoolean);
    types.put("char", typeIdentifierChar);
    types.put("byte", typeIdentifierByte);
    types.put("short", typeIdentifierShort);
    types.put("int", typeIdentifierInt);
    types.put("float", typeIdentifierFloat);
    types.put("long", typeIdentifierLong);
    types.put("double", typeIdentifierDouble);
    PRIMITIVE_TYPES = ImmutableMap.copyOf(types);
  }

  /**
   * Creates a class from give field information and returns byte array of compiled class.
   *
   * @param fqcn      fully qualified class name
   * @param fieldList field list for which POJO needs to be generated.
   *
   * @return byte[] representing compiled class.
   * @throws IOException
   * @throws JSONException
   */
  public static byte[] createAndWriteBeanClass(String fqcn, Map<String, Class> fieldList)
    throws IOException
  {
    return createAndWriteBeanClass(fqcn, fieldList, null);
  }

  /**
   * Creates a class from given field information and writes it to the output stream. Also returns byte[] of compiled
   * class
   *
   * @param fqcn         fully qualified class name
   * @param fieldList    field list describing the class
   * @param outputStream stream to which the class is persisted
   * @throws JSONException
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static byte[] createAndWriteBeanClass(String fqcn, Map<String, Class> fieldList,
      FSDataOutputStream outputStream) throws IOException
  {
    ClassNode classNode = new ClassNode();

    classNode.version = Opcodes.V1_7;  //generated class will only run on JRE 1.7 or above
    classNode.access = Opcodes.ACC_PUBLIC;

    classNode.name = fqcn.replace('.', '/');
    classNode.superName = "java/lang/Object";

    // add default constructor
    addDefaultConstructor(classNode);

    //for each field in json add a field to this class and a getter and setter for it.

    for (Map.Entry<String, Class> fieldInfo : fieldList.entrySet()) {
      String fieldName = fieldInfo.getKey();
      String fieldType = fieldInfo.getValue().getName();
      String fieldJavaType = getJavaType(fieldType);

      addPrivateField(classNode, fieldName, fieldJavaType);

      String fieldNameForMethods = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

      addGetter(classNode, fieldName, fieldNameForMethods, fieldJavaType);
      addSetter(classNode, fieldName, fieldNameForMethods, fieldJavaType);
    }

    addToStringMethod(classNode, fieldList);
    addHashCodeMethod(classNode, fieldList);
    addEqualsMethod(classNode, fieldList);

    //Write the class
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
    classNode.accept(cw);
    cw.visitEnd();

    byte[] classBytes = cw.toByteArray();

    if (outputStream != null) {
      outputStream.write(classBytes);
      outputStream.close();
    }

    return classBytes;
  }

  /**
   * Add Default constructor for POJO
   * @param classNode ClassNode which needs to be populated with constructor
   */
  @SuppressWarnings("unchecked")
  private static void addDefaultConstructor(ClassNode classNode)
  {
    MethodNode constructorNode = new MethodNode(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    constructorNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    constructorNode.instructions
        .add(new MethodInsnNode(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false));
    constructorNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(constructorNode);
  }

  /**
   * Add private field to the class
   * @param classNode ClassNode which needs to be populated with private field.
   * @param fieldName Name of the field
   * @param fieldJavaType Java ASM type of the field
   */
  @SuppressWarnings("unchecked")
  private static void addPrivateField(ClassNode classNode, String fieldName, String fieldJavaType)
  {
    FieldNode fieldNode = new FieldNode(Opcodes.ACC_PRIVATE, fieldName, fieldJavaType, null, null);
    classNode.fields.add(fieldNode);
  }

  /**
   * Add public getter method for given field
   * @param classNode ClassNode which needs to be populated with public getter.
   * @param fieldName Name of the field for which public getter needs to be added.
   * @param fieldNameForMethods Suffix of the getter method, Prefix "is" or "get" is added by this method.
   * @param fieldJavaType Java ASM type of the field
   */
  @SuppressWarnings("unchecked")
  private static void addGetter(ClassNode classNode, String fieldName, String fieldNameForMethods, String fieldJavaType)
  {
    String getterSignature = "()" + fieldJavaType;
    MethodNode getterNode = new MethodNode(Opcodes.ACC_PUBLIC,
        (fieldJavaType.equals(typeIdentifierBoolean) ? "is" : "get") + fieldNameForMethods,
        getterSignature, null, null);
    getterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    getterNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));

    int returnOpCode;
    if (fieldJavaType.equals(Character.toString(typeIdentifierBoolean)) ||
        fieldJavaType.equals(Character.toString(typeIdentifierByte))    ||
        fieldJavaType.equals(Character.toString(typeIdentifierChar))    ||
        fieldJavaType.equals(Character.toString(typeIdentifierShort))   ||
        fieldJavaType.equals(Character.toString(typeIdentifierInt))) {
      returnOpCode = Opcodes.IRETURN;
    } else if (fieldJavaType.equals(Character.toString(typeIdentifierLong))) {
      returnOpCode = Opcodes.LRETURN;
    } else if (fieldJavaType.equals(Character.toString(typeIdentifierFloat))) {
      returnOpCode = Opcodes.FRETURN;
    } else if (fieldJavaType.equals(Character.toString(typeIdentifierDouble))) {
      returnOpCode = Opcodes.DRETURN;
    } else {
      returnOpCode = Opcodes.ARETURN;
    }
    getterNode.instructions.add(new InsnNode(returnOpCode));

    classNode.methods.add(getterNode);
  }

  /**
   * Add public setter for given field
   * @param classNode ClassNode which needs to be populated with public setter
   * @param fieldName Name of the field for which setter needs to be added
   * @param fieldNameForMethods Suffix for setter method. Prefix "set" is added by this method
   * @param fieldJavaType Java ASM type of the field
   */
  @SuppressWarnings("unchecked")
  private static void addSetter(ClassNode classNode, String fieldName, String fieldNameForMethods, String fieldJavaType)
  {
    String setterSignature = '(' + fieldJavaType + ')' + 'V';
    MethodNode setterNode = new MethodNode(Opcodes.ACC_PUBLIC, "set" + fieldNameForMethods, setterSignature, null,
        null);
    setterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));

    int loadOpCode;
    if (fieldJavaType.equals(Character.toString(typeIdentifierBoolean)) ||
        fieldJavaType.equals(Character.toString(typeIdentifierByte))    ||
        fieldJavaType.equals(Character.toString(typeIdentifierChar))    ||
        fieldJavaType.equals(Character.toString(typeIdentifierShort))   ||
        fieldJavaType.equals(Character.toString(typeIdentifierInt))) {
      loadOpCode = Opcodes.ILOAD;
    } else if (fieldJavaType.equals(Character.toString(typeIdentifierLong))) {
      loadOpCode = Opcodes.LLOAD;
    } else if (fieldJavaType.equals(Character.toString(typeIdentifierFloat))) {
      loadOpCode = Opcodes.FLOAD;
    } else if (fieldJavaType.equals(Character.toString(typeIdentifierDouble))) {
      loadOpCode = Opcodes.DLOAD;
    } else {
      loadOpCode = Opcodes.ALOAD;
    }
    setterNode.instructions.add(new VarInsnNode(loadOpCode, 1));

    setterNode.instructions.add(new FieldInsnNode(Opcodes.PUTFIELD, classNode.name, fieldName, fieldJavaType));
    setterNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(setterNode);
  }

  /**
   * Adds a toString method to underlying class. Uses StringBuilder to generate the final string.
   *
   * @param classNode
   * @param fieldList
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  private static void addToStringMethod(ClassNode classNode, Map<String, Class> fieldList)
  {
    MethodNode toStringNode = new MethodNode(Opcodes.ACC_PUBLIC, "toString", "()Ljava/lang/String;", null, null);
    toStringNode.visitAnnotation("Ljava/lang/Override;", true);

    toStringNode.instructions.add(new TypeInsnNode(Opcodes.NEW, "java/lang/StringBuilder"));
    toStringNode.instructions.add(new InsnNode(Opcodes.DUP));
    toStringNode.instructions.add(new LdcInsnNode(classNode.name + "{"));
    toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKESPECIAL, "java/lang/StringBuilder",
        "<init>", "(Ljava/lang/String;)V", false));
    toStringNode.instructions.add(new VarInsnNode(Opcodes.ASTORE, 1));
    toStringNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));

    boolean first = true;
    for (Map.Entry<String, Class> info : fieldList.entrySet()) {
      String fieldName = info.getKey();
      String fieldType = info.getValue().getName();
      String fieldJavaType = getJavaType(fieldType);

      if (!first) {
        toStringNode.instructions.add(new LdcInsnNode(", "));
        toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
            "(Ljava/lang/String;)Ljava/lang/StringBuilder;", false));
      }

      toStringNode.instructions.add(new LdcInsnNode(fieldName + "="));
      toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
          "(Ljava/lang/String;)Ljava/lang/StringBuilder;", false));
      toStringNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
      toStringNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));

      // There is no StringBuilder.append method for short and byte. It takes it as int.
      if (fieldJavaType.equals(Character.toString(typeIdentifierShort)) ||
          fieldJavaType.equals(Character.toString(typeIdentifierByte))) {
        fieldJavaType = "I";
      }

      Character pchar = PRIMITIVE_TYPES.get(fieldType);
      if (pchar == null) {
        // It's not a primitive type. StringBuilder.append method signature takes Object type.
        fieldJavaType = "Ljava/lang/Object;";
      }

      toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
          "(" + fieldJavaType + ")Ljava/lang/StringBuilder;", false));

      first=false;
    }

    toStringNode.instructions.add(new LdcInsnNode("}"));
    toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append",
        "(Ljava/lang/String;)Ljava/lang/StringBuilder;", false));

    toStringNode.instructions.add(new InsnNode(Opcodes.POP));
    toStringNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "toString",
        "()Ljava/lang/String;", false));
    toStringNode.instructions.add(new InsnNode(Opcodes.ARETURN));

    classNode.methods.add(toStringNode);
  }

  /**
   * This will add a hashCode method for class being generated. <br>
   * Algorithm is as follows: <br>
   * <i><p>
   * int hashCode = 7;
   * for (field: all fields) {
   * hashCode = 23 * hashCode + field.hashCode()
   * }
   * </p></i>
   * <br>
   * <b> For primitive field, hashcode implemenented is similar to the one present in its wrapper class. </b>
   *
   * @param classNode
   * @param fieldList
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  private static void addHashCodeMethod(ClassNode classNode, Map<String, Class> fieldList)
  {
    MethodNode hashCodeNode = new MethodNode(Opcodes.ACC_PUBLIC, "hashCode", "()I", null, null);
    hashCodeNode.visitAnnotation("Ljava/lang/Override;", true);

    hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 7));
    hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ISTORE, 1));

    for (Map.Entry<String, Class> fieldInfo : fieldList.entrySet()) {
      String fieldName = fieldInfo.getKey();
      String fieldType = fieldInfo.getValue().getName();
      String fieldJavaType = getJavaType(fieldType);

      hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 23));
      hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ILOAD, 1));
      hashCodeNode.instructions.add(new InsnNode(Opcodes.IMUL));
      hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
      hashCodeNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));

      switch (fieldType) {
        case "boolean":
          LabelNode falseNode = new LabelNode();
          LabelNode trueNode = new LabelNode();
          hashCodeNode.instructions.add(new JumpInsnNode(Opcodes.IFEQ, falseNode));
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.SIPUSH, 1231));
          hashCodeNode.instructions.add(new JumpInsnNode(Opcodes.GOTO, trueNode));
          hashCodeNode.instructions.add(falseNode);
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.SIPUSH, 1237));
          hashCodeNode.instructions.add(trueNode);
          break;
        case "byte":
        case "char":
        case "short":
        case "int":
          break;
        case "float":
          hashCodeNode.instructions
            .add(new MethodInsnNode(Opcodes.INVOKESTATIC, "java/lang/Float", "floatToIntBits", "(F)I", false));
          break;
        case "long":
          hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
          hashCodeNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 32));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LUSHR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LXOR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.L2I));
          break;
        case "double":
          hashCodeNode.instructions
            .add(new MethodInsnNode(Opcodes.INVOKESTATIC, "java/lang/Double", "doubleToLongBits", "(D)J", false));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.DUP2));
          hashCodeNode.instructions.add(new VarInsnNode(Opcodes.LSTORE, 2));
          hashCodeNode.instructions.add(new VarInsnNode(Opcodes.LLOAD, 2));
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 32));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LUSHR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LXOR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.L2I));
          break;
        default:
          String objectOwnerType = fieldType.replace('.', '/');
          LabelNode nullNode = new LabelNode();
          LabelNode continueNode = new LabelNode();
          hashCodeNode.instructions.add(new JumpInsnNode(Opcodes.IFNULL, nullNode));
          hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
          hashCodeNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
          hashCodeNode.instructions
            .add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, objectOwnerType, "hashCode", "()I", false));
          hashCodeNode.instructions.add(new JumpInsnNode(Opcodes.GOTO, continueNode));
          hashCodeNode.instructions.add(nullNode);
          hashCodeNode.instructions.add(new InsnNode(Opcodes.ICONST_0));
          hashCodeNode.instructions.add(continueNode);
          break;
      }
      hashCodeNode.instructions.add(new InsnNode(Opcodes.IADD));
      hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ISTORE, 1));
    }
    hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ILOAD, 1));
    hashCodeNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    classNode.methods.add(hashCodeNode);
  }

  /**
   * Adds a equals method to underlying class. <br>
   * Algorithm is as follows: <br>
   * <i><p>
   * if (this == other) return true;
   * if (other == null) return false;
   * if (other is not instanceof <this class>) return false;
   * for (field: all fields) {
   * if (other.getField() != this.field) return false;
   * }
   * return true;
   * </p></i>
   * <br>
   *
   * @param classNode
   * @param fieldList
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  private static void addEqualsMethod(ClassNode classNode, Map<String, Class> fieldList)
  {
    MethodNode equalsNode = new MethodNode(Opcodes.ACC_PUBLIC, "equals", "(Ljava/lang/Object;)Z", null, null);
    equalsNode.visitAnnotation("Ljava/lang/Override;", true);

    LabelNode l0 = new LabelNode();
    LabelNode l1 = new LabelNode();
    LabelNode l2 = new LabelNode();
    LabelNode l3 = new LabelNode();
    LabelNode l4 = new LabelNode();

    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));

    // if (this == other) return true;
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new JumpInsnNode(Opcodes.IF_ACMPNE, l0));
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_1));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    equalsNode.instructions.add(l0);
    // if (other == null) return false;
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new JumpInsnNode(Opcodes.IFNULL, l1));
    // if (!(other instanceof <this class>)) return false;
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new TypeInsnNode(Opcodes.INSTANCEOF, classNode.name));
    equalsNode.instructions.add(new JumpInsnNode(Opcodes.IFNE, l2));

    equalsNode.instructions.add(l1);
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_0));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    equalsNode.instructions.add(l2);
    // Check if it other object can cast to <this class>
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new TypeInsnNode(Opcodes.CHECKCAST, classNode.name));
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ASTORE, 2));

    int i = 0;
    for (Map.Entry<String, Class> info : fieldList.entrySet()) {
      boolean isLast = ((i + 1) == fieldList.size());
      String fieldName = info.getKey();
      String fieldType = info.getValue().getName();
      String fieldJavaType = getJavaType(fieldType);

      String getterMethodName = (fieldType.equals("boolean") ? "is" : "get") +
          Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
      equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 2));
      equalsNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, classNode.name, getterMethodName,
          "()" + fieldJavaType, false));
      equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
      equalsNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));

      switch (fieldType) {
        case "boolean":
        case "byte":
        case "char":
        case "short":
        case "int":
          equalsNode.instructions
            .add(new JumpInsnNode(isLast ? Opcodes.IF_ICMPEQ : Opcodes.IF_ICMPNE, isLast ? l4 : l3));
          break;
        case "long":
          equalsNode.instructions.add(new InsnNode(Opcodes.LCMP));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFEQ : Opcodes.IFNE, isLast ? l4 : l3));
          break;
        case "float":
          equalsNode.instructions.add(new InsnNode(Opcodes.FCMPL));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFEQ : Opcodes.IFNE, isLast ? l4 : l3));
          break;
        case "double":
          equalsNode.instructions.add(new InsnNode(Opcodes.DCMPL));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFEQ : Opcodes.IFNE, isLast ? l4 : l3));
          break;
        default:
          String objectOwnerType = fieldType.replace('.', '/');

          LabelNode nonNullNode = new LabelNode();
          LabelNode continueNode = new LabelNode();

          equalsNode.instructions.add(new JumpInsnNode(Opcodes.IFNONNULL, nonNullNode));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFNULL : Opcodes.IFNONNULL, isLast ? l4 : l3));

          equalsNode.instructions.add(new JumpInsnNode(Opcodes.GOTO, continueNode));

          equalsNode.instructions.add(nonNullNode);
          equalsNode.instructions.add(new InsnNode(Opcodes.POP));
          equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
          equalsNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
          equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 2));
          equalsNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, classNode.name, getterMethodName,
              "()" + fieldJavaType, false));
          equalsNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, objectOwnerType, "equals",
              "(Ljava/lang/Object;)Z", false));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFNE : Opcodes.IFEQ, isLast ? l4 : l3));

          equalsNode.instructions.add(continueNode);
          break;
      }
      i++;
    }

    equalsNode.instructions.add(l3);
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_0));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    equalsNode.instructions.add(l4);
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_1));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    classNode.methods.add(equalsNode);
  }

  private static String getJavaType(String fieldType)
  {
    Character pchar = PRIMITIVE_TYPES.get(fieldType);
    if (pchar != null) {
      //it is a primitive type
      return Character.toString(pchar);
    }
    //non-primitive so find the internal name of the class.
    return 'L' + fieldType.replace('.', '/') + ';';
  }
}
