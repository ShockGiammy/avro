package myTest;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

public class TestSchema {
	
	Schema schema;
	String schemaName;
	String schemaName2;
	String docSchema;
	String namespace;
	String recordName;
	List<Field> fields;
	String fieldName;
	String fieldName2;
	List<Schema> types;
	Schema.Field field;
	Schema.Field field2;
	String defaultValue;
	String wrongValue;
	Schema schema2;
	String propName;
	String propValue;
	String formatToParse;
	Map<String, Schema> schemaMap;
	Schema.Parser parser;
	
	@Before
	public void configure() {
		schemaName = "sName";
		schemaName2 = "sName2";
		docSchema = "doc";
		namespace = "fooNamespace";
		recordName = "recordName";
		defaultValue = "value";
		wrongValue = "wrong";
		propName = "fooProperty";
		propValue = "fooValue";
		
		fieldName = "fooField";
		fieldName2 = "fooField2";
		schema = Schema.createRecord(schemaName, docSchema, namespace, false);
		schema2 = Schema.createRecord(schemaName2, docSchema, namespace, false);
		field = new Field(fieldName, Schema.create(Type.NULL), null, null);
		field2 = new Field(fieldName2, Schema.create(Type.INT), null, null);
		fields = new ArrayList<>();
		types = new ArrayList<>();
		
		formatToParse = "{\"type\":\"record\"," + "\"name\":\"Child\"," + "\"namespace\":\"org.apache.avro.nested\","
		        + "\"fields\":" + "[{\"name\":\"childField\",\"type\":\"string\"}]}";
		
		schemaMap = new HashMap<String, Schema>();
		schemaMap.put(schemaName, schema);
		parser = new Schema.Parser();
	}
	
	//test set fields

	@Test(expected=AvroRuntimeException.class)
	public void testDefaultRecordWithDuplicateFieldName() {

		fields.add(new Field(fieldName, Schema.create(Type.NULL), null, null));
		fields.add(new Field(fieldName, Schema.create(Type.INT), null, null));
		schema.setFields(fields);
	}
	
	@Test(expected=AvroRuntimeException.class)
	public void testCallTwoTimesSetField() {

		fields.add(new Field(fieldName, Schema.create(Type.NULL), null, null));
		schema.setFields(fields);
		schema.setFields(fields);
		fail("AvroRuntime Fields are already set");
	}
	
	//by implementation
	@Test(expected=AvroRuntimeException.class)
	public void testFieldAlreadyUsed() {

		fields.add(new Field(fieldName, Schema.create(Type.NULL), null, null));
		schema.setFields(fields);
		schema2.setFields(fields);
	}
	
	//Test schema record

	@Test
	public void testRecordWithNullDoc() {
	  
		schema = Schema.createRecord(schemaName, null, namespace, false);
		String schemaString = schema.toString();
		assertNotNull(schemaString);
	}

	@Test
	public void testRecordWithNullNamespace() {
	  
		schema = Schema.createRecord(schemaName, docSchema, null, false);
		String schemaString = schema.toString();
		assertNotNull(schemaString);
	}
	
	@Test
  	public void testSchemaWithNullName() {
    
  		Schema.createRecord(null, docSchema, namespace, false);
  	}
	
	@Test(expected = NullPointerException.class)
  	public void testSchemaWithNullFields() {
    
  		Schema.createRecord(schemaName, docSchema, namespace, false, null);
  		//manca controllo su fields = null;
  	}
	
	@Test
	public void testEmptyRecordSchema() {
	  
		schema = Schema.createRecord(fields);
		assertNotNull(schema.toString());
	}
	
	@Test(expected = NullPointerException.class)
	public void testEmptyRecordSchemaWithNullFields() {
	  
		schema = Schema.createRecord(null);
		//manca controllo su fields = null;
	}
  
	@Test
	public void testRecordIsErrorTrue() {
		
		schema = Schema.createRecord(schemaName, docSchema, namespace, true);
		assertNotNull(schema.toString());
		assertTrue(schema.isError());
	}
	
	@Test
	public void testRecordIsErrorFalse() {
		
		schema = Schema.createRecord(schemaName, docSchema, namespace, false);
		assertNotNull(schema.toString());
		assertFalse(schema.isError());
	}
  
	@Test
	public void testSchemaWithFields() {
	  
		fields.add(field);
		fields.add(field2);
    
		schema.setFields(fields);
		assertNotNull(schema.toString());
		assertEquals(2, schema.getFields().size()); 
	}
	
	@Test(expected = AvroRuntimeException.class)
  	public void testNotFieldSetted() {
	  
  		schema.getField(fieldName);
  	}
	
  
	@Test
	public void equalsFieldsAndPrettyToString() {
	  
		schema.setFields(fields);				//empty array
	  
		schema2 = Schema.createRecord(schemaName, docSchema, namespace, false, fields);
		assertEquals(schema.toString(), schema2.toString());
		assertEquals(schema.toString(true), schema2.toString(true));
		assertEquals(schema.toString(false), schema2.toString(false));
		assertEquals(schema.toString(null, false), schema2.toString(null, false));
	  
		schema2.addProp(propName, propValue);
		assertNotEquals(schema.toString(), schema2.toString());
		schema.addProp(propName, propValue);
		assertEquals(schema.toString(), schema2.toString());
	  
		fields.add(field);
		assertEquals(schema.toString(), schema2.toString());
	}
  
	
	@Test
	public void testDisableDefaultValidate() {
		 
		assertTrue(parser.getValidate());
		parser.setValidateDefaults(false);
		assertFalse(parser.getValidateDefaults());
	}
	  
	@Test
	public void testAddTypes() {
		
		parser.addTypes(schemaMap);
		assertEquals(schemaMap.toString().split("=")[1], parser.getTypes().toString().split("=")[1]);	
	}
  
	@Test
	public void setAndGetAliases() {
	  
		assertTrue(field.aliases().isEmpty());
		field.addAlias(propName);
		assertFalse(field.aliases().isEmpty());
	}
	
	@Test
  	public void testIsNullableOnRecord() {

  		assertFalse(schema.isNullable());
  	}
  
  	@Test
  	public void testAddPropString() {
	  
  		schema.addProp(propName, propValue);
  		assertEquals(propValue, schema.getProp(propName));
  	}
  
  	@Test
  	public void testAddPropObj() {
	  
  		schema.addProp(propName, (Object) propValue);
  		assertEquals(propValue, schema.getProp(propName));
  	}
  	
  	@Test
  	public void testIsUnionOnRecord() {
	  
  		assertFalse(schema.isUnion());
  		assertFalse(schema.isError());
  		assertEquals(docSchema, schema.getDoc());
  		schema.addAlias(defaultValue);
  		assertNotNull(schema.getAliases());
  	}
  	
  	@Test(expected = AvroRuntimeException.class)
  	public void notFixed() {
  		schema.getFixedSize();
  	}
  	
  	@Test(expected = AvroRuntimeException.class)
    public void getIndexNamedTest() {
  		schema.getIndexNamed(defaultValue);
    }
  	
  	@Test(expected = AvroRuntimeException.class)
    public void getElementTypeTest() {
  		schema.getElementType();
    }
    
    @Test(expected = AvroRuntimeException.class)
    public void getValueTypeTest() {
    	schema.getValueType();
    }
    
    @Test(expected = AvroRuntimeException.class)
    public void getTypesTest() {
  	  	schema.getTypes();
    }
    
    @Test(expected = AvroRuntimeException.class)
    public void getEnumSymbolsTest() {
    	schema.getEnumSymbols();
    }
    
    @Test(expected = AvroRuntimeException.class)
    public void getEnumDefaultTest() {
  	  	schema.getEnumDefault();
    }
    
    @Test(expected = AvroRuntimeException.class)
    public void getEnumOrdinalTest() {
  	  	schema.getEnumOrdinal(defaultValue);
    }
    
    @Test(expected = AvroRuntimeException.class)
    public void hasEnumSymbolTest() {
  	  	schema.hasEnumSymbol(defaultValue);
    }
    
  

  	
  	//test Union
  	@Test
  	public void testIsUnionOnUnionWithMultipleElements() {
	  
  		schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
  		assertTrue(schema.isUnion());
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void testAddPropOnUnion() {
	  
  		schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
  		schema.addProp(propName, propValue);
  	}

  	@Test
  	public void testIsUnionOnUnionWithOneElement() {
	  
  		schema = Schema.createUnion(Schema.create(Type.LONG));
  		assertTrue(schema.isUnion());
  		assertNotNull(schema.getName());
  	}

  	@Test
	public void testCreateUnionVarargs() {

		types.add(Schema.create(Type.NULL));
		types.add(Schema.create(Type.LONG));
		Schema expected = Schema.createUnion(types);

		Schema schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
		assertEquals(expected, schema);
	}
  	
  	
  	// test Enum
  	@Test
  	public void testIsUnionOnEnum() {
	  
  		schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"));
  		assertFalse(schema.isUnion());
    
  		schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"), defaultValue);
  		assertFalse(schema.isUnion());
  	}
  	
  	@Test
  	public void NotNullValuesOnEnum() {
  		
  		schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"), defaultValue);
  		assertNotNull(schema.getEnumSymbols());
  		assertNotNull(schema.hasEnumSymbol(defaultValue));
  		assertNotNull(schema.getEnumOrdinal(defaultValue));
  		
  	}
  	
  	@Test(expected = SchemaParseException.class)
    public void testValueNotInSimbolSet() {
  	  
  		schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"), wrongValue);
    }
  	

  	@Test
  	public void testIsNullableOnUnionWithNull() {
	  
  		schema = Schema.createUnion(Schema.create(Type.NULL), Schema.create(Type.LONG));
  		assertTrue(schema.isNullable());
  		assertNotNull(schema.getTypes());
  		assertNull(schema.getIndexNamed(schemaName));
  	}	

  	@Test
  	public void testIsNullableOnUnionWithoutNull() {
	  
  		schema = Schema.createUnion(Schema.create(Type.LONG));
  		assertFalse(schema.isNullable());
  	}
  	
  	
  	
  	//test Array
  	
  	@Test
  	public void testIsUnionOnArray() {
	  
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		assertFalse(schema.isUnion());
  		assertNotNull(schema.getElementType());
  	}

  	
  
  	@Test(expected = AvroRuntimeException.class)
  	public void addAliasTest() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.addAlias(defaultValue);
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void addAliasTest2() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.addAlias(defaultValue, defaultValue);
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void getAliasesTest() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.getAliases();
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void isErrorTest() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.isError();
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void getFieldTest() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.getField(fieldName);
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void getFieldsTest() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.getFields();
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void setFieldsTest() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.setFields(fields);
  	}
  
  
  	@Test(expected = AvroRuntimeException.class)
  	public void getNamespaceTest() {
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		schema.getNamespace();
  	}
  	
  	
  	
  	//test Fixed
  	@Test
  	public void testIsUnionOnFixed() {
	  
  		schema = Schema.createFixed(schemaName, docSchema, namespace, 10);
  		assertFalse(schema.isUnion());
  		assertNotNull(schema.getFixedSize());
  	}
  
  	@Test(expected = IllegalArgumentException.class)
  	public void testFailFixed() {
	  
  		schema = Schema.createFixed(schemaName, docSchema, namespace, -1);
  	}
  	
  	//test Map

  	@Test
  	public void testIsUnionOnMap() {
	  
  		schema = Schema.createMap(Schema.create(Type.LONG));
  		assertFalse(schema.isUnion());
  		assertNull(schema.getDoc());
  		assertNotNull(schema.getValueType());
  	}
  	
  	
  //test On Field (validate Name)
    
  	//test to reach condition coverage
  	@Test(expected = SchemaParseException.class)
  	public void testValidateName() {
  		field = new Field(null, Schema.create(Type.NULL), null, null);
  	}
  
  	@Test(expected = SchemaParseException.class)
  	public void testValidateName2() {
  		field = new Field("", Schema.create(Type.NULL), null, null);
  	}
  
  	@Test
  	public void testValidateName3() {
  		field = new Field("_" + fieldName, Schema.create(Type.NULL), null, null);
  		assertEquals("_" + fieldName, field.name());
  	}
  
  	@Test
  	public void testValidateName4() {
  		field = new Field(fieldName + "_", Schema.create(Type.NULL), null, null);
  		assertEquals(fieldName + "_", field.name());
  	}
  
  	@Test(expected = SchemaParseException.class)
  	public void testValidateName5() {
  		field = new Field("1" + fieldName, Schema.create(Type.NULL), null, null);
  	}
  
  	@Test(expected = SchemaParseException.class)
  	public void testValidateName6() {
  		field = new Field(fieldName + "?", Schema.create(Type.NULL), null, null);
  	}

  	
  	@Test
	public void copyFieldAndEquals() {			//condition coverage
	  
		assertEquals(field, field);
		assertNotNull(field.pos());
	  
		schema = Schema.create(Type.NULL);
		field2 = new Field(field, schema);
		assertEquals(field, field2);
		assertNotEquals(field, fieldName);
	  
		field2 = new Field(fieldName, schema);
		assertEquals(fieldName, field2.name());
		field2 = new Field(fieldName, schema, docSchema);
		assertEquals(fieldName, field2.name());
		assertEquals(docSchema, field2.doc());
		field = new Field(fieldName, schema, docSchema, null, Order.ASCENDING);
	  	assertEquals(field.toString(), field2.toString());
	}
  	
  	
  	


  	@Test
  	public void testSerialization() throws IOException, ClassNotFoundException {
	  
  		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
  			ObjectOutputStream oos = new ObjectOutputStream(bos);
  			InputStream jsonSchema = getClass().getResourceAsStream("/SchemaBuilder.avsc")) {
  			
  			Schema payload = new Schema.Parser().parse(jsonSchema);
  			oos.writeObject(payload);

  			try (ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
  				ObjectInputStream ois = new ObjectInputStream(bis)) {
  				Schema sp = (Schema) ois.readObject();
        assertEquals(payload, sp);
  			}
  		}
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void testApplyAliasesFail() {
	  
  		Schema.applyAliases(schema2, schema);
  	}
  
  	@Test(expected = AvroRuntimeException.class)
  	public void testApplyAliasesFail2() {
	  
  		schema.addAlias(defaultValue);
  		assertEquals(schema2, Schema.applyAliases(schema, schema2));
  	}
  
  	
  	//test ApplyAliases
  	
  	@Test
  	public void testApplyAliasesRecord() {
	  
  		fields.add(field);
  		schema2.setFields(fields);
  		assertEquals(field, schema2.getField(fieldName));
	  
  		schema2.addAlias(defaultValue);
	  
  		fields.remove(0);
  		fields.add(field2);
  		schema.addAlias(defaultValue);
  		schema.setFields(fields);
	  
  		assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
  		schema = Schema.createMap(Schema.create(Type.LONG));			//condition coverage
  		assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
  		schema = Schema.createUnion(Schema.create(Type.LONG));
  		assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
  		schema = Schema.createFixed(schemaName, docSchema, namespace, 10);
  		schema.addAlias(defaultValue);
  		assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
  		schema = Schema.createArray(Schema.create(Type.LONG));
  		assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
  		schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"));
  		schema.addAlias(defaultValue);
  		assertEquals(schema, Schema.applyAliases(schema, schema2));
  	}
  
  @Test
  public void testApplyAliasesMap() {
	  
	  schema2 = Schema.createMap(Schema.create(Type.LONG));

	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema.addAlias(defaultValue);
	  schema.setFields(fields);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createMap(Schema.create(Type.LONG));			//condition coverage
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createUnion(Schema.create(Type.LONG));
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createFixed(schemaName, docSchema, namespace, 10);
	  schema.addAlias(defaultValue);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createArray(Schema.create(Type.LONG));
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"));
	  schema.addAlias(defaultValue);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
  }

  
  @Test
  public void testApplyAliasesUnion() {
	  
	  schema2 = Schema.createUnion(Schema.create(Type.LONG));

	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema.addAlias(defaultValue);
	  schema.setFields(fields);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createMap(Schema.create(Type.LONG));			//condition coverage
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createUnion(Schema.create(Type.LONG));
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createFixed(schemaName, docSchema, namespace, 10);
	  schema.addAlias(defaultValue);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createArray(Schema.create(Type.LONG));
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"));
	  schema.addAlias(defaultValue);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
  }
  
  
  @Test
  public void testApplyAliasesArray() {
	  
	  schema2 = Schema.createArray(Schema.create(Type.LONG));

	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema.addAlias(defaultValue);
	  schema.setFields(fields);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createMap(Schema.create(Type.LONG));			//condition coverage
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createUnion(Schema.create(Type.LONG));
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createFixed(schemaName, docSchema, namespace, 10);
	  schema.addAlias(defaultValue);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createArray(Schema.create(Type.LONG));
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
	  
	  schema = Schema.createEnum(schemaName, docSchema, namespace, Collections.singletonList("value"));
	  schema.addAlias(defaultValue);
	  assertEquals(schema, Schema.applyAliases(schema, schema2));
  }

  
  	//test Parser

  	@Test
  	public void testStringParse() {
	  
  		Schema.Parser parser = new Schema.Parser();
  		assertEquals(formatToParse, parser.parse(formatToParse).toString());
  		assertEquals(formatToParse, Schema.parse(formatToParse, true).toString());
  		assertEquals(formatToParse, Schema.parse(formatToParse, false).toString());
  		assertEquals(formatToParse, Schema.parse(formatToParse).toString());
  	}  
  
  	@Test
  	public void testParseFInputStream() throws IOException {

		InputStream inStream = getClass().getResourceAsStream("/TestRecordWithLogicalTypes.avsc");
		assertNotNull(Schema.parse(inStream).toString());
  	}
  
  	@Test
  	public void testParseFile() {
	  
  		File file = null;
  		try {
  			file = new File(getClass().getResource("/TestRecordWithLogicalTypes.avsc").toURI());
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		try {
			assertEquals(Files.readString(Paths.get(getClass().getResource("/TestRecordWithLogicalTypes.avsc").toURI())).replaceAll("[\\n\\t ]", ""), Schema.parse(file).toString());
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
		Schema.Parser parser = new Schema.Parser();
		try {
			assertEquals(Files.readString(Paths.get(getClass().getResource("/TestRecordWithLogicalTypes.avsc").toURI())).replaceAll("[\\n\\t ]", ""), parser.parse(file).toString());
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
  	}
  
  	@Test
  	public void testParseStrings() throws IOException {
	  
  		Schema.Parser parser = new Schema.Parser();
  		assertEquals(formatToParse + "", parser.parse(formatToParse, "").toString());
  	}
  	
  	
  	//test DefaultValue
  	public void testIntDefaultValue() {
	  
  		field = new Schema.Field(fieldName, Schema.create(Schema.Type.INT), docSchema, 1);
  		assertTrue(field.hasDefaultValue());
    	assertEquals(1, field.defaultVal());
    	assertEquals(1, GenericData.get().getDefaultValue(field));

    	field = new Schema.Field(fieldName, Schema.create(Schema.Type.INT), docSchema, Integer.MIN_VALUE);
    	assertTrue(field.hasDefaultValue());
    	assertEquals(Integer.MIN_VALUE, field.defaultVal());
    	assertEquals(Integer.MIN_VALUE, GenericData.get().getDefaultValue(field));

    	field = new Schema.Field(fieldName, Schema.create(Schema.Type.INT), docSchema, Integer.MAX_VALUE);
    	assertTrue(field.hasDefaultValue());
    	assertEquals(Integer.MAX_VALUE, field.defaultVal());
    	assertEquals(Integer.MAX_VALUE, GenericData.get().getDefaultValue(field));
  	}	

  @Test
  public void testValidLongAsIntDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.INT), docSchema, 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1, field.defaultVal());
    assertEquals(1, GenericData.get().getDefaultValue(field));

    field = new Schema.Field(fieldName, Schema.create(Schema.Type.INT), docSchema, Long.valueOf(Integer.MIN_VALUE));
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MIN_VALUE, field.defaultVal());
    assertEquals(Integer.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Schema.Field(fieldName, Schema.create(Schema.Type.INT), docSchema, Long.valueOf(Integer.MAX_VALUE));
    assertTrue(field.hasDefaultValue());
    assertEquals(Integer.MAX_VALUE, field.defaultVal());
    assertEquals(Integer.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testLongDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.LONG), docSchema, 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1L, field.defaultVal());
    assertEquals(1L, GenericData.get().getDefaultValue(field));

    field = new Schema.Field(fieldName, Schema.create(Schema.Type.LONG), docSchema, Long.MIN_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Long.MIN_VALUE, field.defaultVal());
    assertEquals(Long.MIN_VALUE, GenericData.get().getDefaultValue(field));

    field = new Schema.Field(fieldName, Schema.create(Schema.Type.LONG), docSchema, Long.MAX_VALUE);
    assertTrue(field.hasDefaultValue());
    assertEquals(Long.MAX_VALUE, field.defaultVal());
    assertEquals(Long.MAX_VALUE, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testIntAsLongDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.LONG), docSchema, 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1L, field.defaultVal());
    assertEquals(1L, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testDoubleDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.DOUBLE), docSchema, 1.0);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testIntAsDoubleDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.DOUBLE), docSchema, 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testLongAsDoubleDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.DOUBLE), docSchema, 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testFloatAsDoubleDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.DOUBLE), docSchema, 1.0f);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0d, field.defaultVal());
    assertEquals(1.0d, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testFloatDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.FLOAT), docSchema, 1.0f);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testIntAsFloatDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.FLOAT), docSchema, 1);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }

  @Test
  public void testLongAsFloatDefaultValue() {
	  
    field = new Schema.Field(fieldName, Schema.create(Schema.Type.FLOAT), docSchema, 1L);
    assertTrue(field.hasDefaultValue());
    assertEquals(1.0f, field.defaultVal());
    assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  }
  
  	@Test
	public void testNullDefaultValue() {
	  
	  field = new Schema.Field(fieldName, Schema.create(Schema.Type.NULL), docSchema);
	  assertFalse(field.hasDefaultValue());
	}

  	@Test
  	public void testDoubleAsFloatDefaultValue() {
	  
  		field = new Schema.Field(fieldName, Schema.create(Schema.Type.FLOAT), docSchema, 1.0d);
  		assertTrue(field.hasDefaultValue());
  		assertEquals(1.0f, field.defaultVal());
  		assertEquals(1.0f, GenericData.get().getDefaultValue(field));
  	}

  	@Test(expected = AvroRuntimeException.class)
  	public void testCreateException() {
	  
  		field = new Schema.Field(fieldName, Schema.create(Schema.Type.ARRAY), docSchema, 1.0d);
  	}
}