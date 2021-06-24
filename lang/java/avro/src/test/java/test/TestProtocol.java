package test;

import static org.junit.Assert.*;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.junit.Before;
import org.junit.Test;

public class TestProtocol {
	
	String pName;
	String doc;
	String nullDoc;
	String namespace;
	Protocol p1;
	String propName;
	String propValue;
	String msgName;
	Schema schema;
	Schema schema2;
	String schemaName;
	String docSchema;
	Message msg;
	Collection<Schema> colSchema;
	String jsonFormat;					//white box because no other ways
	String jsonFormatNoTypes;
	String baseMsg;
	String noMsg;
	String wrongMsg;
	String rightOneWayMsg;
	String wrongOneWayMsg;
	String rightTwoWayMsg;
	String noBoolOneWayMsg;
	String toUse;
	JsonProperties prop;
	Map<String, String> propMap;
	
	@Before
	public void configure() {
		pName = "Pname";
		doc = "doc";
		nullDoc = null;
		namespace = "foo";
		propName = "fooProperty";
		propValue = "fooValue";
		msgName = "fooMessage";
		schemaName = "sName";
		docSchema = "doc";
		p1 = new Protocol(pName, nullDoc, namespace);
		p1.addProp(propName, propValue);
		schema = Schema.createRecord(schemaName, docSchema, namespace, false);
		schema2 = Schema.createRecord(pName, docSchema, namespace, true);
		//+ "\"doc\"" + doc + "",\""
		jsonFormat = "{\"protocol\":\"" + pName + "\",\"namespace\":\"" + namespace + "\",\"" + propName + "\":\"" +
				propValue + "\",";
		jsonFormatNoTypes = jsonFormat + "\"types\":[],";
		noMsg = "\"messages\":{}}";
		wrongMsg = "\"messages\":{\"" + msgName + "\": \"" + msgName + "\"}}";
		baseMsg = "\"types\":[{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}],"
				+ "\"messages\":{\"" + msgName + "\":{\"request\":[{\"name\":\"greeting\",\"type\":\"Greeting\"}]";
		wrongOneWayMsg = baseMsg + "} } }";
		rightTwoWayMsg = baseMsg + ",\"response\":\"Greeting\"}}}";
		noBoolOneWayMsg = baseMsg + ",\"one-way\":\"true\"}}}";
		rightOneWayMsg = baseMsg + ",\"response\":\"null\",\"one-way\":true}}}";
		
		colSchema = new ArrayList<>();
		colSchema.add(schema);
		propMap = new HashMap();
		propMap.put(propName, propValue);
	}

	@Test
	public void testPropEquals() {
		
		Protocol p2 = new Protocol(pName, doc, namespace);
		p2.addProp("a", "2");
		assertFalse(p1.equals(p2));
	}
	
	@Test
	public void testCopyProtocolAndEquals() {
		
		Protocol p2 = new Protocol(p1);
		assertEquals(p1, p2);
		assertTrue(p1.equals(p2));
		
		Protocol p3 = new Protocol(pName, namespace);
		p1.addProp("a", "1");
		Protocol p4 = new Protocol(p3);
		assertEquals(p3, p4);				//test equals
		assertTrue(p3.equals(p4));
		assertTrue(p1.equals(p1));
		assertFalse(p1.equals(p4));
		assertFalse(p1.equals(pName));
		Protocol p5 = new Protocol("other Name", null, namespace);			//to reach branch coverage
		Protocol p6 = new Protocol(pName, null, "other namespace");
		Protocol p7 = new Protocol(pName, "other doc", namespace);
		Protocol p8 = new Protocol(pName, doc, namespace);
		p8.addProp("property", "some value");
		
		Protocol p9 = Protocol.parse(jsonFormat + rightOneWayMsg);
		assertFalse(p1.equals(p5));
		assertFalse(p1.equals(p6));
		assertFalse(p1.equals(p7));
		assertFalse(p1.equals(p8));
		assertFalse(p1.equals(p9));
	}
	
	@Test
	public void setAndTestTypes() {
		
		p1.setTypes(colSchema);
		assertEquals("[{\"type\":\"record\",\"name\":\"" + schemaName +  "\",\"namespace\":\"" + namespace + "\",\"doc\":\"doc\"}]", p1.getTypes().toString());
		assertNull(p1.getType(propName));
	}
	
	@Test
	public void testGetMethods() throws NoSuchAlgorithmException {

		toUse = p1.toString();
		assertEquals(pName, p1.getName());
		assertEquals(namespace, p1.getNamespace());
		
		assertEquals(nullDoc, p1.getDoc());
		assertEquals(propValue, p1.getProp(propName));
		assertEquals(jsonFormatNoTypes + noMsg, Protocol.parse(toUse).toString());
		toUse = p1.toString(true);
		assertEquals(jsonFormatNoTypes + noMsg, Protocol.parse(toUse).toString());
		
		p1 = new Protocol(pName, doc, namespace);
		assertEquals(doc, p1.getDoc());
		assertNotNull(Protocol.parse(toUse).toString());
		
		p1 = Protocol.parse(jsonFormat + rightOneWayMsg);
		toUse = p1.toString();
		assertEquals(jsonFormat + rightOneWayMsg, Protocol.parse(toUse).toString());
		
		p1 = Protocol.parse(jsonFormatNoTypes + noMsg);
		toUse = p1.toString();
		assertEquals(jsonFormatNoTypes + noMsg, Protocol.parse(toUse).toString());

		assertNotNull(p1.getMD5());
		assertNotNull(p1.getMD5());				//branch coverage
		assertNotNull(p1.hashCode());
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException1() {
		Protocol.parse(jsonFormat + wrongMsg).toString();
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException2() {
		Protocol.parse(jsonFormat + wrongOneWayMsg).toString();
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException3() {
		Protocol.parse(jsonFormat + noBoolOneWayMsg).toString();
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException4() {
		Protocol.parse(rightOneWayMsg);
	}
	
	@Test(expected=NullPointerException.class)
	public void nullPointer() {
		p1.createMessage(pName, doc, prop, schema).toString();
	}
	
	@Test(expected=NullPointerException.class)
	public void nullPointer2() {
		p1.createMessage(msgName, doc, prop, schema, schema2, schema).toString();
	}
	
	@Test(expected=NullPointerException.class)
	public void nullPointer3() {
		p1.createMessage(msgName, doc, schema, schema, schema).hashCode();
	}
	
	@Test(expected=NullPointerException.class)
	public void nullPointer4() {
		p1.createMessage(msgName, doc, schema, schema, schema).hashCode();
	}
	
	@Test
	public void testParseProtocol() {

		assertNotNull(Protocol.parse(jsonFormat + rightTwoWayMsg).getMessages());
		assertNotNull(Protocol.parse(jsonFormat + rightOneWayMsg).getMessages());
	}
	
	@Test
	public void testOneWayMessage() {
		
		assertEquals("{}", p1.getMessages().toString());
		
		msg = p1.createMessage(msgName, doc, schema);
		assertEquals(schema, msg.getRequest());
		assertEquals(doc, msg.getDoc());
		assertEquals(msgName, msg.getName());
		assertEquals("[]",msg.getErrors().toString());
		assertEquals("\"null\"", msg.getResponse().toString());
		assertTrue(msg.isOneWay());
		//fail(msg.toString());
		//assertNotNull(msg.hashCode());
	}
	
	@Test
	public void testTwoWayMessage() {
		
		msg = p1.createMessage(msgName, doc, schema, schema, schema);
		assertFalse(msg.isOneWay());
		assertNotNull(msg.getErrors());
		assertNotNull(msg.getResponse());
		assertEquals(schema, msg.getRequest());
		assertEquals(doc, msg.getDoc());
		assertEquals(msgName, msg.getName());
		assertEquals(schema, msg.getErrors());
		assertEquals(schema, msg.getResponse());
		
		//System.out.println(msg);
		//fail(msg.toString());
		//assertNotNull(msg.hashCode());
	}
	
	@Test
	public void testCreateTwoWayMessage() {
		
		msg = p1.createMessage(msgName, doc, schema, schema, schema);
		Message msg2 = p1.createMessage(msg, schema, schema, schema);
		assertFalse(msg2.isOneWay());
		Message msg3 = p1.createMessage(msgName, doc, prop, schema, schema2, schema);
		assertFalse(msg2.isOneWay());
		Message msg4 = p1.createMessage(msgName, doc, propMap, schema, schema, schema2);
		assertFalse(msg2.isOneWay());
		Message msg5 = p1.createMessage(msgName, doc, schema);
		
		
		assertTrue(msg.equals(msg));
		assertFalse(msg.equals(msgName));
		assertTrue(msg.equals(msg2));		// to reach branch coverage
		assertFalse(msg.equals(msg3));
		assertFalse(msg.equals(msg4));
		assertFalse(msg.equals(msg5));
	}
	
	@Test
	public void testCreateOneWayMessage() {

		msg = p1.createMessage(msgName, doc, schema);
		Message msg2 = p1.createMessage(msg, schema);
		Message msg3 = p1.createMessage(msgName, doc, schema);
		Message msg4 = p1.createMessage("otherName", doc, schema);
		schema = Schema.createRecord("otherName", docSchema, namespace, false);
		Message msg5 = p1.createMessage("otherName", doc, schema);
				
		assertEquals(pName, msg2.getName());
		assertNull(msg.getObjectProp(namespace));
		assertTrue(msg.equals(msg));
		assertFalse(msg.equals(msg2));
		assertFalse(msg.equals(msgName));				// to reach branch coverage
		assertTrue(msg.equals(msg3));
		assertFalse(msg.equals(msg4));
		assertFalse(msg.equals(msg5));
		

		Message msg6 = p1.createMessage(pName, doc, prop, schema);
		assertTrue(msg6.isOneWay());
		assertFalse(msg.equals(msg6));
		Message msg7 = p1.createMessage(pName, doc, propMap, schema);
		assertTrue(msg7.isOneWay());
		assertFalse(msg.equals(msg7));
	}
}