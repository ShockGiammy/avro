package myTest;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
	Protocol p2;
	String propName;
	String propValue;
	String msgName;
	Schema schema;
	Schema schema2;
	String schemaName;
	String docSchema;
	Message msg;
	Message msg2;
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
	File file;
	InputStream inStream;
	InputStream inStream2;
	
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
		propMap = new HashMap<String, String>();
		propMap.put(propName, propValue);
		file = null;
	}
	
	
	@Test
	public void testCopyProtocol() throws IOException {
		
		p2 = new Protocol(p1);
		assertEquals(p1, p2);
		assertTrue(p1.equals(p2));
		
		p1.addProp(propName, propValue);
		p2 = new Protocol(pName, namespace);
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol(p1);
		assertEquals(p1, p2);				//test equals
		assertTrue(p1.equals(p2));
	}
	
	@Test
	public void testEquals() throws IOException {
		
		
		p1.addProp(propName, propValue);

		assertTrue(p1.equals(p1));
		assertFalse(p1.equals(pName));
		
		p2 = new Protocol(pName, namespace);
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol("other Name", null, namespace);			//to reach branch coverage
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol(pName, null, "other namespace");
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol(pName, "other doc", namespace);
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol(pName, nullDoc, namespace);
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol(pName, doc, namespace);
		p2.addProp(propName, "other value");
		assertFalse(p1.equals(p2));
		
		p2 = Protocol.parse(jsonFormat + rightOneWayMsg);
		assertFalse(p1.equals(p2));
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
	
	
	//test Protocol.parse	
	@Test
	public void testParseProtocolString() {

		assertNotNull(Protocol.parse(jsonFormat + rightTwoWayMsg).getMessages());
		assertNotNull(Protocol.parse(jsonFormat + rightOneWayMsg).getMessages());
		
		p1 = Protocol.parse(jsonFormat + rightOneWayMsg);
		toUse = p1.toString();
		assertEquals(jsonFormat + rightOneWayMsg, Protocol.parse(toUse).toString());
		
		p1 = Protocol.parse(jsonFormatNoTypes + noMsg);
		toUse = p1.toString();
		assertEquals(jsonFormatNoTypes + noMsg, Protocol.parse(toUse).toString());
		
		assertEquals(jsonFormat + rightOneWayMsg, Protocol.parse(jsonFormat + rightOneWayMsg).toString());
		assertEquals(jsonFormat + rightTwoWayMsg, Protocol.parse(jsonFormat + rightTwoWayMsg).toString());
		
		assertEquals(jsonFormat + rightOneWayMsg, Protocol.parse(jsonFormat, rightOneWayMsg).toString());
		assertEquals(jsonFormat + rightTwoWayMsg, Protocol.parse(jsonFormat, rightTwoWayMsg).toString());
	}
	
	public void testParseInputStream() throws IOException {

		InputStream inStream = getClass().getResourceAsStream("/example.avpr");
		assertNotNull(Protocol.parse(inStream).toString());
	}
	
	@Test
	public void testParseDifferentInputStream() throws IOException {
		
		inStream = getClass().getResourceAsStream("/example.avpr");
		inStream2 = getClass().getResourceAsStream("/exampleMsgDifferent.avpr");
		assertFalse(Protocol.parse(inStream).equals(Protocol.parse(inStream2)));
	}

	@Test
	public void testParseFile() throws IOException {

		try {
			file = new File(getClass().getResource("/example.avpr").toURI());
			assertEquals(Files.readString(Paths.get(getClass().getResource("/example.avpr").toURI())).replaceAll("[\\n\\t ]", ""), Protocol.parse(file).toString());
		} catch (URISyntaxException | IOException e) {
			e.printStackTrace();
		}
		try {
			file = new File(getClass().getResource("/exampleNoMsg.avpr").toURI());
			assertNotNull(Protocol.parse(file).toString());
			assertEquals("{}", Protocol.parse(file).getMessages().toString());
		} catch (URISyntaxException | IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test(expected=SchemaParseException.class)
	public void testParseInputStreamWrong() throws IOException {
		InputStream inStream = getClass().getResourceAsStream("/SchemaBuilder.avsc");
		assertEquals(inStream.toString(), Protocol.parse(inStream).toString());
	}
	
	@Test(expected=SchemaParseException.class)
	public void testParseFileWrong() throws IOException {
		File file = null;
		try {
			file = new File(getClass().getResource("/SchemaBuilder.avsc").toURI());
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		assertEquals(file.toString(), Protocol.parse(file).toString());
	}
	
	
	//test One-Way Message	
	@Test
	public void testOneWayMessage() {
		
		assertEquals("{}", p1.getMessages().toString());
		
		msg = p1.createMessage(msgName, doc, schema);
		assertEquals(schema, msg.getRequest());
		assertEquals(doc, msg.getDoc());
		assertEquals(msgName, msg.getName());
		assertEquals("[]",msg.getErrors().toString());
		assertEquals("\"null\"", msg.getResponse().toString());
		assertNull(msg.getObjectProp(namespace));
		assertTrue(msg.isOneWay());
	}
	
	@Test
	public void testCreateOneWayMessage() {

		msg = p1.createMessage(msgName, doc, schema);
		assertTrue(msg.equals(msg));
		
		msg2 = p1.createMessage(msg, schema);
		assertEquals(pName, msg2.getName());
		assertFalse(msg.equals(msg2));
		
		assertFalse(msg.equals(msgName));				// to reach branch coverage
		
		msg2 = p1.createMessage(msgName, doc, schema);
		assertTrue(msg2.isOneWay());
		assertTrue(msg.equals(msg2));

		
		msg2 = p1.createMessage("otherName", doc, schema);
		assertTrue(msg2.isOneWay());
		assertFalse(msg.equals(msg2));
		
		msg2 = p1.createMessage(msgName, doc, schema2);
		assertTrue(msg2.isOneWay());
		assertFalse(msg.equals(msg2));

		msg2 = p1.createMessage(pName, doc, prop, schema);
		assertTrue(msg2.isOneWay());
		assertFalse(msg.equals(msg2));
		
		msg2 = p1.createMessage(pName, doc, propMap, schema);
		assertTrue(msg2.isOneWay());
		assertFalse(msg.equals(msg2));
	}
	
	
	//test Two-Way Message
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
	}
	
	@Test
	public void testCreateTwoWayMessage() {
		
		msg = p1.createMessage(msgName, doc, schema, schema, schema);
		assertTrue(msg.equals(msg));
		assertFalse(msg.equals(msgName));	// to reach branch coverage
		
		msg2 = p1.createMessage(msg, schema, schema, schema);
		assertFalse(msg2.isOneWay());
		assertTrue(msg.equals(msg2));		// to reach branch coverage
		
		msg2 = p1.createMessage(msgName, doc, prop, schema, schema2, schema);
		assertFalse(msg2.isOneWay());
		assertFalse(msg.equals(msg2));
		
		msg2 = p1.createMessage(msgName, doc, propMap, schema, schema, schema2);
		assertFalse(msg2.isOneWay());
		assertFalse(msg.equals(msg2));
		
		msg2 = p1.createMessage(msgName, doc, prop, schema, schema, schema2);
		assertFalse(msg2.isOneWay());
		assertFalse(msg.equals(msg2));
		
		msg2 = p1.createMessage(msgName, doc, schema);
		assertTrue(msg2.isOneWay());
		assertFalse(msg.equals(msg2));
	}
}