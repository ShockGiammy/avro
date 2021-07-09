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
	String wrongMsgName;
	String wrongMsgType;
	String wrongMsgOneWayErrors;
	String wrongMsgOneWayResponse;
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
		p1 = new Protocol(pName, doc, namespace);
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
		wrongMsgName = "\"types\":[{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}],"
				+ "\"messages\":{\"" + msgName + "\":{\"request\":[{\"type\":\"Greeting\"}]} } }";;
		wrongMsgType = "\"types\":[{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"message\",\"type\":\"string\"}]}],"
				+ "\"messages\":{\"" + msgName + "\":{\"request\":[{\"name\":\"greeting\"}]} } }";;
		wrongMsgOneWayErrors = baseMsg + ",\"response\":\"null\",\"one-way\":true,\"errors\":\"Greeting\"}}}";
		wrongMsgOneWayResponse = baseMsg + ",\"response\":\"Greeting\",\"one-way\":true}}}";
		
		colSchema = new ArrayList<>();
		colSchema.add(schema);
		propMap = new HashMap<String, String>();
		propMap.put(propName, propValue);
		file = null;
	}
	
	
	/*@Test
	public void testCopyProtocol() throws IOException {
		
		p2 = new Protocol(p1);
		assertTrue(p1.equals(p2));
		
		p1.addProp(propName, propValue);
		p2 = new Protocol(pName, namespace);
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol(p1);
		assertTrue(p1.equals(p2));
	}*/
	
	@Test
	public void testEquals() throws IOException {
		
		assertTrue(p1.equals(p1));			//same object
		
		assertFalse(p1.equals(pName));		//not valid object
		
		p2 = new Protocol("other Name", namespace);
		assertFalse(p1.equals(p2));			//valid object false
		
		p2 = new Protocol(p1);
		assertTrue(p1.equals(p2));			//valid object true
		
		assertFalse(p1.equals(null));		//null object
	}
	
	@Test
	public void testEquals2() throws IOException {
		
		p1.addProp(propName, propValue);
		p2 = new Protocol(pName, doc, namespace);			//to reach branch coverage
		assertFalse(p1.equals(p2));
		
		p2 = new Protocol(pName, doc, "other namespace");
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

		p1.addProp(propName, propValue);
		assertEquals(pName, p1.getName());
		assertEquals(namespace, p1.getNamespace());
		
		assertEquals(doc, p1.getDoc());
		assertEquals(propValue, p1.getProp(propName));
		
		p1 = new Protocol(pName, nullDoc, namespace);
		assertEquals(nullDoc, p1.getDoc());
		p1.addProp(propName, propValue);
		assertEquals(jsonFormatNoTypes + noMsg, Protocol.parse(p1.toString()).toString());

		assertNotNull(p1.getMD5());				//branch coverage
		assertNotNull(p1.hashCode());
	}
	
	@Test(expected=NullPointerException.class)
	public void nullPointer() {
		p1.createMessage(pName, doc, prop, schema).toString();
	}
	
	
	//test Protocol.parse	
	@Test
	public void testParseProtocolString() {
		
		assertEquals(jsonFormat + rightOneWayMsg, Protocol.parse(jsonFormat + rightOneWayMsg).toString());
		assertEquals(jsonFormat + rightTwoWayMsg, Protocol.parse(jsonFormat + rightTwoWayMsg).toString());
		
		assertEquals(jsonFormat + rightOneWayMsg, Protocol.parse(jsonFormat, rightOneWayMsg).toString());
		assertEquals(jsonFormat + rightTwoWayMsg, Protocol.parse(jsonFormat, rightTwoWayMsg).toString());
		
		assertNotNull(Protocol.parse(jsonFormat + rightTwoWayMsg).getMessages());
		assertNotNull(Protocol.parse(jsonFormat + rightOneWayMsg).getMessages());

	}
	
	public void testParseInputStream() {

		inStream = getClass().getResourceAsStream("/example.avpr");
		try {
			assertEquals(Files.readString(Paths.get(getClass().getResource("/example.avpr").toURI())).replaceAll("[\\n\\t ]", ""), Protocol.parse(inStream).toString());
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testParseDifferentInputStream() {
		
		inStream = getClass().getResourceAsStream("/example.avpr");
		inStream2 = getClass().getResourceAsStream("/exampleMsgDifferent.avpr");
		try {
			assertFalse(Protocol.parse(inStream).equals(Protocol.parse(inStream2)));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testParseFile() {

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
		fail("SchemaParseException: No protocol name specified");
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
		fail("SchemaParseException: No protocol name specified");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException1() {
		Protocol.parse(jsonFormat + wrongMsg).toString();
		fail("No request specified");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException2() {
		Protocol.parse(jsonFormat + wrongOneWayMsg).toString();
		fail("No response specified");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException3() {
		Protocol.parse(jsonFormat + noBoolOneWayMsg).toString();
		fail("one-way must be boolean");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException4() {
		Protocol.parse(rightOneWayMsg);
		fail("No protocol specified");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException5() {
		Protocol.parse(jsonFormat + wrongMsgName);
		fail("No param name");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException6() {
		Protocol.parse(jsonFormat + wrongMsgType);
		fail("No param type");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException7() {
		Protocol.parse(jsonFormat + wrongMsgOneWayErrors);
		fail("one-way can't have errors");
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException8() {
		Protocol.parse(jsonFormat + wrongMsgOneWayResponse);
		fail("One way response must be null");
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
	public void testCreateAndEqualsOneWayMessage() {

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
	public void testCreateAndEqualsTwoWayMessage() {
		
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