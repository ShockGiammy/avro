package test;

import static org.junit.Assert.*;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.junit.Before;
import org.junit.Test;

public class TestProtocol {
	
	String pName;
	String doc;
	String namespace;
	Protocol p1;
	String propName;
	String propValue;
	String msgName;
	Schema schema;
	String schemaName;
	String docSchema;
	Message msg;
	Collection<Schema> colSchema;
	String jsonFormat;					//white box because no other ways
	String baseMsg;
	String NoMsg;
	String WrongMsg;
	String rightOneWayMsg;
	String WrongOneWayMsg;
	String RightTwoWayMsg;
	String noBoolOneWayMsg;
	String toUse;
	
	@Before
	public void configure() {
		pName = "Pname";
		doc = null;
		namespace = "foo";
		propName = "fooProperty";
		propValue = "fooValue";
		msgName = "fooMessage";
		schemaName = "sName";
		docSchema = "doc";
		p1 = new Protocol(pName, doc, namespace);
		p1.addProp(propName, propValue);
		schema = Schema.createRecord(schemaName, docSchema, namespace, false);
		jsonFormat = "{\"protocol\":\"" + pName + "\",\"namespace\":\"" + namespace + "\",\"" + propName + "\":\"" +
				propValue + "\",\"types\":[],";
		NoMsg = "\"messages\":{}}";
		WrongMsg = "\"messages\":{\"" + msgName + "\": \"" + msgName + "\"}}";
		baseMsg = "\"types\": [ {\"name\": \"Greeting\", \"type\": \"record\", \"fields\": [ {\"name\": \"message\", \"type\": \"string\"}]}],"
				+ "\"messages\":{\"" + msgName + "\":{\"request\": [{\"name\": \"greeting\", \"type\": \"Greeting\" }]";
		WrongOneWayMsg = baseMsg + "} } }";
		RightTwoWayMsg = baseMsg + ", \"response\": \"Greeting\" } } }";
		noBoolOneWayMsg = baseMsg + ", \"one-way\" : \"true\"} } }";
		rightOneWayMsg = baseMsg + ", \"one-way\" : true} } }";
		
		colSchema = new ArrayList<>();
		colSchema.add(schema);
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
		
		Protocol p8 = Protocol.parse(jsonFormat + rightOneWayMsg);
		assertFalse(p1.equals(p5));
		assertFalse(p1.equals(p6));
		assertFalse(p1.equals(p7));
		assertFalse(p1.equals(p8));
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
		
		assertEquals(doc, p1.getDoc());
		assertEquals(propValue, p1.getProp(propName));
		assertEquals(jsonFormat + NoMsg, Protocol.parse(toUse).toString());
		
		toUse = p1.toString(true);
		assertEquals(jsonFormat + NoMsg, Protocol.parse(toUse).toString());

		assertNotNull(p1.getMD5());
		assertNotNull(p1.getMD5());				//branch coverage
		assertNotNull(p1.hashCode());
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException1() {
		Protocol.parse(jsonFormat + WrongMsg).toString();
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException2() {
		Protocol.parse(jsonFormat + WrongOneWayMsg).toString();
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException3() {
		Protocol.parse(jsonFormat + noBoolOneWayMsg).toString();
	}
	
	@Test(expected = SchemaParseException.class)
	public void testSchemaParseException4() {
		Protocol.parse(rightOneWayMsg);
	}
	
	@Test
	public void testParseProtocol() {

		assertNotNull(Protocol.parse(jsonFormat + RightTwoWayMsg).getMessages());
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
	}
	
	@Test
	public void testTwoWayMessage() {
		msg = p1.createMessage(msgName, doc, schema, schema, schema);
		assertFalse(msg.isOneWay());
		Message msg2 = p1.createMessage(msg, schema, schema, schema);
		assertFalse(msg2.isOneWay());
	}
	
	@Test
	public void testAddMessage() {

		msg = p1.createMessage(msgName, doc, schema);
		Message msg2 = p1.createMessage(msg, schema);
		Message msg3 = p1.createMessage(msgName, doc, schema);
		Message msg4 = p1.createMessage("otherName", doc, schema);
		schema = Schema.createRecord("otherName", docSchema, namespace, false);
		Message msg5 = p1.createMessage("otherName", doc, schema);

		assertEquals(pName, msg2.getName());
		
		assertNull(msg.getObjectProp(namespace));
		assertTrue(msg.equals(msg));
		assertTrue(msg.equals(msg));
		assertFalse(msg.equals(msgName));				// to reach branch coverage
		assertTrue(msg.equals(msg3));
		assertFalse(msg.equals(msg4));
		assertFalse(msg.equals(msg5));

		p1.createMessage(pName, doc, p1, schema);
		/*System.out.println("first" + p1.getMessages());
		System.out.println("second" + p1.getMessages().toString());
		String toUse = p1.toString();
		System.out.println(Protocol.parse(toUse).toString());*/
	}

	@Test
	public void testSplitProtocolBuild() {
		Protocol p = new Protocol("P", null, "foo");
		p.addProp("property", "some value");
		
		String protocolString = p.toString();
		final int mid = protocolString.length() / 2;
		
		Protocol parsedStringProtocol = Protocol.parse(protocolString);
		Protocol parsedArrayOfStringProtocol = Protocol.parse(protocolString.substring(0, mid),
				protocolString.substring(mid));

		assertNotNull(parsedStringProtocol);
		assertNotNull(parsedArrayOfStringProtocol);
		assertEquals(parsedStringProtocol.toString(), parsedArrayOfStringProtocol.toString());
	}
}