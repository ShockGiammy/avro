package it.uniroma2.dicii.isw2.avro.myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class TestEqualsProtocol {
	
	String pName;
	String doc;
	String namespace;
	String pName2;
	String doc2;
	String namespace2;
	Protocol p1;
	Message msg;
	Message msg2;
	String msgName;
	String msgName2;
	Schema schema;
	Schema schema2;
	String schemaName;
	String schemaName2;
	JsonProperties prop;
	String propName;
	String propValue;
	String nullDoc;
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
		doc = "doc";
		
		p1 = new Protocol(pName, doc, namespace);
		
		schema = Schema.createRecord(schemaName, doc, namespace, false);
		schema2 = Schema.createRecord(schemaName2, doc, namespace, false);
		
		propMap = new HashMap<String, String>();
		propMap.put(propName, propValue);
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
	public void testCreateAndEqualsOneWayMessage() {				// to reach branch coverage
			
		msg = p1.createMessage(msgName, doc, schema);
		assertTrue(msg.equals(msg));
		
		assertFalse(msg.equals(msgName));
		msg2 = p1.createMessage(msgName, doc, schema);
		assertTrue(msg2.isOneWay());
		assertTrue(msg.equals(msg2));
		
		msg2 = p1.createMessage(msg, schema);
		assertEquals(pName, msg2.getName());
		assertFalse(msg.equals(msg2));
			
		msg2 = p1.createMessage(msgName2, doc, schema);
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
