package it.uniroma2.dicii.isw2.avro.myTest;

import static org.junit.Assert.*;

import java.io.IOException;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
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
	Collection<Schema> colSchema;
	
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
		p1 = new Protocol(pName, doc, namespace);
		schema = Schema.createRecord(schemaName, docSchema, namespace, false);
		schema2 = Schema.createRecord(pName, docSchema, namespace, true);

		colSchema = new ArrayList<>();
		colSchema.add(schema);
		propMap = new HashMap<String, String>();
		propMap.put(propName, propValue);
	}

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

		assertNotNull(p1.getMD5());				//branch coverage
		assertNotNull(p1.hashCode());
	}
	
	@Test(expected=NullPointerException.class)
	public void nullPointer() {
		p1.createMessage(pName, doc, prop, schema).toString();
	}
}