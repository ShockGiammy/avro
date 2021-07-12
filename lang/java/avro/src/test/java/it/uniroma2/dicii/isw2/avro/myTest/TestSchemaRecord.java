package it.uniroma2.dicii.isw2.avro.myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestSchemaRecord {
	
	String schemaName;
	String schemaDoc;
	String namespace;
	boolean isError;
	Schema schema;
	
	@Parameters
	public static Collection<Object[]> getTestParameters() {
		return Arrays.asList(new Object[][] {
			{"sName", "doc", "fooNamespace", false},
			{"sName", null, "fooNamespace", false},
			{null, "doc", "fooNamespace", false},
			{"sName", "doc", null, false},
			{"sName", "", "fooNamespace", false},
			{"sName", "doc", "", false},
			{"sName", "doc", "fooNamespace", true},
			/*
			 {"_sName", "doc", "fooNamespace", true},
			 {"sName_", "doc", "fooNamespace", true},
			 {"sName?", "doc", "fooNamespace", true},
			 {"1sName", "doc", "fooNamespace", true},
			 */
		});
	}
	
	public TestSchemaRecord(String schemaName, String schemaDoc, String namespace, boolean isError) {
		this.configure(schemaName, schemaDoc, namespace, isError);
	}
	
	public void configure(String schemaName, String schemaDoc, String namespace, boolean isError) {
		this.schemaName = schemaName;
		this.schemaDoc = schemaDoc;
		this.namespace = namespace;
		this.isError = isError;
				
		schema = Schema.createRecord(schemaName, schemaDoc, namespace, isError);
	}
	
	@Test
	public void testRecord() {
	  
		assertNotNull(schema.toString());
		assertEquals(schemaName, schema.getName());
		assertEquals(schemaDoc, schema.getDoc());
		if (namespace == "")
			assertEquals(null, schema.getNamespace());
		assertFalse(schema.isNullable());
		if (isError) {
			assertTrue(schema.isError());
		}
		else {
			assertFalse(schema.isError());
		}
	}
}
