package it.uniroma2.dicii.isw2.avro.myTest;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(value=Parameterized.class)
public class TestSchemaCreate {
	
	Type type;
	Schema schema;
	
	@Parameters
	public static Collection<Object[]> getTestParameters() {
		return Arrays.asList(new Object[][] {
			{Type.INT},
			{Type.NULL},
			{Type.STRING},
			{Type.BYTES},
			{Type.LONG},
			{Type.FLOAT},
			{Type.DOUBLE},
			{Type.BOOLEAN},
			//{Type.RECORD}
		});
	}
	
	public TestSchemaCreate(Type type) {
		this.configure(type);
	}
	
	public void configure(Type type) {
		this.type =type;
		schema = Schema.create(type);
	}
	
	@Test
	public void TestCreate() {
		assertEquals(type, schema.getType());
	}
}
