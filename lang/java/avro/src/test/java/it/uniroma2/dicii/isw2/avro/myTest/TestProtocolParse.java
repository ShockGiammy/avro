package it.uniroma2.dicii.isw2.avro.myTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.avro.Protocol;
import org.apache.avro.SchemaParseException;
import org.junit.Before;
import org.junit.Test;

public class TestProtocolParse {
	
	String pName;
	String namespace;
	String propName;
	String propValue;
	String msgName;
	String doc;
	String nullDoc;
	
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
	
	File file;
	InputStream inStream;
	InputStream inStream2;
	Protocol p1;
	Protocol p2;
	
	@Before
	public void configure() {
		
		pName = "Pname";
		doc = "doc";
		nullDoc = null;
		namespace = "foo";
		propName = "fooProperty";
		propValue = "fooValue";
		msgName = "fooMessage";
		
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
		file = null;
		p1 = new Protocol(pName, doc, namespace);
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
		
	@Test
	public void testParseNoTypes() {
		p1 = new Protocol(pName, nullDoc, namespace);
		assertEquals(nullDoc, p1.getDoc());
		p1.addProp(propName, propValue);
		assertEquals(jsonFormatNoTypes + noMsg, Protocol.parse(p1.toString()).toString());
	}
	
	@Test
	public void equalsParse() {
		
		p1.addProp(propName, propValue);
		p2 = Protocol.parse(jsonFormat + rightOneWayMsg);
		assertFalse(p1.equals(p2));
	}
}
