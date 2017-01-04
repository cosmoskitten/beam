/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat.unit.tests;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.HadoopInputFormatBoundedSource;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.coders.EmployeeCoder;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.BadCreateRecordReaderInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.BadRecordReaderNoRecordsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.EmployeeInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.EmptyInputSplitsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.ImmutableRecordsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.MutableRecordsInputFormat;
import org.apache.beam.sdk.io.hadoop.inputformat.unit.tests.inputformats.NullInputSplitsBadInputFormat;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.Lists;


/**
 * Unit tests for {@link HadoopInputFormatIO}.
 */
@RunWith(JUnit4.class)
public class HadoopInputFormatIOTest {
	static SerializableConfiguration serConf;
	static SimpleFunction<Text, String> myKeyTranslate;
	static SimpleFunction<Employee, String> myValueTranslate;

	@Rule public final transient TestPipeline p = TestPipeline.create();
	@Rule public ExpectedException thrown = ExpectedException.none();
	@Rule public ExpectedLogs logged = ExpectedLogs.none(HadoopInputFormatIO.class);
	private PBegin input = PBegin.in(p);

	@BeforeClass
	public static void setUp() {
		serConf  = getConfiguration(TypeDescriptor.of(EmployeeInputFormat.class),TypeDescriptor.of(Text.class),TypeDescriptor.of(Employee.class));
		myKeyTranslate = new SimpleFunction<Text, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Text input) {
				return input.toString();
			}
		};

		myValueTranslate = new SimpleFunction<Employee, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Employee input){
				return input.getEmpName();
			}
		};

	}


	//Read validation
	@Test
	public void testReadBuildsCorrectly() {
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(serConf.getConfiguration())
				.withKeyTranslation(myKeyTranslate)
				.withValueTranslation(myValueTranslate);
		assertEquals(EmployeeInputFormat.class, read.getConfiguration().getConfiguration().getClass("mapreduce.job.inputformat.class",Object.class));
		assertEquals(Text.class,read.getConfiguration().getConfiguration().getClass("key.class",Object.class));
		assertEquals(Employee.class,read.getConfiguration().getConfiguration().getClass("value.class",Object.class));
		assertEquals(myKeyTranslate, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslate, read.getSimpleFuncForValueTranslation());
	}

	@Test
	public void testReadBuildsCorrectlyInDifferentOrder() {
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withValueTranslation(myValueTranslate)
				.withConfiguration(serConf.getConfiguration())
				.withKeyTranslation(myKeyTranslate);
		assertEquals(EmployeeInputFormat.class, read.getConfiguration().getConfiguration().getClass("mapreduce.job.inputformat.class",Object.class));
		assertEquals(Text.class,read.getConfiguration().getConfiguration().getClass("key.class",Object.class));
		assertEquals(Employee.class,read.getConfiguration().getConfiguration().getClass("value.class",Object.class));
		assertEquals(myKeyTranslate, read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslate, read.getSimpleFuncForValueTranslation());
	}



	@Test
	public void testReadDisplayData() {
		String KEYSPACE = "mobile_data_usage";
		String COLUMN_FAMILY = "subscriber";
		Configuration cassandraConf = new Configuration();
		cassandraConf.set("cassandra.input.thrift.port", "9160");
		cassandraConf.set("cassandra.input.thrift.address", "127.0.0.1");
		cassandraConf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		cassandraConf.set("cassandra.input.keyspace", KEYSPACE);
		cassandraConf.set("cassandra.input.columnfamily", COLUMN_FAMILY);
		TypeDescriptor<CqlInputFormat> inputFormatClassName = new TypeDescriptor<CqlInputFormat>() {
			private static final long serialVersionUID = 1L;
		};
		cassandraConf.setClass("mapreduce.job.inputformat.class", inputFormatClassName.getRawType(), InputFormat.class);
		TypeDescriptor<Long> keyClass = new TypeDescriptor<Long>() {
			private static final long serialVersionUID = 1L;
		};
		cassandraConf.setClass("key.class", keyClass.getRawType(), Object.class);
		TypeDescriptor<com.datastax.driver.core.Row> valueClass = new TypeDescriptor<Row>() {
			private static final long serialVersionUID = 1L;
		};
		cassandraConf.setClass("value.class", valueClass.getRawType(), Object.class);
		SimpleFunction<Row, String> myValueTranslateFunc = new SimpleFunction<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Row input) {
				return input.getString("subscriber_email");
			}
		};
		SimpleFunction<Long, String> myKeyTranslateFunc = new SimpleFunction<Long, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(Long input) {
				return input.toString();
			}
		};
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(cassandraConf)
				.withKeyTranslation(myKeyTranslateFunc)
				.withValueTranslation(myValueTranslateFunc);
		read.validate(input);
		DisplayData displayData = DisplayData.from(read);
		if (cassandraConf != null) {
			Iterator<Entry<String, String>> propertyElement = cassandraConf.iterator();
			while (propertyElement.hasNext()) {
				Entry<String, String> element = propertyElement.next();
				assertThat(displayData, hasDisplayItem(element.getKey(),element.getValue()));
			}
		}

	}

	// This test validates Read transform object creation if only withConfiguration() is called with null value.
	// withConfiguration() checks configuration is null or not and throws exception if null value is send to withConfiguration().
	@Test
	public void testReadObjectCreationFailsIfConfigurationIsNull() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Configuration cannot be null");
		HadoopInputFormatIO.<Text, Employee>read()
		.withConfiguration(null);

	}


	// This test validates Read transform object creation if only withConfiguration() is called.
	@Test
	public void testReadObjectCreationWithConfiguration() {
		HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
				.withConfiguration(serConf.getConfiguration());
		read.validate(input);
		assertEquals(serConf.getConfiguration(),read.getConfiguration().getConfiguration());
		assertEquals(null,read.getSimpleFuncForKeyTranslation());
		assertEquals(null,read.getSimpleFuncForValueTranslation());
		assertEquals(serConf.getConfiguration().getClass("key.class", Object.class),read.getKeyClass().getRawType());
		assertEquals(serConf.getConfiguration().getClass("value.class", Object.class),read.getValueClass().getRawType());

	}

	// This test validates behavior Read transform object creation if withConfiguration() and withKeyTranslation() are called and null value is passed to kayTranslation.
	// withKeyTranslation() checks keyTranslation is null or not and throws exception if null value is send to withKeyTranslation().
	@Test
	public void testReadObjectCreationFailsIfKeyTranslationFunctionIsNull() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Simple function for key translation cannot be null.");
		HadoopInputFormatIO.<String, Employee>read()
		.withConfiguration(serConf.getConfiguration())
		.withKeyTranslation(null);
	}


	// This test validates Read transform object creation if withConfiguration() and withKeyTranslation() are called.
	@Test
	public void testReadObjectCreationWithConfigurationKeyTranslation() {
		HadoopInputFormatIO.Read<String, Employee> read = HadoopInputFormatIO.<String, Employee>read()
				.withConfiguration(serConf.getConfiguration())
				.withKeyTranslation(myKeyTranslate);
		read.validate(input);
		assertEquals(serConf.getConfiguration(),read.getConfiguration().getConfiguration());
		assertEquals(myKeyTranslate,read.getSimpleFuncForKeyTranslation());
		assertEquals(null,read.getSimpleFuncForValueTranslation());
		assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),read.getKeyClass().getRawType());
		assertEquals(serConf.getConfiguration().getClass("value.class", Object.class),read.getValueClass().getRawType());
	}

	// This test validates behaviour Read transform object creation if withConfiguration() and withValueTranslation() are called and null value is passed to valueTranslation.
	// withValueTranslation() checks valueTranslation is null or not and throws exception if null value is send to withValueTranslation().
	@Test
	public void testReadObjectCreationFailsIfValueTranslationFunctionIsNull() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Simple function for value translation cannot be null.");
		HadoopInputFormatIO.<Text, String>read()
		.withConfiguration(serConf.getConfiguration())
		.withValueTranslation(null);

	}

	// This test validates Read transform object creation if withConfiguration() and withValueTranslation() are called.
	@Test
	public void testReadObjectCreationWithConfigurationValueTranslation() {
		HadoopInputFormatIO.Read<Text, String> read = HadoopInputFormatIO.<Text, String>read()
				.withConfiguration(serConf.getConfiguration())
				.withValueTranslation(myValueTranslate);
		read.validate(input);
		assertEquals(serConf.getConfiguration(),read.getConfiguration().getConfiguration());
		assertEquals(null,read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslate,read.getSimpleFuncForValueTranslation());
		assertEquals(serConf.getConfiguration().getClass("key.class", Object.class),read.getKeyClass().getRawType());
		assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),read.getValueClass().getRawType());
	}


	// This test validates Read transform object creation if withConfiguration() , withKeyTranslation() and withValueTranslation() are called.
	@Test
	public void testReadObjectCreationWithConfigurationKeyTranslationValueTranslation() {
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(serConf.getConfiguration())
				.withKeyTranslation(myKeyTranslate)
				.withValueTranslation(myValueTranslate);
		read.validate(input);
		assertEquals(serConf.getConfiguration(),read.getConfiguration().getConfiguration());
		assertEquals(myKeyTranslate,read.getSimpleFuncForKeyTranslation());
		assertEquals(myValueTranslate,read.getSimpleFuncForValueTranslation());
		assertEquals(myKeyTranslate.getOutputTypeDescriptor().getRawType(),read.getKeyClass().getRawType());
		assertEquals(myValueTranslate.getOutputTypeDescriptor().getRawType(),read.getValueClass().getRawType());
	}


	///This test validates functionality of Read.validate() function when Read transform is created without calling withConfiguration().
	@Test
	public void testReadValidationFailsMissingConfiguration() {
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read();
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("Need to set the configuration of a HadoopInputFormatIO Read using method Read.withConfiguration().");
		read.validate(input);
	}


	//This test validates functionality of Read.validate() function when Hadoop InputFormat class is not provided by user in configuration.
	@Test
	public void testReadValidationFailsMissingInputFormatInConf() {
		Configuration configuration = new Configuration();
		configuration.setClass("key.class", Text.class, Object.class);
		configuration.setClass("value.class", Employee.class, Object.class);
		HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
				.withConfiguration(configuration);
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Hadoop InputFormat class property \"mapreduce.job.inputformat.class\" is not set in configuration."); 
		read.validate(input);
	}

	//This test validates functionality of Read.validate() function when key class is not provided by user in configuration.
	@Test
	public void testReadValidationFailsMissingKeyClassInConf() {
		Configuration configuration = new Configuration();
		configuration.setClass("mapreduce.job.inputformat.class", EmployeeInputFormat.class, InputFormat.class);
		configuration.setClass("value.class", Employee.class, Object.class);
		HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
				.withConfiguration(configuration);
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Configuration property \"key.class\" is not set.");
		read.validate(input);
	}

	//This test validates functionality of Read.validate() function when value class is not provided by user in configuration.
	@Test
	public void testReadValidationFailsMissingValueClassInConf() {
		Configuration configuration = new Configuration();
		configuration.setClass("mapreduce.job.inputformat.class", EmployeeInputFormat.class, InputFormat.class);
		configuration.setClass("key.class", Text.class, Object.class);
		HadoopInputFormatIO.Read<Text, Employee> read = HadoopInputFormatIO.<Text, Employee>read()
				.withConfiguration(configuration);
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Configuration property \"value.class\" is not set.");
		read.validate(input);

	}

	//This test validates functionality of Read.validate() function when myKeyTranslate's (simple function provided by user for key translation)
	//input type is not same as hadoop input format's keyClass(Which is property set in configuration as "key.class").
	@Test
	public void testReadValidationFailsWithWrongInputTypeKeyTranslationFunction() {
		SimpleFunction<LongWritable, String> myKeyTranslateWithWrongInputType = new SimpleFunction<LongWritable, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(LongWritable input) {
				return input.toString();
			}
		};
		HadoopInputFormatIO.Read<String, Employee> read = HadoopInputFormatIO.<String, Employee>read()
				.withConfiguration(serConf.getConfiguration())
				.withKeyTranslation(myKeyTranslateWithWrongInputType);
		thrown.expect(IllegalArgumentException.class);
		String inputFormatClassProperty= serConf.getConfiguration().get("mapreduce.job.inputformat.class") ;
		String keyClassProperty= serConf.getConfiguration().get("key.class");
		thrown.expectMessage("Key translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" key class : "+ keyClassProperty);
		read.validate(input);

	}

	//This test validates functionality of Read.validate() function when myValueTranslate's (simple function provided by user for value translation)
	//input type is not same as hadoop input format's valueClass(Which is property set in configuration as "value.class").
	@Test
	public void testReadValidationFailsWithWrongInputTypeValueTranslationFunction() {
		SimpleFunction<LongWritable, String> myValueTranslateWithWrongInputType = new SimpleFunction<LongWritable, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String apply(LongWritable input) {
				return input.toString();
			}
		};
		HadoopInputFormatIO.Read<Text, String> read = HadoopInputFormatIO.<Text, String>read()
				.withConfiguration(serConf.getConfiguration())
				.withValueTranslation(myValueTranslateWithWrongInputType);
		String inputFormatClassProperty= serConf.getConfiguration().get("mapreduce.job.inputformat.class") ;
		String keyClassProperty= serConf.getConfiguration().get("value.class");
		String expectedMessage="Value translation's input type is not same as hadoop input format : "+inputFormatClassProperty+" value class : "+ keyClassProperty;
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage(expectedMessage);
		read.validate(input);
	}

	@Test
	public void testReadingData()  throws Exception {
		HadoopInputFormatIO.Read<String, String> read = HadoopInputFormatIO.<String, String>read()
				.withConfiguration(serConf.getConfiguration())
				.withKeyTranslation(myKeyTranslate)
				.withValueTranslation(myValueTranslate);
		List<KV<String, String>> expected = getEmployeeNameList();
		PCollection<KV<String, String>> actual = p.apply("ReadTest" , read);
		PAssert.that(actual).containsInAnyOrder(expected);
		p.run();
	}


	// This test validates behavior HadoopInputFormatSource if RecordReader object creation fails in start() method.
	@Test
	public void testReadersStartIfCreateRecordReaderFails() throws Exception {
		long inputDataSize=9,numberOfSplitsOfInputFormat=3;
		SerializableConfiguration serConf = getConfiguration(TypeDescriptor.of(BadCreateRecordReaderInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class)); 
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of());
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(p.getOptions());
		assertEquals(inputDataSize,estimatedSize);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, p.getOptions());
		assertEquals(numberOfSplitsOfInputFormat,boundedSourceList.size());
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> reader=source.createReader(p.getOptions());
			assertEquals(new Double((float)0),reader.getFractionConsumed());
			thrown.expect(Exception.class);
			thrown.expectMessage("Exception in creating RecordReader in BadCreateRecordReaderInputFormat");
			reader.start();
		}

	}


	// This test validate's createReader() and start() methods.
	// This test validates behaviour of createReader() and start() method if InputFormat's getSplits() returns InputSplitList having having no records.  
	@Test
	public void testReadersCreateReaderAndStartWithZeroRecords() throws Exception {
		long inputDataSize=9,numberOfSplitsOfInputFormat=3;
		SerializableConfiguration serConf = getConfiguration(TypeDescriptor.of(BadRecordReaderNoRecordsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class)); 
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of());
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(p.getOptions());
		assertEquals(inputDataSize,estimatedSize);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, p.getOptions());
		assertEquals(numberOfSplitsOfInputFormat,boundedSourceList.size());
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> reader=source.createReader(p.getOptions());
			assertEquals(new Double((float)0),reader.getFractionConsumed());
			assertEquals(false,reader.start());
		}
	}
	@Test
	public void testReadersGetFractionConsumed() throws Exception {
		long inputDataSize=9,numberOfSplitsOfInputFormat=3;
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of(),myKeyTranslate,myValueTranslate,null);
		long estimatedSize = parentHIFSource.getEstimatedSizeBytes(p.getOptions());
		assertEquals(inputDataSize,estimatedSize);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, p.getOptions());
		assertEquals(numberOfSplitsOfInputFormat,boundedSourceList.size());
		List<KV<String, String>> referenceRecords = getEmployeeNameList();
		List<KV<String, String>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			List<KV<String, String>> elements = new ArrayList<KV<String, String>>();
			BoundedReader<KV<String, String>> reader=source.createReader(p.getOptions());
			assertEquals(new Double((float)0),reader.getFractionConsumed());
			assertEquals(new Double((float)0),reader.getFractionConsumed());
			int i=0;
			boolean start = reader.start();
			assertEquals(true,start);
			if(start){
				elements.add(reader.getCurrent());
				assertEquals(new Double((float)++i/numberOfSplitsOfInputFormat),reader.getFractionConsumed());
				boolean advance=reader.advance();
				assertEquals(true,advance);
				while(advance){
					assertEquals(true,advance);
					elements.add(reader.getCurrent());
					assertEquals(new Double((float)++i/numberOfSplitsOfInputFormat),reader.getFractionConsumed());
					advance=reader.advance();
				}
				assertEquals(false,advance);
				bundleRecords.addAll(elements);

			}
			reader.close();
		}
		assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}

	//Validate that the Reader and its parent source reads the same records.
	@Test
	public void testReaderAndParentSourceReadsSameData() throws Exception
	{
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of(),myKeyTranslate,myValueTranslate,null);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource.splitIntoBundles(0, p.getOptions());
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> reader=source.createReader(p.getOptions());
			SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(reader, p.getOptions());
		}

	}


	//This test verifies that the method HadoopInputFormatReader.getCurrentSource() returns correct source object.
	@Test
	public void testGetCurrentSourceFunc() throws Exception
	{
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of(),myKeyTranslate,myValueTranslate,null);
		List<BoundedSource<KV<String, String>>> boundedSourceList = (List<BoundedSource<KV<String, String>>>) parentHIFSource
				.splitIntoBundles(0, p.getOptions());
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			BoundedReader<KV<String, String>> HIFReader=source.createReader(p.getOptions());
			BoundedSource<KV<String, String>> HIFSource = HIFReader.getCurrentSource();
			assertEquals(HIFSource,source);
		}

	}


	//This test validates behavior of HadoopInputFormatSource.createReader() method when HadoopInputFormatSource.splitIntoBundles() is not called.
	@Test
	public void testCreateReaderIfSplitIntoBundlesNotCalled() throws Exception
	{
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of(),myKeyTranslate,myValueTranslate,null);
		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot create reader as source is not split yet.");
		parentHIFSource.createReader(p.getOptions());

	}


	//This test validates behavior of getEstimatedSizeBytes() and splitIntoBundles() when Hadoop InputFormat's getSplits() returns empty list.
	@Test
	public void testSplitIntoBundlesIfGetSplitsReturnsEmptyList() throws Exception
	{
		SerializableConfiguration serConf = getConfiguration(TypeDescriptor.of(EmptyInputSplitsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf,StringUtf8Coder.of(), StringUtf8Coder.of());
		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot split the source as getSplits() is returning empty list.");
		parentHIFSource.getEstimatedSizeBytes(p.getOptions());
		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot split the source as getSplits() is returning empty list.");
		parentHIFSource.splitIntoBundles(0, p.getOptions());


	}


	//This test validates behavior of getEstimatedSizeBytes() and splitIntoBundles() when Hadoop InputFormat's getSplits() returns NULL value.
	@Test
	public void testSplitIntoBundlesIfGetSplitsReturnsNullValue() throws Exception
	{
		SerializableConfiguration serConf = getConfiguration(TypeDescriptor.of(NullInputSplitsBadInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class));
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of());
		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot split the source as getSplits() is returning null value.");
		parentHIFSource.getEstimatedSizeBytes(p.getOptions());

		thrown.expect(IOException.class);
		thrown.expectMessage("Cannot split the source as getSplits() is returning null value.");
		parentHIFSource.splitIntoBundles(0, p.getOptions());

	}

	//This test validates functionality of HadoopInputFormatIO if user sets wrong key class and value class.
	@Test
	public void testHIFSourceIfUserSetsWrongKeyOrValueClass() throws Exception 
	{	
		SerializableConfiguration serConf = getConfiguration(TypeDescriptor.of(EmployeeInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(java.lang.String.class)); 
		HadoopInputFormatBoundedSource<String, String> parentHIFSource = new HadoopInputFormatBoundedSource<String, String>(serConf, StringUtf8Coder.of(), StringUtf8Coder.of());
		List<BoundedSource<KV<String, String>>> boundedSourceList = parentHIFSource.splitIntoBundles(0, p.getOptions());
		List<KV<String, String>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, String>> source : boundedSourceList) {
			thrown.expect(ClassCastException.class);
			List<KV<String, String>> elems = SourceTestUtils.readFromSource(source, p.getOptions());
			bundleRecords.addAll(elems);
		}
	}

	// This test validates records emitted in PCollection are immutable 
	// if InputFormat's recordReader returns same objects(i.e. same locations in memory) but with updated values for each record.
	@Test
	public void testImmutablityOfOutputOfReadIfRecordReaderObjectsAreMutable()  throws Exception {
		SerializableConfiguration serConf = getConfiguration(TypeDescriptor.of(MutableRecordsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(Employee.class)); 
		HadoopInputFormatBoundedSource<String, Employee> parentHIFSource = new HadoopInputFormatBoundedSource<String, Employee>(serConf, StringUtf8Coder.of(), EmployeeCoder.of());
		List<BoundedSource<KV<String, Employee>>> boundedSourceList = parentHIFSource.splitIntoBundles(0, p.getOptions());
		List<KV<String, Employee>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, Employee>> source : boundedSourceList) {
			List<KV<String, Employee>> elems = SourceTestUtils.readFromSource(source, p.getOptions());
			bundleRecords.addAll(elems);
		}
		List<KV<String, String>> referenceRecords = getEmployeeNameList();
		List<KV<String, String>> transformedBundleRecords = Lists.transform(bundleRecords,new Function<KV<String, Employee>, KV<String, String>>(){
			@Override 
			public KV<String, String> apply(KV<String, Employee> input){
				return KV.of(input.getKey().trim(),input.getValue().getEmpName().trim());
			}
		});
		assertThat(transformedBundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}


	//This test validates records emitted in Pcollection are immutable 
	// if InputFormat's recordReader returns different objects (i.e. different locations in memory)
	@Test
	public void testImmutablityOfOutputOfReadIfRecordReaderObjectsAreImmutable()  throws Exception {
		SerializableConfiguration serConf = getConfiguration(TypeDescriptor.of(ImmutableRecordsInputFormat.class),TypeDescriptor.of(java.lang.String.class),TypeDescriptor.of(Employee.class)); 
		HadoopInputFormatBoundedSource<String, Employee> parentHIFSource = new HadoopInputFormatBoundedSource<String, Employee>(serConf, StringUtf8Coder.of(), EmployeeCoder.of());
		List<BoundedSource<KV<String, Employee>>> boundedSourceList = parentHIFSource.splitIntoBundles(0, p.getOptions());
		List<KV<String, Employee>> bundleRecords = new ArrayList<>();
		for (BoundedSource<KV<String, Employee>> source : boundedSourceList) {
			List<KV<String, Employee>> elems = SourceTestUtils.readFromSource(source, p.getOptions());
			bundleRecords.addAll(elems);
		}
		List<KV<String, String>> referenceRecords = getEmployeeNameList();
		List<KV<String, String>> transformedBundleRecords = Lists.transform(bundleRecords,new Function<KV<String, Employee>, KV<String, String>>(){
			@Override 
			public KV<String, String> apply(KV<String, Employee> input){
				return KV.of(input.getKey().trim(),input.getValue().getEmpName().trim());
			}
		});
		assertThat(transformedBundleRecords, containsInAnyOrder(referenceRecords.toArray()));
	}

	private List<KV<Text, Employee>> getEmployeeData(){
		List<KV<Text, Employee>> data = new ArrayList<KV<Text, Employee>>();
		data.add(KV.of( new Text("0"), new Employee ("Prabhanj", "Pune")));
		data.add(KV.of( new Text("1"), new Employee ("Rahul", "Pune")));
		data.add(KV.of( new Text("2"), new Employee ("Saikat", "Mumbai")));
		data.add(KV.of( new Text("3"), new Employee ("Gurumoorthy", "Hyderabad")));
		data.add(KV.of( new Text("4"), new Employee ("Shubham", "Banglore")));
		data.add(KV.of( new Text("5"), new Employee ("Neha", "Chennai")));
		data.add(KV.of( new Text("6"), new Employee ("Priyanka", "Pune")));
		data.add(KV.of( new Text("7"), new Employee ("Nikita", "Chennai")));
		data.add(KV.of( new Text("8"), new Employee ("Pallavi", "Pune")));
		return data;
	}

	private  List<KV<String, String>> getEmployeeNameList(){
		return  Lists.transform(getEmployeeData(),new Function<KV<Text, Employee>, KV<String, String>>(){
			@Override 
			public KV<String, String> apply(KV<Text, Employee> input){
				return KV.of(input.getKey().toString().trim(),input.getValue().getEmpName().trim());
			}
		});
	}

	private static SerializableConfiguration getConfiguration(TypeDescriptor<?> inputFormatClassName, TypeDescriptor<?> keyClass, TypeDescriptor<?> valueClass) {
		Configuration conf = new Configuration();
		conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName.getRawType(), InputFormat.class);
		conf.setClass("key.class", keyClass.getRawType(), Object.class);
		conf.setClass("value.class", valueClass.getRawType(), Object.class);
		return new SerializableConfiguration(conf);
	}

	@DefaultCoder(AvroCoder.class)
	public class Employee implements Serializable{
		private static final long serialVersionUID = 1L;
		private String empAddress;
		private String empName;
		public Employee(String empName, String empAddress) {
			this.empAddress = empAddress;
			this.empName = empName;
		}
		public String getEmpName() {
			return empName;
		}
		public void setEmpName(String empName) {
			this.empName = empName;
		}
		public String getEmpAddress() {
			return empAddress;
		}
		public void setEmpAddress(String empAddress) {
			this.empAddress = empAddress;
		}
	}
}
