/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.hadoop.inputformat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
<<<<<<< HEAD:sdks/java/io/hadoop-input-format/src/test/java/org/apache/beam/sdk/io/hadoop/inputformat/unit/tests/HIFIOWithCassandraTest.java
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
=======
>>>>>>> Modifications according to code review comments.:sdks/java/io/hadoop-input-format/src/test/java/org/apache/beam/sdk/io/hadoop/inputformat/HIFIOWithCassandraTest.java
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

/**
 *
 * Tests to validate HadoopInputFormatIO for embedded Cassandra instance.
 *
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class HIFIOWithCassandraTest implements Serializable {
<<<<<<< HEAD
<<<<<<< HEAD
  private static final long serialVersionUID = 1L;
  private static final String CASSANDRA_KEYSPACE = "hif_keyspace";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "person";
  private static transient Cluster cluster;
  private static transient Session session;
=======
  private static final long serialVersionUID = 1L;
  private static final String CASSANDRA_KEYSPACE = "beamdb";
  private static final String CASSANDRA_HOST = "127.0.0.1";
  private static final String CASSANDRA_TABLE = "scientists";
  private static transient Cluster cluster;
  private static transient Session session;

  // Setting Cassandra embedded server startup timeout to 2 min
  private static final long CASSANDRA_SERVER_STARTUP_TIMEOUT = 120000L;

>>>>>>> Implemented review comments for ITs and embedded tests
  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @BeforeClass
<<<<<<< HEAD
  public static void startCassandra() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml", "target/cassandra",
        Long.MAX_VALUE);
    cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withClusterName("beam").build();
    session = cluster.connect();
  }

  @Before
  public void createTable() throws Exception {
    session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE
        + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};");
    session.execute("USE " + CASSANDRA_KEYSPACE);
    session
        .execute("CREATE TABLE person(person_id int, person_name text, PRIMARY KEY(person_id));");
    session.execute("INSERT INTO person(person_id, person_name) values(0, 'John Foo');");
    session.execute("INSERT INTO person(person_id, person_name) values(1, 'David Bar');");
=======
  public static void startEmbeddedCassandra() throws Exception {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml", "target/cassandra",
        CASSANDRA_SERVER_STARTUP_TIMEOUT);
    cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST).withClusterName("beam").build();
    session = cluster.connect();
    createTable();
  }

  public static void createTable() throws Exception {
    session.execute("CREATE KEYSPACE " + CASSANDRA_KEYSPACE
        + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};");
    session.execute("USE " + CASSANDRA_KEYSPACE);
    session.execute("CREATE TABLE " + CASSANDRA_TABLE
        + "(id int, scientist text, PRIMARY KEY(id));");
    session.execute("INSERT INTO " + CASSANDRA_TABLE + "(id, scientist) values(0, 'Faraday');");
    session.execute("INSERT INTO " + CASSANDRA_TABLE + "(id, scientist) values(1, 'Newton');");
>>>>>>> Implemented review comments for ITs and embedded tests
  }

  /**
   * Test to read data from embedded Cassandra instance and verify whether data is read
   * successfully.
   *
   * @throws Exception
   */
  @Test
  public void testHIFReadForCassandra() throws Exception {
    Pipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
<<<<<<< HEAD
      private static final long serialVersionUID = 1L;

      @Override
      public String apply(Row input) {
        return input.getString("person_name");
      }
    };
    PCollection<KV<Long, String>> cassandraData =
        p.apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(2L);
    List<KV<Long, String>> expectedResults =
        Arrays.asList(KV.of(2L, "John Foo"), KV.of(1L, "David Bar"));
    PAssert.that(cassandraData).containsInAnyOrder(expectedResults);
    p.run();
=======
      @Override
      public String apply(Row input) {
        return input.getString("scientist");
      }
    };
    PCollection<KV<Long, String>> cassandraData = p
                  .apply(HadoopInputFormatIO.<Long, String> read()
                      .withConfiguration(conf)
                      .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(2L);
    List<KV<Long, String>> expectedResults =
        Arrays.asList(KV.of(2L, "Faraday"), KV.of(1L, "Newton"));
    PAssert.that(cassandraData).containsInAnyOrder(expectedResults);
    p.run().waitUntilFinish();
>>>>>>> Implemented review comments for ITs and embedded tests
  }

  /**
   * Test to read data from embedded Cassandra instance based on query and verify whether data is
   * read successfully.
<<<<<<< HEAD
=======
   *
>>>>>>> Implemented review comments for ITs and embedded tests
   * @throws Exception
   */
  @Test
  public void testHIFReadForCassandraQuery() throws Exception {
    Pipeline p = TestPipeline.create();
    Configuration conf = getConfiguration();
<<<<<<< HEAD
    conf.set(
        "cassandra.input.cql",
        "select * from hif_keyspace.person where token(person_id) > ? and token(person_id) <= ? and person_name='David Bar' allow filtering");
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      private static final long serialVersionUID = 1L;

      @Override
      public String apply(Row input) {
        return input.getString("person_name");
      }
    };
    PCollection<KV<Long, String>> cassandraData =
        p.apply(HadoopInputFormatIO.<Long, String>read().withConfiguration(conf)
            .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(1L);

    p.run();
  }

  @After
  public void dropTable() throws Exception {
=======
    conf.set("cassandra.input.cql", "select * from " + CASSANDRA_KEYSPACE + "." + CASSANDRA_TABLE
        + " where token(id) > ? and token(id) <= ? and scientist='Newton' allow filtering");
    SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
      @Override
      public String apply(Row input) {
        return input.getString("scientist");
      }
    };
    PCollection<KV<Long, String>> cassandraData = p
                  .apply(HadoopInputFormatIO.<Long, String>read()
                      .withConfiguration(conf)
                      .withValueTranslation(myValueTranslate));
    PAssert.thatSingleton(cassandraData.apply("Count", Count.<KV<Long, String>>globally()))
        .isEqualTo(1L);

    p.run().waitUntilFinish();
  }

  public static void dropTable() throws Exception {
>>>>>>> Implemented review comments for ITs and embedded tests
    session.execute("Drop TABLE " + CASSANDRA_TABLE);
    session.execute("Drop KEYSPACE " + CASSANDRA_KEYSPACE);
  }

  @AfterClass
<<<<<<< HEAD
  public static void stopCassandra() throws Exception {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Table(name = "person", keyspace = CASSANDRA_KEYSPACE)
  public static class Person implements Serializable {
    private static final long serialVersionUID = 1L;
    @Column(name = "person_name")
    private String name;
    @Column(name = "person_id")
=======
  public static void stopEmbeddedCassandra() throws Exception {
    dropTable();
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
  }

  @Table(name = CASSANDRA_TABLE, keyspace = CASSANDRA_KEYSPACE)
  public static class Scientist implements Serializable {
    private static final long serialVersionUID = 1L;
    @Column(name = "scientist")
    private String name;
    @Column(name = "id")
>>>>>>> Implemented review comments for ITs and embedded tests
    private int id;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String toString() {
      return id + ":" + name;
    }
  }

<<<<<<< HEAD
=======
  /**
   * Returns configuration of CqlInutFormat. Mandatory parameters required apart from inputformat
   * class name, key class, value class are thrift port, thrift address, partitioner class, keyspace
   * and columnfamily name
   */
>>>>>>> Implemented review comments for ITs and embedded tests
  public Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("cassandra.input.thrift.port", "9061");
    conf.set("cassandra.input.thrift.address", CASSANDRA_HOST);
    conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
    conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
    conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
<<<<<<< HEAD
    conf.setClass("mapreduce.job.inputformat.class",
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
    conf.setClass("key.class", java.lang.Long.class, Object.class);
    conf.setClass("value.class", com.datastax.driver.core.Row.class, Object.class);
    return conf;
  }
=======
	private static final long serialVersionUID = 1L;
	private static final String CASSANDRA_KEYSPACE = "hif_keyspace";
	private static final String CASSANDRA_HOST = "127.0.0.1";
	private static final String CASSANDRA_TABLE = "person";
	private static transient Cluster cluster;
	private static transient Session session;
	@Rule
	public final transient TestPipeline p = TestPipeline.create();

	@BeforeClass
	public static void startEmbeddedCassandra() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml",
				"target/cassandra", Long.MAX_VALUE);
		cluster = Cluster.builder().addContactPoint(CASSANDRA_HOST)
				.withClusterName("beam").build();
		session = cluster.connect();
	}

	@Before
	public void createTable() throws Exception {
		session.execute("CREATE KEYSPACE "
				+ CASSANDRA_KEYSPACE
				+ " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};");
		session.execute("USE " + CASSANDRA_KEYSPACE);
		session.execute("CREATE TABLE person(person_id int, person_name text, PRIMARY KEY(person_id));");
		session.execute("INSERT INTO person(person_id, person_name) values(0, 'John Foo');");
		session.execute("INSERT INTO person(person_id, person_name) values(1, 'David Bar');");
	}

	/**
	 * Test to read data from embedded Cassandra instance and verify whether
	 * data is read successfully.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHIFReadForCassandra() throws Exception {
		Pipeline p = TestPipeline.create();
		Configuration conf = getConfiguration();
		SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
			@Override
			public String apply(Row input) {
				return input.getString("person_name");
			}
		};
		PCollection<KV<Long, String>> cassandraData = p
				.apply(HadoopInputFormatIO.<Long, String> read()
						.withConfiguration(conf)
						.withValueTranslation(myValueTranslate));
		PAssert.thatSingleton(
				cassandraData.apply("Count",
						Count.<KV<Long, String>> globally())).isEqualTo(2L);
		List<KV<Long, String>> expectedResults = Arrays.asList(
				KV.of(2L, "John Foo"), KV.of(1L, "David Bar"));
		PAssert.that(cassandraData).containsInAnyOrder(expectedResults);
		p.run();
	}

	/**
	 * Test to read data from embedded Cassandra instance based on query and
	 * verify whether data is read successfully.
	 *
	 * @throws Exception
	 */
	@Test
	public void testHIFReadForCassandraQuery() throws Exception {
		Pipeline p = TestPipeline.create();
		Configuration conf = getConfiguration();
		conf.set(
				"cassandra.input.cql",
				"select * from hif_keyspace.person where token(person_id) > ? and token(person_id) <= ? and person_name='David Bar' allow filtering");
		SimpleFunction<Row, String> myValueTranslate = new SimpleFunction<Row, String>() {
			@Override
			public String apply(Row input) {
				return input.getString("person_name");
			}
		};
		PCollection<KV<Long, String>> cassandraData = p
				.apply(HadoopInputFormatIO.<Long, String> read()
						.withConfiguration(conf)
						.withValueTranslation(myValueTranslate));
		PAssert.thatSingleton(
				cassandraData.apply("Count",
						Count.<KV<Long, String>> globally())).isEqualTo(1L);

		p.run();
	}

	@After
	public void dropTable() throws Exception {
		session.execute("Drop TABLE " + CASSANDRA_TABLE);
		session.execute("Drop KEYSPACE " + CASSANDRA_KEYSPACE);
	}

	@AfterClass
	public static void stopEmbeddedCassandra() throws Exception {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	@Table(name = "person", keyspace = CASSANDRA_KEYSPACE)
	public static class Person implements Serializable {
		private static final long serialVersionUID = 1L;
		@Column(name = "person_name")
		private String name;
		@Column(name = "person_id")
		private int id;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public String toString() {
			return id + ":" + name;
		}
	}

	/**
	 * Returns configuration of CqlInutFormat. Mandatory parameters required
	 * apart from inputformat class name, key class, value class are thrift
	 * port, thrift address, partitioner class, keyspace and columnfamily name
	 *
	 */
	public Configuration getConfiguration() {
		Configuration conf = new Configuration();
		conf.set("cassandra.input.thrift.port", "9061");
		conf.set("cassandra.input.thrift.address", CASSANDRA_HOST);
		conf.set("cassandra.input.partitioner.class", "Murmur3Partitioner");
		conf.set("cassandra.input.keyspace", CASSANDRA_KEYSPACE);
		conf.set("cassandra.input.columnfamily", CASSANDRA_TABLE);
		conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
				org.apache.cassandra.hadoop.cql3.CqlInputFormat.class,
				InputFormat.class);
		conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS,
				java.lang.Long.class, Object.class);
		conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS,
				com.datastax.driver.core.Row.class, Object.class);
		return conf;
	}
>>>>>>> Elastic, Cassandra embedded code and ITs
=======
    conf.setClass(HadoopInputFormatIOConstants.INPUTFORMAT_CLASSNAME,
        org.apache.cassandra.hadoop.cql3.CqlInputFormat.class, InputFormat.class);
    conf.setClass(HadoopInputFormatIOConstants.KEY_CLASS, java.lang.Long.class, Object.class);
    conf.setClass(HadoopInputFormatIOConstants.VALUE_CLASS, com.datastax.driver.core.Row.class,
        Object.class);
    return conf;
  }
>>>>>>> Implemented review comments for ITs and embedded tests

}
