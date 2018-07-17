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
package org.apache.beam.sdk.extensions.sql.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLineTestingUtils.buildArgs;
import static org.apache.beam.sdk.extensions.sql.jdbc.BeamSqlLineTestingUtils.toLines;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.testing.TestPipeline;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.hamcrest.collection.IsIn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** BeamSqlLine integration tests. */
public class BeamSqlLineITTest implements Serializable {

  @Rule public transient TestPubsub eventsTopic = TestPubsub.create();

  private static String project;
  private static String createPubsubTableStatement;
  private static String setProject;
  private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static final String publicTopic = "projects/pubsub-public-data/topics/taxirides-realtime";

  private ExecutorService pool;

  @BeforeClass
  public static void setUpClass() {
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setProject = String.format("SET project = '%s';", project);

    createPubsubTableStatement =
        "CREATE TABLE taxi_rides (\n"
            + "         event_timestamp TIMESTAMP,\n"
            + "         attributes MAP<VARCHAR, VARCHAR>,\n"
            + "         payload ROW<\n"
            + "           ride_id VARCHAR,\n"
            + "           point_idx INT,\n"
            + "           latitude DOUBLE,\n"
            + "           longitude DOUBLE,\n"
            + "           meter_reading DOUBLE,\n"
            + "           meter_increment DOUBLE,\n"
            + "           ride_status VARCHAR,\n"
            + "           passenger_count TINYINT>)\n"
            + "       TYPE pubsub \n"
            + "       LOCATION '%s'\n"
            + "       TBLPROPERTIES '{\"timestampAttributeKey\": \"ts\"}';";

    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Before
  public void setUp() {
    pool = Executors.newFixedThreadPool(1);
  }

  @After
  public void tearDown() {
    pool.shutdown();
  }

  // @Ignore
  // @Test
  // public void testSelectFromPubsub() throws Exception {
  //   String[] args =
  //       buildArgs(
  //           String.format(createPubsubTableStatement, eventsTopic.topicPath()),
  //           setProject,
  //           "SELECT event_timestamp, taxi_rides.payload.ride_status, taxi_rides.payload.latitude, "
  //               + "taxi_rides.payload.longitude from taxi_rides LIMIT 3;");
  //
  //   Future<List<List<String>>> expectedResult = runQueryInBackground(args);
  //   eventsTopic.checkIfTopicExists(project, 10);
  //
  //   List<PubsubMessage> messages =
  //       ImmutableList.of(
  //           message(
  //               "2018-07-01T21:25:20Z",
  //               taxiRideJSON("id1", 1, 40.702, -74.001, 1000, 10, "enroute", 2)),
  //           message(
  //               ("2018-07-01T21:26:06Z"),
  //               taxiRideJSON("id2", 2, 40.703, -74.002, 1000, 10, "enroute", 4)),
  //           message(
  //               ("2018-07-02T13:26:06Z"),
  //               taxiRideJSON("id3", 3, 30.0, -72.32324, 2000, 20, "enroute", 7)));
  //
  //   eventsTopic.publish(messages);
  //
  //   Object[] l = expectedResult.get(30, TimeUnit.SECONDS).toArray();
  //   assertThat(
  //       Arrays.asList(
  //           Arrays.asList("2018-07-01 21:25:20", "enroute", "40.702", "-74.001"),
  //           Arrays.asList("2018-07-01 21:26:06", "enroute", "40.703", "-74.002"),
  //           Arrays.asList("2018-07-02 13:26:06", "enroute", "30.0", "-72.32324")),
  //       everyItem(IsIn.isOneOf(l)));
  // }
  //
  // @Ignore
  // @Test
  // public void testFilterForSouthManhattan() throws Exception {
  //   String[] args =
  //       buildArgs(
  //           String.format(createPubsubTableStatement, eventsTopic.topicPath()),
  //           setProject,
  //           "SELECT event_timestamp, taxi_rides.payload.ride_status, \n"
  //               + "taxi_rides.payload.latitude, taxi_rides.payload.longitude from taxi_rides\n"
  //               + "       WHERE taxi_rides.payload.longitude > -74.747\n"
  //               + "         AND taxi_rides.payload.longitude < -73.969\n"
  //               + "         AND taxi_rides.payload.latitude > 40.699\n"
  //               + "         AND taxi_rides.payload.latitude < 40.720 LIMIT 2;");
  //
  //   Future<List<List<String>>> expectedResult = runQueryInBackground(args);
  //   eventsTopic.checkIfTopicExists(project, 10);
  //
  //   List<PubsubMessage> messages =
  //       ImmutableList.of(
  //           message(
  //               convertTimestampToMillis("2018-07-01 21:25:20"),
  //               taxiRideJSON("id1", 1, 40.701, -74.001, 1000, 10, "enroute", 2)),
  //           message(
  //               convertTimestampToMillis("2018-07-01 21:26:06"),
  //               taxiRideJSON("id2", 2, 40.702, -74.002, 1000, 10, "enroute", 4)),
  //           message(
  //               convertTimestampToMillis("2018-07-02 13:26:06"),
  //               taxiRideJSON("id3", 3, 30, -72.32324, 2000, 20, "enroute", 7)),
  //           message(
  //               convertTimestampToMillis("2018-07-02 14:28:22"),
  //               taxiRideJSON("id4", 4, 34, -73.32324, 2000, 20, "enroute", 8)));
  //
  //   eventsTopic.publish(messages);
  //
  //   assertThat(
  //       Arrays.asList(
  //           Arrays.asList("2018-07-01 21:25:20", "enroute", "40.701", "-74.001"),
  //           Arrays.asList("2018-07-01 21:26:06", "enroute", "40.702", "-74.002")),
  //       everyItem(IsIn.isOneOf(expectedResult.get(30, TimeUnit.SECONDS).toArray())));
  // }

  @Test
  public void testFixedWindow() throws Exception {
    String[] args =
        buildArgs(
            String.format(createPubsubTableStatement, eventsTopic.topicPath()),
            setProject,
            "WITH geo_cells AS (\n"
                + "         SELECT FLOOR(taxi_rides.payload.latitude / 0.05) * 0.05 AS reduced_lat,\n"
                + "                FLOOR(taxi_rides.payload.longitude / 0.05) * 0.05 AS reduced_lon,\n"
                + "                taxi_rides.event_timestamp\n"
                + "         FROM taxi_rides)\n"
                + "       SELECT COUNT(*) as num_events,\n"
                + "              geo_cells.reduced_lat,\n"
                + "              geo_cells.reduced_lon, \n"
                + "              TUMBLE_START(geo_cells.event_timestamp, INTERVAL '1' SECOND)\n"
                + "       FROM geo_cells    \n"
                + "       GROUP BY geo_cells.reduced_lat,\n"
                + "                geo_cells.reduced_lon,\n"
                + "                TUMBLE(geo_cells.event_timestamp, INTERVAL '1' SECOND)\n"
                + "       LIMIT 2;");

    Future<List<List<String>>> expectedResult = runQueryInBackground(args);

    // for (int i = 0; i < 10; i++) {
      List<PubsubMessage> messages =
          ImmutableList.of(
              message(
                  // convertTimestampToMillis("2018-07-01 21:25:20"),
                  "2018-07-01T21:25:20Z",
                  taxiRideJSON("id1", 1, 40.701, -74.001, 1000, 10, "enroute", 2)),
              message(
                  // convertTimestampToMillis("2018-07-01 21:25:21"),
                  "2018-07-01T21:25:21Z",
                  taxiRideJSON("id2", 2, 40.702, -74.002, 1000, 10, "enroute", 4)));
      eventsTopic.publish(messages);

      // Thread.sleep(1000);
    // }

    assertThat(
        Arrays.asList(
            Arrays.asList(1, "40.701", "-74.001", "2018-07-01 21:25:19"),
            Arrays.asList(1, "40.702", "-74.002", "2018-07-01 21:25:20")),
        everyItem(IsIn.isOneOf(expectedResult.get(500, TimeUnit.SECONDS).toArray())));
  }

  // @Test
  // public void testSlidingWindow() throws Exception {
  //   Future<List<List<String>>> expectedResult =
  //       pool.submit(
  //           (Callable)
  //               () -> {
  //                 String[] args =
  //                     buildArgs(
  //                         String.format(createPubsubTableStatement, publicTopic),
  //                         setProject,
  //                         "SELECT COUNT(*) AS num_events,\n"
  //                             + "              SUM(taxi_rides.payload.meter_increment) as revenue,\n"
  //                             + "              HOP_END(\n"
  //                             + "                 taxi_rides.event_timestamp, \n"
  //                             + "                 INTERVAL '1' SECOND, \n"
  //                             + "                 INTERVAL '2' SECOND) as minute_end\n"
  //                             + "              FROM taxi_rides\n"
  //                             + "              GROUP BY HOP(\n"
  //                             + "                 taxi_rides.event_timestamp,\n"
  //                             + "                 INTERVAL '1' SECOND, \n"
  //                             + "                 INTERVAL '2' SECOND) LIMIT 2");
  //                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
  //                 BeamSqlLine.runSqlLine(args, null, outputStream, null);
  //                 return toLines(outputStream);
  //               });
  //
  //   // LIMIT 2 is supposed to be parsed into 6 lines.
  //   assertTrue(expectedResult.get().size() == 6);
  // }

  private String taxiRideJSON(
      String rideId,
      int pointIdex,
      double latitude,
      double longitude,
      int meterReading,
      int meterIncrement,
      String rideStatus,
      int passengerCount) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode objectNode = mapper.createObjectNode();
    objectNode.put("ride_id", rideId);
    objectNode.put("point_idx", pointIdex);
    objectNode.put("latitude", latitude);
    objectNode.put("longitude", longitude);
    objectNode.put("meter_reading", meterReading);
    objectNode.put("meter_increment", meterIncrement);
    objectNode.put("ride_status", rideStatus);
    objectNode.put("passenger_count", passengerCount);
    return objectNode.toString();
  }

  private Future<List<List<String>>> runQueryInBackground(String[] args) {
    return pool.submit(
        (Callable)
            () -> {
              ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
              BeamSqlLine.runSqlLine(args, null, outputStream, null);
              return toLines(outputStream);
            });
  }

  private long convertTimestampToMillis(String timestamp) throws ParseException {
    return dateFormat.parse(timestamp).getTime();
  }

  private PubsubMessage message(String timestamp, String jsonPayload) {
    return new PubsubMessage(
        jsonPayload.getBytes(UTF_8), ImmutableMap.of("ts", String.valueOf(timestamp)));
  }

  // private PubsubMessage message(long timestampInMillis, String jsonPayload) {
  //   return new PubsubMessage(
  //       jsonPayload.getBytes(UTF_8), ImmutableMap.of("ts", String.valueOf(timestampInMillis)));
  // }
}
