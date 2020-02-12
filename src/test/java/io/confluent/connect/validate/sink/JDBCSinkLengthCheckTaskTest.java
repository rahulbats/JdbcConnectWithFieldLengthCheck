package io.confluent.connect.validate.sink;

import static org.junit.Assert.*;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.PlainListener;
import io.confluent.connect.jdbc.util.TableId;
import junit.framework.TestCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hsqldb.Server;
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import scala.sys.Prop;


public class JDBCSinkLengthCheckTaskTest{
    // Testing necessary methods for JDBCSinkLengthCheckTask
    private static final Logger log = LoggerFactory.getLogger(JDBCSinkLengthCheckTaskTest.class);

    //In-memory SQL DB get connection
    private static Connection initDatabase() throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:9001/TESTINGDB", "SA", "");
    }

    @ClassRule
    public static SharedKafkaTestResource kafkaBroker = new SharedKafkaTestResource().withBrokers(1).registerListener(new PlainListener().onPorts(9092));
    private KafkaTestUtils utils = kafkaBroker.getKafkaTestUtils();


    @BeforeClass
    public static void setUp() throws Exception {

        Server server = new Server();
        server.setDatabaseName(0, "TESTINGDB");
        server.setDatabasePath(0, "inmem:TESTINGDB");
        server.setPort(9001); // this is the default port
        server.setSilent(true);
        server.setRestartOnShutdown(false);
        server.signalCloseAllServerConnections();
        server.setNoSystemExit(true);
        server.start();

        // Init DB
        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        Connection dbconn = initDatabase();
        Statement statement = dbconn.createStatement();

        //Create table in DB with restrictions
        statement.execute("SET PROPERTY \"sql.enforce_strict_size\" TRUE");
        statement.execute("CREATE TABLE TESTINGDB (ID VARCHAR(3) NOT NULL, NAME VARCHAR(6) NOT NULL, COMMENT VARCHAR(4) NOT NULL)");
        dbconn.commit();

    }


    @AfterClass
    public static void tearDown() throws IOException, SQLException {
        Connection dbconn = initDatabase();
        dbconn.createStatement().execute("DROP TABLE TESTINGDB");
        dbconn.createStatement().execute("TRUNCATE SCHEMA PUBLIC RESTART IDENTITY AND COMMIT NO CHECK");
        dbconn.createStatement().execute("SHUTDOWN");
        dbconn.close();
    }

    @Test
    public void testStart() throws Exception{

        //Get Properties
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:hsqldb:hsql://localhost:9001/TESTINGDB");
        props.put("connector.class", "io.confluent.connect.validate.JDBCSinkLengthCheckConnector");
        props.put("LENGTH_CHECK_DEAD_LETTER_TOPIC", "DLQTopic");
        props.put("LENGTH_CHECK_DEAD_LETTER_TOPIC_BOOTSTRAP_SERVERS", "localhost:9092");
        props.put("connection.user", "SA");
        props.put("connection.password", "");


        JDBCSinkLengthCheckTask jdbctask = new JDBCSinkLengthCheckTask();
        jdbctask.start(props);

        try {
            assertEquals(props.get("LENGTH_CHECK_DEAD_LETTER_TOPIC"), jdbctask.deadLetterTopic);
        }catch (Exception noDLQTopic){
            log.debug("No DLQ topic provided");
        }

        assertNotNull(jdbctask.dlqProducer);

    }


    @Test
    public void testPut() throws Exception{
        utils.createTopic("DLQTopic", 3, (short) 1);

        JDBCSinkLengthCheckTask jdbctask2 = new JDBCSinkLengthCheckTask();
        //Get Properties
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:hsqldb:hsql://localhost:9001/TESTINGDB");
        props.put("connector.class", "io.confluent.connect.validate.JDBCSinkLengthCheckConnector");
        props.put("LENGTH_CHECK_DEAD_LETTER_TOPIC", "DLQTopic");
        props.put("LENGTH_CHECK_DEAD_LETTER_TOPIC_BOOTSTRAP_SERVERS", "localhost:9092");
        props.put("connection.user", "SA");
        props.put("connection.password", "");

        jdbctask2.start(props);


        String topic = "TESTINGDB";

        Schema keySchema = Schema.INT64_SCHEMA;

        Schema valueSchema = SchemaBuilder.struct()
                .field("ID", Schema.STRING_SCHEMA)
                .field("NAME", Schema.STRING_SCHEMA)
                .field("COMMENT", Schema.STRING_SCHEMA)
                .build();

        Struct failWrite = new Struct(valueSchema)
                .put("ID", "1010")
                .put("NAME", "Abe")
                .put("COMMENT","HA");

        Struct failWrite2 = new Struct(valueSchema)
                .put("ID", "101")
                .put("NAME", "Abraham")
                .put("COMMENT","HA");

        Struct failWrite3 = new Struct(valueSchema)
                .put("ID", "101")
                .put("NAME", "Abe")
                .put("COMMENT","Hello");

        Struct failWriteNull = new Struct(valueSchema)
                .put("ID", "")
                .put("NAME", "Abraham")
                .put("COMMENT","Hello");


        jdbctask2.put(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWrite, 0)));
        jdbctask2.put(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWrite2, 1)));
        jdbctask2.put(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWrite3, 2)));
        jdbctask2.put(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWriteNull, 3)));

        List<ConsumerRecord<byte[], byte[]>> recordsConsumed = utils.consumeAllRecordsFromTopic("DLQTopic");

        assertEquals(4, recordsConsumed.size());

    }

}