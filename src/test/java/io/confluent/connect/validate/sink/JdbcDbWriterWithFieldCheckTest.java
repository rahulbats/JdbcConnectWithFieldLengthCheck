package io.confluent.connect.validate.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hsqldb.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

public class JdbcDbWriterWithFieldCheckTest {
    private static final Logger log = LoggerFactory.getLogger(JdbcDbWriterWithFieldCheck.class);

    //Writer instance
    private JdbcDbWriterWithFieldCheck testWriter;
    DatabaseDialect dialect;
    Server server;

    //In-memory SQL DB
    private static Connection initDatabase() throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:9001/TESTINGDB", "SA", "");
    }


    @Before
    public void setUp() throws Exception {

        server = new Server();
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

        //Get Properties
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:hsqldb:hsql://localhost:9001/TESTINGDB");
        props.put("connector.class", "io.confluent.connect.validate.JDBCSinkLengthCheckConnector");
        props.put("LENGTH_CHECK_DEAD_LETTER_TOPIC", "DLQTopic");
        props.put("LENGTH_CHECK_DEAD_LETTER_TOPIC_BOOTSTRAP_SERVERS", "localhost:9092");
        props.put("connection.user", "SA");
        props.put("connection.password", "");


        JdbcSinkConfig JDBConfig = new JdbcSinkConfig(props);
        dialect = DatabaseDialects.findBestFor(JDBConfig.connectionUrl, JDBConfig);
        DbStructure dbStructure = new DbStructure(dialect);

        testWriter = new JdbcDbWriterWithFieldCheck(JDBConfig, dialect, dbStructure);

    }

    @After
    public void tearDown() throws IOException, SQLException {
        if (testWriter != null)
            testWriter.closeQuietly();
        Connection dbconn = initDatabase();
        dbconn.createStatement().execute("DROP TABLE TESTINGDB");
        dbconn.createStatement().execute("TRUNCATE SCHEMA PUBLIC RESTART IDENTITY AND COMMIT NO CHECK");
        server.shutdown();


    }


    @Test
    public void testWrite() throws SQLException{

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

        Struct successfulWrite = new Struct(valueSchema)
                .put("ID", "101")
                .put("NAME", "Abe")
                .put("COMMENT","HA");

        Struct failWriteNull = new Struct(valueSchema)
                .put("ID", "")
                .put("NAME", "Abraham")
                .put("COMMENT","Hello");


        // Failure due to length in all columns
        List<SinkRecord> failedToWrite = testWriter.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWrite, 0)));
        failedToWrite.addAll(testWriter.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWrite2, 1))));
        failedToWrite.addAll(testWriter.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWrite3, 2))));

        // Success
        testWriter.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, successfulWrite, 3)));

        // Failure due to null
        List<SinkRecord> failedToWriteNull = testWriter.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema, failWriteNull, 4)));

        // Test failure due to length in all columns
        assertEquals(3, failedToWrite.size());

        // Test failure due to null
        assertEquals(1,failedToWriteNull.size());

        // Test success
        Connection dbconn = initDatabase();
        Statement checkRows = dbconn.createStatement();
        ResultSet response = checkRows.executeQuery("SELECT COUNT(*) AS TOTAL FROM TESTINGDB");
        response.next();
        assertEquals(1,response.getInt(1));



    }
}