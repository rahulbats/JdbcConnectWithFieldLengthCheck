package io.confluent.connect.validate.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.BufferedRecords;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.util.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class JdbcDbWriterWithFieldCheck {
    private static final Logger log = LoggerFactory.getLogger(JdbcDbWriterWithFieldCheck.class);

    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    final CachedConnectionProvider cachedConnectionProvider;
    private final TableDefinitions tableDefns;

    JdbcDbWriterWithFieldCheck(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.tableDefns = new TableDefinitions(dbDialect);
        this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
            @Override
            protected void onConnect(Connection connection) throws SQLException {
                log.info("JdbcDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    List< SinkRecord> write(final Collection<SinkRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();
        final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
        Map<TableId, Map<String, Integer>> tableColumnSizes = new HashMap<>();
        ResultSet rsColumns = null;
        DatabaseMetaData meta = connection.getMetaData();
        final List< SinkRecord> rejectedRecords = new ArrayList<>();
        for (SinkRecord record : records) {
            final TableId tableId = destinationTable(record.topic());
            List<String> textFields =  record.valueSchema().fields().stream()
                    .filter(field -> field.schema().type().equals(Schema.Type.STRING))
                    .map(field -> field.name())
                    .collect(Collectors.toList());

            long startofTableDef = new Date().getTime();
            TableDefinition tableDefinition = tableDefns.get(connection, tableId);
            log.debug("got tabledef in "+(new Date().getTime()-startofTableDef));


            boolean correct =true;

            //Set<ColumnId> columnIds = columnDefs.keySet();
            String rejectionMessage="";
            for(String columnName: tableDefinition.columnNames()) {

                if(textFields.contains(columnName)){
                    String recordValue= (String) ((Struct) record.value()).get(columnName);

                    ColumnDefinition columnDefinition = tableDefinition.definitionForColumn(columnName);
                    //log.info("Column: "+columnId.name()+" Length: "+recordValue.length()+", DB Len: "+columnDefinition.precision()+", isNull: "+ columnDefinition.isOptional());
                    if(recordValue!=null && columnDefinition!=null)
                    {

                        if(recordValue.length() > columnDefinition.precision())
                        {
                            correct= false;
                            rejectionMessage = columnName+" size:"+recordValue.length()+" is greater than the DB length:"+columnDefinition.precision();
                            break;
                        }
                        if(!columnDefinition.isOptional() ) {
                            correct = recordValue.length() > 0;
                            if(!correct){
                                rejectionMessage = columnName+" is empty but its not a optional field in Database";
                            }
                        }
                    }

                    if(correct==false)
                    {
                        log.info("Can't send the record to DB, breaking");
                        break;
                    }
                }
            }


            if(correct) {
                BufferedRecords buffer = bufferByTable.get(tableId);
                if (buffer == null) {
                    buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
                    bufferByTable.put(tableId, buffer);
                }
                buffer.add(record);
            }
            else {
                log.error("failed writing record :"+record);
                record.headers().addString("DBErrorMessage",rejectionMessage);
                log.info("error record headers "+record.headers());
                rejectedRecords.add(record);
            }
        }
        for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
            TableId tableId = entry.getKey();
            BufferedRecords buffer = entry.getValue();
            log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
            buffer.flush();
            buffer.close();
        }
        connection.commit();
        return rejectedRecords;
    }

    void closeQuietly() {
        cachedConnectionProvider.close();
    }

    TableId destinationTable(String topic) {
        final String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (tableName.isEmpty()) {
            throw new ConnectException(String.format(
                    "Destination table name for topic '%s' is empty using the format string '%s'",
                    topic,
                    config.tableNameFormat
            ));
        }
        return dbDialect.parseTableIdentifier(tableName);
    }
}
