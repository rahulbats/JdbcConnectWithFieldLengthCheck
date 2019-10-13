package io.confluent.connect.validate.sink;


import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.DbStructure;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.SQLException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class JDBCSinkLengthCheckTask extends JdbcSinkTask {
    private static final Logger log = LoggerFactory.getLogger(JDBCSinkLengthCheckTask.class);


    DatabaseDialect dialect;
    JdbcSinkConfig config;
    JdbcDbWriterWithFieldCheck writer;
    int remainingRetries;

    String deadLetterTopic = null;


    Producer<String, String> dlqProducer = null;
    boolean DLQConfigured = false;



    @Override
    public void start(final Map<String, String> props) {
        log.debug("Starting JDBC Sink task");
        config = new JdbcSinkConfig(props);
        initWriter();
        remainingRetries = config.maxRetries;

        String prefix = "LENGTH_CHECK_DEAD_LETTER_TOPIC";

        DLQConfigured= props.containsKey(prefix);
        if(DLQConfigured )
        {
            deadLetterTopic = props.get(prefix);
            Properties deadletterProps = new Properties();
            String underscoredPrefix = prefix+"_";
            int beginIndex = underscoredPrefix.length();

            props.keySet().stream()
                    .filter(key->key.startsWith(underscoredPrefix))
                    .forEach(key-> {

                        String generatedKey = key.substring(beginIndex).toLowerCase().replace('_','.');
                        deadletterProps.put( generatedKey, props.get(key));
                    } );

            deadletterProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            deadletterProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());

            log.debug("creating producer with props :"+props.toString());

            dlqProducer = new KafkaProducer<String, String>(deadletterProps);

            //initDLQProducer();
        }

    }

    void initWriter() {
        if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(config.dialectName, config);
        } else {
            dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        log.debug("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new JdbcDbWriterWithFieldCheck(config, dialect, dbStructure);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                        + "database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            List<SinkRecord> rejections = writer.write(records);

            log.info("rejected records "+rejections);
            log.debug("is DLQ configured "+DLQConfigured);
            //log.info("sending to DLQ procuder with boostrap server :"+dlqBootStrapServer+": topic :"+deadLetterTopic+":SASL jaas config: "+saslJaasConfig+":sasl mechanism: "+saslMechanism);
            if(DLQConfigured) {

                rejections.forEach(sinkRecord -> {
                    dlqProducer.send(new ProducerRecord<String, String>(this.deadLetterTopic,((Struct)sinkRecord.value()).toString()));
                });
            }


        } catch (SQLException sqle) {
            log.warn(
                    "Write of {} records failed, remainingRetries={}",
                    records.size(),
                    remainingRetries,
                    sqle
            );
            String sqleAllMessages = "";
            for (Throwable e : sqle) {
                sqleAllMessages += e + System.lineSeparator();
            }
            if (remainingRetries == 0) {
                throw new ConnectException(new SQLException(sqleAllMessages));
            } else {
                writer.closeQuietly();
                initWriter();
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(new SQLException(sqleAllMessages));
            }
        } finally {
            if(dlqProducer!=null)
                dlqProducer.flush();

        }
        remainingRetries = config.maxRetries;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    public void stop() {
        log.info("Stopping task");
        try {
            writer.closeQuietly();
        } finally {
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
        if(dlqProducer!=null)
            dlqProducer.close();
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }


}
