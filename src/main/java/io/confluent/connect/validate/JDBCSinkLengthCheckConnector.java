package io.confluent.connect.validate;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.validate.sink.JDBCSinkLengthCheckTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class JDBCSinkLengthCheckConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(JDBCSinkLengthCheckConnector.class);


    private Map<String, String> configProps;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JDBCSinkLengthCheckTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since FileStreamSinkConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return JdbcSinkConfig.CONFIG_DEF;
    }
}
