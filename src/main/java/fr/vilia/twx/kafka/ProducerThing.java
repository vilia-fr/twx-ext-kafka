package fr.vilia.twx.kafka;

import ch.qos.logback.classic.Logger;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.*;
import com.thingworx.system.ContextType;
import com.thingworx.things.Thing;
import com.thingworx.types.constants.CommonPropertyNames;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

@ThingworxConfigurationTableDefinitions(
        tables = {@ThingworxConfigurationTableDefinition(
                name = "ConnectionInfo",
                description = "Connection Settings",
                isMultiRow = false,
                dataShape = @ThingworxDataShapeDefinition(
                        fields = {@ThingworxFieldDefinition(
                                name = "bootstrapServers",
                                description = "host1:port1,...,hostN:portN",
                                baseType = "STRING"
                        ), @ThingworxFieldDefinition(
                                name = "groupId",
                                description = "Consumer group ID",
                                baseType = "STRING"
                        ), @ThingworxFieldDefinition(
                                name = "topics",
                                description = "Comma-separated list of topic names",
                                baseType = "STRING"
                        ), @ThingworxFieldDefinition(
                                name = "username",
                                description = "SASL SCRAM username",
                                baseType = "STRING"
                        ), @ThingworxFieldDefinition(
                                name = "password",
                                description = "SASL SCRAM password",
                                baseType = "PASSWORD"
                        )}
                )
        )}
)
public class ProducerThing extends Thing {
    protected static final Logger LOGGER = LogUtilities.getInstance().getApplicationLogger(ProducerThing.class);

    private Producer<String, String> producer = null;

    @Override
    protected void startThing(ContextType contextType) throws Exception {
        init();
        super.startThing(contextType);
    }

    private void init() throws Exception {
        String bootstrapServers = (String) this.getConfigurationSetting("ConnectionInfo", "bootstrapServers");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            throw new IllegalArgumentException("Kafka ConsumerThing: bootstrapServers parameter cannot be empty");
        }
        String username = ((String) this.getConfigurationSetting("ConnectionInfo", "username"));
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Kafka ConsumerThing: username parameter cannot be empty");
        }
        String password = ((String) this.getConfigurationSetting("ConnectionInfo", "password"));
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Kafka ConsumerThing: password parameter cannot be empty");
        }
        String topics = (String) this.getConfigurationSetting("ConnectionInfo", "topics");
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("Kafka ConsumerThing: topics parameter cannot be empty");
        }
        String groupId = (String) this.getConfigurationSetting("ConnectionInfo", "groupId");
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("Kafka ConsumerThing: groupId parameter cannot be empty");
        }

        final Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" +
                        username +
                        "\" password=\"" +
                        password +
                        "\";"
        );
        producer = new KafkaProducer<String, String>(props);
        LOGGER.info("Kafka producer started");
    }

    @ThingworxServiceDefinition(name="Send", description="Produce a message on a Kafka topic")
    @ThingworxServiceResult(name=CommonPropertyNames.PROP_RESULT, baseType="NOTHING")
    public void Send(
            @ThingworxServiceParameter(name = "key", baseType = "STRING") String key,
            @ThingworxServiceParameter(name = "value", baseType = "STRING") String value,
            @ThingworxServiceParameter(name = "topic", baseType = "STRING") String topic,
            @ThingworxServiceParameter(name = "sync", baseType = "BOOLEAN", aspects = {"defaultValue:false"}) Boolean sync
    ) throws Exception {
        Future<RecordMetadata> f = producer.send(new ProducerRecord<String, String>(topic, key, value));
        if (sync != null && sync) {
            f.get();
        }
    }

    @ThingworxServiceDefinition(name="Flush", description="Flush the producer buffer")
    @ThingworxServiceResult(name=CommonPropertyNames.PROP_RESULT, baseType="NOTHING")
    public void Flush() throws Exception {
        producer.flush();
    }
}

