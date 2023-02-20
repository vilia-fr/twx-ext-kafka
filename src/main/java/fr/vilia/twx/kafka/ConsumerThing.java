package fr.vilia.twx.kafka;

import ch.qos.logback.classic.Logger;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.*;
import com.thingworx.system.ContextType;
import com.thingworx.things.Thing;
import com.thingworx.types.BaseTypes;
import com.thingworx.types.collections.ValueCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Properties;

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
@ThingworxEventDefinitions(events = {
        @ThingworxEventDefinition(
                name = "MessageReceived",
                description = "Executed for each message received from any of the Kafka topics",
                dataShape = "KafkaMessage"
        )}
)
public class ConsumerThing extends Thing {

    protected static final Logger LOGGER = LogUtilities.getInstance().getApplicationLogger(ConsumerThing.class);

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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        props.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" +
                        username +
                        "\" password=\"" +
                        password +
                        "\";"
        );
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topics.split(",")));
        LOGGER.info("Kafka consumer started. Subscribed to topics: " + topics);

        new Thread() {
            @Override
            public void run() {
                try {
                    while (isRunning() && !isInterrupted()) {
                        final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
                        consumerRecords.forEach(record -> {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Received a message from Kafka: " + record);
                            }
                            try {
                                ValueCollection vc = new ValueCollection();
                                vc.put("key", BaseTypes.ConvertToPrimitive(record.key(), BaseTypes.STRING));
                                vc.put("value", BaseTypes.ConvertToPrimitive(record.value(), BaseTypes.STRING));
                                vc.put("topic", BaseTypes.ConvertToPrimitive(record.topic(), BaseTypes.STRING));
                                fireEvent(getInstanceEventDefinition("MessageReceived"), new DateTime(), vc);
                            } catch (Exception e) {
                                LOGGER.error("Cannot fire OnMessage", e);
                                e.printStackTrace();
                            }
                        });
                        consumer.commitAsync();
                    }
                } finally {
                    consumer.close();
                    LOGGER.info("Kafka consumer closed. Unsubscribed to topics: " + topics);
                }
            }
        }.start();
    }
}

