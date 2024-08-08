package org.mansouri.connector;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.utils.StringUtils;
import org.apache.pulsar.functions.api.Record;

public class SQSRecord implements Record<String> {
    private static final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final String queueName;
    private final String messageId;
    private final String value;
    private final Map<String, String> properties;

    public SQSRecord(String queueName, Message record) {
        this.properties = record.attributesAsStrings();
        this.messageId = record.messageId();
        this.queueName = queueName;
        this.value = record.body();
    }

    @Override
    public Optional<String> getKey() {
        return Optional.ofNullable(messageId);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Optional<String> getTopicName() {
        return Optional.ofNullable(queueName);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }
}
