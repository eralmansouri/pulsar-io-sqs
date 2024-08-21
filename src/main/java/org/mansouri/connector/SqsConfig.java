package org.mansouri.connector;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SqsConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Name of the SQS queue to consume messages from"
    )
    private final String queueName;

    @FieldDoc(
        required = false,
        defaultValue = "us-east-1",
        help = "AWS region of the SQS queue"
    )
    private final String region;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Fully-Qualified class name of implementation of AwsCredentialProviderPlugin.")
    private final String awsCredentialPluginName;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "A map object or json-parameters to initialize `AwsCredentialsProviderPlugin`")
    private final String awsCredentialPluginParam;

    @JsonCreator
    public static SqsConfig load(Map<String, Object> map) {
        var builder = SqsConfig.builder();
        var conf = createParser(map);
        conf.accept("queueName", builder::queueName);
        conf.accept("region", builder::region);
        conf.accept("awsCredentialPluginName", builder::awsCredentialPluginName);
        conf.accept("awsCredentialPluginParam", builder::awsCredentialPluginParam);
        return builder.build();
    }

    public static SqsConfig load(String yaml) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        return load(objectMapper.readValue(yaml, new TypeReference<Map<String, Object>>() {}));
    }

    private static BiConsumer<String, Consumer<String>> createParser(Map<String, Object> configMap) {
        // We need config maps to be <string, string> so for convenience
        // we will JSON.stringify the values if they are not already stringly.
        ObjectMapper mapper = new ObjectMapper();
        return (key, setter) -> {
            setter.accept((String) configMap.compute(key, (k, v) -> {
                if (v == null || v instanceof String)
                    return v;

                try {
                    return mapper.writeValueAsString(v);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }));
        };
    }
}