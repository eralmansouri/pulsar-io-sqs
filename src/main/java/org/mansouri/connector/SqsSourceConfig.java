package org.mansouri.connector;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@Data
public class SqsSourceConfig implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Name of the SQS queue to consume messages from"
    )
    private String queueName;

    @FieldDoc(
        required = false,
        defaultValue = "us-east-1",
        help = "AWS region of the SQS queue"
    )
    private String region;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "Fully-Qualified class name of implementation of AwsCredentialProviderPlugin.")
    private String awsCredentialPluginName;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "json-parameters to initialize `AwsCredentialsProviderPlugin`")
    private String awsCredentialPluginParam;

    public static SqsSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), SqsSourceConfig.class);
    }
}