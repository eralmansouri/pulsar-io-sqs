package org.mansouri.connector;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

@Getter
@RequiredArgsConstructor
public class SQSSourceConfig implements Serializable {
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
        help = "Fully-Qualified class name of implementation of AwsCredentialProviderPlugin."
            + " It is a factory class which creates an AWSCredentialsProvider that will be used by dynamodb."
            + " If it is empty then dynamodb will create a default AWSCredentialsProvider which accepts "
            + "json-map of credentials in `awsCredentialPluginParam`")
    private final String awsCredentialPluginName;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "json-parameters to initialize `AwsCredentialsProviderPlugin`")
    private final String awsCredentialPluginParam;

    public static SQSSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), SQSSourceConfig.class);
    }

    public SqsClient createSqsClient(AwsCredentialProviderPlugin credPlugin) {
        return SqsClient.builder()
            .region(Region.of(region))
            .credentialsProvider(credPlugin.getV2CredentialsProvider())
            .build();
    }
}