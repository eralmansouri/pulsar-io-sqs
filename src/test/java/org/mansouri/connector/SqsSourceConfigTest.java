package org.mansouri.connector;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import static org.junit.jupiter.api.Assertions.*;
class SqsSourceConfigTest {
    @Test
    @DisplayName("Load standard JSON config")
    void loadJsonConfig() throws Exception {
        String json = """
        {
            "queueName": "example-queue-name",
            "region": "us-west-2",
            "awsCredentialPluginName": "org.apache.pulsar.io.aws.STSAssumeRoleProviderPlugin",
            "awsCredentialPluginParam": {
              "accessKey": "secret",
              "secretKey": "secret"
            }
        }
        """;

        // Test that we can load the config from a JSON string
        SqsSourceConfig directLoad = SqsSourceConfig.load(json);

        // Test that we can use object mapper to automatically
        // call SqsSourceConfig.load() on the JSON string
        SqsSourceConfig config = new ObjectMapper().readValue(json, SqsSourceConfig.class);

        // Test that the direct load and the object mapper load are the same
        assertEquals(directLoad, config);

        // Verify that the values are correct
        assertEquals("example-queue-name", config.getQueueName());
        assertEquals("us-west-2", config.getRegion());
        assertEquals("org.apache.pulsar.io.aws.STSAssumeRoleProviderPlugin", config.getAwsCredentialPluginName());
        assertEquals("{\"accessKey\":\"secret\",\"secretKey\":\"secret\"}", config.getAwsCredentialPluginParam());
    }

    @Test
    @DisplayName("Load standard YAML config")
    void loadYamlConfig() throws Exception {
        String yaml = """
        queueName: "example-queue-name"
        region: "us-west-2"
        awsCredentialPluginName: "org.apache.pulsar.io.aws.STSAssumeRoleProviderPlugin"
        awsCredentialPluginParam:
          accessKey: "secret"
          secretKey: "secret"
        """;

        SqsSourceConfig directLoad = SqsSourceConfig.load(yaml);

        // Test that we can use object mapper to automatically
        // call SqsSourceConfig.load() on the YAML string
        SqsSourceConfig config = new ObjectMapper(new YAMLFactory())
            .readValue(yaml, SqsSourceConfig.class);

        // Test that the direct load and the object mapper load are the same
        assertEquals(directLoad, config);

        // Verify that the values are correct
        assertEquals("example-queue-name", config.getQueueName());
        assertEquals("us-west-2", config.getRegion());
        assertEquals("org.apache.pulsar.io.aws.STSAssumeRoleProviderPlugin", config.getAwsCredentialPluginName());
        assertEquals("{\"accessKey\":\"secret\",\"secretKey\":\"secret\"}", config.getAwsCredentialPluginParam());
    }

    @Test
    @DisplayName("Load plugin parameter config defined as json-parameters")
    void loadJsonConfigWithJsonParameters() throws Exception {
        String stringifiedJson = """
        {
            "queueName": "example-queue-name",
            "region": "us-west-2",
            "awsCredentialPluginName": "org.apache.pulsar.io.aws.STSAssumeRoleProviderPlugin",
            "awsCredentialPluginParam": "{\\"accessKey\\":\\"secret\\",\\"secretKey\\":\\"secret\\"}"
        }
        """;

        String json = """
        {
            "queueName": "example-queue-name",
            "region": "us-west-2",
            "awsCredentialPluginName": "org.apache.pulsar.io.aws.STSAssumeRoleProviderPlugin",
            "awsCredentialPluginParam": {
              "accessKey": "secret",
              "secretKey": "secret"
            }
        }
        """;

        var config = SqsSourceConfig.load(json);
        assertEquals("{\"accessKey\":\"secret\",\"secretKey\":\"secret\"}", config.getAwsCredentialPluginParam());
        assertEquals(config, SqsSourceConfig.load(stringifiedJson));
    }
}