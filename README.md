# Amazon SQS Source Connector for Apache Pulsar

This is an Amazon SQS source connector for Apache Pulsar. It allows you to consume messages from an SQS queue and publish them to a Pulsar topic.

I created it as an alternative to StreamNative's [AWS SQS Connector](https://github.com/streamnative/pulsar-io-sqs), which seems to have been deleted or became closed-source (a dick move considering they were featured on [Apache Pulsar's Ecosystem page](https://pulsar.apache.org/ecosystem/)). 

## Building the connector

To build the connector, run the following command in the project root directory:

```
mvn clean package
```

This will create a NAR file in the `target/` directory.

## Configuring the connector

Create a configuration file (e.g., `sqs-source-config.yaml`) with the following content:
```yaml
configs:
  queueName: "example-queue-name"
  region: "us-west-2"
  awsCredentialPluginParam:
    accessKey: "secret"
    secretKey: "secret
```

You can also configure the connector to use an IAM role for accessing the SQS queue. To do this, use the following configuration:
```yaml
configs:
  queueName: "example-queue-name"
  region: "us-west-2"
  awsCredentialPluginName: "org.apache.pulsar.io.aws.STSAssumeRoleProviderPlugin"
  awsCredentialPluginParam:
    roleArn: "arn:aws:iam::{account-id}:role/{role-name}"
    roleSessionName: "pulsar-io-sqs-source"
```

if no credentials are provided, the connector will use the [default AWS credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default) to access the SQS queue.
Replace the placeholders with your actual SQS queue name, region, and IAM role ARN. The connector will retrieve the queue URL from the SQS service and use it to consume messages.

## Using the connector

To use this connector in your Pulsar instance:

1. Copy the NAR file to the Pulsar connectors directory.
2. Create the configuration file as described above.
3. Start the connector using the Pulsar admin CLI:

```
bin/pulsar-admin sources create \
  --name sqs-source \
  --archive /path/to/pulsar-io-sqs-source.nar \
  --classname org.mansouri.connector.SqsSource \
  --tenant public \
  --namespace default \
  --destination-topic-name sqs-messages \
  --source-config-file sqs-source-config.yaml
```

The connector will start consuming messages from the specified SQS queue and publish them to the Pulsar topic.

## Limitations

This connector does not currently have support for FIFO, batch acknowledgement or message deduplication. It also has some hard coded values for wait time and visibility timeout that is not configurable. It is a very basic implementation that is not suitable for production use.