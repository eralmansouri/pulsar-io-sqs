package org.mansouri.connector;

import org.apache.pulsar.io.aws.AbstractAwsConnector;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.util.Map;

@Connector(
    name = "sqs-source",
    type = IOType.SOURCE,
    help = "A source connector that copies messages from Amazon SQS to Pulsar",
    configClass = SQSSourceConfig.class)
public class SQSSource extends PushSource<String> {
    private SQSMessageReceiver receiver;

    @Override
    public void open(Map<String, Object> configMap, SourceContext sourceContext) throws Exception {
        receiver = new SQSMessageReceiver(SQSSourceConfig.load(configMap), this::consume);
        receiver.start();
    }

    @Override
    public void close() throws Exception {
        if (receiver != null) {
            receiver.stop();
        }
    }
}