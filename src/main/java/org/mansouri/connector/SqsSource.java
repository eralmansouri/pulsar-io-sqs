package org.mansouri.connector;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.util.Map;

@Connector(
    name = "sqs-source",
    type = IOType.SOURCE,
    help = "A source connector that copies messages from Amazon SQS to Pulsar",
    configClass = SqsSourceConfig.class)
public class SqsSource extends PushSource<String> {
    private SqsMessageReceiver receiver;

    @Override
    public void open(Map<String, Object> configMap, SourceContext sourceContext) throws Exception {
        receiver = new SqsMessageReceiver(SqsSourceConfig.load(configMap), this::consume);
        receiver.start();
    }

    @Override
    public void close() throws Exception {
        if (receiver != null) {
            receiver.stop();
        }
    }
}