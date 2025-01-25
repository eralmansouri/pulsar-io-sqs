package org.mansouri.connector;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.aws.AbstractAwsConnector;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
public class SqsMessageReceiver extends AbstractAwsConnector {
    private final Consumer<Record<String>> consumeFunction;
    private final ExecutorService executorService;
    private volatile boolean running;
    private final SqsClient sqsClient;
    private final Map<String, String> queues;

    public SqsMessageReceiver(SqsConfig config, Consumer<Record<String>> consumeFunction) {
        this.consumeFunction = consumeFunction;
        this.executorService = Executors.newSingleThreadExecutor();
        this.sqsClient = createSqsClient(config);
        this.queues = resolveDestinations(config, sqsClient);
    }

    public void start() {
        running = true;
        executorService.submit(this::run);
    }

    public void stop() {
        running = false;
        executorService.shutdown();
        sqsClient.close();
    }

    private void run() {
        while (running) {
            try {
                for (Map.Entry<String, String> queue : queues.entrySet()) {
                    String queueName = queue.getKey();
                    String queueUrl = queue.getValue();
                    log.info("Consuming messages from queue: {}", queueName);

                    ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(20)
                        .attributeNames(QueueAttributeName.ALL)
                        .build();

                    List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

                    for (Message message : messages) {
                        consume(queueName, message);
                        sqsClient.deleteMessage(builder -> builder
                            .queueUrl(queueUrl)
                            .receiptHandle(message.receiptHandle()));
                    }
                }
            } catch (Exception e) {
                log.error("Error processing SQS messages", e);
            }
        }
    }

    private void consume(String queueName, Message message) {
        log.info("Consuming message. Id: {}", message.messageId());
        consumeFunction.accept(new SqsRecord(queueName, message));
    }

    private AwsCredentialsProvider createV2CredentialProvider(SqsConfig config) {
        try {
            AwsCredentialProviderPlugin credPlugin =
                createCredentialProvider(config.getAwsCredentialPluginName(), config.getAwsCredentialPluginParam());
            return credPlugin.getV2CredentialsProvider();
        }
        catch (Exception e) {
            log.error("Error creating credential provider. Using DefaultCredentialProvider instead.", e);
            return DefaultCredentialsProvider.create();
        }
    }

    private SqsClient createSqsClient(SqsConfig config) {
        return SqsClient.builder()
            .credentialsProvider(createV2CredentialProvider(config))
            .region(Region.of(config.getRegion()))
            .build();
    }

    // For now we only support one queue but we can extend this to support multiple queues
    private static Map<String, String> resolveDestinations(SqsConfig config, SqsClient client) {
        Map<String, String> queues = new HashMap<>();
        for (String queueName : List.of(config.getQueueName())) {
            GetQueueUrlResponse response =
                client.getQueueUrl(req -> req.queueName(queueName));
            queues.put(config.getQueueName(), response.queueUrl());
        }
        return queues;
    }
}
