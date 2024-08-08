package org.mansouri.connector;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.aws.AbstractAwsConnector;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
public class SQSMessageReceiver extends AbstractAwsConnector {
    private final SQSSourceConfig config;
    private final Consumer<Record<String>> consumeFunction;
    private final ExecutorService executorService;
    private volatile boolean running;
    private final SqsClient sqsClient;

    public SQSMessageReceiver(SQSSourceConfig config, Consumer<Record<String>> consumeFunction) {
        this.config = config;
        this.consumeFunction = consumeFunction;
        this.executorService = Executors.newSingleThreadExecutor();
        this.sqsClient = config.createSqsClient(
            createCredentialProvider(config.getAwsCredentialPluginName(), config.getAwsCredentialPluginParam()));
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
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(config.getQueueName())
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .build();

                List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

                for (Message message : messages) {
                    consume(message);
                    sqsClient.deleteMessage(builder -> builder
                        .queueUrl(config.getQueueName())
                        .receiptHandle(message.receiptHandle()));
                }
            } catch (Exception e) {
                log.error("Error processing SQS messages", e);
            }
        }
    }

    private void consume(Message message) {
        log.info("Consuming message. Id: {}", message.messageId());
        consumeFunction.accept(new SQSRecord(config.getQueueName(), message));
    }
}