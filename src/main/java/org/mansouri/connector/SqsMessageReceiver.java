package org.mansouri.connector;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.aws.AbstractAwsConnector;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Slf4j
public class SqsMessageReceiver extends AbstractAwsConnector {
    private final SqsSourceConfig config;
    private final Consumer<Record<String>> consumeFunction;
    private final ExecutorService executorService;
    private volatile boolean running;
    private final SqsClient sqsClient;

    public SqsMessageReceiver(SqsSourceConfig config, Consumer<Record<String>> consumeFunction) {
        this.config = config;
        this.consumeFunction = consumeFunction;
        this.executorService = Executors.newSingleThreadExecutor();
        this.sqsClient = createSqsClient();
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
        consumeFunction.accept(new SqsRecord(config.getQueueName(), message));
    }

    private AwsCredentialsProvider createV2CredentialProvider() {
        AwsCredentialProviderPlugin credPlugin =
            createCredentialProvider(config.getAwsCredentialPluginName(), config.getAwsCredentialPluginParam());
        return credPlugin.getV2CredentialsProvider();
    }

    private SqsClient createSqsClient() {
        return SqsClient.builder()
            .credentialsProvider(createV2CredentialProvider())
            .region(Region.of(config.getRegion()))
            .build();
    }
}