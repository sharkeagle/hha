package com.test.testmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@SpringBootTest
@Slf4j
class TestMqApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void test() throws ClientException, IOException {
        // 接入点地址，需要设置成 Proxy 的地址和端口列表，一般是xxx:8081
        String endpoint = "127.0.0.1:8081";
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "TopicTest";
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();

        // 初始化Producer时需要设置通信配置以及预绑定的Topic
        Producer producer = provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .build();

        // 普通消息发送
        for (int i = 0; i < 10; i++) {
            Message message = provider.newMessageBuilder()
                    .setTopic(topic)
                    // 设置消息索引键，可根据关键字精确查找某条消息
                    .setKeys("messageKey"+i)
                    // 设置消息Tag，用于消费端根据指定Tag过滤消息
                    .setTag("messageTag")
                    // 消息内容实体（byte[]）
                    .setBody(("hello rocketMQ "+i).getBytes())
                    .build();
            try {
                // 发送消息，需要关注发送结果，并捕获失败等异常。
                SendReceipt sendReceipt = producer.send(message);
                log.info("send message successfully, messageId={}", sendReceipt.getMessageId());
            } catch (ClientException e) {
                log.error("failed to send message", e);
            }
        }


        // 关闭
        producer.close();
    }
    @Test
    public void test2() throws ClientException, IOException {
        // 接入点地址，需要设置成 Proxy 的地址和端口列表，一般是xxx:8081
        String endpoint = "127.0.0.1:8081";
        // 消息发送的目标Topic名称，需要提前创建。
        String topic = "TopicTest";
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder().setEndpoints(endpoint);
        ClientConfiguration configuration = builder.build();

        // 初始化Producer时需要设置通信配置以及预绑定的Topic
        Producer producer = provider.newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(configuration)
                .build();

        // 普通消息发送
        Message message = provider.newMessageBuilder()
                .setTopic(topic)
                // 设置消息索引键，可根据关键字精确查找某条消息
                .setKeys("messageKey1")
                // 设置消息Tag，用于消费端根据指定Tag过滤消息
                .setTag("messageTag1")
                // 消息内容实体（byte[]）
                .setBody("hello rocketMQ 1".getBytes())
                .build();
        try {
            // 发送消息，需要关注发送结果，并捕获失败等异常。
            SendReceipt sendReceipt = producer.send(message);
            log.info("send message successfully, messageId={}", sendReceipt.getMessageId());
        } catch (ClientException e) {
            log.error("failed to send message", e);
        }
        // 关闭
        producer.close();
    }
    @Test
    public void pushConsumerTest() throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 接入点地址，需要设置成Proxy的地址和端口列表，一般是xxx:8081;xxx:8081
        String endpoint = "localhost:8081";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(endpoint)
                .build();
        // 订阅消息的过滤规则，表示订阅所有Tag的消息
        String tag = "*";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // 为消费者指定所属的消费者分组，Group需要提前创建
        String consumerGroup = "GroupTest";

        // 指定需要订阅哪个目标Topic，Topic需要提前创建
        String topic = "TopicTest";
        // 初始化 PushConsumer，需要绑定消费者分组ConsumerGroup、通信参数以及订阅关系
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                // 设置消费者分组
                .setConsumerGroup(consumerGroup)
                // 设置预绑定的订阅关系
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                // 设置消费监听器
                .setMessageListener(messageView -> {
                    // 处理消息并返回消费结果
                    log.info("consume message successfully, messageId={}", messageView.getMessageId());
                    // 消息内容处理
                    ByteBuffer body = messageView.getBody();
                    String Key=messageView.getKeys().toString();
                    String message = StandardCharsets.UTF_8.decode(body).toString();
                    body.flip();
                    log.info("messageKey={}",Key);
                    log.info("message body={}", message);
                    return ConsumeResult.SUCCESS;
                }).build();
        Thread.sleep(1000*40);
        // 如果不需要再使用 PushConsumer，可关闭该实例。
        pushConsumer.close();
    }
}
