package com.db.stream;

import com.db.dto.TradeDto;
import com.db.service.TradeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"trades"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@ActiveProfiles("test")
@DirtiesContext
class KafkaIntegrationTest {

    @Autowired
    private TradeProducer tradeProducer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    @Mock
    private TradeService tradeService;

    @Value("${app.kafka.topic:trades}")
    private String tradeTopic;

    @Test
    void shouldPublishAndConsumeTradeMessage() throws Exception {
        // Given
        BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

        // Set up consumer
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        );

        ContainerProperties containerProperties = new ContainerProperties(tradeTopic);
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> {
            records.add(record);
        });
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        records.clear();
        // Create test trade
        UUID tradeId = UUID.randomUUID();
        TradeDto tradeDto = TradeDto.builder()
                .tradeId(tradeId)
                .version(1)
                .bookId("KAFKA_TEST")
                .counterPartyId("CP_KAFKA")
                .maturityDate(LocalDate.now().plusDays(30))
                .build();

        // When
        tradeProducer.publishTrade(tradeDto);

        // Then
        ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received).isNotNull();
        assertThat(received.topic()).isEqualTo(tradeTopic);

        // Verify message content
        TradeDto receivedDto = objectMapper.readValue(received.value(), TradeDto.class);
        assertThat(receivedDto.getTradeId()).isEqualTo(tradeId);
        assertThat(receivedDto.getBookId()).isEqualTo("KAFKA_TEST");
        assertThat(receivedDto.getCounterPartyId()).isEqualTo("CP_KAFKA");
        assertThat(receivedDto.getVersion()).isEqualTo(1);
        assertThat(receivedDto.getMaturityDate()).isEqualTo(LocalDate.now().plusDays(30));

        container.stop();
    }

    @Test
    void shouldHandleMultipleTradeMessages() throws Exception {
        // Given
        BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup2", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        );

        ContainerProperties containerProperties = new ContainerProperties("tradeTopic");
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> records.add(record));
        container.start();
        records.clear();
        // Create multiple test trades
        UUID[] tradeIds = new UUID[3];
        for (int i = 0; i < 3; i++) {
            tradeIds[i] = UUID.randomUUID();
            TradeDto tradeDto = TradeDto.builder()
                    .tradeId(tradeIds[i])
                    .version(1)
                    .bookId("MULTI_TEST_" + i)
                    .counterPartyId("CP_MULTI_" + i)
                    .maturityDate(LocalDate.now().plusDays(30 + i))
                    .build();

            tradeProducer.publishTrade(tradeDto,"tradeTopic");
        }

        // Then - verify all messages are received
        for (int i = 0; i < 3; i++) {
            ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
            assertThat(received).isNotNull();
            assertThat(received.topic()).isEqualTo("tradeTopic");

            TradeDto receivedDto = objectMapper.readValue(received.value(), TradeDto.class);
            assertThat(receivedDto.getBookId()).startsWith("MULTI_TEST_");
            assertThat(receivedDto.getCounterPartyId()).startsWith("CP_MULTI_");
        }

        container.stop();
    }

    @Test
    void shouldHandleTradeWithDifferentVersions() throws Exception {
        // Given
        BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup3", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        );

        ContainerProperties containerProperties = new ContainerProperties("trade-version");
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> records.add(record));
        container.start();
        records.clear();
        UUID tradeId = UUID.randomUUID();

        // Publish version 1
        TradeDto tradeV1 = TradeDto.builder()
                .tradeId(tradeId)
                .version(1)
                .bookId("VERSION_TEST")
                .counterPartyId("CP_VERSION")
                .maturityDate(LocalDate.now().plusDays(30))
                .build();

        // Publish version 2
        TradeDto tradeV2 = TradeDto.builder()
                .tradeId(tradeId)
                .version(2)
                .bookId("VERSION_TEST_V2")
                .counterPartyId("CP_VERSION")
                .maturityDate(LocalDate.now().plusDays(35))
                .build();

        // When
        tradeProducer.publishTrade(tradeV1, "trade-version");
        tradeProducer.publishTrade(tradeV2, "trade-version");

        // Then
        ConsumerRecord<String, String> received1 = records.poll(10, TimeUnit.SECONDS);
        ConsumerRecord<String, String> received2 = records.poll(10, TimeUnit.SECONDS);

        assertThat(received1).isNotNull();
        assertThat(received2).isNotNull();

        TradeDto receivedDto1 = objectMapper.readValue(received1.value(), TradeDto.class);
        TradeDto receivedDto2 = objectMapper.readValue(received2.value(), TradeDto.class);

        System.out.println("Raw received1 value: " + received1.value());
        System.out.println("Raw received2 value: " + received2.value());

        // Both should have same trade ID but different versions
        assertThat(receivedDto1.getTradeId()).isEqualTo(tradeId);
        assertThat(receivedDto2.getTradeId()).isEqualTo(tradeId);
        assertThat(receivedDto1.getVersion()).isNotEqualTo(receivedDto2.getVersion());

        container.stop();
    }

    @Test
    void shouldHandleJsonSerializationCorrectly() throws Exception {
        // Given
        BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup4", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        );

        ContainerProperties containerProperties = new ContainerProperties("trade-serialize");
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> records.add(record));
        container.start();
        records.clear();
        UUID tradeId = UUID.randomUUID();
        LocalDate maturityDate = LocalDate.now().plusDays(45);

        TradeDto tradeDto = TradeDto.builder()
                .tradeId(tradeId)
                .version(5)
                .bookId("JSON_TEST")
                .counterPartyId("CP_JSON")
                .maturityDate(maturityDate)
                .build();

        // When
        tradeProducer.publishTrade(tradeDto, "trade-serialize");

        // Then
        ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received).isNotNull();

        // Verify JSON structure
        String jsonMessage = received.value();
        assertThat(jsonMessage).contains("tradeId");

        // Verify deserialization
        TradeDto deserializedDto = objectMapper.readValue(jsonMessage, TradeDto.class);
        assertThat(deserializedDto.getTradeId()).isEqualTo(tradeId);
        assertThat(deserializedDto.getVersion()).isEqualTo(5);
        assertThat(deserializedDto.getBookId()).isEqualTo("JSON_TEST");
        assertThat(deserializedDto.getCounterPartyId()).isEqualTo("CP_JSON");
        assertThat(deserializedDto.getMaturityDate()).isEqualTo(maturityDate);

        container.stop();
    }

    @Test
    void shouldVerifyPartitioningWorks() throws Exception {
        // Given
        BlockingQueue<ConsumerRecord<String, String>> records = new LinkedBlockingQueue<>();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup5", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new StringDeserializer()
        );

        ContainerProperties containerProperties = new ContainerProperties("trade-partition");
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(cf, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record -> records.add(record));
        container.start();
        records.clear();
        // When - Send multiple messages
        for (int i = 0; i < 5; i++) {
            TradeDto tradeDto = TradeDto.builder()
                    .tradeId(UUID.randomUUID())
                    .version(1)
                    .bookId("PARTITION_TEST_" + i)
                    .counterPartyId("CP_" + i)
                    .maturityDate(LocalDate.now().plusDays(30))
                    .build();

            tradeProducer.publishTrade(tradeDto, "trade-partition");
        }

        // Then - All messages should be received (partitioning is transparent to consumer)
        for (int i = 0; i < 5; i++) {
            ConsumerRecord<String, String> received = records.poll(10, TimeUnit.SECONDS);
            assertThat(received).isNotNull();
            assertThat(received.topic()).isEqualTo("trade-partition");
            // Verify partition is within expected range (0, 1, 2 based on config)
            assertThat(received.partition()).isBetween(0, 2);
        }

        container.stop();
    }
}