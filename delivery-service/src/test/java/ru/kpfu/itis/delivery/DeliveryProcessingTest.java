package ru.kpfu.itis.delivery;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import ru.kpfu.itis.batch.avro.DeliveryBatch;
import ru.kpfu.itis.batch.avro.ProductQuantity;
import ru.kpfu.itis.delivery.avro.Delivery;
import ru.kpfu.itis.delivery.domain.model.Topic;
import ru.kpfu.itis.delivery.metrics.ApplicationMetrics;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class DeliveryProcessingTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final List<Delivery> deliveryInputValues = Arrays.asList(
            Delivery.newBuilder().setId("1").setShopId(1).setProductId(1).setQuantity(1).build(),
            Delivery.newBuilder().setId("2").setShopId(2).setProductId(1).setQuantity(2).build(),
            Delivery.newBuilder().setId("3").setShopId(1).setProductId(2).setQuantity(3).build(),
            Delivery.newBuilder().setId("4").setShopId(2).setProductId(2).setQuantity(4).build(),
            Delivery.newBuilder().setId("5").setShopId(1).setProductId(3).setQuantity(5).build()
    );

    private static final List<DeliveryBatch> expectedBatchOutputValues = Arrays.asList(
            DeliveryBatch.newBuilder().setProductQuantities(
                    Arrays.asList(
                            ProductQuantity.newBuilder().setProductId(1).setQuantity(1).build(),
                            ProductQuantity.newBuilder().setProductId(2).setQuantity(3).build(),
                            ProductQuantity.newBuilder().setProductId(3).setQuantity(5).build()
                    )
            )
                    .setId("1")
                    .setShopId(1)
                    .build(),
            DeliveryBatch.newBuilder().setProductQuantities(
                    Arrays.asList(
                            ProductQuantity.newBuilder().setProductId(1).setQuantity(2).build(),
                            ProductQuantity.newBuilder().setProductId(2).setQuantity(4).build()
                    )
            )
                    .setId("2")
                    .setShopId(2)
                    .build()
    );

    private static final Logger log = LoggerFactory.getLogger(DeliveryProcessingTest.class);
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    private KafkaStreams kafkaStreams;

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        if (CLUSTER.isRunning()) {
            CLUSTER.stop();
        }
    }

/*    @Test
    public void shouldDemonstrateInteractiveQueries() throws InterruptedException {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wait-for-output-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        List<KeyValue<Object, Object>> keyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, INPUT_TOPIC, inputValues.size());
        keyValues.size();
    }*/

    @Test
    public void shouldDemonstrateInteractiveQueries2() throws InterruptedException, ExecutionException {
        final Topic<String, Delivery> INPUT_TOPIC = getDeliveryTopic();
        final Topic<Long, DeliveryBatch> OUTPUT_TOPIC = getDeliveryBatchTopic();

        CLUSTER.createTopic(INPUT_TOPIC.name(), 1, (short) 1);
        CLUSTER.createTopic(OUTPUT_TOPIC.name(), 1, (short) 1);

        kafkaStreams = getProcessingStreams(INPUT_TOPIC, OUTPUT_TOPIC);
        kafkaStreams.start();

        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, INPUT_TOPIC.keySerde().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, INPUT_TOPIC.valueSerde().serializer().getClass());
        producerConfig.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        IntegrationTestUtils.produceValuesSynchronously(INPUT_TOPIC.name(), deliveryInputValues, producerConfig);

        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, OUTPUT_TOPIC.keySerde().deserializer().getClass());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OUTPUT_TOPIC.valueSerde().deserializer().getClass());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wait-for-output-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());

        List<KeyValue<Long, DeliveryBatch>> keyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC.name(), expectedBatchOutputValues.size());
        System.out.println("HELLO SIZE: " + keyValues.size());

        List<DeliveryBatch> actualBatchOutputValues = keyValues.stream()
                .map(kv -> kv.value)
                .sorted(Comparator.comparing(DeliveryBatch::getId))
                .collect(Collectors.toList());

        Assert.assertEquals(expectedBatchOutputValues, actualBatchOutputValues);
    }

    private Topic<String, Delivery> getDeliveryTopic() {
        SpecificAvroSerde<Delivery> deliverySpecificAvroSerde = new SpecificAvroSerde<>();
        deliverySpecificAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl()), false);
        return new Topic<>("delivery", Serdes.String(), deliverySpecificAvroSerde);
    }

    private Topic<Long, DeliveryBatch> getDeliveryBatchTopic() {
        SpecificAvroSerde<DeliveryBatch> deliveryBatchSpecificAvroSerde = new SpecificAvroSerde<>();
        deliveryBatchSpecificAvroSerde.configure(
                Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl()), false);
        return new Topic<>("delivery-batch", Serdes.Long(), deliveryBatchSpecificAvroSerde);
    }

    private KafkaStreams getProcessingStreams(Topic<String, Delivery> deliveryTopic, Topic<Long, DeliveryBatch> deliveryBatchTopic) {
        return new KafkaStreams(getTopology(deliveryTopic, deliveryBatchTopic), getProperties());
    }

    private Topology getTopology(final Topic<String, Delivery> deliveryTopic, final Topic<Long, DeliveryBatch> deliveryBatchTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Delivery> deliveries = builder.stream(
                deliveryTopic.name(), Consumed.with(deliveryTopic.keySerde(), deliveryTopic.valueSerde()));

        deliveries
                .groupBy((key, delivery) -> delivery.getShopId(), Grouped.with(Serdes.Long(), deliveryTopic.valueSerde()))
                .aggregate(
                        DeliveryBatch.newBuilder()::build,
                        (aggKey, newValue, aggValue) -> {
                            aggValue.setId(aggKey.toString());
                            aggValue.setShopId(aggKey);
                            List<ProductQuantity> productQuantities = aggValue.getProductQuantities();
                            productQuantities.add(
                                    ProductQuantity.newBuilder()
                                            .setProductId(newValue.getProductId())
                                            .setQuantity(newValue.getQuantity())
                                            .build()
                            );
                            return aggValue;
                        },
                        Materialized.<Long, DeliveryBatch, KeyValueStore<Bytes, byte[]>>as("delivery-batch-store-" + UUID.randomUUID().toString())
                                .withKeySerde(Serdes.Long()).withValueSerde(deliveryBatchTopic.valueSerde())
                )
                .toStream()
                .to(deliveryBatchTopic.name(), Produced.with(deliveryBatchTopic.keySerde(), deliveryBatchTopic.valueSerde()));
        return builder.build();
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "applicationId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    @Test
    public void shouldDemonstrateInteractiveQueriesInApplication() throws ExecutionException, InterruptedException {
        final Topic<String, Delivery> INPUT_TOPIC = getDeliveryTopic();
        final Topic<Long, DeliveryBatch> OUTPUT_TOPIC = getDeliveryBatchTopic();

        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, INPUT_TOPIC.keySerde().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, INPUT_TOPIC.valueSerde().serializer().getClass());
        producerConfig.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());

        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, OUTPUT_TOPIC.keySerde().deserializer().getClass());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OUTPUT_TOPIC.valueSerde().deserializer().getClass());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wait-for-output-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());

        IntegrationTestUtils.produceValuesSynchronously(INPUT_TOPIC.name(), deliveryInputValues, producerConfig);
        try (ConfigurableApplicationContext context = applicationContext()) {
            List<KeyValue<Long, DeliveryBatch>> keyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC.name(), expectedBatchOutputValues.size());


            List<DeliveryBatch> actualBatchOutputValues = keyValues.stream()
                    .map(kv -> kv.value)
                    .sorted(Comparator.comparing(DeliveryBatch::getId))
                    .collect(Collectors.toList());

            ApplicationMetrics applicationMetrics = (ApplicationMetrics) context.getBean("applicationMetrics");

            Assert.assertEquals(deliveryInputValues.size(), (int) applicationMetrics.getDeliveryCounter().count());
            Assert.assertEquals(expectedBatchOutputValues.size(), (int) applicationMetrics.getDeliveryBatchCounter().count());
            Assert.assertEquals(expectedBatchOutputValues, actualBatchOutputValues);
        }
    }

    private ConfigurableApplicationContext applicationContext() {
        return SpringApplication.run(
                Application.class,
                "--server.port=8085",
                "--delivery-service.kafka.server=" + CLUSTER.bootstrapServers(),
                "--delivery-service.schema-registry.server=" + CLUSTER.schemaRegistryUrl(),
                "--delivery-service.kafka.application-id=delivery-service",
                "--kafka.stateDir=/tmp/kafka-streams",
                "--environment=test",
                "--kafka.refreshTopicsMs=1000");
    }
}
