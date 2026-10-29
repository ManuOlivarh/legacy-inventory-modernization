package dev.joseherrera.inventory.availability;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.joseherrera.inventory.events.StockLevelChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AvailabilityApiIT {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String REGISTRY_IMAGE = "confluentinc/cp-schema-registry:8.3.2";
    private static final String POSTGRES_IMAGE = "postgres:18.1-alpine";

    private static final String LEVELS = "inventory.stock-levels.v1";
    private static final String DLQ = "inventory.stock-levels.v1.availability-api.dlq";
    private static final String GROUP = "availability-api";

    private static final Network network = Network.newNetwork();

    private static final KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE)
            .withNetwork(network)
            .withListener("kafka:19092");

    private static final GenericContainer<?> registry = new GenericContainer<>(REGISTRY_IMAGE)
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .dependsOn(kafka);

    private static final PostgreSQLContainer postgres = new PostgreSQLContainer(POSTGRES_IMAGE);

    private static final HttpClient http = HttpClient.newHttpClient();
    private static final ObjectMapper json = new ObjectMapper();

    static {
        Startables.deepStart(kafka, registry, postgres).join();
        createTopics();
        registerContract();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        properties.add("spring.kafka.properties.schema.registry.url", AvailabilityApiIT::registryUrl);
        properties.add("spring.datasource.url", () -> postgres.getJdbcUrl()
                + (postgres.getJdbcUrl().contains("?") ? "&" : "?") + "socketTimeout=2");
        properties.add("spring.datasource.username", postgres::getUsername);
        properties.add("spring.datasource.password", postgres::getPassword);
    }

    @Value("${local.server.port}")
    private int port;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meters;

    @Test
    void olderEventsAndDuplicatesNeverOverwriteANewerValue() throws Exception {
        String sku = newSku();
        send(level(sku, "T001", "10.00", "0000002c:00000a38:0005"));
        send(level(sku, "T001", "99.00", "0000002c:00000a38:0001"));
        RecordMetadata last = send(level(sku, "T001", "88.00", "0000002c:00000a38:0002"));

        awaitConsumed(last);

        assertThat(onHand(sku, "T001")).isEqualByComparingTo("10.00");
    }

    @Test
    void anExactRedeliveryIsNotRewrittenNorCountedAsFresh() throws Exception {
        String sku = newSku();
        StockLevelChanged event = level(sku, "T001", "9.00", "0000002c:00000a38:0003");
        awaitConsumed(send(event));
        long observed = meters.get("inventory.freshness").timer().count();
        Instant updatedAt = updatedAt(sku, "T001");

        awaitConsumed(send(event));

        assertThat(meters.get("inventory.freshness").timer().count()).isEqualTo(observed);
        assertThat(updatedAt(sku, "T001")).isEqualTo(updatedAt);
    }

    @Test
    void laterUpdatesFromTheSameErpTransactionStillApply() throws Exception {
        String sku = newSku();
        send(level(sku, "T001", "5.00", "0000002c:00000a38:0007"));
        RecordMetadata last = send(level(sku, "T001", "4.00", "0000002c:00000a38:0007"));

        awaitConsumed(last);

        assertThat(onHand(sku, "T001")).isEqualByComparingTo("4.00");
    }

    @Test
    void poisonGoesToTheDlqByteForByteAndTheNextEventIsApplied() throws Exception {
        String sku = newSku();
        String key = sku + ":T001";
        byte[] garbage = "not avro at all".getBytes(StandardCharsets.UTF_8);
        sendRaw(key, garbage);
        send(level(sku, "T001", "3.00", "0000002c:00000a38:0001"));

        await(() -> onHand(sku, "T001") != null);
        assertThat(onHand(sku, "T001")).isEqualByComparingTo("3.00");

        ConsumerRecord<String, byte[]> deadLetter = deadLetter(key, Duration.ofSeconds(30));
        assertThat(deadLetter).isNotNull();
        assertThat(deadLetter.value()).isEqualTo(garbage);
        assertThat(headersText(deadLetter)).contains("InvalidEventException");
    }

    @Test
    void eventsThatBreakTheContractRulesAreDeadLettered() throws Exception {
        String sku = newSku();
        StockLevelChanged wrong = level(sku, "T001", "10.00", "0000002c:00000a38:0001");
        wrong.setAvailable(new BigDecimal("12.00"));
        send(wrong);

        assertThat(deadLetter(sku + ":T001", Duration.ofSeconds(30))).isNotNull();
        assertThat(onHand(sku, "T001")).isNull();
    }

    @Test
    void aDatabaseOutageStallsTheConsumerInsteadOfDeadLettering() throws Exception {
        String sku = newSku();
        var docker = DockerClientFactory.instance().client();
        docker.pauseContainerCmd(postgres.getContainerId()).exec();
        try {
            send(level(sku, "T001", "21.00", "0000002c:00000a38:0001"));
            assertThat(deadLetter(sku + ":T001", Duration.ofSeconds(25))).isNull();
        } finally {
            docker.unpauseContainerCmd(postgres.getContainerId()).exec();
        }

        await(() -> onHand(sku, "T001") != null);
        assertThat(onHand(sku, "T001")).isEqualByComparingTo("21.00");
    }

    @Test
    void theApiSumsLocationsAndReportsTheOldestValueAsAsOf() throws Exception {
        String sku = newSku();
        StockLevelChanged store = level(sku, "T001", "4.00", "0000002c:00000a38:0001");
        store.setChangedAt(Instant.parse("2026-10-28T10:00:00Z"));
        StockLevelChanged warehouse = level(sku, "C001", "40.00", "0000002c:00000a38:0002");
        warehouse.setReserved(new BigDecimal("6.00"));
        warehouse.setAvailable(new BigDecimal("34.00"));
        warehouse.setChangedAt(Instant.parse("2026-10-28T11:00:00Z"));
        send(store);
        send(warehouse);

        await(() -> onHand(sku, "T001") != null && onHand(sku, "C001") != null);

        HttpResponse<String> response = get("/availability/" + sku);
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = json.readTree(response.body());
        assertThat(body.get("onHand").decimalValue()).isEqualByComparingTo("44.00");
        assertThat(body.get("available").decimalValue()).isEqualByComparingTo("38.00");
        assertThat(body.get("asOf").asText()).isEqualTo("2026-10-28T10:00:00Z");
        assertThat(body.get("locations")).hasSize(2);

        assertThat(get("/availability/NEVER-SEEN").statusCode()).isEqualTo(404);
    }

    @Test
    void freshnessAndLagAreExported() throws Exception {
        String sku = newSku();
        send(level(sku, "T001", "1.00", "0000002c:00000a38:0001"));
        await(() -> onHand(sku, "T001") != null);

        String metrics = get("/actuator/prometheus").body();
        assertThat(metrics).contains("inventory_freshness_seconds_bucket{le=\"5.0\"");
        assertThat(metrics).contains("kafka_consumer_fetch_manager_records_lag_max");
    }

    private BigDecimal onHand(String sku, String location) {
        return jdbc.sql("SELECT on_hand FROM stock_level WHERE sku = ? AND location = ?")
                .params(sku, location)
                .query(BigDecimal.class)
                .optional()
                .orElse(null);
    }

    private Instant updatedAt(String sku, String location) {
        return jdbc.sql("SELECT updated_at FROM stock_level WHERE sku = ? AND location = ?")
                .params(sku, location)
                .query(java.sql.Timestamp.class)
                .single()
                .toInstant();
    }

    private static String newSku() {
        return "IT" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    private static StockLevelChanged level(String sku, String location, String onHand, String lsn) {
        return StockLevelChanged.newBuilder()
                .setSku(sku)
                .setLocation(location)
                .setOnHand(new BigDecimal(onHand))
                .setReserved(new BigDecimal("0.00"))
                .setAvailable(new BigDecimal(onHand))
                .setChangedAt(Instant.now())
                .setSourceLsn(lsn)
                .build();
    }

    private static RecordMetadata send(StockLevelChanged event) throws Exception {
        var serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl(),
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false,
                AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true), false);
        try (var producer = new KafkaProducer<>(producerConfig(), new StringSerializer(), serializer)) {
            return producer.send(new ProducerRecord<>(LEVELS, event.getSku() + ":" + event.getLocation(), event)).get();
        }
    }

    private static void sendRaw(String key, byte[] value) throws Exception {
        try (var producer = new KafkaProducer<>(producerConfig(), new StringSerializer(), new ByteArraySerializer())) {
            producer.send(new ProducerRecord<>(LEVELS, key, value)).get();
        }
    }

    private static Map<String, Object> producerConfig() {
        return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    }

    private static void awaitConsumed(RecordMetadata record) {
        var partition = new TopicPartition(record.topic(), record.partition());
        try (var admin = admin()) {
            await(() -> {
                try {
                    var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get();
                    var committed = offsets.get(partition);
                    return committed != null && committed.offset() > record.offset();
                } catch (InterruptedException | ExecutionException e) {
                    return false;
                }
            });
        }
    }

    private static ConsumerRecord<String, byte[]> deadLetter(String key, Duration within) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(DLQ));
            long deadline = System.nanoTime() + within.toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        return null;
    }

    private static String headersText(ConsumerRecord<String, byte[]> record) {
        StringBuilder text = new StringBuilder();
        for (Header header : record.headers()) {
            text.append(header.key()).append('=').append(new String(header.value(), StandardCharsets.UTF_8)).append('\n');
        }
        return text.toString();
    }

    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("condition not met within 60 s");
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static Admin admin() {
        return Admin.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    private static void createTopics() {
        try (var admin = admin()) {
            List<NewTopic> topics = new ArrayList<>();
            topics.add(new NewTopic(LEVELS, 3, (short) 1).configs(Map.of("cleanup.policy", "compact")));
            topics.add(new NewTopic(DLQ, 3, (short) 1));
            admin.createTopics(topics).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("could not create topics", e);
        }
    }

    private static void registerContract() {
        try {
            String subject = LEVELS + "-value";
            String schema = Files.readString(Path.of("..", "contracts", "StockLevelChanged.avsc"));
            post("PUT", "/config/" + subject, json.writeValueAsString(Map.of("compatibility", "BACKWARD")));
            post("POST", "/subjects/" + subject + "/versions", json.writeValueAsString(Map.of("schema", schema)));
        } catch (IOException e) {
            throw new IllegalStateException("could not register the contract", e);
        }
    }

    private static String registryUrl() {
        return "http://" + registry.getHost() + ":" + registry.getMappedPort(8081);
    }

    private static void post(String method, String path, String body) {
        var request = HttpRequest.newBuilder(URI.create(registryUrl() + path))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new IllegalStateException(method + " " + path + ": " + response.body());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
