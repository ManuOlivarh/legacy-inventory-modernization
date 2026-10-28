package dev.joseherrera.inventory.translator;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.joseherrera.inventory.events.MovementReason;
import dev.joseherrera.inventory.events.StockLevelChanged;
import dev.joseherrera.inventory.events.StockMoved;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

@SpringBootTest
class TranslatorIT {

    private static final String KAFKA_IMAGE = "apache/kafka:4.3.1";
    private static final String REGISTRY_IMAGE = "confluentinc/cp-schema-registry:8.3.2";

    private static final String MOVEMENTS_IN = "erp.raw.ERPDB.dbo.INV_MOV";
    private static final String STOCK_IN = "erp.raw.ERPDB.dbo.INV_STK";
    private static final String MOVEMENTS_OUT = "inventory.stock-movements.v1";
    private static final String LEVELS_OUT = "inventory.stock-levels.v1";

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

    private static final HttpClient http = HttpClient.newHttpClient();
    private static final ObjectMapper json = new ObjectMapper();

    static {
        Startables.deepStart(kafka, registry).join();
        createTopics();
        registerContracts();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        properties.add("spring.kafka.properties.schema.registry.url", TranslatorIT::registryUrl);
        properties.add("spring.kafka.streams.properties.replication.factor", () -> "1");
        properties.add("spring.kafka.streams.properties.min.insync.replicas", () -> "1");
    }

    @Test
    void erpChangeEventsBecomeCommittedDomainEvents() throws Exception {
        String sku = "IT" + UUID.randomUUID().toString().substring(0, 8);
        try (var producer = producer()) {
            producer.send(new ProducerRecord<>(MOVEMENTS_IN, movement(sku, "VT", "3.00"))).get();
            producer.send(new ProducerRecord<>(STOCK_IN, stock(sku, "42.00", "5.00"))).get();
        }

        StockMoved moved = (StockMoved) firstCommitted(MOVEMENTS_OUT, sku + ":T001");
        assertThat(moved.getQuantity()).isEqualByComparingTo(new BigDecimal("-3.00"));
        assertThat(moved.getReason()).isEqualTo(MovementReason.SALE);

        StockLevelChanged level = (StockLevelChanged) firstCommitted(LEVELS_OUT, sku + ":T001");
        assertThat(level.getAvailable()).isEqualByComparingTo(new BigDecimal("37.00"));
        assertThat(level.getSourceLsn()).isEqualTo("0000002c:00000a38:0004");
    }

    @Test
    void outputUsesOnlyThePublishedContracts() throws Exception {
        String sku = "IT" + UUID.randomUUID().toString().substring(0, 8);
        try (var producer = producer()) {
            producer.send(new ProducerRecord<>(STOCK_IN, stock(sku, "1.00", "0.00"))).get();
        }
        firstCommitted(LEVELS_OUT, sku + ":T001");

        String versions = get("/subjects/" + LEVELS_OUT + "-value/versions");
        assertThat(versions).isEqualTo("[1]");
        assertThat(get("/subjects")).doesNotContain(MOVEMENTS_IN, STOCK_IN);
    }

    private static Object firstCommitted(String topic, String key) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl(),
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        try (var consumer = new KafkaConsumer<String, Object>(config, new StringDeserializer(), avroDeserializer(config))) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record.value();
                    }
                }
            }
        }
        throw new AssertionError("no committed record for " + key + " on " + topic);
    }

    @SuppressWarnings("unchecked")
    private static org.apache.kafka.common.serialization.Deserializer<Object> avroDeserializer(Map<String, Object> config) {
        var deserializer = new KafkaAvroDeserializer();
        deserializer.configure(config, false);
        return (org.apache.kafka.common.serialization.Deserializer<Object>) (Object) deserializer;
    }

    private static KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
                new StringSerializer(), new StringSerializer());
    }

    private static String movement(String sku, String type, String quantity) {
        ObjectNode after = json.createObjectNode()
                .put("MOV_NUM", 7L)
                .put("ART_COD", sku + "  ")
                .put("BOD_COD", "T001")
                .put("MOV_TIP", type)
                .put("MOV_CAN", quantity)
                .put("MOV_FEC", 1789678847267L)
                .put("MOV_USR", "SIMULADOR")
                .put("MOV_REF", "SIM00000007");
        return envelope(after);
    }

    private static String stock(String sku, String onHand, String reserved) {
        ObjectNode after = json.createObjectNode()
                .put("ART_COD", sku + "  ")
                .put("BOD_COD", "T001")
                .put("STK_EXI", onHand)
                .put("STK_RES", reserved)
                .put("FEC_ULT", 1789678847267L);
        return envelope(after);
    }

    private static String envelope(ObjectNode after) {
        ObjectNode change = json.createObjectNode();
        change.putNull("before");
        change.set("after", after);
        change.put("op", "c");
        change.put("ts_ms", 1789678851263L);
        change.putObject("source").put("commit_lsn", "0000002c:00000a38:0004");
        return change.toString();
    }

    private static void createTopics() {
        try (var admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            List<NewTopic> topics = new ArrayList<>();
            for (String name : List.of(MOVEMENTS_IN, STOCK_IN, MOVEMENTS_OUT, "inventory.translator.dlq")) {
                topics.add(new NewTopic(name, 3, (short) 1));
            }
            topics.add(new NewTopic(LEVELS_OUT, 3, (short) 1).configs(Map.of("cleanup.policy", "compact")));
            admin.createTopics(topics).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("could not create topics", e);
        }
    }

    private static void registerContracts() {
        Map<String, String> subjects = Map.of(
                MOVEMENTS_OUT + "-value", "StockMoved.avsc",
                LEVELS_OUT + "-value", "StockLevelChanged.avsc");
        subjects.forEach((subject, file) -> {
            try {
                String schema = Files.readString(Path.of("..", "contracts", file));
                send("PUT", "/config/" + subject, json.writeValueAsString(Map.of("compatibility", "BACKWARD")));
                send("POST", "/subjects/" + subject + "/versions", json.writeValueAsString(Map.of("schema", schema)));
            } catch (IOException e) {
                throw new IllegalStateException("could not register " + file, e);
            }
        });
    }

    private static String registryUrl() {
        return "http://" + registry.getHost() + ":" + registry.getMappedPort(8081);
    }

    private static String get(String path) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create(registryUrl() + path)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    private static void send(String method, String path, String body) {
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
