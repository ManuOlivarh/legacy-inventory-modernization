package dev.joseherrera.inventory.translator;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.joseherrera.inventory.events.MovementReason;
import dev.joseherrera.inventory.events.StockLevelChanged;
import dev.joseherrera.inventory.events.StockMoved;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

class TranslationTopologyTest {

    private static final String REGISTRY = "mock://translation-topology-test";
    private static final ZoneId GUATEMALA = ZoneId.of("America/Guatemala");

    private static final TranslatorProperties PROPERTIES = new TranslatorProperties(
            "erp.raw.ERPDB.dbo.INV_MOV",
            "erp.raw.ERPDB.dbo.INV_STK",
            "inventory.stock-movements.v1",
            "inventory.stock-levels.v1",
            "inventory.translator.dlq",
            GUATEMALA);

    private final ObjectMapper json = new ObjectMapper();

    private TopologyTestDriver driver;
    private TestInputTopic<String, JsonNode> movementsIn;
    private TestInputTopic<String, JsonNode> stockIn;
    private TestOutputTopic<String, StockMoved> movementsOut;
    private TestOutputTopic<String, StockLevelChanged> levelsOut;
    private TestOutputTopic<String, JsonNode> deadLetters;

    @BeforeEach
    void setUp() {
        Serde<JsonNode> changeEvents = new ChangeEventSerde();
        Serde<StockMoved> movedSerde = testSerde();
        Serde<StockLevelChanged> levelSerde = testSerde();

        StreamsBuilder builder = new StreamsBuilder();
        new TranslationTopology(PROPERTIES, changeEvents, movedSerde, levelSerde).translation(builder);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "translation-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        driver = new TopologyTestDriver(builder.build(), config);
        movementsIn = driver.createInputTopic(PROPERTIES.movementsInputTopic(),
                Serdes.String().serializer(), changeEvents.serializer());
        stockIn = driver.createInputTopic(PROPERTIES.stockInputTopic(),
                Serdes.String().serializer(), changeEvents.serializer());
        movementsOut = driver.createOutputTopic(PROPERTIES.movementsOutputTopic(),
                Serdes.String().deserializer(), movedSerde.deserializer());
        levelsOut = driver.createOutputTopic(PROPERTIES.stockLevelsOutputTopic(),
                Serdes.String().deserializer(), levelSerde.deserializer());
        deadLetters = driver.createOutputTopic(PROPERTIES.deadLetterTopic(),
                Serdes.String().deserializer(), changeEvents.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void salesBecomeNegativeQuantitiesKeyedByStockPosition() {
        movementsIn.pipeInput(null, movement("VT", "3.00"));

        var record = movementsOut.readRecord();
        assertThat(record.key()).isEqualTo("000000010001:T001");
        assertThat(record.value().getQuantity()).isEqualByComparingTo(new BigDecimal("-3.00"));
        assertThat(record.value().getReason()).isEqualTo(MovementReason.SALE);
    }

    @Test
    void receiptsStayPositive() {
        movementsIn.pipeInput(null, movement("RC", "25.00"));

        assertThat(movementsOut.readValue().getQuantity()).isEqualByComparingTo(new BigDecimal("25.00"));
    }

    @Test
    void fixedWidthCodesAreTrimmed() {
        movementsIn.pipeInput(null, movement("VT", "1.00", "000000010001  ", "T001"));

        assertThat(movementsOut.readRecord().value().getSku()).isEqualTo("000000010001");
    }

    @Test
    void localErpTimestampsBecomeUtcInstants() {
        long wallClock = LocalDateTime.of(2026, 9, 17, 8, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        var change = movement("VT", "1.00");
        ((com.fasterxml.jackson.databind.node.ObjectNode) change.get("after")).put("MOV_FEC", wallClock);

        movementsIn.pipeInput(null, change);

        assertThat(movementsOut.readValue().getOccurredAt())
                .isEqualTo(Instant.parse("2026-09-17T14:00:00Z"));
    }

    @Test
    void unknownMovementTypesAreDeadLetteredInsteadOfStoppingThePipeline() {
        var unknown = movement("XX", "1.00");
        movementsIn.pipeInput("{\"MOV_NUM\":7}", unknown);
        movementsIn.pipeInput(null, movement("VT", "1.00"));

        assertThat(movementsOut.readValuesToList()).hasSize(1);
        var deadLetter = deadLetters.readRecord();
        assertThat(deadLetter.key()).isEqualTo("{\"MOV_NUM\":7}");
        assertThat(deadLetter.value().toString()).isEqualTo(unknown.toString());
        assertThat(new String(deadLetter.headers().lastHeader(TranslationTopology.DLQ_REASON_HEADER).value(),
                java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("unknown legacy movement type: XX");
    }

    @Test
    void deletesAndTombstonesAreIgnored() {
        var tombstone = json.createObjectNode();
        tombstone.putNull("after");
        tombstone.put("op", "d");

        movementsIn.pipeInput(null, tombstone);

        assertThat(movementsOut.isEmpty()).isTrue();
    }

    @Test
    void stockLevelsCarryAvailabilityAndTheSourceVersion() {
        stockIn.pipeInput(null, stock("42.00", "5.00"));

        var record = levelsOut.readRecord();
        assertThat(record.key()).isEqualTo("000000010001:T001");
        assertThat(record.value().getOnHand()).isEqualByComparingTo(new BigDecimal("42.00"));
        assertThat(record.value().getAvailable()).isEqualByComparingTo(new BigDecimal("37.00"));
        assertThat(record.value().getSourceLsn()).isEqualTo("0000002c:00000a38:0004");
    }

    @Test
    void negativeStockSurvivesTranslation() {
        stockIn.pipeInput(null, stock("-2.00", "0.00"));

        assertThat(levelsOut.readValue().getAvailable()).isEqualByComparingTo(new BigDecimal("-2.00"));
    }

    private JsonNode movement(String type, String quantity) {
        return movement(type, quantity, "000000010001", "T001");
    }

    private JsonNode movement(String type, String quantity, String sku, String location) {
        var after = json.createObjectNode()
                .put("MOV_NUM", 7L)
                .put("ART_COD", sku)
                .put("BOD_COD", location)
                .put("MOV_TIP", type)
                .put("MOV_CAN", quantity)
                .put("MOV_FEC", 1789678847267L)
                .put("MOV_USR", "SIMULADOR")
                .put("MOV_REF", "SIM00000007");
        return envelope(after);
    }

    private JsonNode stock(String onHand, String reserved) {
        var after = json.createObjectNode()
                .put("ART_COD", "000000010001")
                .put("BOD_COD", "T001")
                .put("STK_EXI", onHand)
                .put("STK_RES", reserved)
                .put("FEC_ULT", 1789678847267L);
        return envelope(after);
    }

    private JsonNode envelope(com.fasterxml.jackson.databind.node.ObjectNode after) {
        var change = json.createObjectNode();
        change.set("after", after);
        change.put("op", "c");
        change.put("ts_ms", 1789678851263L);
        change.putObject("source").put("commit_lsn", "0000002c:00000a38:0004");
        return change;
    }

    private static <T extends org.apache.avro.specific.SpecificRecord> Serde<T> testSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY), false);
        return serde;
    }
}
