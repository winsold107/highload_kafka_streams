package ru.curs.counting;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.configuration.KafkaConfiguration;
import ru.curs.counting.configuration.TopologyConfiguration;
import ru.curs.counting.model.*;
import ru.curs.counting.transformer.TotallingTransformer;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.curs.counting.model.TopicNames.*;

public class TestTopology {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Bet> inputTopic;
    private TestInputTopic<String, EventScore> scoreTopic;
    private TestOutputTopic<String, Fraud> fraudTopic;

    @BeforeEach
    public void setUp() {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(topology, config.asProperties());
        inputTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(Bet.class).serializer());

        scoreTopic = topologyTestDriver.createInputTopic(EVENT_SCORE_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(EventScore.class).serializer());

        fraudTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, Serdes.String().deserializer(),
                new JsonSerde<>(Fraud.class).deserializer());
    }

    @AfterEach
    public void closeUp() {
        topologyTestDriver.close();
    }

    void placeBet(Bet value) {
        inputTopic.pipeInput(value.key(), value);
    }

    void placeEvent(EventScore value) {
        scoreTopic.pipeInput(value.getEvent(), value);
    }
    @Test
    void simpleBettorTest() {
        placeBet(new Bet("John", "A-B", Outcome.A, 20, 1.1, 0));
        placeBet(new Bet("John", "C-A", Outcome.H, 10, 1.1,3));

        TotallingTransformer kv_store = new TotallingTransformer("bettor");
        KeyValueStore<String, Long> store = topologyTestDriver.getKeyValueStore(kv_store.STORE_NAME);
        assertEquals(30, store.get("John").intValue());
    }

    @Test
    void bettorTest() {
        placeBet(new Bet("John", "A-B", Outcome.A, 20, 1.1, 0));
        placeBet(new Bet("Marley", "A-B", Outcome.H, 30, 1.1,0));
        placeBet(new Bet("John", "C-D", Outcome.H, 20, 1.1,1));
        placeBet(new Bet("John", "C-D", Outcome.A, 10, 1.1,2));
        placeBet(new Bet("John", "C-A", Outcome.H, 10, 1.1,3));

        TotallingTransformer kv_store = new TotallingTransformer("bettor");
        KeyValueStore<String, Long> store = topologyTestDriver.getKeyValueStore(kv_store.STORE_NAME);
        assertEquals(60, store.get("John").intValue());
        assertEquals(30, store.get("Marley").intValue());
    }

    @Test
    void simpleCommandTest() {
        placeBet(new Bet("John", "A-B", Outcome.A, 20, 1.1, 0));
        placeBet(new Bet("Marley", "A-B", Outcome.H, 30, 1.1,0));

        TotallingTransformer kv_store = new TotallingTransformer("command");
        KeyValueStore<String, Long> store = topologyTestDriver.getKeyValueStore(kv_store.STORE_NAME);
        assertEquals(30, store.get("A").intValue());
        assertEquals(20, store.get("B").intValue());
    }

    @Test
    void commandTest() {
        placeBet(new Bet("John", "A-B", Outcome.A, 20, 1.1, 0));
        placeBet(new Bet("Marley", "A-B", Outcome.H, 30, 1.1,0));
        placeBet(new Bet("John", "C-A", Outcome.H, 20, 1.1,1));
        placeBet(new Bet("John", "B-A", Outcome.A, 10, 1.1,2));
        placeBet(new Bet("Marley", "C-B", Outcome.H, 10, 1.1,3));

        TotallingTransformer kv_store = new TotallingTransformer("command");
        KeyValueStore<String, Long> store = topologyTestDriver.getKeyValueStore(kv_store.STORE_NAME);
        assertEquals(40, store.get("A").intValue());
        assertEquals(20, store.get("B").intValue());
        assertEquals(30, store.get("C").intValue());
    }

    @Test
    public void simpleFraudTest() {
        long current = System.currentTimeMillis();
        placeEvent(new EventScore("Turkey-Moldova", new Score().goalHome(), current));
        placeBet(new Bet("alice", "Turkey-Moldova", Outcome.A, 1, 1.5, current - 100));
        placeBet(new Bet("bob", "Turkey-Moldova", Outcome.H, 1, 1.5, current - 100));
        Fraud expectedFraud = Fraud.builder()
                .bettor("bob")
                .match("Turkey-Moldova")
                .outcome(Outcome.H)
                .amount(1)
                .odds(1.5)
                .lag(100)
                .build();

        assertEquals(expectedFraud, fraudTopic.readValue());
        assertTrue(fraudTopic.isEmpty());
    }

    @Test
    public void fraudTest() {
        long current = System.currentTimeMillis();
        placeEvent(new EventScore("Turkey-Moldova", new Score().goalHome(), current));
        placeEvent(new EventScore("A-B", new Score().goalAway(), current));

        placeBet(new Bet("alice", "Turkey-Moldova", Outcome.A, 1, 1.5, current - 100));
        placeBet(new Bet("bob", "Turkey-Moldova", Outcome.H, 1, 1.5, current - 100));
        placeBet(new Bet("bob", "Turkey-Moldova", Outcome.H, 1, 1.5, current - 4000));
        placeBet(new Bet("bob", "Turkey-Moldova", Outcome.H, 1, 1.5, current - 5000));

        placeBet(new Bet("bob", "A-B", Outcome.A, 1, 1.5, current));
        placeBet(new Bet("bob", "A-B", Outcome.A, 1, 1.5, current - 10000));
        placeBet(new Bet("bob", "A-B", Outcome.H, 1, 1.5, current - 10));

        Fraud expectedFraud1 = Fraud.builder()
                .bettor("bob")
                .match("Turkey-Moldova")
                .outcome(Outcome.H)
                .amount(1)
                .odds(1.5)
                .lag(100)
                .build();

        Fraud expectedFraud2 = Fraud.builder()
                .bettor("bob")
                .match("A-B")
                .outcome(Outcome.A)
                .amount(1)
                .odds(1.5)
                .lag(0)
                .build();

        assertEquals(expectedFraud1, fraudTopic.readValue());
        assertEquals(expectedFraud2, fraudTopic.readValue());
        assertTrue(fraudTopic.isEmpty());
    }
}
