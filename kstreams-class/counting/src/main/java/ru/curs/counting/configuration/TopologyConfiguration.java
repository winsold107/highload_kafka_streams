package ru.curs.counting.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.counting.transformer.TotallingTransformer;
import ru.curs.counting.transformer.ScoreTransformer;

import java.time.Duration;

import static ru.curs.counting.model.TopicNames.*;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {
    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, Bet> input = streamsBuilder.
                stream(BET_TOPIC,
                        Consumed.with(
                                Serdes.String(), new JsonSerde<>(Bet.class))
                                .withTimestampExtractor((record, previousTimestamp) ->
                                        ((Bet) record.value()).getTimestamp()
                                )
                );

        KStream<String, Long> gainBettorAmount
                = input.map((k, v) -> {
                    String bettor = v.getBettor();
                    Long amount = v.getAmount();
                    return KeyValue.pair(bettor, amount);
        });

        KTable<String, Long> betsTable = gainBettorAmount
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Long.class)))
                .reduce((new Reducer<Long>() {
                    @Override
                    public Long apply(Long currentSum, Long v) {
                        Long sum = currentSum + v;
                        return sum;
                    }
                }));

        KStream<String, Long> betsStream
                = betsTable.toStream();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        KStream<String, Long> gainCommandAmount
                = input.map((k, v) -> {
            String match = v.getMatch();
            Outcome result = v.getOutcome();
            String[] commands = match.split("-");
            Long amount = v.getAmount();
            if (result == Outcome.H) {
                return KeyValue.pair(commands[0], amount);
            }
            return KeyValue.pair(commands[1], amount);
        });

        KTable<String, Long> commandGain
                = gainCommandAmount
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Long.class)))
                .reduce((new Reducer<Long>() {
                    @Override
                    public Long apply(Long currentSum, Long v) {
                        Long sum = currentSum + v;
                        return sum;
                    }
                }));

        KStream<String, Long> commandsStream
                = commandGain.toStream();

        //////////////////////////////////////////////////////////////////////////////////////////////////////////////

        KStream<String, EventScore> eventScores = streamsBuilder.stream(EVENT_SCORE_TOPIC,
                Consumed.with(
                        Serdes.String(), new JsonSerde<>(EventScore.class))
                        .withTimestampExtractor((record, previousTimestamp) ->
                                ((EventScore) record.value()).getTimestamp()));

        KStream<String, Bet> winningBets = new ScoreTransformer("fraud").transformStream(streamsBuilder, eventScores);

        KStream<String, Fraud> join = input.join(winningBets,
                (bet, winningBet) ->
                        Fraud.builder()
                                .bettor(bet.getBettor())
                                .outcome(bet.getOutcome())
                                .amount(bet.getAmount())
                                .match(bet.getMatch())
                                .odds(bet.getOdds())
                                .lag(winningBet.getTimestamp() - bet.getTimestamp())
                                .build(),
                JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                StreamJoined.with(Serdes.String(),
                        new JsonSerde<>(Bet.class),
                        new JsonSerde<>(Bet.class)
                )).selectKey((k, v) -> v.getBettor());


        join.to(FRAUD_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));
        commandsStream.to(COMMAND_AMOUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        betsStream.to(BETTOR_AMOUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        Topology topology = streamsBuilder.build();
        System.out.println("==============================");
        System.out.println(topology.describe());
        System.out.println("==============================");
        return topology;
    }
}
