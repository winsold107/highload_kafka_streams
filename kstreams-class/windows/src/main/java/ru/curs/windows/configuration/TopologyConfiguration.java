package ru.curs.windows.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {

        Topology topology = streamsBuilder.build();
        System.out.println("==========================");
        System.out.println(topology.describe());
        System.out.println("==========================");
        return topology;
    }

}
