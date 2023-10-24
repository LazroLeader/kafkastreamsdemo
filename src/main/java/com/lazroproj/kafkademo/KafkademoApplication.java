package com.lazroproj.kafkademo;

import com.liderbet.sportradar.MatchChange;
import com.liderbet.sportradar.RawMatch;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

@SpringBootApplication
@EnableKafkaStreams
public class KafkademoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkademoApplication.class, args);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return fb -> fb.setStateListener((newState, oldState) -> {
            System.out.println("State transition from " + oldState + " to " + newState);
        });
    }

    @Bean
    public KStream<String, RawMatch> kStream(StreamsBuilder kStreamBuilder) {
        var stream = kStreamBuilder.<String, RawMatch>stream("matches.live.sportradar");


        stream
                .mapValues((readOnlyKey, value) -> MatchChange.newBuilder().setMatch(value).setChanged(false).build())
                .groupByKey()
                .reduce((value1, value2) -> {
                    if (value2 == null) {
                        return MatchChange.newBuilder(value1).setChanged(true).build();
                    }
                    if (value1.getMatch().getMAXMESSAGECREATED().equals(value2.getMatch().getMAXMESSAGECREATED())
                            || value1.getMatch().getMAXMESSAGECREATED().isAfter(value2.getMatch().getMAXMESSAGECREATED())) {
                        return MatchChange.newBuilder(value2).setChanged(false).build();
                    } else {
                        return MatchChange.newBuilder(value2).setChanged(true).build();
                    }
                })
                .toStream()
                .filter((key, value) -> value.getChanged())
                .peek((key, value) -> System.out.println(key + " " + value));


        return stream;
    }


}
