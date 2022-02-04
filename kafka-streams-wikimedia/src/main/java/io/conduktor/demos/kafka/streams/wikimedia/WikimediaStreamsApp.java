package io.conduktor.demos.kafka.streams.wikimedia;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class WikimediaStreamsApp {
//
//    public static class JSONSerde<T extends JSONSerdeCompatible> implements Serializer<T>, Deserializer<T>, Serde<T> {
//        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
//
//        @Override
//        public void configure(final Map<String, ?> configs, final boolean isKey) {}
//
//        @SuppressWarnings("unchecked")
//        @Override
//        public T deserialize(final String topic, final byte[] data) {
//            if (data == null) {
//                return null;
//            }
//
//            try {
//                return (T) OBJECT_MAPPER.readValue(data, JSONSerdeCompatible.class);
//            } catch (final IOException e) {
//                throw new SerializationException(e);
//            }
//        }
//
//        @Override
//        public byte[] serialize(final String topic, final T data) {
//            if (data == null) {
//                return null;
//            }
//
//            try {
//                return OBJECT_MAPPER.writeValueAsBytes(data);
//            } catch (final Exception e) {
//                throw new SerializationException("Error serializing JSON message", e);
//            }
//        }
//
//        @Override
//        public void close() {}
//
//        @Override
//        public Serializer<T> serializer() {
//            return this;
//        }
//
//        @Override
//        public Deserializer<T> deserializer() {
//            return this;
//        }
//    }
//
//    /**
//     * An interface for registering types that can be de/serialized with {@link JSONSerde}.
//     */
//    @SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
//    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
//    @JsonSubTypes({
//            @JsonSubTypes.Type(value = PageView.class, name = "pv"),
//            @JsonSubTypes.Type(value = UserProfile.class, name = "up"),
//            @JsonSubTypes.Type(value = PageViewByRegion.class, name = "pvbr"),
//            @JsonSubTypes.Type(value = WindowedPageViewByRegion.class, name = "wpvbr"),
//            @JsonSubTypes.Type(value = RegionCount.class, name = "rc")
//    })
//    public interface JSONSerdeCompatible {
//
//    }
//
//    // POJO classes
//    static public class PageView implements JSONSerdeCompatible {
//        public String user;
//        public String page;
//        public Long timestamp;
//    }
//
//    static public class UserProfile implements JSONSerdeCompatible {
//        public String region;
//        public Long timestamp;
//    }
//
//    static public class PageViewByRegion implements JSONSerdeCompatible {
//        public String user;
//        public String page;
//        public String region;
//    }
//
//    static public class WindowedPageViewByRegion implements JSONSerdeCompatible {
//        public long windowStart;
//        public String region;
//    }
//
//    static public class RegionCount implements JSONSerdeCompatible {
//        public long count;
//        public String region;
//    }

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("wikimedia.recentchange");

        KStream<String, String> filteredStream = inputTopic.filter(

                // filter for tweets which has a user of over 10000 followers
                (k, jsonTweet) ->  extractUserFollowersInTweet(jsonTweet) > 10000
        );
        filteredStream.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        // start our streams application
        kafkaStreams.start();
    }

}
