package com.demoproject.flinkjob;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.base.DeliveryGuarantee;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import org.bson.Document;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class FlinkJob {
    private static ObjectMapper mapper = new ObjectMapper();
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("transactions-events")
                .setGroupId("flink-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> transactionSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                        .setTopicSelector((String event)->sinkSelector(event))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Client events");

        DataStream<String> enrichedStream = stream.map(new EnrichmentMap());
        enrichedStream.sinkTo(transactionSink);

        /*
         * //Before using setTopicSelector I had 2 sinks one for normal transactions and
         * one for dlq
         * KafkaSink<String> dqlSink = KafkaSink.<String>builder()
         * .setBootstrapServers("kafka:9092")
         * .setRecordSerializer(
         * KafkaRecordSerializationSchema.builder()
         * .setTopic("transactions-dlq")
         * .setValueSerializationSchema(new SimpleStringSchema())
         * .build()
         * ).build();
         * 
         * //and then after the stream happened I filtered and assigned.
         * DataStream<JsonNode> successStream = enrichedStream.filter(node ->
         * !"NOT_FOUND".equals(node.get("enrichmentStatus").asText()));
         * DataStream<JsonNode> dqlStream = enrichedStream.filter(node ->
         * "NOT_FOUND".equals(node.get("enrichmentStatus").asText()));
         * 
         * successStream.map(JsonNode::toString).sinkTo(transactionSink);
         * dqlStream.map(JsonNode::toString).sinkTo(dqlSink);
         * //But not that is not necessary by using setTopicSelector logic
         */

        env.execute("Message enrichment demo");
    }

    private static String sinkSelector(String event){
        try{
        JsonNode eventJson = mapper.readTree(event);
        String enrichementStatus = eventJson.get("enrichmentStatus").asText();
        if ("NOT_FOUND".equals(enrichementStatus)) {
            return "transactions-dlq";
        } else {
            return "transactions-enriched";}
        }catch(Exception e){
            return "transactions-dlq";
        }
    };

    private static class EnrichmentMap extends RichMapFunction<String, String> {
        private transient MongoClient mongoClient;
        private transient MongoCollection<Document> clientsCollection;

        @Override
        public void open(Configuration parameters) {
            mongoClient = MongoClients.create("mongodb://mongo:27017");
            MongoDatabase db = mongoClient.getDatabase("clientData");
            clientsCollection = db.getCollection("clients");
            System.out.println("---------------Clients Colletion " + clientsCollection.toString());
        }

        @Override
        public String map(String value) throws Exception {
            JsonNode streamClient = mapper.readTree(value);
            Integer accountID = streamClient.get("accountNumber").asInt();

            System.out.println("Account ID ----" + accountID);

            Document clientById = clientsCollection.find(Filters.eq("_id", accountID)).first();

            if (clientById == null) {

                ObjectNode result = mapper.createObjectNode();
                result.set("accountNumber", streamClient.get("accountNumber"));
                result.set("amount", streamClient.get("amount"));
                result.set("timestamp", streamClient.get("timestamp"));
                result.set("location", streamClient.get("location"));
                result.put("enrichmentStatus", "NOT_FOUND");

                System.out.println(result.toString());
                return result.toString();

            }
            JsonNode clientDBNode = mapper.readTree(clientById.toJson());

            ObjectNode result = mapper.createObjectNode();
            result.set("accountNumber", streamClient.get("accountNumber"));
            result.set("amount", streamClient.get("amount"));
            result.set("timestamp", streamClient.get("timestamp"));
            result.set("location", streamClient.get("location"));
            result.set("deviceId", clientDBNode.get("deviceId"));
            result.set("phoneNumber", clientDBNode.get("phoneNumber"));
            result.set("email", clientDBNode.get("email"));
            result.put("enrichmentStatus", "ENRICHED");

            System.out.println(result.toString());
            return result.toString();
        }

        @Override
        public void close() {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

    }
}