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

import com.mongodb.client.MongoClient; 
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import org.bson.Document;
import java.time.LocalDate;

import com.fasterxml.jackson.databind.JsonNode; 
import com.fasterxml.jackson.databind.ObjectMapper; 
import com.fasterxml.jackson.databind.node.ObjectNode;

public class FlinkJob{
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source =  KafkaSource.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setTopics("transactions-events")
        .setGroupId("flink-consumer")
        .setStartingOffsets(OffsetsInitializer.earliest()) 
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        KafkaSink<String> transactionSink = KafkaSink.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setRecordSerializer( 
            KafkaRecordSerializationSchema.builder()
            .setTopic("transactions-enriched")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
            ).build();

        KafkaSink<String> dqlSink = KafkaSink.<String>builder()
        .setBootstrapServers("kafka:9092")
        .setRecordSerializer( 
            KafkaRecordSerializationSchema.builder()
            .setTopic("transactions-dlq")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
            ).build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Client events");

        DataStream<JsonNode> enrichedStream = stream.map(new EnrichmentMap());

        DataStream<JsonNode> successStream = enrichedStream.filter(node -> !"NOT_FOUND".equals(node.get("enrichmentStatus").asText()));
        DataStream<JsonNode> dqlStream = enrichedStream.filter(node -> "NOT_FOUND".equals(node.get("enrichmentStatus").asText()));

       successStream .map(JsonNode::toString).sinkTo(transactionSink);
       dqlStream .map(JsonNode::toString).sinkTo(dqlSink);

        env.execute("Message enrichment demo");
    }

    private static class EnrichmentMap extends RichMapFunction<String, JsonNode>{
        private transient MongoClient mongoClient;
        private transient MongoCollection<Document> clientsCollection;
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters){
            mongoClient = MongoClients.create("mongodb://mongo:27017");
            MongoDatabase db = mongoClient.getDatabase("clientData");
            clientsCollection = db.getCollection("clients");
            System.out.println("---------------Clients Colletion " + clientsCollection.toString());
            mapper = new ObjectMapper();
        }

        @Override
        public JsonNode map(String value) throws Exception{
            JsonNode streamClient = mapper.readTree(value);
            Integer accountID = streamClient.get("accountNumber").asInt();

            System.out.println("Account ID ----" + accountID);

            Document clientById = clientsCollection.find(Filters.eq("_id", accountID)).first();

            if(clientById == null){

               ObjectNode result = mapper.createObjectNode();
               result.set("accountNumber", streamClient.get("accountNumber"));
               result.set("amount", streamClient.get("amount"));
               result.set("timestamp", streamClient.get("timestamp"));
               result.set("location", streamClient.get("location"));
               result.put("enrichmentStatus", "NOT_FOUND");

               System.out.println(result.toString());
               return result;   

            }
            JsonNode clientDBNode =  mapper.readTree(clientById.toJson());

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
               return result;
               }

        @Override
        public void close(){
            if(mongoClient != null){
                mongoClient.close();
            }
        }

    }
}