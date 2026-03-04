package com.demostream.streaming;

import org.apache.kafka.clients.producer.KafkaProducer; 
import org.apache.kafka.clients.producer.ProducerRecord; 
import org.springframework.web.bind.annotation.*; 
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutionException;
import java.util.Properties;
import java.util.Map;

@RestController
public class RequestHandler{

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private String serialization = "org.apache.kafka.common.serialization.StringSerializer";
    private String broker = "";
    private String apiKey = "";
    private String password = "";

    public RequestHandler(){
        // this would ususlly by handle by a VCAP_SERVICES, but I going to hardcode for now. 
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("security.protocol", "SASL_SSL"); 
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"" + apiKey + "\" " + "password=\"" + password + "\";");
        props.put("key.serializer", serialization );
        props.put("value.serializer", serialization );

        this.producer = new KafkaProducer<>(props);
    }


    @PostMapping("/clientTransaction")
    public ResponseEntity<Object> clientTransaction(@RequestBody Map<String, Object> request){

        String[] requiredFields = { "accountNumber", "amount", "timestamp", "location"};

        for (String requiredField : requiredFields){
            if(!request.containsKey(requiredField) || request.get(requiredField) == null){
                return ResponseEntity.badRequest().body("missing field " + requiredField);
            }
        }
        String message; 

        try{
            message = mapper.writeValueAsString(request);
        } catch (Exception e){
            return ResponseEntity.badRequest().body("Invalid Json Format");
        }

        try { 
            producer.send(new ProducerRecord<>("transactions-events", message)).get(); 
            System.out.println("Sent message: " + message); 
            } catch (InterruptedException | ExecutionException e) { 
                return ResponseEntity.status(500).body("Error sending message: " + e.getMessage()); }
        return ResponseEntity.ok("Message Sent");
    }

    @PreDestroy
    public void shutdown(){
        producer.close();
    }
}