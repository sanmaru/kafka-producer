package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;

import java.net.Inet4Address;
import java.net.InetAddress;

@Slf4j
@SpringBootApplication
public class KafkaProducerApplication {


    public static void main(String[] args){
        log.info("Start Kafka");
        SpringApplication.run(KafkaProducerApplication.class, args);
    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template){
        return args -> {
            InetAddress inet4ddress = Inet4Address.getLocalHost();
            log.info(inet4ddress.getHostAddress());

            log.info("====================================1");
            log.info("{}", template.getDefaultTopic());
            log.info("====================================2");
            template.send("quickstart-events", "Java Producer");
            template.send("quickstart-events", "Java Producer1");
            template.send("quickstart-events", "Java Producer2");

//            ProducerRecord<String, String> record = new ProducerRecord<>();
//            record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "Sample".getBytes()));


//            log.info("====================================6");

        };
    }

    @KafkaListener(topics = "quickstart-events", id="consumer-group" )
    @SendTo(value = "quickstart-events-result")
    public void listen1(@Payload String in
    , @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) Integer key
//    , @Header(name = KafkaHeaders.ACKNOWLEDGMENT)Acknowledgment acknowledgment
    ,@Header(name = KafkaHeaders.REPLY_TOPIC) ReplyingKafkaTemplate reply
                        ){
        log.info("consumer 1 key = {}, payload = {}, reply = {}", key, in, reply);
//        acknowledgment.acknowledge();
    }

}
