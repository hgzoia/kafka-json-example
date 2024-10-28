package com.hugo.json.consumer.listener;

import com.hugo.json.consumer.model.Payment;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
@Log4j2
public class JsonListener {

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "create-group", containerFactory = "jsonContainerFactory")
    public void antiFraud(@Payload Payment payment){
        log.info("Payment received: {}", payment.toString());
        Thread.sleep(2000);

        log.info("Validating fraud...");
        Thread.sleep(1000);

        log.info("Purchase approved..");
    }

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "pdf-group", containerFactory = "jsonContainerFactory")
    public void pdfGenerator(@Payload Payment payment){
        Thread.sleep(4000);
        log.info("Generating PDF with Product ID: {}", payment.getProductId().toString());
    }

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "email-group", containerFactory = "jsonContainerFactory")
    public void sendEmail(){
        Thread.sleep(6000);
        log.info("Sending email confirmation...");
    }

}
