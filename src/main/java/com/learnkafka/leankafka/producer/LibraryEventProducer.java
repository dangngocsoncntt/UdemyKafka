package com.learnkafka.leankafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.leankafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class LibraryEventProducer {
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    String topic = "library-events1";

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException{
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);

        // THUC HIEN GOI KAFKA SYNCHRONOUS NHU SAU
//        SendResult<Integer, String> listenableFuture = kafkaTemplate.sendDefault(key, value).get();
//        SendResult<Integer, String> listenableFuture = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

    }

    public void sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException{
        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = createProducerRecord(topic, key, value);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });

    }

    private ProducerRecord<Integer, String> createProducerRecord(String topic, Integer key, String value) {
        List<Header> listHeader = List.of(
                    new RecordHeader("event-source", "scanner".getBytes()),
                    new RecordHeader("event-source1", "scanner".getBytes()),
                    new RecordHeader("event-source3", "scanner".getBytes())
                );
        return new ProducerRecord<>(topic, null, key, value, listHeader);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Send message successfully with key {} and value {} and partition {}",
                key, value, result.getRecordMetadata().partition());
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.info("Send message successfully with key {} and value {} and error {}",
                key, value, ex.getMessage());
    }
}
