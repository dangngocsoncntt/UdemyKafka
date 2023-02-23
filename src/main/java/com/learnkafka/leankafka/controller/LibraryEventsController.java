package com.learnkafka.leankafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.leankafka.domain.LibraryEvent;
import com.learnkafka.leankafka.domain.LibraryEventType;
import com.learnkafka.leankafka.producer.LibraryEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;
    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent (@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
//        libraryEventProducer.sendLibraryEvent(libraryEvent);
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
