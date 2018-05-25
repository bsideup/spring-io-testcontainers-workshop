package com.example.demo.streams;

import com.example.demo.model.Rating;
import com.example.demo.repository.RatingsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class RatingsListener {

    final RatingsRepository ratingsRepository;

    @KafkaListener(groupId = "ratings", topics = "ratings")
    public void handle(@Payload Rating rating) throws Exception {
        log.info("Received rating: {}", rating);

        ratingsRepository.add(rating.getTalkId(), rating.getValue()).block();
    }
}
