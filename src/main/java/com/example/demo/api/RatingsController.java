package com.example.demo.api;

import com.example.demo.model.Rating;
import com.example.demo.repository.RatingsRepository;
import com.example.demo.repository.TalksRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;

@RestController
@RequestMapping("/ratings")
@RequiredArgsConstructor
public class RatingsController {

    final ObjectMapper objectMapper;

    final KafkaTemplate<String, Rating> kafkaTemplate;

    final RatingsRepository ratingsRepository;

    final TalksRepository talksRepository;

    @PostMapping
    public Mono<ResponseEntity<Object>> recordRating(@RequestBody Rating rating) throws Exception {
        return talksRepository.exists(rating.getTalkId())
                .filter(Boolean.TRUE::equals)
                .flatMap(__ -> Mono
                        .fromCompletionStage(
                                kafkaTemplate.send("ratings", rating).completable()
                        )
                        .thenReturn(ResponseEntity.accepted().build())
                )
                .defaultIfEmpty(ResponseEntity.badRequest().build());
    }

    @GetMapping
    public Mono<Map<Integer, Integer>> getRatings(@RequestParam String talkId) {
        return ratingsRepository.findAll(talkId)
                .defaultIfEmpty(Collections.emptyMap());
    }
}
