package com.imdb.controller;

import com.imdb.dto.APIResponse;
import com.imdb.dto.TitleDTO;
import com.imdb.service.ImdbService;
import com.imdb.stats.RequestCounterFilter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api")
public class ImdbController {

    private final ImdbService service;
    private final RequestCounterFilter counterFilter;

    public ImdbController(ImdbService service, RequestCounterFilter counterFilter) {
        this.service = service;
        this.counterFilter = counterFilter;
    }

    @GetMapping("/same-director-writer")
    public APIResponse<Set<TitleDTO>> titlesWithSameDirectorWriter() {
        return APIResponse.<Set<TitleDTO>>builder()
                .success(true)
                .data(service.titlesWithSameDirectorWriter())
                .build();
    }

    @GetMapping("/shared")
    public APIResponse<Set<TitleDTO>> sharedTitles(@RequestParam("actor1") String a1,
                                                   @RequestParam("actor2") String a2) {
        return APIResponse.<Set<TitleDTO>>builder()
                .success(true)
                .data(service.sharedTitles(a1, a2))
                .build();
    }

    @GetMapping("/genre-yearly-ranking")
    public APIResponse<Map<Integer, TitleDTO>> genreYearlyRanking(@RequestParam("genre") String genre) {
        return APIResponse.<Map<Integer, TitleDTO>>builder()
                .success(true)
                .data(service.genreYearlyRanking(genre))
                .build();
    }

    @GetMapping("/stats")
    public APIResponse<Map<String, Object>> stats() {
        return APIResponse.<Map<String, Object>>builder()
                .success(true)
                .data(Map.of(
                        "total", counterFilter.getTotalCount(),
                        "perApi", counterFilter.getPerApiCount()
                ))
                .build();
    }
}