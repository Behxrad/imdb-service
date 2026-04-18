package com.imdb.controller;

import com.imdb.dto.APIResponse;
import com.imdb.dto.TitleDTO;
import com.imdb.dto.TitlesDTO;
import com.imdb.service.IMDBService;
import com.imdb.stats.RequestCounterFilter;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

@Validated
@RestController
@RequestMapping("/api")
public class IMDBController {

    private final IMDBService service;
    private final RequestCounterFilter counterFilter;

    public IMDBController(IMDBService service, RequestCounterFilter counterFilter) {
        this.service = service;
        this.counterFilter = counterFilter;
    }

    @GetMapping("/same-director-writer")
    public APIResponse<TitlesDTO> titlesWithSameDirectorWriter(
            @RequestParam(defaultValue = "1") @Min(1) int page,
            @RequestParam(defaultValue = "10") @Min(10) int size) {

        return APIResponse.<TitlesDTO>builder()
                .success(true)
                .data(service.titlesWithSameDirectorWriter(page, size))
                .build();
    }

    @GetMapping("/shared")
    public APIResponse<Set<TitleDTO>> sharedTitles(
            @RequestParam("actor1") @NotBlank @Size(min = 2, max = 100) String a1,
            @RequestParam("actor2") @NotBlank @Size(min = 2, max = 100) String a2) {

        return APIResponse.<Set<TitleDTO>>builder()
                .success(true)
                .data(service.sharedTitles(a1, a2))
                .build();
    }

    @GetMapping("/genre-yearly-ranking")
    public APIResponse<Map<Integer, TitleDTO>> genreYearlyRanking(
            @RequestParam("genre") @NotBlank @Size(min = 2, max = 50) String genre) {

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