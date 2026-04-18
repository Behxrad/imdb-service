package com.imdb.service;

import com.imdb.dto.TitleDTO;
import com.imdb.mapper.IMDBMapper;
import com.imdb.repository.TitleRepository;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ImdbService {

    private final TitleRepository titleRepo;

    public ImdbService(TitleRepository titleRepo) {
        this.titleRepo = titleRepo;
    }

    public Set<TitleDTO> titlesWithSameDirectorWriter() {
        return titleRepo.findTitlesSameDirectorWriterAlive()
                .stream()
                .map(IMDBMapper::toDTO)
                .collect(Collectors.toSet());
    }

    public Set<TitleDTO> sharedTitles(String actor1, String actor2) {
        if (actor1.isEmpty() || actor2.isEmpty()) {
            throw new IllegalArgumentException("Actors cannot be empty");
        }
        if (actor1.equalsIgnoreCase(actor2)) {
            throw new IllegalArgumentException("Actors cannot be the same");
        }
        return titleRepo.findCommonTitlesBetweenActors(actor1, actor2)
                .stream()
                .map(IMDBMapper::toDTO)
                .collect(Collectors.toSet());
    }

    public Map<Integer, TitleDTO> genreYearlyRanking(String genre) {
        return titleRepo.findBestTitlesByYearForGenre(genre)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> IMDBMapper.toDTO(e.getValue())
                ));
    }
}