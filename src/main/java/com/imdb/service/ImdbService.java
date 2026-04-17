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