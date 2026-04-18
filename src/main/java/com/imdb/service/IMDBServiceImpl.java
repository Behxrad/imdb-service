package com.imdb.service;

import com.imdb.dto.TitleDTO;
import com.imdb.dto.TitlesDTO;
import com.imdb.mapper.IMDBMapper;
import com.imdb.model.Titles;
import com.imdb.repository.TitleRepository;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IMDBServiceImpl implements IMDBService {

    private final TitleRepository titleRepo;

    public IMDBServiceImpl(TitleRepository titleRepo) {
        this.titleRepo = titleRepo;
    }

    public TitlesDTO titlesWithSameDirectorWriter(int page, int size) {
        Titles sharedTitles = titleRepo.findTitlesSameDirectorWriterAlive(page, size);
        return TitlesDTO.builder()
                .titles(sharedTitles.getTitles().stream()
                        .map(IMDBMapper::toDTO)
                        .collect(Collectors.toList()))
                .page(sharedTitles.getPage())
                .size(sharedTitles.getSize())
                .totalSize(sharedTitles.getTotalSize())
                .build();
    }

    public Set<TitleDTO> sharedTitles(String actor1, String actor2) {
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