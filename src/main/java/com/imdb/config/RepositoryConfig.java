package com.imdb.config;

import com.imdb.repository.TitleRepository;
import com.imdb.repository.tsv.CachedTitleRepositoryImpl;
import com.imdb.repository.tsv.TSVDataStore;
import com.imdb.repository.tsv.TitleRepositoryImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RepositoryConfig {

    @Bean
    public TitleRepository titleRepository(TSVDataStore store) {
        return new CachedTitleRepositoryImpl(new TitleRepositoryImpl(store));
    }
}