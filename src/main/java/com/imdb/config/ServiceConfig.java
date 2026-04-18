package com.imdb.config;

import com.imdb.repository.TitleRepository;
import com.imdb.service.CachedIMDBServiceImpl;
import com.imdb.service.IMDBService;
import com.imdb.service.IMDBServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceConfig {

    @Bean
    public IMDBService imdbService(TitleRepository repository) {
        return new CachedIMDBServiceImpl(new IMDBServiceImpl(repository));
    }
}
