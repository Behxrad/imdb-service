package com.imdb.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder
@Data
public class Titles {

    private List<Title> titles;
    private int page;
    private int size;
    private int totalSize;
}
