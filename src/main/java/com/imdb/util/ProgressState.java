package com.imdb.util;

import java.util.concurrent.atomic.AtomicInteger;

public class ProgressState {

    public final AtomicInteger people = new AtomicInteger(0);
    public final AtomicInteger titles = new AtomicInteger(0);
    public final AtomicInteger ratings = new AtomicInteger(0);
    public final AtomicInteger principals = new AtomicInteger(0);
}