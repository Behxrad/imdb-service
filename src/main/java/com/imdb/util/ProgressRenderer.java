package com.imdb.util;

public class ProgressRenderer implements Runnable {

    private final ProgressState state;
    private volatile boolean running = true;

    public ProgressRenderer(ProgressState state) {
        this.state = state;
    }

    @Override
    public void run() {

        while (running) {

            // move cursor to column 0 explicitly
            System.out.print("\r\033[2K");

            System.out.print(
                    String.format(
                            "People: %3d%% | Titles: %3d%% | Ratings: %3d%% | Principals: %3d%%",
                            state.people.get(),
                            state.titles.get(),
                            state.ratings.get(),
                            state.principals.get()
                    )
            );

            System.out.flush();

            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }

        System.out.println(); // clean exit
    }

    public void stop() {
        running = false;
    }
}