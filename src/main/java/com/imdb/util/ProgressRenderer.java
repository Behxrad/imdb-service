package com.imdb.util;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ProgressRenderer implements Runnable {

    private final ProgressState progress;
    private volatile boolean running = true;

    public ProgressRenderer(ProgressState state) {
        this.progress = state;
    }

    @Override
    public void run() {

        while (running) {

            // move cursor to column 0 explicitly
            System.out.print("\r\033[2K");

            StringBuilder builder = new StringBuilder();
            int count = 1;
            for (Map.Entry<String, AtomicInteger> entry : progress.state.entrySet()) {
                builder.append(entry.getKey());
                builder.append(": ");
                builder.append(entry.getValue().get());
                builder.append(count < progress.state.size() ? " | " : "");
                count++;
            }

            System.out.print(
                    builder
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