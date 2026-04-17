package com.imdb.repository.tsv;

import com.imdb.util.ProgressRenderer;
import com.imdb.util.ProgressState;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class TSVIndexer implements CommandLineRunner {

    static final String TITLE_BASICS = "title.basics.tsv";
    static final String TITLE_RATINGS = "title.ratings.tsv";
    static final String NAME_BASICS = "name.basics.tsv";
    static final String TITLE_PRINCIPALS = "title.principals.tsv";

    static final String IDX_TITLE_OFFSET = "title_offset";
    static final String IDX_RATING_OFFSET = "rating_offset";
    static final String IDX_PEOPLE_OFFSET = "people_offset";
    static final String IDX_PERSON_BY_NAME = "person_id_by_their_name";
    static final String IDX_PRINCIPALS_OFFSET = "principals_offset";
    static final String IDX_TITLES_BY_ACTOR = "titles_by_actor_id";
    static final String IDX_TITLES_BY_GENRE_YEAR = "titles_by_genre_year";

    @Value("${imdb.dataset.path}")
    private String datasetPath;

    @Value("${imdb.indices.path}")
    private String indicesPath;

    @Override
    public void run(String... args) throws Exception {
        validatePaths();

        if (indexesExist()) {
            System.out.print("Indexes already exist. Recreate? (Y/N): ");
            Scanner scanner = new Scanner(System.in);
            String input = scanner.nextLine();
            if (!input.equalsIgnoreCase("Y")) {
                System.out.println("Skipping indexing");
                return;
            }
            deleteIndexes();
        }

        long start = System.nanoTime();

        System.out.println("=== IMDB TSV indexing/loading started ===");

        ProgressState state = new ProgressState();
        ProgressRenderer renderer = new ProgressRenderer(state);
        Thread progressThread = new Thread(renderer);
        progressThread.start();

        try (ExecutorService executor = Executors.newFixedThreadPool(4)) {

            executor.submit(() -> run(this::loadPeopleOffsetAndIndexing, state));
            executor.submit(() -> run(this::loadTitlesOffsetAndIndexing, state));
            executor.submit(() -> run(this::loadRatingsOffsetAndIndexing, state));
            executor.submit(() -> run(this::loadPrincipalsOffsetAndIndexing, state));

            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.MINUTES);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        renderer.stop();
        try {
            progressThread.join();
        } catch (InterruptedException ignored) {
        }

        long end = System.nanoTime();
        System.out.printf("=== Completed in %.2f seconds ===%n", (end - start) / 1_000_000_000.0);
    }

    private void validatePaths() {
        Path dataset = Paths.get(datasetPath);
        Path indices = Paths.get(indicesPath);

        if (!Files.exists(dataset)) {
            System.out.println("WARNING: dataset path does not exist: " + dataset);
        }

        if (!Files.exists(indices)) {
            System.out.println("WARNING: indices path does not exist: " + indices);
        }

        try {
            if (Files.isDirectory(dataset)) {
                Files.list(dataset).forEach(p -> {
                    String name = p.getFileName().toString();
                    if (name.endsWith(".tsv")) {
                        System.out.println("DATASET FILE FOUND: " + name);
                    }
                });
            }
        } catch (Exception ignored) {
        }

        try {
            if (Files.isDirectory(indices)) {
                Files.list(indices).forEach(p -> {
                    System.out.println("INDEX FILE FOUND: " + p.getFileName());
                });
            }
        } catch (Exception ignored) {
        }
    }

    private void run(ThrowingConsumer<ProgressState> task, ProgressState state) {
        try {
            task.accept(state);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean indexesExist() {
        Path dir = Paths.get(indicesPath);
        return Files.exists(dir.resolve(Strings.concat(IDX_TITLE_OFFSET, ".idx")));
    }

    private void deleteIndexes() {
        try {
            Files.walk(Paths.get(indicesPath))
                    .filter(Files::isRegularFile)
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadTitlesOffsetAndIndexing(ProgressState state) throws IOException {
        Path file = Paths.get(datasetPath, TITLE_BASICS);
        long totalSize = Files.size(file);

        try (FastIndexWriter titlesOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_TITLE_OFFSET).toString());
             FastIndexWriter genreYearIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_TITLES_BY_GENRE_YEAR).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();
            int last = 0;

            while ((line = raf.readLine()) != null) {
                String[] f = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8).split("\t");

                long id = parseId(f[0]);
                String year = f[5];

                titlesOffsetIndexer.writeToLine(id, String.valueOf(offset), ",");

                for (String genre : f[8].split(",")) {
                    genreYearIndexer.writeToLine(concatKeys(genre.toLowerCase(), year), Long.toString(id), ",");
                }

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                if (progress - last >= 1) {
                    state.titles.set(progress);
                    last = progress;
                }
            }
        }
    }

    private void loadRatingsOffsetAndIndexing(ProgressState state) throws IOException {
        Path file = Paths.get(datasetPath, TITLE_RATINGS);
        long totalSize = Files.size(file);

        try (FastIndexWriter indexer = new FastIndexWriter(Paths.get(indicesPath, IDX_RATING_OFFSET).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();
            int last = 0;

            while ((line = raf.readLine()) != null) {
                String[] f = line.split("\t");
                indexer.writeToLine(parseId(f[0]), String.valueOf(offset), ",");

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                if (progress - last >= 1) {
                    state.ratings.set(progress);
                    last = progress;
                }
            }
        }
    }

    private void loadPeopleOffsetAndIndexing(ProgressState state) throws IOException {
        Path file = Paths.get(datasetPath, NAME_BASICS);
        long totalSize = Files.size(file);

        try (FastIndexWriter offsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_PEOPLE_OFFSET).toString());
             FastIndexWriter nameIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_PERSON_BY_NAME).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();
            int last = 0;

            while ((line = raf.readLine()) != null) {
                String[] f = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8).split("\t");

                long id = parseId(f[0]);
                offsetIndexer.writeToLine(id, String.valueOf(offset), ",");
                nameIndexer.writeToLine(f[1].toLowerCase(), Long.toString(id), ",");

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                if (progress - last >= 1) {
                    state.people.set(progress);
                    last = progress;
                }
            }
        }
    }

    private void loadPrincipalsOffsetAndIndexing(ProgressState state) throws IOException {
        Path file = Paths.get(datasetPath, TITLE_PRINCIPALS);
        long totalSize = Files.size(file);

        try (FastIndexWriter offsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_PRINCIPALS_OFFSET).toString());
             FastIndexWriter actorIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_TITLES_BY_ACTOR).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();
            int last = 0;

            while ((line = raf.readLine()) != null) {
                String[] f = line.split("\t");

                long titleId = parseId(f[0]);
                long personId = parseId(f[2]);
                String category = f[3];

                offsetIndexer.writeToLine(titleId, String.valueOf(offset), ",");

                if (category.equals("actor") || category.equals("actress")) {
                    actorIndexer.writeToLine(personId, Long.toString(titleId), ",");
                }

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                if (progress - last >= 1) {
                    state.principals.set(progress);
                    last = progress;
                }
            }
        }
    }

    static long parseId(String raw) {
        return Long.parseLong(raw.replace("tt", "").replace("nm", ""));
    }

    static String concatKeys(String k1, String k2) {
        return k1 + ":" + k2;
    }

    @FunctionalInterface
    interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }
}