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
import java.util.Set;
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
    static final String IDX_PRINCIPALS_OFFSET = "principals_offset";
    static final String IDX_PERSON_BY_NAME = "person_id_by_their_name";
    static final String IDX_TITLES_BY_PRINCIPAL_ID = "titles_by_principal_id";
    static final String IDX_TITLES_BY_GENRE_YEAR = "titles_by_genre_year";

    @Value("${imdb.dataset.path}")
    private String datasetPath;

    @Value("${imdb.indices.path}")
    private String indicesPath;

    private ProgressState state;

    @Override
    public void run(String... args) {
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

        state = new ProgressState(Set.of(TITLE_BASICS, TITLE_RATINGS, NAME_BASICS, TITLE_PRINCIPALS));
        ProgressRenderer renderer = new ProgressRenderer(state);
        Thread progressThread = new Thread(renderer);
        progressThread.start();

        try (ExecutorService executor = Executors.newFixedThreadPool(4)) {

            executor.submit(ThrowingRunnable.wrap(this::loadPeopleOffsetAndIndexing));
            executor.submit(ThrowingRunnable.wrap(this::loadTitlesOffsetAndIndexing));
            executor.submit(ThrowingRunnable.wrap(this::loadRatingsOffsetAndIndexing));
            executor.submit(ThrowingRunnable.wrap(this::loadPrincipalsOffsetAndIndexing));

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

    private void loadTitlesOffsetAndIndexing() throws IOException {
        Path file = Paths.get(datasetPath, TITLE_BASICS);
        long totalSize = Files.size(file);

        try (DiskKVStore titlesOffsetIndexer = new DiskKVStore(indicesPath, IDX_TITLE_OFFSET, DiskKVStore.Mode.BUILD, ",", state);
             DiskKVStore genreYearIndexer = new DiskKVStore(indicesPath, IDX_TITLES_BY_GENRE_YEAR, DiskKVStore.Mode.BUILD, ",", state);
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();

            while ((line = raf.readLine()) != null) {
                String[] f = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8).split("\t");

                long id = parseId(f[0]);
                String year = f[5];

                titlesOffsetIndexer.put(id, String.valueOf(offset));

                for (String genre : f[8].split(",")) {
                    genreYearIndexer.put(concatKeys(genre.toLowerCase(), year), Long.toString(id));
                }

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                state.setProgress(TITLE_BASICS, progress);
            }
        }
    }

    private void loadRatingsOffsetAndIndexing() throws IOException {
        Path file = Paths.get(datasetPath, TITLE_RATINGS);
        long totalSize = Files.size(file);

        try (DiskKVStore indexer = new DiskKVStore(indicesPath, IDX_RATING_OFFSET, DiskKVStore.Mode.BUILD, ",", state);
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();

            while ((line = raf.readLine()) != null) {
                String[] f = line.split("\t");
                indexer.put(parseId(f[0]), String.valueOf(offset));

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                state.setProgress(TITLE_RATINGS, progress);
            }
        }
    }

    private void loadPeopleOffsetAndIndexing() throws IOException {
        Path file = Paths.get(datasetPath, NAME_BASICS);
        long totalSize = Files.size(file);

        try (DiskKVStore offsetIndexer = new DiskKVStore(indicesPath, IDX_PEOPLE_OFFSET, DiskKVStore.Mode.BUILD, ",", state);
             DiskKVStore nameIndexer = new DiskKVStore(indicesPath, IDX_PERSON_BY_NAME, DiskKVStore.Mode.BUILD, ",", false, state);
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();

            while ((line = raf.readLine()) != null) {
                String[] f = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8).split("\t");

                long id = parseId(f[0]);
                offsetIndexer.put(id, String.valueOf(offset));
                nameIndexer.put(f[1].toLowerCase(), Long.toString(id));

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                state.setProgress(NAME_BASICS, progress);
            }
        }
    }

    private void loadPrincipalsOffsetAndIndexing() throws IOException {
        Path file = Paths.get(datasetPath, TITLE_PRINCIPALS);
        long totalSize = Files.size(file);

        try (DiskKVStore offsetIndexer = new DiskKVStore(indicesPath, IDX_PRINCIPALS_OFFSET, DiskKVStore.Mode.BUILD, ",", state);
             DiskKVStore principalIndexer = new DiskKVStore(indicesPath, IDX_TITLES_BY_PRINCIPAL_ID, DiskKVStore.Mode.BUILD, ",", state);
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            raf.readLine();
            String line;
            long offset = raf.getFilePointer();

            while ((line = raf.readLine()) != null) {
                String[] f = line.split("\t");

                long titleId = parseId(f[0]);
                long personId = parseId(f[2]);

                offsetIndexer.put(titleId, String.valueOf(offset));
                principalIndexer.put(personId, Long.toString(titleId));

                offset = raf.getFilePointer();
                int progress = (int) ((offset * 100) / totalSize);

                state.setProgress(TITLE_PRINCIPALS, progress);
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
    public interface ThrowingRunnable {

        void run() throws Exception;

        static Runnable wrap(ThrowingRunnable r) {
            return () -> {
                try {
                    r.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }
}