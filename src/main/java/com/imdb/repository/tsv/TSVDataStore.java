package com.imdb.repository.tsv;

import com.imdb.model.Person;
import com.imdb.model.Title;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static com.imdb.repository.tsv.TSVIndexer.*;


@Data
@Component
public class TSVDataStore {

    @Value("${imdb.dataset.path}")
    private String dataPath;

    @Value("${imdb.indices.path}")
    private String indicesPath;

    public Title readTitleBasics(long titleID) {
        Path dir = Paths.get(dataPath).toAbsolutePath();
        Path file = dir.resolve(TITLE_BASICS);

        try (FastIndexWriter titlesOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, "title_offset").toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            String rawOffset = titlesOffsetIndexer.readLine(titleID);
            if (rawOffset == null) return null;
            long offset = Long.parseLong(rawOffset);

            raf.seek(offset);
            String line = raf.readLine();

            String[] f = line.split("\t");
            if (f.length < 9) return null;

            return Title.builder()
                    .id(f[0])
                    .type(f[1])
                    .primaryTitle(f[2])
                    .originalTitle(f[3])
                    .isAdult(Integer.parseInt(f[4]) != 0)
                    .startYear(!f[5].equals("\\N") ? Integer.parseInt(f[5]) : null)
                    .endYear(!f[6].equals("\\N") ? Integer.parseInt(f[6]) : null)
                    .runtimeMinutes(!f[7].equals("\\N") ? Integer.parseInt(f[7]) : null)
                    .genres(new HashSet<>(Arrays.asList(f[8].split(","))))
                    .build();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Set<Title.Principal>> readTitlePrincipals(long titleID) {
        Path dir = Paths.get(dataPath).toAbsolutePath();
        Path file = dir.resolve(TITLE_PRINCIPALS);

        Map<String, Set<Title.Principal>> principalsGroupedByCategory = new HashMap<>();

        try (FastIndexWriter principalsOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_PRINCIPALS_OFFSET).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {

            String offsetsLine = principalsOffsetIndexer.readLine(titleID);
            if (offsetsLine == null) return new HashMap<>();

            String[] principals = offsetsLine.split(",");

            for (String principalOffset : principals) {
                raf.seek(Long.parseLong(principalOffset));
                String line = raf.readLine();

                String[] f = line.split("\t");
                if (f.length < 6) return null;

                principalsGroupedByCategory.computeIfAbsent(f[3], v -> new HashSet<>())
                        .add(Title.Principal.builder()
                                .ordering(Integer.parseInt(f[1]))
                                .category(f[3])
                                .job(!f[4].equals("\\N") ? f[4] : null)
                                .characters(!f[5].equals("\\N") ? new HashSet<>(Arrays.asList(f[5].split(","))) : null)
                                .person(readPerson(TSVIndexer.parseId(f[2])))
                                .build());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return principalsGroupedByCategory;
    }

    public Title.Rating readTitleRating(long titleID) {
        Path dir = Paths.get(dataPath).toAbsolutePath();
        Path file = dir.resolve(TITLE_RATINGS);

        try (FastIndexWriter ratingsOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_RATING_OFFSET).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            String rawOffset = ratingsOffsetIndexer.readLine(titleID);
            if (rawOffset == null) return null;
            long offset = Long.parseLong(rawOffset);

            raf.seek(offset);
            String line = raf.readLine();

            String[] f = line.split("\t");
            if (f.length < 3) return null;

            return Title.Rating.builder()
                    .averageRating(Float.parseFloat(f[1]))
                    .numberOfVotes(Integer.parseInt(f[2]))
                    .build();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Person readPerson(long personID) {
        Path dir = Paths.get(dataPath).toAbsolutePath();
        Path file = dir.resolve(NAME_BASICS);

        try (FastIndexWriter peopleOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_PEOPLE_OFFSET).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            String rawOffset = peopleOffsetIndexer.readLine(personID);
            if (rawOffset == null) return null;
            long offset = Long.parseLong(rawOffset);

            raf.seek(offset);
            String line = raf.readLine();

            String[] f = line.split("\t");
            if (f.length < 6) return null;

            return Person.builder()
                    .id(f[0])
                    .primaryName(f[1])
                    .birthYear(!f[2].equals("\\N") ? Integer.parseInt(f[2]) : null)
                    .deathYear(!f[3].equals("\\N") ? Integer.parseInt(f[3]) : null)
                    .primaryProfessions(new HashSet<>(Arrays.asList(f[4].split(","))))
                    .knownForTitles(!f[5].equals("\\N") ? Arrays.stream(f[5].split(","))
                            .map(x -> readTitleBasics(TSVIndexer.parseId(x))).collect(Collectors.toSet()) : new HashSet<>())
                    .build();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Boolean isPersonAlive(long personID) {
        Path dir = Paths.get(dataPath).toAbsolutePath();
        Path file = dir.resolve(NAME_BASICS);

        try (FastIndexWriter peopleOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_PEOPLE_OFFSET).toString());
             RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            String rawOffset = peopleOffsetIndexer.readLine(personID);
            if (rawOffset == null) return null;
            long offset = Long.parseLong(rawOffset);

            raf.seek(offset);
            String line = raf.readLine();

            String[] f = line.split("\t");
            if (f.length < 6) return null;

            return f[3].equals("\\N");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getTitlesCount() {
        try (FastIndexWriter titlesOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_TITLE_OFFSET).toString())) {
            return titlesOffsetIndexer.count();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getMaxTitleID() {
        try (FastIndexWriter titlesOffsetIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_TITLE_OFFSET).toString())) {
            return titlesOffsetIndexer.getMaxLineNumber();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Title> readTitlesByActorID(long actorId) {
        Set<Title> titles = new HashSet<>();

        try (FastIndexWriter titlesByActorIndexer = new FastIndexWriter(Paths.get(indicesPath, IDX_TITLES_BY_ACTOR).toString())) {
            String rawLine = titlesByActorIndexer.readLine(actorId);
            if (rawLine == null) return titles;

            for (String titleID : rawLine.split(",")) {
                Title title = readTitleBasics(TSVIndexer.parseId(titleID));
                title.setRating(readTitleRating(TSVIndexer.parseId(titleID)));
                titles.add(title);
            }

            return titles;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Long getPersonIDByTheirName(String actorName) {
        try (FastIndexWriter idx = new FastIndexWriter(Paths.get(indicesPath, IDX_PERSON_BY_NAME).toString())) {
            String line = idx.readLine(actorName.toLowerCase());
            if (line == null) return null;

            return Long.parseLong(line);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Title> readGenreTilesYearly(String genre, Integer year) {
        Set<Title> titles = new HashSet<>();

        try (FastIndexWriter titlesByGenreYearly = new FastIndexWriter(Paths.get(indicesPath, IDX_TITLES_BY_GENRE_YEAR).toString())) {
            String rawLine = titlesByGenreYearly.readLine(TSVIndexer.concatKeys(genre.toLowerCase(), year.toString()));
            if (rawLine == null) return titles;

            for (String titleID : rawLine.split(",")) {
                Title title = readTitleBasics(TSVIndexer.parseId(titleID));
                title.setRating(readTitleRating(TSVIndexer.parseId(titleID)));
                titles.add(title);
            }

            return titles;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}