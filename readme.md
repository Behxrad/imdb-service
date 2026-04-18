# IMDB TSV Service

This project provides a fast REST API for querying the IMDb dataset using TSV files with pre-built indexes.

## Features

- Retrieve titles where director and writer are the same and still alive.
- Find titles shared by two actors.
- Get best titles by year for a specific genre based on ratings and votes.
- Request statistics per API and total.
- Lightweight caching layer in repository for repeated queries.
- Efficient TSV reading using pre-built indexes.

## Requirements

- Java 21+
- Spring Boot 4.x
- IMDb TSV dataset files:
    - `title.basics.tsv`
    - `title.ratings.tsv`
    - `name.basics.tsv`
    - `title.principals.tsv`

## Configuration

By default, the project reads the dataset and indices folders located next to the jar file:

- `imdb.dataset.path=Dataset` - Path to the folder containing IMDb TSV files.
- `imdb.indices.path=Indices` - Path to store the generated index files.

Alternatively, you can override these paths using environment variables:

- `IMDB_DATASET_PATH=/path/to/dataset`
- `IMDB_INDICES_PATH=/path/to/indices`


## How it Works

1. **Indexing**  
   On startup, the application checks if index files exist under `imdb.indices.path`.  
   If they exist, it asks whether to recreate them.  
   Indexing builds fast-access files for:
    - Title offsets
    - Title by genre/year
    - Title principals
    - People offsets
    - Person ID by name
    - Titles by actor ID
    - Ratings offsets

2. **Data Retrieval**
    - Titles and people are read using offsets from TSV files.
    - All string keys in indexes (like person names or genres) are stored in lowercase for consistent lookup.
    - Ratings and principal info are fetched and mapped to DTOs for API responses.

3. **API Endpoints**

   | Endpoint                    | Method | Parameters         | Description                                                          |
   |-----------------------------|--------|--------------------|----------------------------------------------------------------------|
   | `/api/same-director-writer` | GET    | `page`, `size`     | Returns titles where the director and writer are the same and alive. |
   | `/api/shared`               | GET    | `actor1`, `actor2` | Returns titles in which both actors participated.                    |
   | `/api/genre-yearly-ranking` | GET    | `genre`            | Returns best titles per year for a genre based on rating and votes.  |
   | `/api/stats`                | GET    | None               | Returns total and per-API request counts.                            |

4. **Caching**
    - Repository methods cache results based on input parameters.
    - Expired entries or least recently used entries are removed when the quota is reached.

5. **Request Counting**
    - Tracks total API calls and per-endpoint call counts.
    - `/api/stats` endpoint shows the counts.

## Build and Run

## Build

`./mvnw clean package`

## Run with default dataset and indices folders

`java -jar target/imdb_services-0.0.1-SNAPSHOT.jar`

## Run with custom dataset and indices paths using environment variables

`export IMDB_DATASET_PATH=/path/to/dataset`

`export IMDB_INDICES_PATH=/path/to/indices`

`java -jar target/imdb_services-0.0.1-SNAPSHOT.jar`

## Notes

- Indexing might take a few ours but after building them they accelerate querying.