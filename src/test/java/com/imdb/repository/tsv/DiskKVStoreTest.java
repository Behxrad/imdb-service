package com.imdb.repository.tsv;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DiskKVStoreTest {

    private static final String BASE_DIR = ".";
    private static final String FILE_NAME = "test_index";
    private static final String SEPARATOR = ",";

    private DiskKVStore store;

    @BeforeEach
    void setUp() throws IOException {
        cleanup();
        store = new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.BUILD, SEPARATOR);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (store != null) {
            store.close();
        }
        cleanup();
    }

    private void cleanup() {
        try {
            Files.deleteIfExists(Path.of(BASE_DIR, FILE_NAME + ".dat"));
            Files.deleteIfExists(Path.of(BASE_DIR, FILE_NAME + ".idx"));
        } catch (IOException ignored) {
        }
    }

    @Test
    @Order(1)
    void testWriteAndReadNumericLine() throws IOException {
        store.put(1L, "Drama,Short");
        store.put(42L, "Comedy,Romance");

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            assertEquals("Drama,Short", reader.get(1L));
            assertEquals("Comedy,Romance", reader.get(42L));
            assertNull(reader.get(100L));
        }
    }

    @Test
    @Order(2)
    void testWriteAndReadStringKey() throws IOException {
        store.put("tt0000123", "Animation,Short");
        store.put("tt9999999", "Documentary");

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            assertEquals("Animation,Short", reader.get("tt0000123"));
            assertEquals("Documentary", reader.get("tt9999999"));
        }
    }

    @Test
    @Order(3)
    void testAppendBehavior() throws IOException {
        String key = "tt1234567";

        store.put(key, "Drama,Short");
        store.put(key, "Short,Crime");
        store.put(key, "Drama");

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            String result = reader.get(key);

            assertEquals("Drama,Short,Short,Crime,Drama", result);
        }
    }

    @Test
    @Order(4)
    void testOverwriteMode() throws IOException {
        store = new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.BUILD, SEPARATOR, false);

        store.put("tt1", "A");
        store.put("tt1", "B");

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            assertEquals("B", reader.get("tt1"));
        }
    }

    @Test
    @Order(5)
    void testMixedNumericAndStringKeys() throws IOException {
        store.put(100L, "Action");
        store.put("tt555", "Sci-Fi,Thriller");
        store.put(100L, "Adventure");
        store.put("tt555", "Mystery");

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            assertEquals("Action,Adventure", reader.get(100L));
            assertEquals("Sci-Fi,Thriller,Mystery", reader.get("tt555"));
        }
    }

    @Test
    @Order(6)
    void testLargeLineNumber() throws IOException {
        long largeLine = 50_000_000L;
        store.put(largeLine, "Horror,Short");

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            assertEquals("Horror,Short", reader.get(largeLine));
        }
    }

    @Test
    @Order(7)
    void testHeaderValues() throws IOException {
        store.put(5L, "Test1");
        store.put("tt100", "Test2");
        store.put(1000L, "Test3");

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            assertEquals(3, reader.getRecordCount());
            assertTrue(reader.getMaxNumericKey() >= 1000);
            assertNotNull(reader.getLastKey());
        }
    }

    @Test
    void testInvalidData() {
        assertThrows(IllegalStateException.class, () -> store.put(10L, ""));
        assertThrows(IllegalStateException.class, () -> store.put(11L, null));
        assertThrows(IllegalStateException.class, () -> store.put("tt999", ""));
    }

    @Test
    void testInvalidKey() {
        assertThrows(IllegalStateException.class, () -> store.put(null, "Data"));
        assertThrows(IllegalStateException.class, () -> store.put("", "Data"));
    }

    @Test
    void testMultipleWritesToSameKey() throws IOException {
        String key = "tt7777777";

        for (int i = 0; i < 10; i++) {
            store.put(key, "Genre" + (i % 5));
        }

        store.close();

        try (DiskKVStore reader =
                     new DiskKVStore(BASE_DIR, FILE_NAME, DiskKVStore.Mode.READ, SEPARATOR)) {

            String result = reader.get(key);

            assertTrue(result.contains("Genre0"));
            assertTrue(result.contains("Genre1"));
            assertTrue(result.contains("Genre2"));
            assertTrue(result.contains("Genre3"));
            assertTrue(result.contains("Genre4"));
        }
    }
}