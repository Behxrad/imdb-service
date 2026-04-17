package com.imdb.repository.tsv;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FastIndexWriterTest {

    private static final String TEST_BASE_NAME = "test_index";
    private FastIndexWriter writer;

    @BeforeEach
    void setUp() throws IOException {
        cleanup();
        writer = new FastIndexWriter(TEST_BASE_NAME);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (writer != null) {
            writer.close();
        }
        cleanup();
    }

    private void cleanup() {
        try {
            Files.deleteIfExists(Path.of(TEST_BASE_NAME + ".dat"));
            Files.deleteIfExists(Path.of(TEST_BASE_NAME + ".idx"));
        } catch (IOException ignored) {
        }
    }

    @Test
    @Order(1)
    void testWriteAndReadNumericLine() throws IOException {
        writer.writeToLine(1L, "Drama,Short", ",");
        writer.writeToLine(42L, "Comedy,Romance", ",");

        assertEquals("Drama,Short", writer.readLine(1L));
        assertEquals("Comedy,Romance", writer.readLine(42L));
        assertNull(writer.readLine(100L)); // non-existent
    }

    @Test
    @Order(2)
    void testWriteAndReadStringKey() throws IOException {
        writer.writeToLine("tt0000123", "Animation,Short", ",");
        writer.writeToLine("tt9999999", "Documentary", ",");

        assertEquals("Animation,Short", writer.readLine("tt0000123"));
        assertEquals("Documentary", writer.readLine("tt9999999"));
    }

    @Test
    @Order(3)
    void testAppendWithSeparatorAndDeduplication() throws IOException {
        String key = "tt1234567";

        writer.writeToLine(key, "Drama,Short", ",");
        writer.writeToLine(key, "Short,Crime", ",");           // append
        writer.writeToLine(key, "Drama", ",");                 // duplicate should be ignored

        String result = writer.readLine(key);
        assertEquals("Drama,Short,Crime", result);             // order preserved, no duplicates
    }

    @Test
    @Order(4)
    void testMixedNumericAndStringKeys() throws IOException {
        writer.writeToLine(100L, "Action", ",");
        writer.writeToLine("tt555", "Sci-Fi,Thriller", ",");
        writer.writeToLine(100L, "Adventure", ",");           // append to numeric
        writer.writeToLine("tt555", "Mystery", ",");          // append to string

        assertEquals("Adventure,Action", writer.readLine(100L));
        assertEquals("Mystery,Sci-Fi,Thriller", writer.readLine("tt555"));
    }

    @Test
    @Order(5)
    void testLargeLineNumber() throws IOException {
        long largeLine = 50_000_000L;
        writer.writeToLine(largeLine, "Horror,Short", ",");

        assertEquals("Horror,Short", writer.readLine(largeLine));
    }

    @Test
    @Order(6)
    void testCountAndMaxLineNumber() throws IOException {
        writer.writeToLine(5L, "Test1", ",");
        writer.writeToLine("tt100", "Test2", ",");
        writer.writeToLine(1000L, "Test3", ",");

        assertEquals(3, writer.count());           // rough count
        assertTrue(writer.getMaxLineNumber() >= 1000);
    }

    @Test
    void testEmptyAndNullData() throws IOException {
        writer.writeToLine(10L, "", ",");
        writer.writeToLine(11L, null, ",");
        writer.writeToLine("tt999", "", ",");

        assertNull(writer.readLine(10L));
        assertNull(writer.readLine(11L));
        assertNull(writer.readLine("tt999"));
    }

    @Test
    void testExceptionOnInvalidKey() {
        assertThrows(IllegalArgumentException.class, () ->
                writer.writeToLine((String) null, "Data", ","));

        assertThrows(IllegalArgumentException.class, () ->
                writer.writeToLine("", "Data", ","));
    }

    @Test
    void testMultipleWritesToSameLine() throws IOException {
        String key = "tt7777777";

        for (int i = 0; i < 10; i++) {
            writer.writeToLine(key, "Genre" + (i % 5), ",");
        }

        String result = writer.readLine(key);
        assertTrue(result.contains("Genre0"));
        assertTrue(result.contains("Genre1"));
        assertTrue(result.contains("Genre2"));
        assertTrue(result.contains("Genre3"));
        assertTrue(result.contains("Genre4"));
    }
}