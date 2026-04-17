package com.imdb.repository.tsv;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class FastIndexWriter implements AutoCloseable {

    private final RandomAccessFile dataFile;   // .idx
    private final RandomAccessFile indexFile;  // .dat

    private static final int OFFSET_SIZE = Long.BYTES;

    private static final int COUNT_POS = 0;
    private static final int MAX_LINE_POS = 8;
    private static final int HEADER_SIZE = 16;

    public FastIndexWriter(String baseFileName) throws IOException {
        this.dataFile = new RandomAccessFile(baseFileName + ".idx", "rw");
        this.indexFile = new RandomAccessFile(baseFileName + ".dat", "rw");

        // Initialize header if new
        if (dataFile.length() == 0) {
            dataFile.seek(COUNT_POS);
            dataFile.writeLong(0L); // count

            dataFile.seek(MAX_LINE_POS);
            dataFile.writeLong(0L); // maxLineNumber
        }
    }

    public void writeToLine(long lineNumber, String data, String separator) throws IOException {
        if (data == null || data.isEmpty() || separator == null || separator.isEmpty()) {
            return;
        }

        if (lineNumber < 1) {
            throw new IllegalArgumentException("lineNumber must be >= 1");
        }

        String existing = readLine(lineNumber);

        Set<String> values = new LinkedHashSet<>();

        if (data != null && !data.isBlank()) {
            values.addAll(Arrays.asList(data.split(separator)));
        }

        if (existing != null && !existing.isBlank()) {
            values.addAll(Arrays.asList(existing.split(separator)));
        }

        String newContent = String.join(separator, values) + "\n";
        byte[] bytes = newContent.getBytes(StandardCharsets.UTF_8);

        long offset = dataFile.length();
        dataFile.seek(offset);
        dataFile.write(bytes);

        long indexPosition = (lineNumber - 1) * OFFSET_SIZE;

        boolean wasEmpty = true;

        if (indexPosition + OFFSET_SIZE <= indexFile.length()) {
            indexFile.seek(indexPosition);
            long existingOffset = indexFile.readLong();
            wasEmpty = !(existingOffset >= HEADER_SIZE && existingOffset < dataFile.length());
        }

//        ensureIndexSize(indexPosition);

        indexFile.seek(indexPosition);
        indexFile.writeLong(offset);

        // update count
        if (wasEmpty) {
            setCount(getCount() + 1);
        }

        // update maxLineNumber
        long currentMax = getMaxLineNumber();
        if (lineNumber > currentMax) {
            setMaxLineNumber(lineNumber);
        }
    }

    public String readLine(long lineNumber) throws IOException {

        if (lineNumber < 1) {
            throw new IllegalArgumentException("lineNumber must be >= 1");
        }

        long indexPosition = (lineNumber - 1) * OFFSET_SIZE;

        if (indexPosition + OFFSET_SIZE > indexFile.length()) {
            return null;
        }

        indexFile.seek(indexPosition);
        long offset = indexFile.readLong();

        if (offset < HEADER_SIZE || offset >= dataFile.length()) {
            return null;
        }

        dataFile.seek(offset);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        int b;
        while ((b = dataFile.read()) != -1) {
            if (b == '\n') break;
            baos.write(b);
        }

        return baos.toString(StandardCharsets.UTF_8);
    }

    public void writeToLine(String key, String data, String separator) throws IOException {
        long lineNumber = getLineNumberForKey(key);
        writeToLine(lineNumber, data, separator);
    }

    public String readLine(String key) throws IOException {
        long lineNumber = getLineNumberForKey(key);
        return readLine(lineNumber);
    }

    public long count() throws IOException {
        return getCount();
    }

    public long getMaxLineNumber() throws IOException {
        dataFile.seek(MAX_LINE_POS);
        return dataFile.readLong();
    }

    private void setMaxLineNumber(long max) throws IOException {
        dataFile.seek(MAX_LINE_POS);
        dataFile.writeLong(max);
    }

    private long getCount() throws IOException {
        dataFile.seek(COUNT_POS);
        return dataFile.readLong();
    }

    private void setCount(long count) throws IOException {
        dataFile.seek(COUNT_POS);
        dataFile.writeLong(count);
    }

    private long getLineNumberForKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        String trimmed = key.trim();

        // Fast path: if it's a pure number
        try {
            return Long.parseLong(trimmed);
        } catch (NumberFormatException ignored) {
        }

        // For string keys (like "tt1234567"), use high-quality 64-bit hash
        long hash = murmurHash64(trimmed);
        return (hash & 0x7FFFFFFFFFFFFFFFL) % 10_000_000_000L + 1; // Support up to 10 billion keys
    }

    private static long murmurHash64(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        long h = 0xC6A4A7935BD1E995L;
        for (byte b : bytes) {
            h ^= b;
            h *= 0x5BD1E995L;
            h ^= h >>> 47;
        }
        return h;
    }

    @Override
    public void close() throws IOException {
        indexFile.close();
        dataFile.close();
    }
}