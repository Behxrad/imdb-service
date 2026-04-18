package com.imdb.repository.tsv;

import lombok.Getter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DiskKVStore implements AutoCloseable {

    private static final int MAX_KEY_BYTES = 64;
    private static final int RECORD_SIZE = MAX_KEY_BYTES + 8 + 4;
    private static final int CHUNK_SIZE = 1_000_000;
    private static final int HEADER_SIZE = 256;

    public enum Mode {BUILD, READ}

    private final boolean isBuildMode;
    private final String baseName;
    private final String separator;
    private final boolean appendMode;

    private boolean built = false;

    private RandomAccessFile indexRaf;
    private RandomAccessFile dataRaf;
    private long numRecords;

    @Getter
    private long recordCount;
    @Getter
    private long maxNumericKey;
    @Getter
    private String lastKey;

    private RandomAccessFile dataFile;
    private final List<File> runFiles = new ArrayList<>();
    private final Map<String, StringBuilder> currentChunk = new LinkedHashMap<>();
    private long currentDataOffset = HEADER_SIZE;

    public DiskKVStore(String baseName, Mode mode, String separator) throws IOException {
        this(baseName, mode, separator, true);
    }

    public DiskKVStore(String baseName, Mode mode, String separator, boolean appendMode) throws IOException {
        this.baseName = baseName;
        this.isBuildMode = (mode == Mode.BUILD);
        this.separator = separator;
        this.appendMode = appendMode;

        String dataPath = baseName + ".dat";
        String indexPath = baseName + ".idx";

        if (isBuildMode) {
            this.dataFile = new RandomAccessFile(dataPath, "rw");
            this.dataFile.setLength(0);
            this.dataFile.seek(HEADER_SIZE);
        } else {
            this.indexRaf = new RandomAccessFile(indexPath, "r");
            this.dataRaf = new RandomAccessFile(dataPath, "r");
            this.numRecords = indexRaf.length() / RECORD_SIZE;
            readHeader();
        }
    }

    // ================= PUT =================

    public void put(String key, String value) throws IOException {
        if (!isBuildMode) throw new IllegalStateException("Not in BUILD mode");
        if (key == null || value == null || key.isEmpty() || value.isEmpty())
            throw new IllegalStateException("Invalid key/value");

        if (!appendMode) {
            currentChunk.put(key, new StringBuilder(value));
            return;
        }

        currentChunk.merge(key, new StringBuilder(value), (oldVal, newVal) -> {
            if (!oldVal.isEmpty()) oldVal.append(separator);
            return oldVal.append(newVal);
        });

        if (currentChunk.size() >= CHUNK_SIZE) {
            flushChunk();
        }
    }

    public void put(long key, String value) throws IOException {
        put(String.format("%020d", key), value);
    }

    // ================= BUILD =================

    public void build() throws IOException {
        if (!isBuildMode) throw new IllegalStateException("Not in BUILD mode");
        if (built) return;

        flushChunk();

        String indexPath = baseName + ".idx";

        if (runFiles.isEmpty()) {
            try (RandomAccessFile idx = new RandomAccessFile(indexPath, "rw")) {
                idx.setLength(0);
            }
        } else {
            mergeRuns(indexPath);
        }

        built = true;
    }

    private void flushChunk() throws IOException {
        if (currentChunk.isEmpty()) return;

        List<String> keys = new ArrayList<>(currentChunk.keySet());
        keys.sort(null);

        File runFile = File.createTempFile("kv-run-", ".tmp");
        runFiles.add(runFile);

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(runFile)))) {

            for (String key : keys) {
                byte[] valBytes = currentChunk.get(key).toString().getBytes(StandardCharsets.UTF_8);

                long offset = currentDataOffset;

                dataFile.seek(offset);
                dataFile.write(valBytes);

                currentDataOffset += valBytes.length;

                out.writeUTF(key);
                out.writeLong(offset);
                out.writeInt(valBytes.length);
            }
        }

        currentChunk.clear();
    }

    private void mergeRuns(String indexPath) throws IOException {

        List<DataInputStream> streams = new ArrayList<>();
        PriorityQueue<MergedRecord> pq =
                new PriorityQueue<>(Comparator.comparing(m -> m.key));

        for (File f : runFiles) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(f)));
            streams.add(in);
            try {
                pq.offer(readRecord(in, streams.size() - 1));
            } catch (EOFException ignored) {
            }
        }

        long recordCount = 0;
        long maxNumericKey = -1;
        String lastKey = null;

        try (RandomAccessFile index = new RandomAccessFile(indexPath, "rw")) {
            index.setLength(0);

            while (!pq.isEmpty()) {

                MergedRecord first = pq.poll();
                String key = first.key;

                List<MergedRecord> group = new ArrayList<>();
                group.add(first);

                while (!pq.isEmpty() && pq.peek().key.equals(key)) {
                    group.add(pq.poll());
                }

                StringBuilder merged = new StringBuilder();

                for (MergedRecord rec : group) {
                    dataFile.seek(rec.offset);
                    byte[] val = new byte[rec.valLen];
                    dataFile.readFully(val);

                    if (merged.length() > 0) merged.append(separator);
                    merged.append(new String(val, StandardCharsets.UTF_8));
                }

                byte[] finalBytes = merged.toString().getBytes(StandardCharsets.UTF_8);
                long newOffset = currentDataOffset;

                dataFile.seek(newOffset);
                dataFile.write(finalBytes);
                currentDataOffset += finalBytes.length;

                index.write(padKey(key));
                index.writeLong(newOffset);
                index.writeInt(finalBytes.length);

                recordCount++;
                lastKey = key;

                if (isNumeric(key)) {
                    long k = Long.parseLong(key);
                    if (k > maxNumericKey) maxNumericKey = k;
                }

                for (MergedRecord rec : group) {
                    DataInputStream in = streams.get(rec.streamIndex);
                    try {
                        pq.offer(readRecord(in, rec.streamIndex));
                    } catch (EOFException ignored) {}
                }
            }
        }

        writeHeader(recordCount, maxNumericKey, lastKey);
        cleanup(streams);
    }

    // ================= HEADER =================

    private void writeHeader(long count, long maxKey, String lastKey) throws IOException {
        dataFile.seek(0);

        dataFile.writeLong(count);
        dataFile.writeLong(maxKey);

        byte[] padded = padKey(lastKey == null ? "" : lastKey);
        dataFile.write(padded);
    }

    private void readHeader() throws IOException {
        dataRaf.seek(0);

        recordCount = dataRaf.readLong();
        maxNumericKey = dataRaf.readLong();

        byte[] buf = new byte[MAX_KEY_BYTES];
        dataRaf.readFully(buf);

        lastKey = unpadKey(buf);
    }

    // ================= GET =================

    public String get(String key) throws IOException {
        if (isBuildMode) throw new IllegalStateException("In BUILD mode");

        long low = 0, high = numRecords - 1;

        while (low <= high) {
            long mid = (low + high) >>> 1;

            indexRaf.seek(mid * RECORD_SIZE);

            byte[] buf = new byte[MAX_KEY_BYTES];
            indexRaf.readFully(buf);

            String readKey = unpadKey(buf);
            int cmp = key.compareTo(readKey);

            if (cmp == 0) {
                long offset = indexRaf.readLong();
                int len = indexRaf.readInt();

                dataRaf.seek(offset);
                byte[] val = new byte[len];
                dataRaf.readFully(val);

                return new String(val, StandardCharsets.UTF_8);
            } else if (cmp < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return null;
    }

    public String get(long key) throws IOException {
        return get(String.format("%020d", key));
    }

    // ================= HELPERS =================

    private MergedRecord readRecord(DataInputStream in, int idx) throws IOException {
        return new MergedRecord(in.readUTF(), in.readLong(), in.readInt(), idx);
    }

    private void cleanup(List<DataInputStream> streams) throws IOException {
        for (DataInputStream s : streams) s.close();
        for (File f : runFiles) f.delete();
    }

    private boolean isNumeric(String s) {
        return s.length() == 20 && s.chars().allMatch(Character::isDigit);
    }

    private byte[] padKey(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] padded = new byte[MAX_KEY_BYTES];
        System.arraycopy(bytes, 0, padded, 0, Math.min(bytes.length, MAX_KEY_BYTES));
        return padded;
    }

    private String unpadKey(byte[] padded) {
        int len = 0;
        while (len < MAX_KEY_BYTES && padded[len] != 0) len++;
        return new String(padded, 0, len, StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        if (isBuildMode) {
            if (!built) build();
            if (dataFile != null) dataFile.close();
        } else {
            if (indexRaf != null) indexRaf.close();
            if (dataRaf != null) dataRaf.close();
        }
    }

    private record MergedRecord(String key, long offset, int valLen, int streamIndex) {}
}