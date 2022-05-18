package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.*;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {
    private final long MAX_SIZE = 100000;
    private final String name;
    private final Path path;
    private SegmentIndex index;
    private long size;

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        if (segmentName == null) {
            throw new DatabaseException("Segment should have a name");
        }
        if (tableRootPath == null) {
            throw new DatabaseException("Segment should have a path");
        }

        File file = new File(String.valueOf(tableRootPath.resolve(segmentName)));
        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new DatabaseException("Problem with directory");
                }
            } catch (IOException e) {
                throw new DatabaseException("Problem with directory");
            }
        }

        return new SegmentImpl(segmentName, tableRootPath);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(
                context.getSegmentName(),
                context.getSegmentPath().getParent(),
                context.getIndex(),
                context.getCurrentSize());
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private SegmentImpl(String name, Path tableRootPath) {
        this.name = name;
        path = tableRootPath.resolve(name);
        index = new SegmentIndex();
        size = 0;
    }

    private SegmentImpl(String name, Path tableRootPath, SegmentIndex index, long size) {
        this.name = name;
        path = tableRootPath.resolve(name);
        this.index = index;
        this.size = size;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (objectKey == null) {
            throw new IOException("Key could not be a null");
        }

        if (isReadOnly()) {
            return false;
        }

        WritableDatabaseRecord record = new SetDatabaseRecord(objectKey.getBytes(), objectValue);
        int offset = streamWrite(record);

        index.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
        size += offset;

        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        if (objectKey == null) {
            throw new IOException("Key could not be a null");
        }

        try (InputStream stream = new FileInputStream(path.toFile())) {
            DatabaseInputStream databaseStream = new DatabaseInputStream(stream);

            Optional<SegmentOffsetInfo> readSegmentOffsetInfo = index.searchForKey(objectKey);
            if (readSegmentOffsetInfo.isEmpty()) {
                return Optional.empty();
            }

            if (databaseStream.skip(readSegmentOffsetInfo.get().getOffset()) !=
                    readSegmentOffsetInfo.get().getOffset()) {
                throw new IOException("IOException while reading");
            }

            Optional<DatabaseRecord> record = databaseStream.readDbUnit();
            if (record.isPresent()) {
                if (record.get().isValuePresented())
                    return Optional.ofNullable(record.get().getValue());
            }
            return Optional.empty();
        }
    }

    @Override
    public boolean isReadOnly() {
        return size >= MAX_SIZE;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        if (objectKey == null) {
            throw new IOException("Key could not be a null");
        }

        if (isReadOnly()) {
            return false;
        }

        WritableDatabaseRecord record = new RemoveDatabaseRecord(objectKey.getBytes());
        int offset = streamWrite(record);

        index.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(size));
        size += offset;

        return true;
    }

    private int streamWrite(WritableDatabaseRecord record) throws IOException {
        try (OutputStream stream = new FileOutputStream(path.toFile(), true)) {
            DatabaseOutputStream databaseStream = new DatabaseOutputStream(stream);
            return databaseStream.write(record);
        }
    }
}
