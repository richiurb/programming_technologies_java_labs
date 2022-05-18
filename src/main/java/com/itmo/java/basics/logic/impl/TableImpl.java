package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String name;
    private final Path path;
    private TableIndex index;
    private Segment actualSegment;

    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table should have a name");
        }
        if (pathToDatabaseRoot == null) {
            throw new DatabaseException("Table should have a root");
        }

        File directory = new File(String.valueOf(pathToDatabaseRoot.resolve(tableName)));
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new DatabaseException("Problem with directory");
            }
        }

        if (tableIndex == null) {
            throw new DatabaseException("Problem with table index");
        }

        Table table = new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
        return new CachingTable(table);
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        name = tableName;
        path = pathToDatabaseRoot.resolve(tableName);
        index = tableIndex;
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex, Segment actualSegment) {
        name = tableName;
        path = pathToDatabaseRoot.resolve(tableName);
        index = tableIndex;
        this.actualSegment = actualSegment;
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        TableImpl table = new TableImpl(
                context.getTableName(),
                context.getTablePath().getParent(),
                context.getTableIndex(),
                context.getCurrentSegment());
        return new CachingTable(table);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }

        try {
            if (actualSegment == null || actualSegment.isReadOnly()) {
                actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), path);
            }
            if (actualSegment.write(objectKey, objectValue)) {
                index.onIndexedEntityUpdated(objectKey, actualSegment);
            }
        } catch (IOException e) {
            throw new DatabaseException("IOException while writing", e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }

        Optional<Segment> readSegment = index.searchForKey(objectKey);
        if (readSegment.isEmpty()) {
            return Optional.empty();
        }

        try {
            return readSegment.get().read(objectKey);
        } catch (IOException e) {
            throw new DatabaseException("IOException while reading", e);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }

        Optional<Segment> readSegment = index.searchForKey(objectKey);
        if (readSegment.isEmpty()) {
            throw new DatabaseException("Key was not found");
        }

        try {
            if (actualSegment == null || actualSegment.isReadOnly()) {
                actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(name), path);
            }

            if (readSegment.get().delete(objectKey)) {
                index.onIndexedEntityUpdated(objectKey, actualSegment);
            }
        } catch (IOException e) {
            throw new DatabaseException("IOException while deleting", e);
        }
    }
}
