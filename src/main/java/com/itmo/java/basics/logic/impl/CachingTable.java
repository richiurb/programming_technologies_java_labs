package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

/**
 * Декоратор для таблицы. Кэширует данные
 */
public class CachingTable implements Table {
    private final Table decoratingTable;
    private final DatabaseCache databaseCache;

    public CachingTable(Table table) {
        decoratingTable = table;
        databaseCache = new DatabaseCacheImpl();
    }

    @Override
    public String getName() {
        return decoratingTable.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }

        try {
            decoratingTable.write(objectKey, objectValue);
            databaseCache.set(objectKey, objectValue);
        } catch (DatabaseException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }

        byte[] getValueFromCache = databaseCache.get(objectKey);
        if (getValueFromCache == null) {
            try {
                Optional<byte[]> getValue = decoratingTable.read(objectKey);

                if (getValue.isEmpty()) {
                    return Optional.empty();
                }

                databaseCache.set(objectKey, getValue.get());
                return getValue;
            } catch (DatabaseException e) {
                throw new DatabaseException(e);
            }
        }

        return Optional.of(getValueFromCache);
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }

        try {
            decoratingTable.delete(objectKey);
            databaseCache.delete(objectKey);
        } catch (DatabaseException e) {
            throw new DatabaseException("IOException while deleting", e);
        }
    }
}
