package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {
    private final String name;
    private final Path path;
    private Map<String, Table> tables;

    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null) {
            throw new DatabaseException("Database should have a name");
        }
        if (databaseRoot == null) {
            throw new DatabaseException("Database should have a root");
        }

        File directory = new File(String.valueOf(databaseRoot.resolve(dbName)));
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new DatabaseException("Problem with directory");
            }
        }

        return new DatabaseImpl(dbName, databaseRoot);
    }

    private DatabaseImpl(String name, Path root) {
        this.name = name;
        this.path = root.resolve(name);
        tables = new HashMap<>();
    }

    private DatabaseImpl(String name, Path root, Map<String, Table> tables) {
        this.name = name;
        this.path = root.resolve(name);
        this.tables = tables;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(
                context.getDbName(),
                context.getDatabasePath().getRoot(),
                new HashMap<>(context.getTables()));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table name should have a name");
        }
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table already exists");
        }

        Table newTable = TableImpl.create(tableName, path, new TableIndex());
        tables.put(tableName, newTable);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table name should have a name");
        }
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("This table isn't existing");
        }

        tables.get(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table name should have a name");
        }
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }
        if (!tables.containsKey(tableName)) {
            return Optional.empty();
        }

        return tables.get(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Table name should have a name");
        }
        if (objectKey == null) {
            throw new DatabaseException("Key could not be a null");
        }
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("This table isn't existing");
        }

        tables.get(tableName).delete(objectKey);
    }
}
