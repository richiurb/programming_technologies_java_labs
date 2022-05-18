package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final Path workingPath;
    private final Map<String, Database> databases;

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        workingPath = Paths.get(config.getWorkingPath());
        databases = new HashMap<>();
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        if (!databases.containsKey(name)) {
            return Optional.empty();
        }
        return Optional.of(databases.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        if (db != null) {
            databases.put(db.getName(), db);
        }
    }

    @Override
    public Path getWorkingPath() {
        return workingPath;
    }
}
