package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Segment;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TableInitializationContextImpl implements TableInitializationContext {
    private final String tableName;
    private final Path tablePath;
    public TableIndex tableIndex;
    private Segment segment;

    public TableInitializationContextImpl(String tableName, Path databasePath, TableIndex tableIndex) {
        this.tableName = tableName;
        this.tablePath = Paths.get(databasePath.toString(), tableName);
        this.tableIndex = tableIndex;
        segment = null;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return tablePath;
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return segment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        this.segment = segment;
    }

    @Override
    public void setTableIndex(TableIndex index) {
        this.tableIndex = index;
    }
}
