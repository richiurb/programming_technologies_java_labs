package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая добавление значения по ключу
 */
public class SetDatabaseRecord implements WritableDatabaseRecord {
    private final int KEY_LENGTH_SIZE = 4;
    private final int VALUE_LENGTH_SIZE = 4;
    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public long size() {
        return key.length + ((value == null) ? 0 : value.length) + KEY_LENGTH_SIZE + VALUE_LENGTH_SIZE;
    }

    @Override
    public boolean isValuePresented() {
        return (value != null);
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() { return (value == null) ? -1 : value.length; }
}
