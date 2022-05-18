package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';

    public static final int NULL_STRING_SIZE = -1;

    private final byte[] data;

    public RespBulkString(byte[] data) {
        this.data = data;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        if (dataSize() == NULL_STRING_SIZE) {
            return null;
        }

        return new String(data);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(dataSize()).getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);

        if (data != null) {
            os.write(data);
            os.write(CRLF);
        }
    }

    private int dataSize() {
        if (data == null) {
            return NULL_STRING_SIZE;
        }

        return data.length;
    }
}
