package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Id
 */
public class RespCommandId implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '!';

    private final int commandId;

    public RespCommandId(int commandId) {
        this.commandId = commandId;
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

    @Override
    public String asString() {
        return String.valueOf(commandId);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(toFourBytes(commandId));
        os.write(CRLF);
    }

    private byte[] toFourBytes(int number) {
        return new byte[] {
                (byte)(number >>> 24),
                (byte)(number >>> 16),
                (byte)(number >>> 8),
                (byte)number};
    }
}
