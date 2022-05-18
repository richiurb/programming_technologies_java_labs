package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';

    private final List<RespObject> respObjects;

    public RespArray(RespObject... objects) {
        respObjects = new ArrayList<>();
        respObjects.addAll(Arrays.asList(objects));
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() { return false; }

    /**
     * Строковое представление
     *
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        return respObjects.stream().map(RespObject::asString).collect(Collectors.joining(" "));
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(String.valueOf(respObjects.size()).getBytes(StandardCharsets.UTF_8));
        os.write(CRLF);
        for (var e : respObjects) {
            e.write(os);
        }
    }

    public List<RespObject> getObjects() {
        return respObjects;
    }
}
