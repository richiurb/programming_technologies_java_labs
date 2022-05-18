package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workingPath = context.currentSegmentContext().getSegmentPath();

        File file = new File(String.valueOf(workingPath));

        try (DatabaseInputStream input = new DatabaseInputStream(new FileInputStream(file))) {
            Set<String> set = new HashSet<>();
            long currentSize = 0;
            if (file.length() != 0) {
                Optional<DatabaseRecord> databaseRecord = input.readDbUnit();

                while (databaseRecord.isPresent()) {
                    String key = new String(databaseRecord.get().getKey());
                    set.add(key);

                    context.currentSegmentContext().getIndex().onIndexedEntityUpdated(key, new SegmentOffsetInfoImpl(currentSize));
                    currentSize += databaseRecord.get().size();

                    databaseRecord = input.readDbUnit();
                }
            }

            SegmentInitializationContext newContext =
                    new SegmentInitializationContextImpl(
                            context.currentSegmentContext().getSegmentName(),
                            workingPath,
                            (int) currentSize,
                            context.currentSegmentContext().getIndex()
                    );

            Segment initializeSegment = SegmentImpl.initializeFromContext(newContext);
            context.currentTableContext().updateCurrentSegment(initializeSegment);

            for (var key : set) {
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(key, initializeSegment);
            }
        } catch (FileNotFoundException e) {
            throw new DatabaseException("File not found exception", e);
        } catch (IOException e) {
            throw new DatabaseException("IOException", e);
        }
    }
}
