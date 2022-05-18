package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class TableInitializer implements Initializer {
    private SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workingPath = context.currentTableContext().getTablePath();

        File rootDirectory = new File(String.valueOf(workingPath));
        if (!rootDirectory.exists()) {
            throw new DatabaseException("Problem with directory");
        }

        File[] files = rootDirectory.listFiles();
        if (files != null) {
            Arrays.sort(files);

                for(var file : files) {
                    if (file.isFile()) {
                        SegmentInitializationContext segmentContext =
                                new SegmentInitializationContextImpl(
                                        file.getName(),
                                        Paths.get(workingPath.toString(), file.getName()),
                                        0,
                                        new SegmentIndex());

                        InitializationContext newContext =
                                new InitializationContextImpl(
                                        context.executionEnvironment(),
                                        context.currentDbContext(),
                                        context.currentTableContext(),
                                        segmentContext);

                        segmentInitializer.perform(newContext);
                    }
                }

            context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
        }
    }
}
