package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Path;

public class DatabaseInitializer implements Initializer {
    private TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        Path workingPath = initialContext.currentDbContext().getDatabasePath();

        File rootDirectory = new File(String.valueOf(workingPath));
        if (!rootDirectory.exists()) {
            throw new DatabaseException("Problem with directory");
        }

        File[] files = rootDirectory.listFiles();

        if (files != null) {
            for (var file : files) {
                if (file.isDirectory()) {
                    TableInitializationContext tableContext =
                            new TableInitializationContextImpl(
                                    file.getName(),
                                    workingPath,
                                    new TableIndex()
                            );

                    InitializationContext newContext =
                            new InitializationContextImpl(
                                    initialContext.executionEnvironment(),
                                    initialContext.currentDbContext(),
                                    tableContext,
                                    initialContext.currentSegmentContext()
                            );

                    tableInitializer.perform(newContext);
                }
            }

            initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
        }
    }
}
