package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

import java.io.File;
import java.nio.file.Path;

public class DatabaseServerInitializer implements Initializer {
    private DatabaseInitializer databaseInitializer;
    
    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, начинает их инициализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Path workingPath = context.executionEnvironment().getWorkingPath();

        File rootDirectory = new File(String.valueOf(workingPath));
        if (!rootDirectory.exists()) {
            if (!rootDirectory.mkdirs()) {
                throw new DatabaseException("Problem with directory");
            }
        }

        File[] files = rootDirectory.listFiles();

        if (files != null) {
            for (var file : files) {
                if (file.isDirectory()) {
                    DatabaseInitializationContext dbContext =
                            new DatabaseInitializationContextImpl(
                                    file.getName(),
                                    workingPath);

                    InitializationContext newContext =
                            new InitializationContextImpl(
                                    context.executionEnvironment(),
                                    dbContext,
                                    context.currentTableContext(),
                                    context.currentSegmentContext()
                            );

                    databaseInitializer.perform(newContext);
                }
            }
        }
    }
}
