package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {
    private final ExecutionEnvironment env;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private DatabaseServer(ExecutionEnvironment env) {
        this.env = env;
    }
    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        InitializationContext context =
                new InitializationContextImpl(
                        env,
                        null,
                        null,
                        null);
        initializer.perform(context);
        return new DatabaseServer(env);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() ->
            DatabaseCommands
                    .valueOf(DatabaseCommandArgPositions.COMMAND_NAME.name())
                    .getCommand(env, message.getObjects())
                    .execute()
        , executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }
}