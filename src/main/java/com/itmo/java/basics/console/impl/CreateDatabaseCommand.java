package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final DatabaseFactory factory;
    private final int id;
    private final String databaseName;
    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param factory     функция создания базы данных (пример: DatabaseImpl::create)
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> commandArgs) {
        if (env == null) {
            throw new IllegalArgumentException("env is null");
        }

        if (factory == null) {
            throw new IllegalArgumentException("factory is null");
        }

        if (commandArgs.size() != 3) {
            throw new IllegalArgumentException("invalid commandArgs");
        }

        this.env = env;
        this.factory = factory;
        try {
            id = Integer.parseInt(commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).asString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid commandId");
        }
        databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            env.addDatabase(factory.createNonExistent(databaseName, env.getWorkingPath()));
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }

        return DatabaseCommandResult.success(("Database " + databaseName + " created").getBytes(StandardCharsets.UTF_8));
    }
}
