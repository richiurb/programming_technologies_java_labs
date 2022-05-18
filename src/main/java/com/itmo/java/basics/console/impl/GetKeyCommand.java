package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для чтения данных по ключу
 */
public class GetKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final int id;
    private final String databaseName;
    private final String tableName;
    private final String objectKey;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (env == null) {
            throw new IllegalArgumentException("env is null");
        }

        if (commandArgs.size() != 5) {
            throw new IllegalArgumentException("invalid commandArgs");
        }

        this.env = env;
        try {
            id = Integer.parseInt(commandArgs.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex()).asString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid commandId");
        }
        databaseName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        objectKey = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
    }

    /**
     * Читает значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с прочитанным значением. Например, "previous". Null, если такого нет
     */
    @Override
    public DatabaseCommandResult execute() {
        Optional<byte[]> readingValue;
        try {
            Optional<Database> database = env.getDatabase(databaseName);
            if (database.isEmpty()) {
                return DatabaseCommandResult.error("Database " + databaseName + " is not present");
            }

            readingValue = database.get().read(tableName, objectKey);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }

        if (readingValue.isEmpty()) {
            return DatabaseCommandResult.success(null);
        }

        return DatabaseCommandResult.success(readingValue.get());
    }
}
