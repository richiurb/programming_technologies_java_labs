package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {
    private final ExecutionEnvironment env;
    private final int id;
    private final String databaseName;
    private final String tableName;

    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (env == null) {
            throw new IllegalArgumentException("env is null");
        }

        if (commandArgs.size() != 4) {
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
    }

    /**
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            Optional<Database> database = env.getDatabase(databaseName);
            if (database.isEmpty()) {
                return DatabaseCommandResult.error("Database " + databaseName + " is not present");
            }

            database.get().createTableIfNotExists(tableName);
        } catch (DatabaseException e) {
            return DatabaseCommandResult.error(e);
        }

        return DatabaseCommandResult.success(("Table " + tableName + " in database " + databaseName + " created").getBytes(StandardCharsets.UTF_8));
    }
}
