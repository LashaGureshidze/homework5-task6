package com.epam.homework5.task6.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {

    private static ConnectionManager instance;
    private Connection connection;

    private ConnectionManager() {
        try {
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static ConnectionManager getInstance() {
        if (instance != null) {
            return instance;
        } else {
            synchronized (ConnectionManager.class) {
                if (instance == null) {
                    instance = new ConnectionManager();
                }
                return instance;
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
