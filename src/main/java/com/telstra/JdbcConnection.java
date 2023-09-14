package com.telstra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcConnection {
	
	public static Connection getMySQLConnection(String jdbcUrl, String username, String password) throws SQLException, ClassNotFoundException {
        // Load the MySQL JDBC driver
        Class.forName("com.mysql.cj.jdbc.Driver");
 
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    public static Connection getPostgreSQLConnection(String jdbcUrl, String username, String password) throws SQLException, ClassNotFoundException {
        // Load the PostgreSQL JDBC driver
        Class.forName("org.postgresql.Driver");

        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    public static Connection getOracleConnection(String jdbcUrl, String username, String password) throws SQLException, ClassNotFoundException {
        // Load the Oracle JDBC driver
        Class.forName("oracle.jdbc.driver.OracleDriver");

        return DriverManager.getConnection(jdbcUrl, username, password);
    }
    
    public static boolean createRecord(Connection connection, String tableName, String[] columnNames, Object[] values) {
        try {
            // Construct the SQL INSERT query dynamically based on column names and values
            StringBuilder insertQuery = new StringBuilder("INSERT INTO " + tableName + " (");
            StringBuilder placeholders = new StringBuilder();

            for (int i = 0; i < columnNames.length; i++) {
                insertQuery.append(columnNames[i]);
                placeholders.append("?");
                if (i < columnNames.length - 1) {
                    insertQuery.append(", ");
                    placeholders.append(", ");
                }
            }

            insertQuery.append(") VALUES (").append(placeholders).append(")");

            // Prepare and execute the INSERT statement
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery.toString());

            for (int i = 0; i < values.length; i++) {
                preparedStatement.setObject(i + 1, values[i]);
            }

            int rowsAffected = preparedStatement.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static ResultSet readRecords(Connection connection, String tableName) {
        try {
            // Construct the SQL SELECT query for reading all records from the table
            String selectQuery = "SELECT * FROM " + tableName;

            // Prepare and execute the SELECT statement
            PreparedStatement preparedStatement = connection.prepareStatement(selectQuery);
            return preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static boolean updateRecord(Connection connection, String tableName, String[] columnNames, Object[] values, String whereClause) {
        try {
            // Construct the SQL UPDATE query dynamically based on column names, values, and a WHERE clause
            StringBuilder updateQuery = new StringBuilder("UPDATE " + tableName + " SET ");

            for (int i = 0; i < columnNames.length; i++) {
                updateQuery.append(columnNames[i]).append(" = ?");
                if (i < columnNames.length - 1) {
                    updateQuery.append(", ");
                }
            }

            updateQuery.append(" WHERE ").append(whereClause);

            // Prepare and execute the UPDATE statement
            PreparedStatement preparedStatement = connection.prepareStatement(updateQuery.toString());

            for (int i = 0; i < values.length; i++) {
                preparedStatement.setObject(i + 1, values[i]);
            }

            int rowsAffected = preparedStatement.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean deleteRecord(Connection connection, String tableName, String whereClause) {
        try {
            // Construct the SQL DELETE query based on a WHERE clause
            String deleteQuery = "DELETE FROM " + tableName + " WHERE " + whereClause;

            // Prepare and execute the DELETE statement
            PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery);

            int rowsAffected = preparedStatement.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    public static void closeResources(Connection connection, Statement statement) {
        try {
            if (statement != null) statement.close();
            if (connection != null) connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
