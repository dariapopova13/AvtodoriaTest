package com.popova.avtodoria;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class DatabaseService {

    public static final int n = 10;
    private final static Logger logger = Logger.getLogger(DatabaseService.class.getName());
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource dataSource;

    static {
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/avtodoriaTest");
        config.setUsername("admin");
        config.setPassword("qwerty");
//        config.setJdbcUrl("jdbc:postgresql://raja.db.elephantsql.com:5432/xdusdjpb");
//        config.setUsername("xdusdjpb");
//        config.setPassword("MrFFl3sRJfwo9YbGE0_RQFvjcW7xINYU");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(4);
        dataSource = new HikariDataSource(config);
    }

    public static List<Long> insertIntoDatabase() throws SQLException {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(
                "insert into samples (creation_timestamp, sample) values (?,?) returning id",
                Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < n; i++) {
            preparedStatement.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setInt(2, getRandomNumber());
            preparedStatement.addBatch();
        }
        int[] a = preparedStatement.executeBatch();
        ResultSet resultSet = preparedStatement.getGeneratedKeys();
        List<Long> ids = new ArrayList<>();
        while (resultSet.next()) {
            ids.add(resultSet.getLong("id"));
        }
        close(connection, preparedStatement, resultSet);
        return ids;
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static void clearTables() {
        try {
            Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "delete from  processed_samples; delete from samples;"
            );
            preparedStatement.execute();
            close(connection, preparedStatement);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void moveDataInDatabase(List<Long> idsToBeMoved, long threadId) throws SQLException {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(
                "insert into processed_samples (creation_timestamp, sample_id, thread_id) values (?,?,?);"
        );

        for (Long id : idsToBeMoved) {
            preparedStatement.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            preparedStatement.setLong(2, id);
            preparedStatement.setLong(3, threadId);
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();
        close(connection, preparedStatement);
    }

    public static void printStatistics() {
        try {
            printCount();
            printMax();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static int getRandomNumber() {
        Random random = new Random();
        return random.nextInt(100);
    }

    private static void close(Connection connection, PreparedStatement preparedStatement,
                              ResultSet resultSet) throws SQLException {
        resultSet.close();
        close(connection, preparedStatement);
    }

    private static void close(Connection connection, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.close();
        connection.close();
    }

    private static void printCount() throws SQLException {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select thread_id, count(*) as count_value  from processed_samples " +
                        "group by thread_id;"
        );

        preparedStatement.executeQuery();
        ResultSet resultSet = preparedStatement.getResultSet();
        while (resultSet.next()) {
            long threadId = resultSet.getLong("thread_id");
            int countValue = resultSet.getInt("count_value");
            logger.info(String.format("Thread %s -> count: %s", threadId, countValue));
        }
        close(connection, preparedStatement, resultSet);
    }

    private static void printMax() throws SQLException {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(
                "select thread_id, max(sample) as max_value from processed_samples ps, samples s " +
                        "where s.id=ps.sample_id group by thread_id;"
        );
        preparedStatement.executeQuery();
        ResultSet resultSet = preparedStatement.getResultSet();
        while (resultSet.next()) {
            long threadId = resultSet.getLong("thread_id");
            int maxValue = resultSet.getInt("max_value");
            logger.info(String.format("Thread %s -> max: %s", threadId, maxValue));
        }
        close(connection, preparedStatement, resultSet);
    }

}
