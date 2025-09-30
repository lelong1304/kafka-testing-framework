package com.github.lelong1304.kafka.testing.database;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Utility class for verifying database state in tests.
 * Provides methods for querying and asserting database content.
 */
@Component
public class DatabaseVerifier {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Get all rows from a table
     */
    public List<Map<String, Object>> getAllRows(String tableName) {
        String sql = "SELECT * FROM " + tableName;
        return jdbcTemplate.queryForList(sql);
    }

    /**
     * Get row count for a table
     */
    public int getRowCount(String tableName) {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class);
        return count != null ? count : 0;
    }

    /**
     * Insert a row into a table
     */
    public void insertRow(String tableName, Map<String, Object> data) {
        if (data.isEmpty()) {
            return;
        }

        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        Object[] params = new Object[data.size()];
        int i = 0;

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (i > 0) {
                columns.append(", ");
                values.append(", ");
            }
            columns.append(entry.getKey());
            values.append("?");
            params[i++] = entry.getValue();
        }

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", 
                                   tableName, columns.toString(), values.toString());
        jdbcTemplate.update(sql, params);
    }

    /**
     * Clear all data from a table
     */
    public void clearTable(String tableName) {
        String sql = "DELETE FROM " + tableName;
        jdbcTemplate.update(sql);
    }

    /**
     * Check if a row exists matching the given criteria
     */
    public boolean rowExists(String tableName, Map<String, Object> criteria) {
        if (criteria.isEmpty()) {
            return false;
        }

        StringBuilder where = new StringBuilder();
        Object[] params = new Object[criteria.size()];
        int i = 0;

        for (Map.Entry<String, Object> entry : criteria.entrySet()) {
            if (i > 0) {
                where.append(" AND ");
            }
            where.append(entry.getKey()).append(" = ?");
            params[i++] = entry.getValue();
        }

        String sql = String.format("SELECT COUNT(*) FROM %s WHERE %s", tableName, where.toString());
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, params);
        return count != null && count > 0;
    }

    /**
     * Execute a custom SQL query
     */
    public void executeSql(String sql) {
        jdbcTemplate.execute(sql);
    }
}
