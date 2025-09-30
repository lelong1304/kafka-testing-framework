package com.github.lelong1304.kafka.testing.steps;

import com.github.lelong1304.kafka.testing.database.DatabaseVerifier;
import io.cucumber.datatable.DataTable;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cucumber step definitions for database operations.
 * Handles data setup and verification.
 */
public class DatabaseSteps {

    @Autowired
    private DatabaseVerifier databaseVerifier;

    @Given("the database has the following customers:")
    public void theDatabaseHasTheFollowingCustomers(DataTable dataTable) {
        insertDataIntoTable("customers", dataTable);
    }

    @Given("the database has the following products:")
    public void theDatabaseHasTheFollowingProducts(DataTable dataTable) {
        insertDataIntoTable("products", dataTable);
    }

    @Given("the database has the following orders:")
    public void theDatabaseHasTheFollowingOrders(DataTable dataTable) {
        insertDataIntoTable("orders", dataTable);
    }

    @Given("the database table {string} has the following data:")
    public void theDatabaseTableHasTheFollowingData(String tableName, DataTable dataTable) {
        insertDataIntoTable(tableName, dataTable);
    }

    @And("the database should have the following customers:")
    public void theDatabaseShouldHaveTheFollowingCustomers(DataTable dataTable) {
        verifyDataInTable("customers", dataTable);
    }

    @And("the database should have the following orders:")
    public void theDatabaseShouldHaveTheFollowingOrders(DataTable dataTable) {
        verifyDataInTable("orders", dataTable);
    }

    @And("the database should have the following products:")
    public void theDatabaseShouldHaveTheFollowingProducts(DataTable dataTable) {
        verifyDataInTable("products", dataTable);
    }

    @And("the database table {string} should contain:")
    public void theDatabaseTableShouldContain(String tableName, DataTable dataTable) {
        verifyDataInTable(tableName, dataTable);
    }

    @And("the database table {string} should be empty")
    public void theDatabaseTableShouldBeEmpty(String tableName) {
        int rowCount = databaseVerifier.getRowCount(tableName);
        assertThat(rowCount).isZero();
    }

    @And("the database table {string} should have {int} row(s)")
    public void theDatabaseTableShouldHaveRows(String tableName, int expectedCount) {
        int rowCount = databaseVerifier.getRowCount(tableName);
        assertThat(rowCount).isEqualTo(expectedCount);
    }

    @Given("the database table {string} is empty")
    public void theDatabaseTableIsEmpty(String tableName) {
        databaseVerifier.clearTable(tableName);
    }

    private void insertDataIntoTable(String tableName, DataTable dataTable) {
        List<Map<String, String>> rows = dataTable.asMaps(String.class, String.class);
        
        for (Map<String, String> row : rows) {
            Map<String, Object> data = new HashMap<>();
            for (Map.Entry<String, String> entry : row.entrySet()) {
                data.put(entry.getKey(), convertValue(entry.getValue()));
            }
            databaseVerifier.insertRow(tableName, data);
        }
    }

    private void verifyDataInTable(String tableName, DataTable dataTable) {
        List<Map<String, String>> expectedRows = dataTable.asMaps(String.class, String.class);
        
        for (Map<String, String> expectedRow : expectedRows) {
            Map<String, Object> criteria = new HashMap<>();
            for (Map.Entry<String, String> entry : expectedRow.entrySet()) {
                criteria.put(entry.getKey(), convertValue(entry.getValue()));
            }
            
            boolean exists = databaseVerifier.rowExists(tableName, criteria);
            assertThat(exists)
                .withFailMessage("Expected row not found in table %s: %s", tableName, expectedRow)
                .isTrue();
        }
    }

    private Object convertValue(String value) {
        if (value == null) {
            return null;
        }
        
        // Try to parse as number
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // Try to parse as boolean
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                return Boolean.parseBoolean(value);
            }
            // Return as string
            return value;
        }
    }
}
