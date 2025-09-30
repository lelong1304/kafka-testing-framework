Feature: Database State Verification

  Scenario: Verify database state after processing
    Given the database has the following customers:
      | id | name        | email               |
      | 1  | Alice Smith | alice@example.com   |
      | 2  | Bob Jones   | bob@example.com     |
    And the database has the following products:
      | id | name   | price  | stock |
      | P1 | Laptop | 999.99 | 10    |
      | P2 | Mouse  | 29.99  | 50    |
    Then the database table "customers" should have 2 rows
    And the database table "products" should have 2 rows
    And the database table "customers" should contain:
      | id | name        | email               |
      | 1  | Alice Smith | alice@example.com   |

  Scenario: Verify empty table
    Given the database table "orders" is empty
    Then the database table "orders" should be empty
