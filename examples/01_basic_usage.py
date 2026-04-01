#!/usr/bin/env python3
"""Basic USQLEngine usage: DDL, DML, and queries.

Demonstrates creating tables with various column types and constraints,
inserting/updating/deleting rows, and running SELECT queries.
"""

from usqldb import USQLEngine


def main() -> None:
    engine = USQLEngine()

    # ---- DDL: Create tables with constraints ----------------------------

    engine.sql("""
        CREATE TABLE departments (
            id    SERIAL PRIMARY KEY,
            name  TEXT NOT NULL UNIQUE,
            budget NUMERIC
        )
    """)

    engine.sql("""
        CREATE TABLE employees (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            email      VARCHAR(255) UNIQUE,
            department_id INTEGER REFERENCES departments(id),
            salary     REAL,
            active     BOOLEAN DEFAULT TRUE
        )
    """)

    print("Tables created.\n")

    # ---- DML: Insert rows -----------------------------------------------

    engine.sql("INSERT INTO departments (name, budget) VALUES ('Engineering', 500000)")
    engine.sql("INSERT INTO departments (name, budget) VALUES ('Marketing', 200000)")
    engine.sql("INSERT INTO departments (name, budget) VALUES ('Sales', 300000)")

    engine.sql(
        "INSERT INTO employees (name, email, department_id, salary) "
        "VALUES ('Alice', 'alice@example.com', 1, 120000)"
    )
    engine.sql(
        "INSERT INTO employees (name, email, department_id, salary) "
        "VALUES ('Bob', 'bob@example.com', 1, 110000)"
    )
    engine.sql(
        "INSERT INTO employees (name, email, department_id, salary) "
        "VALUES ('Charlie', 'charlie@example.com', 2, 95000)"
    )
    engine.sql(
        "INSERT INTO employees (name, email, department_id, salary, active) "
        "VALUES ('Diana', 'diana@example.com', 3, 105000, FALSE)"
    )

    print("Data inserted.\n")

    # ---- SELECT: Basic queries ------------------------------------------

    print("=== All departments ===")
    result = engine.sql("SELECT id, name, budget FROM departments ORDER BY id")
    for row in result:
        print(f"  {row['id']:>2}  {row['name']:<15} budget={row['budget']}")

    print("\n=== Active employees with salary > 100000 ===")
    result = engine.sql(
        "SELECT name, email, salary "
        "FROM employees "
        "WHERE active = TRUE AND salary > 100000 "
        "ORDER BY salary DESC"
    )
    for row in result:
        print(f"  {row['name']:<10} {row['email']:<25} ${row['salary']:>10,.0f}")

    # ---- UPDATE ---------------------------------------------------------

    engine.sql("UPDATE employees SET salary = 125000 WHERE name = 'Alice'")
    result = engine.sql("SELECT name, salary FROM employees WHERE name = 'Alice'")
    print(f"\nAlice's updated salary: ${result.rows[0]['salary']:,.0f}")

    # ---- DELETE ---------------------------------------------------------

    engine.sql("DELETE FROM employees WHERE active = FALSE")
    result = engine.sql("SELECT COUNT(*) AS cnt FROM employees")
    print(f"Active employees remaining: {result.rows[0]['cnt']}")

    # ---- Aggregation ----------------------------------------------------

    print("\n=== Salary statistics ===")
    result = engine.sql("""
        SELECT
            COUNT(*) AS headcount,
            AVG(salary) AS avg_salary,
            MIN(salary) AS min_salary,
            MAX(salary) AS max_salary
        FROM employees
    """)
    row = result.rows[0]
    print(f"  Headcount: {row['headcount']}")
    print(f"  Avg salary: ${row['avg_salary']:>10,.0f}")
    print(f"  Min salary: ${row['min_salary']:>10,.0f}")
    print(f"  Max salary: ${row['max_salary']:>10,.0f}")


if __name__ == "__main__":
    main()
