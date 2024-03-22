# sqlitejs

A simple encapsulation for the npm package sqlite3 ([NPM](https://www.npmjs.com/package/sqlite3), [GitHub](https://github.com/TryGhost/node-sqlite3)), with additional implementation utilizing Promises for enhanced functionality.

# Features

- **Promise-based Implementation**: Utilizes Promises for asynchronous operations, making it easier to work with SQLite databases in a non-blocking manner.
- **Encapsulation of sqlite3**: Provides a simplified interface for interacting with SQLite databases, abstracting away some of the complexities of the underlying sqlite3 package.
- **Insert Operations Optimization**: Offers efficient bulk insert operations with the ability to pause and resume inserts, optimizing database performance.
- **Data Sanitization**: Includes methods for sanitizing data to prevent SQL injection attacks and ensure data integrity.
- **Flexible Querying**: Supports a variety of query types including SELECT, INSERT, UPDATE, DELETE, and COUNT, with customizable options for filtering, sorting, and pagination.
- **Error Handling**: Directly exposes database errors to users without additional processing.
- **Configurable**: Allows configuration options such as file path for the database file, verbosity level, and chunk size for bulk inserts, providing flexibility to tailor the behavior according to specific requirements.
- **Test Coverage**: Comes with a comprehensive suite of tests using Mocha and Chai to ensure reliability and correctness of the implementation.

Feel free to add more features or elaborate on the existing ones based on your project's specific functionalities and enhancements.

# Installing

You can use [`npm`](https://github.com/npm/cli) to install.

```bash
npm install @wu__yu/sqlite
```

# Usage

```javascript
import { Db } from '@wu__yu/sqlite';

const db = new Db({ file: 'demo.db' });
await db.exec(`
    CREATE TABLE IF NOT EXISTS example_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT
    );
`);
// Suspend bulk insert operations
db.suspendInsert();
for (let i = 1; i <= 1000000; i++) {
    // Loop to generate 1,000,000 rows of data
    db.insert('example_table', ['name'], { name: `Entry ${i}` });
}
// Commit the suspended inserts in batches of 1000 rows each
await db.commitInserts();
// Done
console.log(`total rows: ${await db.count('example_table')}`);
await db.close();
```

# API

## Db Class

### `new Db({file, isVerbose})`
Initializes a new Db object.

### `exec(sql)`
Executes a SQL statement.

### `run(sql, {params})`
Runs a SQL statement with optional parameters.

### `all(sql, {params, resultType, resultKey, resultField, fieldTypes})`
Runs a SQL query that retrieves all rows.

### `get(sql, {params, resultField, fieldTypes})`
Runs a SQL query that retrieves a single row.

### `each(sql, {params, fn})`
Runs a SQL query and calls a function for each row returned.

### `inserts(table, fields, objects, {chunkSize})`
Inserts multiple rows into a table in a single operation, with an optional chunk size for optimization.

### `insert(table, fields, object, {immediate})`
Inserts a single row into a table, with an option to immediately execute or suspend.

### `selects(table, {distinct, fields, where, order, descending, limit, page, resultType, resultKey, resultField, fieldTypes})`
Selects rows on a table.

### `select(table, {fields, where, resultField, fieldTypes})`
Selects a row on a table.

### `update(table, fields, object, {where})`
Updates rows in a table.

### `replace(table, fields, object)`
Inserts a new row into a table or replaces an existing row.

### `delete(table, {where})`
Delete rows from a table.

### `count(table)`
Counts the number of rows in a table.

## Table Class

### Constructor
- `new Table(db, name, fieldTypes, {pk, uk})`: Initializes a new Table object.

### Methods
- `inserts(objects, {chunkSize})`: Inserts multiple objects into the table.
- `insert(object, {immediate})`: Inserts a single object into the table.
- `suspendInsert()`: Suspends bulk insert operations.
- `commitInserts({chunkSize})`: Commits the suspended inserts in batches.
- `select({fields, pk, uk, where, resultField})`: Selects a row on the table.
- `selectByPk`: Retrieves a row from the table based on the provided primary key value.
- `selectByUk`: Retrieves a row from the table based on the provided unique key value.
- `selects({distinct, fields, where, order, descending, limit, page, resultType, resultKey, resultField, pkAsResultKey, ukAsResultKey})`: Selects rows on the table.
- `selectsPk`: Retrieve the primary key values as an array.
- `selectsPkSet`: Retrieve the primary key values as a Set.
- `selectsUk`: Retrieve the unique key values as an array.
- `selectsUkSet`: Retrieve the unique key values as a Set.
- `selectsPkUk`: Retrieve the primary key and unique key values as an array.
- `selectsPkUkMap`: Retrieve the primary key and unique key values as a Map (pk as key, uk as value).
- `selectsPkUkObject`: Retrieve the primary key and unique key values as an Object (pk as key, uk as value).
- `selectsUkPkMap`: Retrieve the unique key and primary key as a Map (uk as key, pk as value).
- `selectsUkPkObject`: Retrieve the unique key and primary key as an Object. (uk as key, pk as value).
- `selectsUksPkMap`: Retrieve the unique keys and primary key as a nested Map (uks as keys, pk as value).
- `selectsUksPkObject`: Retrieve the unique keys and primary key as a nested Object (uks as keys, pk as value).
- `update(object, {where, pk})`: Updates rows in the table.
- `replace(object)`: Inserts a new row into the table or replaces an existing row.
- `delete({where, pk})`: Delete rows from the table.
- `deleteByPk(pk)`: Delete a row from the table based on the provided primary key value.
- `softDelete({where, pk})`: Soft delete rows from the table.
- `softDeleteByPk(pk)`: Soft delete a row from the table based on the provided primary key value.
- `unDelete({where, pk})`: Undelete rows from the table.
- `unDeleteByPk(pk)`: Undelete a row from the table based on the provided primary key value.
- `count()`: Counts the number of rows in the table.

# Testing

```bash
npm test
```

# Get in Touch

### Your Input Matters

Thank you for choosing `@wu__yu/sqlite`! We're at the beginning of our project journey. Our philosophy is centered on simplicity, avoiding unnecessary complexity. We focus on practical problem-solving, addressing real-world issues such as long wait times when inserting large datasets. We aim to streamline data handling by empowering users to interact directly with SQL, facilitating swift and efficient operations. If you have ideas to share, bugs to report, or can help us improve code quality, please contribute on [GitHub](https://github.com/wuyu2015/sqlitejs). Your input is crucial in shaping our project's direction. Let's collaborate to create something impactful for the community.

# License

This project is licensed under the [BSD-3-Clause License](https://opensource.org/licenses/BSD-3-Clause).

Date: 2024-03-08
