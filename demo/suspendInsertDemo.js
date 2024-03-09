import Db from "../index.js";

const db = new Db({file: 'demo.db'});
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
