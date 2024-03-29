import { describe, it } from 'mocha';
import { assert } from 'chai';
import { Db, Table } from '../index.js';

let db;
let exampleTable;

const objectsWithPk = [
    {id: 1, name: 'Alice', age: 30, male: false, books: ['Book 1', 'Book 2', 'Book 3'], json: {a: 1, b: 2, c: 3}},
    {id: 2, name: 'Bob', age: 25, male: true, books: ['Book 4'], json: {a: 4}},
    {id: 3, name: 'Charlie', age: 35, male: true, books: ['Book 5', 'Book 6'], json: {d: 5}},
    {id: 4, name: 'David', age: 40, male: true, books: [], json: {}},
];

const objects = [
    {name: 'Alice', age: 30, male: false, books: ['Book 1', 'Book 2', 'Book 3'], json: {a: 1, b: 2, c: 3}},
    {name: 'Bob', age: 25, male: true, books: ['Book 4'], json: {a: 4}},
    {name: 'Charlie', age: 35, male: true, books: ['Book 5', 'Book 6'], json: {d: 5}},
    {name: 'David', age: 40, male: true, books: [], json: {}},
];

async function createTable() {
    db = new Db();
    await db.exec(`
        CREATE TABLE IF NOT EXISTS example_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            age INTEGER,
            male INTEGER,
            books TEXT,
            json TEXT
        );
    `);
    exampleTable = new Table(db, 'example_table', {
        id: 'int',
        name: 'string',
        age: 'int',
        male: 'boolean',
        books: 'array',
        json: 'object',
    }, {
        pk: 'id',
        uk: 'name',
    });
}

async function closeTable() {
    await db.close();
}

describe('Table', () => {
    describe('Table Constructor', () => {
        before(createTable);
        after(closeTable);

        it('should initialize', async () => {
            assert.deepStrictEqual(exampleTable.fieldTypes, {
                id: 'int',
                name: 'string',
                age: 'int',
                male: 'boolean',
                books: 'array',
                json: 'object',
            });
            assert.deepStrictEqual(exampleTable.fieldsWithPk, ['id', 'name', 'age', 'male', 'books', 'json']);
            assert.deepStrictEqual(exampleTable.fields, ['name', 'age', 'male', 'books', 'json']);
            assert.deepStrictEqual(exampleTable.pk, 'id');
            assert.deepStrictEqual(exampleTable.uk, 'name');
            assert.deepStrictEqual(exampleTable.uniqueIndexes, {name: {name: {descending: false}}});
            assert.deepStrictEqual(exampleTable.indexes, {});
        });
    });

    describe('inserts()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return number of rows', async () => {
            assert.strictEqual(await exampleTable.inserts(objects), 4);
            assert.deepStrictEqual(await exampleTable.selects(), objectsWithPk);
        });

        it('should return number of rows 2', async () => {
            const objects2 = [...objects];
            objects2[0].someKey = 'someValue';
            assert.strictEqual(await exampleTable.inserts(objects2), 4);
            assert.deepStrictEqual(await exampleTable.selects(), objectsWithPk);
        });
    });

    describe('insert()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return lastID', async () => {
            assert.strictEqual(await exampleTable.insert({ name: 'Entry 1' }), 1);
            assert.strictEqual(await exampleTable.insert({ name: 'Entry 2' }), 2);
            assert.strictEqual(await exampleTable.insert({}), 0);
            assert.strictEqual(await exampleTable.insert({invalidField: 'Entry 1'}), 0);
            try {
                await exampleTable.insert({name: 'Entry 1'});
                assert.fail('Expected an error to be thrown')
            } catch (err) {
                assert.include(err.message, 'UNIQUE');
            }
        });
    });

    describe('suspendInsert()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return lastID', async () => {
            assert.strictEqual(await exampleTable.lastId(), 0);
            exampleTable.suspendInsert();
            assert.strictEqual(await exampleTable.lastId(), 0);
            assert.strictEqual(await exampleTable.insert({ name: 'Entry 1' }), 1);
            assert.strictEqual(await exampleTable.lastId(), 0);
            assert.strictEqual(await exampleTable.insert({}), 0);
            assert.strictEqual(await exampleTable.lastId(), 0);
            assert.strictEqual(await exampleTable.insert({invalidField: 'Entry 1'}), 0);
            assert.strictEqual(await exampleTable.lastId(), 0);
            assert.strictEqual(await exampleTable.insert({ name: 'Entry 2' }), 2);
            assert.strictEqual(await exampleTable.lastId(), 0);
            exampleTable.commitInserts();
            assert.strictEqual(await exampleTable.lastId(), 2);
            assert.strictEqual(await exampleTable.insert({ name: 'Entry 3' }), 3);
            assert.strictEqual(await exampleTable.lastId(), 3);
        });
    });

    describe('select()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return a row', async () => {
            await exampleTable.inserts(objects);
            assert.deepStrictEqual(await exampleTable.select(), objectsWithPk[0]);
            assert.deepStrictEqual(await exampleTable.select({pk: 1}), objectsWithPk[0]);
            assert.deepStrictEqual(await exampleTable.select({pk: 2}), objectsWithPk[1]);
            assert.deepStrictEqual(await exampleTable.select({pk: 2, resultField: 'name'}), objectsWithPk[1].name);
            assert.deepStrictEqual(await exampleTable.select({pk: 2, resultField: 'age'}), objectsWithPk[1].age);
            assert.deepStrictEqual(await exampleTable.select({pk: 2, resultField: 'male'}), objectsWithPk[1].male);
            assert.deepStrictEqual(await exampleTable.select({pk: 2, resultField: 'books'}), objectsWithPk[1].books);
            assert.deepStrictEqual(await exampleTable.select({pk: 2, resultField: 'json'}), objectsWithPk[1].json);
            assert.deepStrictEqual(await exampleTable.select({uk: 'Bob'}), objectsWithPk[1]);
            assert.deepStrictEqual(await exampleTable.select({where: [['age', 30, '<']]}), objectsWithPk[1]);
            assert.deepStrictEqual(await exampleTable.select({where: [['age', 30, '<']], resultField: 'books'}), objectsWithPk[1].books);
        });
    });

    describe('selects()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return rows', async () => {
            await exampleTable.inserts(objects);
            assert.deepStrictEqual(await exampleTable.selects(), objectsWithPk);
            const objectsMap = new Map();
            objectsWithPk.forEach(row => objectsMap.set(row.id, row));
            assert.deepStrictEqual(await exampleTable.selects({resultType: 'map', pkAsResultKey: true}), objectsMap);
            const objectsObject = {};
            objectsWithPk.forEach(row => objectsObject[row.id] = row);
            assert.deepStrictEqual(await exampleTable.selects({resultType: 'object', pkAsResultKey: true}), objectsObject);
            const objectsSet = new Set();
            objectsWithPk.forEach(row => objectsSet.add(row.name));
            assert.deepStrictEqual(await exampleTable.selects({resultType: 'set', resultField: 'name', pkAsResultKey: true}), objectsSet);
        });
    });

    describe('selectsUksPkMap(), selectsUksPkObject()', () => {
        before(async () => {
            db = new Db();
            await db.exec(`
                CREATE TABLE IF NOT EXISTS example_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    age INTEGER
                );
                CREATE UNIQUE INDEX IF NOT EXISTS "main"."example_table_name_age" ON "example_table" (name, age);
            `);
            exampleTable = new Table(db, 'example_table', {
                id: 'int',
                name: 'string',
                age: 'int',
            }, {
                pk: 'id',
                uks: ['name', 'age'],
            });
            await exampleTable.inserts([
                {name: 'Alice', age: 30},
                {name: 'Bob', age: 25},
                {name: 'Bob', age: 30},
                {name: 'Charlie', age: 35},
                {name: 'David', age: 40},
            ]);
        });
        after(closeTable);

        it('should return a nested map', async () => {
            assert.deepStrictEqual(await exampleTable.selectsUksPkMap(), new Map([
                ['Alice', new Map([[30, 1]])],
                ['Bob', new Map([[25, 2], [30, 3]])],
                ['Charlie', new Map([[35, 4]])],
                ['David', new Map([[40, 5]])],
            ]));
        });

        it('should return a nested object', async () => {
            assert.deepStrictEqual(await exampleTable.selectsUksPkObject(), {
                Alice: {30: 1},
                Bob: {25: 2, 30: 3},
                Charlie: {35: 4},
                David: {40: 5},
            });
        });
    });

    describe('lastId()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return last id', async () => {
            assert.strictEqual(await exampleTable.lastId(), 0);
            await exampleTable.insert(objects[0]);
            assert.strictEqual(await exampleTable.lastId(), 1);
            await exampleTable.insert(objects[1]);
            assert.strictEqual(await exampleTable.lastId(), 2);
            await exampleTable.insert(objects[2]);
            assert.strictEqual(await exampleTable.lastId(), 3);
        });
    });

    describe('update()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should update rows', async () => {
            await exampleTable.inserts(objects);
            assert.strictEqual(await exampleTable.update({name: 'Eric'}, {pk: 5}), 0);
            assert.strictEqual(await exampleTable.update({name: 'Bill'}, {pk: 2}), 1);
            assert.strictEqual(await exampleTable.select({pk: 2, resultField: 'name'}), 'Bill');
            assert.strictEqual(await exampleTable.update({name: 'Bob'}, {where: [['name', 'Bill']]}), 1);
            assert.strictEqual(await exampleTable.select({pk: 2, resultField: 'name'}), 'Bob');
            assert.strictEqual(await exampleTable.update({male: false}, {where: [['male', true]]}), 3);
            assert.deepStrictEqual(await exampleTable.selects({resultType: 'set', resultField: 'male'}), new Set([false]));
        });
    });

    describe('replace()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should replace a row', async () => {
            for (const row of objects) {
                assert.strictEqual(await exampleTable.replace(row), 1);
            }
            assert.strictEqual(await exampleTable.count(), 4);
            for (const row of objects) {
                assert.strictEqual(await exampleTable.replace(row), 1);
            }
            assert.strictEqual(await exampleTable.count(), 4);
            for (const row of objectsWithPk) {
                assert.strictEqual(await exampleTable.replace(row), 1);
            }
            assert.strictEqual(await exampleTable.count(), 4);
            assert.strictEqual(await exampleTable.replace({id: 2, name: 'Bob'}), 1);
            assert.deepStrictEqual(await exampleTable.select({pk: 2}), {id: 2, name: 'Bob', age: 0, male: false, books: [], json: {}});
        });
    });

    describe('delete()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return the count of deleted rows', async () => {
            assert.strictEqual(await exampleTable.delete({pk: 1}), 0);
            await exampleTable.inserts(objects);
            assert.strictEqual(await exampleTable.count(), 4);
            assert.strictEqual(await exampleTable.delete({pk: 999}), 0);
            assert.strictEqual(await exampleTable.delete({pk: 2}), 1);
            assert.strictEqual(await exampleTable.count(), 3);
            assert.strictEqual(await exampleTable.delete({pk: 2}), 0);
            assert.strictEqual(await exampleTable.count(), 3);
            assert.strictEqual(await exampleTable.delete({where: [['name', 'Bob']]}), 0);
            assert.strictEqual(await exampleTable.count(), 3);
            assert.strictEqual(await exampleTable.delete({where: [['name', 'David']]}), 1);
            assert.strictEqual(await exampleTable.count(), 2);
            assert.strictEqual(await exampleTable.delete({where: [['id', 1, '>']]}), 1);
            assert.strictEqual(await exampleTable.count(), 1);
        });
    });

    describe('count()', () => {
        beforeEach(createTable);
        afterEach(closeTable);

        it('should return the count of rows', async () => {
            assert.strictEqual(await exampleTable.count(), 0);
            await exampleTable.inserts(objects);
            assert.strictEqual(await exampleTable.count(), 4);
        });
    });

    describe('Table.getTimeFieldTypes()', () => {
        it('should return time field types object', () => {
            assert.deepStrictEqual(Table.getTimeFieldTypes(), {
                created_at: 'date',
                updated_at: ['date', 0],
                deleted_at: ['date', 0],
            });
        });
    });

    describe('getFieldDef(), getTableDef()', () => {
        let table;

        before(() => {
            table = new Table(db, 'example_table', {
                id: 'int',
                name: 'string',
                age: 'int',
                male: 'boolean',
                books: 'array',
                json: 'object',
            }, {
                pk: 'id',
                uk: 'name',
                uniqueIndexes: [
                    ['name', 'age', 'male'],
                ],
                indexes: [
                    'male',
                    {
                        age: {descending: true}
                    }
                ],
            });
        });

        it('should make indexes', () => {
            assert.deepStrictEqual(table.uniqueIndexes, {
                name: {name: {descending: false}},
                name_age_male: {
                    name: {descending: false},
                    age: {descending: false},
                    male: {descending: false},
                },
            });
            assert.deepStrictEqual(table.indexes, {
                male: {male: {descending: false}},
                age: {age: {descending: true}}
            });
        });

        it('should return field def', () => {
            assert.deepStrictEqual(table.getPkFieldDef(), '"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT');
            assert.deepStrictEqual(table.getPkFieldDef({autoIncrement: false}), '"id" INTEGER NOT NULL PRIMARY KEY');
            assert.deepStrictEqual(table.getPkFieldDef({
                collate: 'BINARY',
                autoIncrement: false
            }), '"id" INTEGER NOT NULL PRIMARY KEY');
            assert.deepStrictEqual(table.getPkFieldDef({
                collate: 'BINARY',
                onConflict: 'ignore',
                autoIncrement: false
            }), '"id" INTEGER NOT NULL ON CONFLICT IGNORE PRIMARY KEY');
            assert.deepStrictEqual(table.getUkFieldDef(), '"name" TEXT NOT NULL');
            assert.deepStrictEqual(table.getUkFieldDef({collate: 'binary'}), '"name" TEXT NOT NULL COLLATE BINARY');
            assert.deepStrictEqual(table.getUkFieldDef({onConflict: 'ignore'}), '"name" TEXT NOT NULL ON CONFLICT IGNORE');
            assert.deepStrictEqual(table.getFieldDef('age'), '"age" INTEGER NOT NULL');
            assert.deepStrictEqual(table.getFieldDef('age', {defaultValue: 0}), '"age" INTEGER NOT NULL DEFAULT 0');
            assert.deepStrictEqual(table.getFieldDef('age', {defaultValue: 20}), '"age" INTEGER NOT NULL DEFAULT 20');
            assert.deepStrictEqual(table.getFieldDef('age', {defaultValue: ''}), `"age" INTEGER NOT NULL DEFAULT ''`);
            assert.deepStrictEqual(table.getFieldDef('age', {defaultValue: -1}), '"age" INTEGER NOT NULL DEFAULT -1');
            assert.deepStrictEqual(table.getFieldDef('male'), '"male" INTEGER NOT NULL');
            assert.deepStrictEqual(table.getFieldDef('male', {defaultValue: true}), '"male" INTEGER NOT NULL DEFAULT 1');
            assert.deepStrictEqual(table.getFieldDef('male', {defaultValue: false}), '"male" INTEGER NOT NULL DEFAULT 0');
            assert.deepStrictEqual(table.getFieldDef('books'), `"books" TEXT NOT NULL`);
            assert.deepStrictEqual(table.getFieldDef('books', {defaultValue: []}), `"books" TEXT NOT NULL DEFAULT ''`);
            assert.deepStrictEqual(table.getFieldDef('books', {defaultValue: ['a', 'b', 'c']}), `"books" TEXT NOT NULL DEFAULT '["a","b","c"]'`);
            assert.deepStrictEqual(table.getFieldDef('json'), `"json" TEXT NOT NULL`);
            assert.deepStrictEqual(table.getFieldDef('json', {defaultValue: {}}), `"json" TEXT NOT NULL DEFAULT ''`);
            assert.deepStrictEqual(table.getFieldDef('json', {
                defaultValue: {
                    a: 1,
                    b: 2,
                    c: 3
                }
            }), `"json" TEXT NOT NULL DEFAULT '{"a":1,"b":2,"c":3}'`);
        });

        it('should return table def', () => {
            assert.strictEqual(table.getTableDef(), `CREATE TABLE IF NOT EXISTS "example_table" (
  "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL,
  "age" INTEGER NOT NULL,
  "male" INTEGER NOT NULL,
  "books" TEXT NOT NULL,
  "json" TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "main"."example_table_name"
ON "example_table" (
  name ASC
);
CREATE UNIQUE INDEX IF NOT EXISTS "main"."example_table_name_age_male"
ON "example_table" (
  name ASC,
  age ASC,
  male ASC
);
CREATE INDEX IF NOT EXISTS "main"."example_table_male"
ON "example_table" (
  male ASC
);
CREATE INDEX IF NOT EXISTS "main"."example_table_age"
ON "example_table" (
  age DESC
);`);
        });
    });

    describe('getTableDef()', () => {
        let table;

        before(() => {
            table = new Table(db, 'example_table', {
                id: ['int', {onConflict: 'ignore'}],
                name: ['string', {onConflict: 'ignore', collate: 'noCase'}],
                age: ['int', {defaultValue: 0}],
                male: ['boolean', {defaultValue: true}],
                books: ['array', {defaultValue: []}],
                json: ['object', {defaultValue: {}}],
            }, {
                pk: 'id',
                uk: 'name',
            });
        });

        it('should return table def', () => {
            assert.strictEqual(table.getTableDef(), `CREATE TABLE IF NOT EXISTS "example_table" (
  "id" INTEGER NOT NULL ON CONFLICT IGNORE PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL COLLATE NOCASE ON CONFLICT IGNORE,
  "age" INTEGER NOT NULL DEFAULT 0,
  "male" INTEGER NOT NULL DEFAULT 1,
  "books" TEXT NOT NULL DEFAULT '',
  "json" TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS "main"."example_table_name"
ON "example_table" (
  name ASC
);`);
        });
    });

    describe('getTableDef() 2', () => {
        let table;

        before(() => {
            table = new Table(db, 'example_table', {
                id: ['int', {onConflict: 'ignore'}],
                name: ['string', {onConflict: 'ignore', collate: 'noCase'}],
                age: ['int', 0],
                male: ['boolean', true],
                books: ['array', []],
                json: ['object', {}],
            }, {
                pk: 'id',
                uk: 'name',
            });
        });

        it('should return table def', () => {
            assert.strictEqual(table.getTableDef(), `CREATE TABLE IF NOT EXISTS "example_table" (
  "id" INTEGER NOT NULL ON CONFLICT IGNORE PRIMARY KEY AUTOINCREMENT,
  "name" TEXT NOT NULL COLLATE NOCASE ON CONFLICT IGNORE,
  "age" INTEGER NOT NULL DEFAULT 0,
  "male" INTEGER NOT NULL DEFAULT 1,
  "books" TEXT NOT NULL DEFAULT '',
  "json" TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS "main"."example_table_name"
ON "example_table" (
  name ASC
);`);
        });
    });

    describe('_getIndexes()', () => {
        it('should return indexes', () => {
            const fieldsSet = new Set(['a', 'b', 'c']);
            assert.deepStrictEqual(Table._getIndexes([], fieldsSet), {});
            assert.deepStrictEqual(Table._getIndexes([
                'a'
            ], fieldsSet), {
                a: {a: {descending: false}},
            });
            assert.deepStrictEqual(Table._getIndexes([
                ['a']
            ], fieldsSet), {
                a: {a: {descending: false}},
            });
            assert.deepStrictEqual(Table._getIndexes([
                'a', 'b', 'c'
            ], fieldsSet), {
                a: {a: {descending: false}},
                b: {b: {descending: false}},
                c: {c: {descending: false}},
            });
            assert.deepStrictEqual(Table._getIndexes([
                ['a', 'b', 'c']
            ], fieldsSet), {
                a_b_c: {
                    a: {descending: false},
                    b: {descending: false},
                    c: {descending: false},
                },
            });
            assert.deepStrictEqual(Table._getIndexes([
                ['a', 'b'],
                'c',
            ], fieldsSet), {
                a_b: {
                    a: {descending: false},
                    b: {descending: false},
                },
                c: {c: {descending: false}},
            });
            assert.deepStrictEqual(Table._getIndexes([{
                a: {descending: false},
            }], fieldsSet), {
                a: {a: {descending: false}},
            });
            assert.deepStrictEqual(Table._getIndexes([{
                a: {descending: true},
            }], fieldsSet), {
                a: {a: {descending: true}},
            });
            assert.deepStrictEqual(Table._getIndexes([{
                a: {descending: true},
                b: {descending: false},
                c: {descending: true},
            }], fieldsSet), {
                a_b_c: {
                    a: {descending: true},
                    b: {descending: false},
                    c: {descending: true},
                },
            });
        });
    });
});
