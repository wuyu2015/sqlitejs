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
            assert.deepStrictEqual(await exampleTable.select({pk: 2}), {id: 2, name: 'Bob', age: NaN, male: false, books: [], json: {}});
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
});
