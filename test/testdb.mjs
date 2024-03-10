import { describe, it } from 'mocha';
import { assert, expect } from 'chai';
import { Db } from '../index.js';
import fs from "fs";

describe('Db', function() {
    describe('timestamp()', function() {
        it('should quote timestamp correctly', function() {
            const t = Math.floor(new Date().getTime() / 1000);
            assert.equal(Db.timestamp(), t);
            const t0 = 0;
            assert.equal(Db.timestamp(new Date(0)), t0); // 1970-01-01 0:00:00
        });
    });

    describe('identifier()', () => {
        it('should return the same string for a simple string', () => {
            const input = 'simpleString';
            const output = Db.identifier(input);
            expect(output).to.equal(input);
        });

        it('should return the same string for a string with letters, numbers, and underscores', () => {
            const input = 'string_With_123_Numbers';
            const output = Db.identifier(input);
            expect(output).to.equal(input);
        });

        it('should remove special characters from the string', () => {
            const input = '!@#$%^&*()+{}|:"<>?';
            const output = Db.identifier(input);
            expect(output).to.equal('');
        });

        it('should return an empty string for an empty input', () => {
            const input = '';
            const output = Db.identifier(input);
            expect(output).to.equal('');
        });

        it('should remove spaces from the string', () => {
            const input = 'string with spaces';
            const output = Db.identifier(input);
            expect(output).to.equal('stringwithspaces');
        });
    });

    describe('stripInvisibleCharacters()', () => {
        it('should remove invisible characters from a string', () => {
            const input = 'Hello\x00\x1f\tWorld\x08\r\n';
            const output = Db.stripInvisibleCharacters(input);
            expect(output).to.equal('Hello\tWorld\n');
        });

        it('should handle empty string', () => {
            const input = '';
            const output = Db.stripInvisibleCharacters(input);
            expect(output).to.equal('');
        });

        it('should handle string with no invisible characters', () => {
            const input = 'NoInvisibleCharacters';
            const output = Db.stripInvisibleCharacters(input);
            expect(output).to.equal(input);
        });

        it('should handle string with only invisible characters', () => {
            const input = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\x1f\x7f';
            const output = Db.stripInvisibleCharacters(input);
            expect(output).to.equal('');
        });

        it('should handle string with non-ASCII characters', () => {
            const input = '你好！\x00\x01\x02\x03\x04\x05\x06\x07\x08\x1f\x7f';
            const output = Db.stripInvisibleCharacters(input);
            expect(output).to.equal('你好！');
        });
    });

    describe('escape()', function() {
        it('should escape all special characters correctly', function() {
            assert.equal(Db.escape('\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\t\r\n' +
                '\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f' +
                ' !"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`' +
                'abcdefghijklmnopqrstuvwxyz{|}~\x7f'
            ),'\\t\\n\\t\\n' +
                '' +
                ' !"#$%%&\'\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`' +
                'abcdefghijklmnopqrstuvwxyz{|}~');
        });
    });

    describe('quote()', function() {
        it('should quote undefined correctly', function() {
            assert.equal(Db.quote(undefined), `''`);
        });

        it('should quote null correctly', function() {
            assert.equal(Db.quote(null), `''`);
        });

        it('should quote string correctly', function() {
            assert.equal(Db.quote(''), `''`);
            assert.equal(Db.quote('test string'), `'test string'`);
            assert.equal(Db.quote("string with 'single' quotes"), `'string with ''single'' quotes'`);
            assert.equal(Db.quote('string with "double" quotes'), `'string with "double" quotes'`);
            assert.equal(Db.quote('string with \\ backslashes'), `'string with \\\\ backslashes'`);
            assert.equal(
                Db.quote("special characters: \x00\t\r\n\x7f\"'\\%"),
                `'special characters: \\t\\n"''\\\\%%'`);
        });

        it('should quote object correctly', function() {
            assert.equal(Db.quote({}), `''`);
            assert.equal(Db.quote({ key: "can't" }), `'{"key":"can''t"}'`);
        });

        it('should quote date object correctly', function() {
            assert.equal(Db.quote(new Date(0)), `0`); //  1970/1/1
            assert.equal(Db.quote(new Date(1614892800123)), `1614892800`); //  2021/3/5
        });

        it('should quote primitive object correctly', function() {
            assert.equal(Db.quote(new Object('')), `''`);
            assert.equal(Db.quote(new Object("can't")), `'can''t'`);
            assert.equal(Db.quote(new Object(true)), `1`);
            assert.equal(Db.quote(new Object(false)), `0`);
            assert.equal(Db.quote(new Object(123)), `123`);
            assert.equal(Db.quote(new Object(123.0)), `123`);
            assert.equal(Db.quote(new Object(123.4)), `123.4`);
            assert.equal(Db.quote(new Object(Number.POSITIVE_INFINITY)), `''`);
            assert.equal(Db.quote(new Object(Number.NEGATIVE_INFINITY)), `''`);
            assert.equal(Db.quote(new Object(12345678901234567890n)), `'12345678901234567890'`);
        });

        it('should quote array correctly', function() {
            assert.equal(Db.quote([]), `''`);
            assert.equal(Db.quote([1, 2, 3]), `'[1,2,3]'`);
            assert.equal(Db.quote([1, 2, 3, [4, 5]]), `'[1,2,3,[4,5]]'`);
            assert.equal(Db.quote(['a', 'b', 'c', "can't"]), `'["a","b","c","can''t"]'`);
        });

        it('should quote boolean correctly', function() {
            assert.equal(Db.quote(true), `1`);
            assert.equal(Db.quote(false), `0`);
        });

        it('should quote number correctly', function() {
            assert.equal(Db.quote(123), `123`);
            assert.equal(Db.quote(123.0), `123`);
            assert.equal(Db.quote(123.4), `123.4`);
            assert.equal(Db.quote(Number.NaN), `''`);
            assert.equal(Db.quote(Number.POSITIVE_INFINITY), `''`);
            assert.equal(Db.quote(Number.NEGATIVE_INFINITY), `''`);
        });

        it('should quote bigint correctly', function() {
            assert.equal(Db.quote(12345678901234567890n), `'12345678901234567890'`);
        });

        it('should quote function correctly', function() {
            assert.equal(Db.quote(function () { return 'a' }), `''`);
        });

        it('should quote symbol correctly', function() {
            assert.equal(Db.quote(Symbol()), `''`);
            assert.equal(Db.quote(Symbol('test')), `''`);
        });
    });

    describe('constructor()', function() {
        it('should create a new Db instance with the default settings', async function () {
            const db = new Db();
            await db.close();
            assert.ok(db instanceof Db);
        });

        it('should create a new Db instance with custom file path and verbose mode', async function () {
            const db = new Db({file: 'test/test.db', isVerbose: true});
            await db.close();
            fs.unlinkSync('test/test.db');
            assert.ok(db instanceof Db);
        });

        it('should create a new Db instance with custom file path and non-verbose mode', async function () {
            const db = new Db({file: 'test/test.db', isVerbose: false});
            await db.close();
            fs.unlinkSync('test/test.db');
            assert.ok(db instanceof Db);
        });
    });

    describe('all()', () => {
        let db;

        before(() => {
            db = new Db({ file: ':memory:' });
            db.exec(`
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT
            );
        `);
            db.exec(`
            INSERT INTO test_table (name) VALUES ('Entry 1');
            INSERT INTO test_table (name) VALUES ('Entry 2');
            INSERT INTO test_table (name) VALUES ('Entry 3');
        `);
        });

        it('should return an array containing only field', async () => {
            const result = await db.all('SELECT name FROM test_table');
            const expected = [{ name: 'Entry 1' }, { name: 'Entry 2' }, { name: 'Entry 3' }];
            expect(result).to.deep.equal(expected);
        });

        it('should return an object with pk as keys and field as values', async () => {
            const result = await db.all('SELECT id, name FROM test_table', { field: 'name', pk: 'id' });
            const expected = { '1': 'Entry 1', '2': 'Entry 2', '3': 'Entry 3' };
            expect(result).to.deep.equal(expected);
        });

        it('should return an object with pk as keys and row as values', async () => {
            const result = await db.all('SELECT * FROM test_table', { pk: 'id' });
            const expected = { '1': { id: 1, name: 'Entry 1' }, '2': { id: 2, name: 'Entry 2' }, '3': { id: 3, name: 'Entry 3' } };
            expect(result).to.deep.equal(expected);
        });

        it('should return undefined if no rows match the query', async () => {
            const result = await db.all('SELECT * FROM test_table WHERE id = 100');
            expect(result).to.equal(undefined);
        });

        it('should return undefined if no rows match the query and field is specified', async () => {
            const result = await db.all('SELECT * FROM test_table WHERE id = 100', { field: 'name' });
            expect(result).to.equal(undefined);
        });

        it('should return undefined if no rows match the query and pk is specified', async () => {
            const result = await db.all('SELECT * FROM test_table WHERE id = 100', { pk: 'id' });
            expect(result).to.equal(undefined);
        });

        it('should return undefined if field or pk does not exist', async () => {
            const result1 = await db.all('SELECT * FROM test_table', { field: 'invalid_field' });
            expect(result1).to.equal(undefined);

            const result2 = await db.all('SELECT * FROM test_table', { pk: 'invalid_pk' });
            expect(result2).to.equal(undefined);
        });

        after(() => {
            db.close();
        });
    });

    describe('inserts()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({file: ':memory:', isVerbose: false});
            const sql = `CREATE TABLE users (name TEXT PRIMARY KEY, age INTEGER);`;
            await db.exec(sql);
            const rows = await db.all("SELECT name FROM sqlite_master WHERE type='table' AND name=?", {params: ['users']});
            assert.strictEqual(rows.length, 1);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should insert records into the table', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
            ];

            await db.inserts(table, fields, objects);
            const count = await db.count(table);
            expect(count).to.equal(objects.length);
        });

        it('should handle empty objects array', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = [];

            await db.inserts(table, fields, objects);

            const count = await db.count(table);
            expect(count).to.equal(0);
        });

        it('should insert records in chunks', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = Array.from({ length: 2000 }, (_, i) => ({
                name: `User ${i}`,
                age: Math.floor(Math.random() * 100),
            }));

            await db.inserts(table, fields, objects, { chunkSize: 100 });

            const count = await db.count(table);
            expect(count).to.equal(objects.length);
        });
    });

    describe('insert()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({file: ':memory:', isVerbose: false});
            const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should insert a record into the table', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const object = { name: 'Alice', age: 30 };

            const id = await db.insert(table, fields, object);

            const insertedName = await db.get(`SELECT name FROM ${table} WHERE id=?`, { params: [id], field: 'name' });
            expect(insertedName).to.equal(object.name);
        });

        it('should handle inserting with suspended mode', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const object = { name: 'Alice', age: 30 };
            const object2 = { name: 'Bob', age: 25 };
            db.suspendInsert();
            await db.insert(table, fields, object, { suspend: true });
            expect(await db.count(table)).to.equal(0);
            await db.insert(table, fields, object2, { suspend: true });
            expect(await db.count(table)).to.equal(0);
            await db.commitInserts();
            expect(await db.count(table)).to.equal(2);
            await db.commitInserts();
            expect(await db.count(table)).to.equal(2);
        });
    });

    describe('select()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({ file: ':memory:' });
            const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
                { name: 'Charlie', age: 35 },
            ];
            await db.inserts(table, fields, objects);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should select all records from the table', async () => {
            const table = 'users';
            const result = await db.select(table);
            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.deep.equal({ id: 1, name: 'Alice', age: 30 });
            expect(result[1]).to.deep.equal({ id: 2, name: 'Bob', age: 25 });
            expect(result[2]).to.deep.equal({ id: 3, name: 'Charlie', age: 35 });
        });

        it('should select specific fields from the table', async () => {
            const table = 'users';
            const result = await db.select(table, { fields: ['name'] });
            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.have.property('name');
            expect(result[0]).to.not.have.property('age');
        });

        it('should select records with pagination', async () => {
            const table = 'users';
            const result = await db.select(table, { limit: 2, page: 2 });
            expect(result).to.have.lengthOf(1);
            expect(result[0]).to.deep.equal({ id: 3, name: 'Charlie', age: 35 });
        });

        it('should select records with sorting', async () => {
            const table = 'users';
            const result = await db.select(table, { order: 'age', descending: true });
            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.deep.equal({ id: 3, name: 'Charlie', age: 35 });
            expect(result[1]).to.deep.equal({ id: 1, name: 'Alice', age: 30 });
            expect(result[2]).to.deep.equal({ id: 2, name: 'Bob', age: 25 });
        });
    });

    describe('select({pkAsRowKey})', () => {
        let db;

        before(async () => {
            db = new Db({file: ':memory:'});
            await db.exec(`
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT
                );
            `);
            await db.insert('test_table', ['name'], {name: 'Entry 1'});
            await db.insert('test_table', ['name'], {name: 'Entry 2'});
            await db.insert('test_table', ['name'], {name: 'Entry 3'});
        });

        it('should return an object with pk as keys if pkAsRowKey is true', async () => {
            const result = await db.select('test_table', {pk: 'id', pkAsRowKey: true});
            expect(result).to.deep.equal({
                1: {id: 1, name: 'Entry 1'},
                2: {id: 2, name: 'Entry 2'},
                3: {id: 3, name: 'Entry 3'}
            });
        });

        it('should return an array of objects if pkAsRowKey is false or not specified', async () => {
            const result1 = await db.select('test_table', {pk: 'id', pkAsRowKey: false});
            expect(result1).to.deep.equal([
                {id: 1, name: 'Entry 1'},
                {id: 2, name: 'Entry 2'},
                {id: 3, name: 'Entry 3'}
            ]);

            const result2 = await db.select('test_table', {pk: 'id'});
            expect(result2).to.deep.equal([
                {id: 1, name: 'Entry 1'},
                {id: 2, name: 'Entry 2'},
                {id: 3, name: 'Entry 3'}
            ]);
        });

        it('should return the row with primary key as key and specified field when pkAsRowKey is true and field is specified', async () => {
            const result3 = await db.select('test_table', { pk: 'id', pkAsRowKey: true, field: 'name', debug: true });
            expect(result3).to.deep.equal({
                1: 'Entry 1',
                2: 'Entry 2',
                3: 'Entry 3'
            });
        });

        it('should return an array of specified fields when pkAsRowKey is false or not specified and field is specified', async () => {
            const result4 = await db.select('test_table', { pk: 'id', field: 'name' });
            expect(result4).to.deep.equal(['Entry 1', 'Entry 2', 'Entry 3']);
        });

        after(() => {
            db.close();
        });
    });

    describe('update()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({ file: ':memory:', isVerbose: false });
            const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
                { name: 'Charlie', age: 35 }
            ];
            await db.inserts(table, fields, objects);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should update a record in the table', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const updateData = { name: 'David', age: 40 };
            const idToUpdate = 2;
            const changes = await db.update(table, fields, updateData, { pk: 'id', id: idToUpdate });
            expect(changes).to.equal(1);
            const updatedName = await db.get(`SELECT name FROM ${table} WHERE id=?`, { params: [idToUpdate], field: 'name' });
            expect(updatedName).to.equal('David');
        });

        it('should not update a record if the provided id does not exist', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const updateData = { name: 'David', age: 40 };
            const nonExistingId = 100;
            const changes = await db.update(table, fields, updateData, { pk: 'id', id: nonExistingId });
            expect(changes).to.equal(0);
        });
    });

    describe('replace()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({ file: ':memory:', isVerbose: false });
            const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
                { name: 'Charlie', age: 35 }
            ];
            await db.inserts(table, fields, objects);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should insert a new record into the table', async () => {
            const table = 'users';
            const fields = ['name', 'age'];
            const object = { name: 'David', age: 40 };
            const changes = await db.replace(table, fields, object);
            expect(changes).to.equal(1);
            const insertedName = await db.select(table, {pk: 'id', id: 4, field: 'name'});
            expect(insertedName).to.equal('David');
        });

        it('should replace an existing record in the table', async () => {
            const table = 'users';
            const fields = ['id', 'name', 'age'];
            const changes = await db.replace(table, fields, { id: 2, name: 'David', age: 40 });
            expect(changes).to.equal(1);
            const updatedName = await db.select(table, {pk: 'id', id: 2, field: 'name'});
            expect(updatedName).to.equal('David');
        });
    });

    describe('delete()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({ file: ':memory:', isVerbose: false });
            const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
                { name: 'Charlie', age: 35 }
            ];
            await db.inserts(table, fields, objects);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should delete a record from the table', async () => {
            const table = 'users';
            const idToDelete = 2;
            const changes = await db.delete(table, { pk: 'id', id: idToDelete });
            expect(changes).to.equal(1);
            const deletedRecord = await db.select(table, { pk: 'id', id: idToDelete });
            expect(deletedRecord).to.be.undefined;
        });

        it('should not delete a record if the provided id does not exist', async () => {
            const table = 'users';
            const nonExistingId = 100;
            const changes = await db.delete(table, { pk: 'id', id: nonExistingId });
            expect(changes).to.equal(0);
        });
    });

    describe('count()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({ file: ':memory:', isVerbose: false });
            const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
            const table = 'users';
            const fields = ['name', 'age'];
            const objects = [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
                { name: 'Charlie', age: 35 }
            ];
            await db.inserts(table, fields, objects);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should return the total number of records in the table', async () => {
            const table = 'users';
            const count = await db.count(table);
            expect(count).to.equal(3);
        });

        it('should throw an error if the table does not exist', async () => {
            const table = 'non_existing_table';
            try {
                await db.count(table);
                expect.fail('Expected error but did not occur');
            } catch (error) {
                expect(error.message).to.contains('non_existing_table');
            }
        });
    });
});
