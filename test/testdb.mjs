import { describe, it } from 'mocha';
import { assert, expect } from 'chai';
import {Db} from '../index.js';
import fs from "fs";

describe('Db', function() {
    describe('timestamp()', function() {
        it('should quote timestamp correctly', function() {
            assert.strictEqual(Db.timestamp(), Math.floor(new Date().getTime() / 1000));
            assert.strictEqual(Db.timestamp(new Date(0)), 0); // 1970-01-01 0:00:00
        });
    });

    describe('identifier()', () => {
        it('should valid', () => {
            assert.strictEqual(Db.identifier('_'), '_');
            for (let i = 97; i <= 122; i++) { // a-z
                const s = String.fromCharCode(i);
                assert.strictEqual(Db.identifier(s), s);
            }
            for (let i = 65; i <= 90; i++) { // A-Z
                const s = String.fromCharCode(i);
                assert.strictEqual(Db.identifier(s), s);
            }
            assert.strictEqual(Db.identifier('_0'), '_0');
            assert.strictEqual(Db.identifier('a0'), 'a0');
        });

        it('should invalid', () => {
            for (let i = 0; i <= 9; i++) { // 0-9
                assert.throw(() => {Db.identifier(`${i}`)}, `Identifier '${i}' invalid.`);
            }
            for (let i = 0; i < 60; i++) {
                const s = String.fromCharCode(i);
                assert.throw(() => {Db.identifier(s)}, `Identifier '${s}' invalid.`);
            }
            for (let i = 133; i <= 140; i++) {
                const s = String.fromCharCode(i);
                assert.throw(() => {Db.identifier(s)}, `Identifier '${s}' invalid.`);
            }
            for (let i = 173; i <= 2000; i++) {
                const s = String.fromCharCode(i);
                assert.throw(() => {Db.identifier(s)}, `Identifier '${s}' invalid.`);
            }
            assert.throw(() => {Db.identifier('')}, `Identifier '' invalid.`);
            assert.throw(() => {Db.identifier('a你好')}, `Identifier 'a你好' invalid.`);
        });
    });

    describe('stripInvisibleCharacters()', () => {
        it('should remove invisible characters from a string', () => {
            const input = 'Hello\x00\x1f\tWorld\x08\r\n';
            const output = Db._stripInvisibleCharacters(input);
            expect(output).to.equal('Hello\tWorld\n');
        });

        it('should handle empty string', () => {
            const input = '';
            const output = Db._stripInvisibleCharacters(input);
            expect(output).to.equal('');
        });

        it('should handle string with no invisible characters', () => {
            const input = 'NoInvisibleCharacters';
            const output = Db._stripInvisibleCharacters(input);
            expect(output).to.equal(input);
        });

        it('should handle string with only invisible characters', () => {
            const input = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\x1f\x7f';
            const output = Db._stripInvisibleCharacters(input);
            expect(output).to.equal('');
        });

        it('should handle string with non-ASCII characters', () => {
            const input = '你好！\x00\x01\x02\x03\x04\x05\x06\x07\x08\x1f\x7f';
            const output = Db._stripInvisibleCharacters(input);
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
                ' !"#$%&\'\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\\\]^_`' +
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
                `'special characters: \\t\\n"''\\\\%'`);
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

        before(async () => {
            db = new Db();
            await db.exec('CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);');
            await db.inserts('test_table', ['name'], [
                {name: 'Entry 1'},
                {name: 'Entry 2'},
                {name: 'Entry 3'},
            ]);
        });

        after(async () => {
            await db.close();
        });

        it('should reject with an error if the table does not exist', async () => {
            const nonExistingTable = 'non_existing_table';
            const sql = `SELECT * FROM ${nonExistingTable};`;
            try {
                await db.all(sql);
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.include(error.message, `no such table: ${nonExistingTable}`);
            }
        });

        it("should return an array|set|map|object", async () => {
            const sql = `SELECT * FROM test_table;`;
            const arr1 = [{ id: 1, name: 'Entry 1' }, { id: 2, name: 'Entry 2' }, { id: 3, name: 'Entry 3' }];
            const arr2 = ['Entry 1', 'Entry 2', 'Entry 3'];
            const arr3 = [1, 2, 3];
            const arr4 = ['1', '2', '3'];
            const arr5 = [true, true, true];
            const set1 = new Set(['Entry 1', 'Entry 2', 'Entry 3']);
            const set2 = new Set([1, 2, 3]);
            const set3 = new Set(['1', '2', '3']);
            const map1 = new Map([[1, { id: 1, name: 'Entry 1' }], [2, { id: 2, name: 'Entry 2' }], [3, { id: 3, name: 'Entry 3' }]]);
            const map2 = new Map([[1, 'Entry 1'], [2, 'Entry 2'], [3, 'Entry 3']]);
            const map3 = new Map([['Entry 1', 1], ['Entry 2', 2], ['Entry 3', 3]]);
            const obj1 = {1: { id: 1, name: 'Entry 1' }, 2: { id: 2, name: 'Entry 2' }, 3: { id: 3, name: 'Entry 3' }};
            const obj2 = {1: 'Entry 1', 2: 'Entry 2', 3: 'Entry 3'};
            const obj3 = {'Entry 1': 1, 'Entry 2': 2, 'Entry 3': 3};
            assert.deepStrictEqual(await db.all(sql), arr1);
            assert.deepStrictEqual(await db.all(sql, { resultField: 'name'}), arr2);
            assert.deepStrictEqual(await db.all(sql, { resultField: 'id'}), arr3);
            assert.deepStrictEqual(await db.all(sql, { resultField: 'id', fieldTypes: {id: 'int'}}), arr3);
            assert.deepStrictEqual(await db.all(sql, { resultField: 'id', fieldTypes: {id: 'float'}}), arr3);
            assert.deepStrictEqual(await db.all(sql, { resultField: 'id', fieldTypes: {id: 'string'}}), arr4);
            assert.deepStrictEqual(await db.all(sql, { resultField: 'id', fieldTypes: {id: 'boolean'}}), arr5);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'set', resultField: 'name' }), set1);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'set', resultField: 'id' }), set2);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'set', resultField: 'id', fieldTypes: {id: 'string'}}), set3);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'map', resultKey: 'id' }), map1);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'map', resultKey: 'id', resultField: 'name' }), map2);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'map', resultKey: 'name', resultField: 'id' }), map3);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'object', resultKey: 'id' }), obj1);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'object', resultKey: 'id', resultField: 'name' }), obj2);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'object', resultKey: 'name', resultField: 'id' }), obj3);
        });

        it("should return empty data", async () => {
            const sql = `SELECT * FROM test_table WHERE id>3;`;
            assert.deepStrictEqual(await db.all(sql), []);
            assert.deepStrictEqual(await db.all(sql, { resultType: '' }), []);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'array' }), []);
            assert.deepStrictEqual(await db.all(sql, { resultType: 'set' }), new Set());
            assert.deepStrictEqual(await db.all(sql, { resultType: 'map' }), new Map());
            assert.deepStrictEqual(await db.all(sql, { resultType: 'object' }), {});
        });

        it('should reject with an error', async () => {
            try {
                await db.all('SELECT * FROM non_existing_table;');
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.include(error.message, 'no such table: non_existing_table');
            }
            const sql = 'SELECT * FROM test_table;';
            try {
                await db.all(sql, {resultType: 'set'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultField '' does not exist.");
            }
            try {
                await db.all(sql, {resultType: 'set', resultKey: 'id'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultField '' does not exist.");
            }
            try {
                await db.all(sql, {resultType: 'map'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultKey '' does not exist.");
            }
            try {
                await db.all(sql, {resultType: 'object'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultKey '' does not exist.");
            }
            try {
                await db.all(sql, {resultType: 'map', resultKey: 'nonexistent'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultKey 'nonexistent' does not exist.");
            }
            try {
                await db.all(sql, {resultType: 'object', resultKey: 'nonexistent'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultKey 'nonexistent' does not exist.");
            }
            try {
                await db.all(sql, {resultType: 'map', resultKey: 'id', resultField: 'nonexistent'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultField 'nonexistent' does not exist.");
            }
            try {
                await db.all(sql, {resultType: 'object', resultKey: 'id', resultField: 'nonexistent'});
                assert.fail('Expected an error to be thrown');
            } catch (error) {
                assert.strictEqual(error.message, "resultField 'nonexistent' does not exist.");
            }
        });
    });

    describe('get()', () => {
        let db;

        before(async () => {
            db = new Db();
            await db.exec(`
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    age INTEGER,
                    male INTEGER
                );
            `);
            await db.inserts('test_table', ['name', 'age', 'male'], [
                {name: 'Alice', age: 30, male: 0},
                {name: 'Bob', age: 25, male: 1},
                {name: 'Charlie', age: 35, male: 1},
            ]);
        });

        after(async () => {
            await db.close();
        });

        it("should return an string|int|float|boolean|array|object", async () => {
            const sql = 'SELECT * FROM test_table WHERE id=?;';
            assert.deepStrictEqual(await db.get(sql, {params: 1}), { id: 1, name: 'Alice', age: 30, male: 0 });
            assert.strictEqual(await db.get(sql, {params: 1, resultField: 'name'}), 'Alice');
            assert.strictEqual(await db.get(sql, {params: 1, resultField: 'age'}), 30);
            assert.strictEqual(await db.get(sql, {params: 1, resultField: 'age', fieldTypes: {age: 'string'}}), '30');
            assert.strictEqual(await db.get(sql, {params: 1, resultField: 'male'}), 0);
            assert.strictEqual(await db.get(sql, {params: 1, resultField: 'male', fieldTypes: {male: 'boolean'}}), false);
            assert.strictEqual(await db.get(sql, {params: 2, resultField: 'male'}), 1);
            assert.strictEqual(await db.get(sql, {params: 2, resultField: 'male', fieldTypes: {male: 'boolean'}}), true);
        });
    });

    describe('inserts()', () => {
        let db;

        beforeEach(async () => {
            db = new Db();
            await db.exec('CREATE TABLE test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);');
        });

        afterEach(async () => {
            await db.close();
        });

        it('should insert records into the table', async () => {
            const num = await db.inserts('test_table', ['name', 'age'], [
                { name: 'Alice', age: 30 },
                { name: 'Bob', age: 25 },
                { name: 'Charlie', age: 35 },
            ]);
            assert.strictEqual(num, 3);
            assert.strictEqual(await db.count('test_table'), 3);
        });

        it('should handle empty objects array', async () => {
            const num = await db.inserts('test_table', ['name', 'age'], []);
            assert.strictEqual(num, 0);
            assert.strictEqual(await db.count('test_table'), 0);
        });

        it('should handle duplicated objects array', async () => {
            try {
                await db.inserts('test_table', ['id', 'name', 'age'], [
                    { id: 1, name: 'Alice', age: 30 },
                    { id: 2, name: 'Bob', age: 25 },
                    { id: 2, name: 'Charlie', age: 35 },
                ]);
                assert.fail('Expected an error to be thrown');
            } catch (err) {
                assert.strictEqual(await db.count('test_table'), 0);
            }
        });

        it('should insert records in chunks', async () => {
            const objects = Array.from({ length: 2000 }, (_, i) => ({
                name: `User ${i}`,
                age: Math.floor(Math.random() * 100),
            }));
            const num = await db.inserts('test_table', ['name', 'age'], objects, { chunkSize: 100 });
            assert.strictEqual(num, objects.length);
            assert.strictEqual(await db.count('test_table'), objects.length);
        });
    });

    describe('insert()', () => {
        let db;

        beforeEach(async () => {
            db = new Db({});
            const sql = `CREATE TABLE test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should insert a record into the table', async () => {
            const table = 'test_table';
            const fields = ['name', 'age'];
            assert.strictEqual(await db.insert(table, fields, { name: 'Alice', age: 30 }), 1);
            assert.deepStrictEqual(await db.select(table, { where: [['id', 1]] }), {id: 1, name: 'Alice', age: 30 });
            assert.strictEqual(await db.insert(table, fields, { name: 'Bob', age: 25 }), 2);
            assert.deepStrictEqual(await db.select(table, { where: [['id', 2]] }), {id: 2, name: 'Bob', age: 25 });
            try {
                await db.insert(table, ['id', 'name', 'age'], { id: 2, name: 'Charlie', age: 35 });
                assert.fail('Expected an error to be thrown');
            } catch (err) {
                assert.strictEqual(await db.count(table), 2);
            }
        });

        it('should handle inserting with suspended mode', async () => {
            const table = 'test_table';
            const fields = ['name', 'age'];
            db.suspendInsert();
            assert.strictEqual(await db.insert(table, fields, { name: 'Alice', age: 30 }), 0);
            assert.strictEqual(await db.count(table), 0);
            assert.strictEqual(await db.insert(table, fields, { name: 'Bob', age: 25 }), 0);
            assert.strictEqual(await db.count(table), 0);
            assert.strictEqual(await db.insert(table, fields, { name: 'Charlie', age: 35 }, {immediate: true}), 1);
            assert.strictEqual(await db.count(table), 1);
            await db.commitInserts();
            assert.strictEqual(await db.count(table), 3);
            assert.deepStrictEqual(await db.selects(table), [
                {id: 1, name: 'Charlie', age: 35},
                {id: 2, name: 'Alice', age: 30},
                {id: 3, name: 'Bob', age: 25},
            ]);
            db.suspendInsert();
            assert.strictEqual(await db.insert(table, fields, { name: 'David', age: 40 }), 0);
            assert.strictEqual(await db.count(table), 3);
            await db.commitInserts();
            assert.strictEqual(await db.count(table), 4);
            assert.deepStrictEqual(await db.selects(table), [
                {id: 1, name: 'Charlie', age: 35},
                {id: 2, name: 'Alice', age: 30},
                {id: 3, name: 'Bob', age: 25},
                {id: 4, name: 'David', age: 40},
            ]);
        });
    });

    describe('select()', () => {
        let db;
        const table = 'test_table';
        const objects = [
            {id: 1, name: 'Alice', age: 30, male: 0, books: ['Book 1', 'Book 2', 'Book 3']},
            {id: 2, name: 'Bob', age: 25, male: 1, books: []},
            {id: 3, name: 'Charlie', age: 35, male: 1, books: ['Book 4']},
        ];

        before(async () => {
            db = new Db({});
            await db.exec(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, male INTEGER, books TEXT);`);
            await db.inserts(table, ['name', 'age', 'male', 'books'], objects);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should select record from the table', async () => {
            assert.deepStrictEqual(await db.select(table, {fieldTypes: {books: 'array'}}), objects[0]);
        });
    });

    describe('lastId()', () => {
        let db;
        const table = 'test_table';
        const objects = [
            {id: 1, name: 'Alice', age: 30, male: 0, books: ['Book 1', 'Book 2', 'Book 3']},
            {id: 2, name: 'Bob', age: 25, male: 1, books: []},
            {id: 3, name: 'Charlie', age: 35, male: 1, books: ['Book 4']},
        ];

        before(async () => {
            db = new Db({});
            await db.exec(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER, male INTEGER, books TEXT);`);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should return last id', async () => {
            assert.strictEqual(await db.lastId(table, 'id'), 0);
            await db.insert(table, ['name', 'age', 'male', 'books'], objects[0]);
            assert.strictEqual(await db.lastId(table, 'id'), 1);
            await db.insert(table, ['name', 'age', 'male', 'books'], objects[1]);
            assert.strictEqual(await db.lastId(table, 'id'), 2);
            await db.insert(table, ['name', 'age', 'male', 'books'], objects[2]);
            assert.strictEqual(await db.lastId(table, 'id'), 3);
        });
    });

    describe('update()', () => {
        let db;
        const table = 'test_table';

        beforeEach(async () => {
            db = new Db();
            const sql = `CREATE TABLE ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
            const objects = [
                { id: 1, name: 'Alice', age: 30 },
                { id: 2, name: 'Bob', age: 25 },
                { id: 3, name: 'Charlie', age: 35 }
            ];
            await db.inserts(table, ['id', 'name', 'age'], objects);
        });

        afterEach(async () => {
            await db.close();
        });

        it('should update a record in the table', async () => {
            assert.strictEqual(await db.update(table, ['name', 'age'], { name: 'David', age: 40 }, {where: [['id', 2]]}), 1)
            assert.strictEqual(await db.select(table, {resultField: 'name', where: [['id', 2]]}), 'David');
        });

        it('should not update a record if the provided id does not exist', async () => {
            assert.strictEqual(await db.update(table, ['name', 'age'], { name: 'David', age: 40 }, {where: [['id', 100]]}), 0);
        });
    });

    describe('replace()', () => {
        let db;
        const table = 'test_table';

        beforeEach(async () => {
            db = new Db();
            const sql = `CREATE TABLE ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
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
            assert.strictEqual(await db.replace(table, ['name', 'age'], { name: 'David', age: 40 }), 1)
            assert.strictEqual(await db.select(table, {where: [['id', 4]], resultField: 'name'}), 'David');
        });

        it('should replace an existing record in the table', async () => {
            assert.strictEqual(await db.replace(table, ['id', 'name', 'age'], { id: 2, name: 'David', age: 40 }), 1);
            assert.strictEqual(await db.select(table, {where: [['id', 2]], resultField: 'name'}), 'David');
        });
    });

    describe('delete()', () => {
        let db;
        const table = 'test_table';

        beforeEach(async () => {
            db = new Db({});
            const sql = `CREATE TABLE ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
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
            assert.strictEqual(await db.delete(table, {where: [['id', 2]]}), 1);
            assert.strictEqual(await db.select(table, {where: [['id', 2]]}), null);
        });

        it('should not delete a record if the provided id does not exist', async () => {
            assert.strictEqual(await db.delete(table, {where: [['id', 100]]}), 0);
        });
    });

    describe('count()', () => {
        let db;
        const table = 'test_table';

        beforeEach(async () => {
            db = new Db({});
            const sql = `CREATE TABLE ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER);`;
            await db.exec(sql);
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
            assert.strictEqual(await db.count(table), 3);
        });

        it('should throw an error if the table does not exist', async () => {
            try {
                await db.count('non_existing_table');
                expect.fail('Expected error but did not occur');
            } catch (error) {
                expect(error.message).to.contains('non_existing_table');
            }
        });
    });
});
