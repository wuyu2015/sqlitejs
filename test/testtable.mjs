import { expect } from 'chai';
import { Db, Table } from '../index.js';

let db;
let exampleTable;
async function createTable() {
    db = new Db({file: ':memory:'});
    await db.exec(`
        CREATE TABLE IF NOT EXISTS example_table (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        );
    `);
    exampleTable = new Table(db, 'example_table', {pk: 'id', fields: ['name']});
}

async function closeTable() {
    await db.close();
}

describe('Table', () => {

    describe('Table Constructor', () => {
        before(createTable);

        it('should correctly initialize the Table object with primary key and fields', async () => {
            const exampleTable = new Table(db, 'example_table', { pk: 'id', fields: ['name', 'age'] });
            expect(exampleTable.db).to.equal(db);
            expect(exampleTable.name).to.equal('example_table');
            expect(exampleTable.pk).to.equal('id');
            expect(exampleTable.fields).to.deep.equal(['name', 'age']);
        });

        it('should correctly initialize the Table object with primary key and fields excluding primary key', async () => {
            const exampleTable = new Table(db, 'example_table', { pk: 'id', fields: ['id', 'name', 'age'] });
            expect(exampleTable.db).to.equal(db);
            expect(exampleTable.name).to.equal('example_table');
            expect(exampleTable.pk).to.equal('id');
            expect(exampleTable.fields).to.deep.equal(['name', 'age']);
        });

        it('should remove duplicate fields from the fields array', async () => {
            const exampleTable = new Table(db, 'example_table', { pk: 'id', fields: ['id', 'name', 'name', 'age'] });
            expect(exampleTable.db).to.equal(db);
            expect(exampleTable.name).to.equal('example_table');
            expect(exampleTable.pk).to.equal('id');
            expect(exampleTable.fields).to.deep.equal(['name', 'age']);
        });

        after(closeTable);
    });

    describe('inserts()', () => {
        before(createTable);

        it('should insert multiple objects into the table', async () => {
            expect(await exampleTable.inserts([{ name: 'Entry 1' }, { name: 'Entry 2' }, { name: 'Entry 3' }])).to.equal(3);
        });

        after(closeTable);
    });

    describe('insert()', () => {
        before(createTable);

        it('should insert a single object into the table', async () => {
            expect(await exampleTable.insert({ name: 'Entry 1' })).to.equal(1);
            expect(await exampleTable.insert({ name: 'Entry 2' })).to.equal(2);
        });

        it('should return 0 if object does not contain primary key value', async () => {
            expect(await exampleTable.insert({ invalidField: 'Entry 1' })).to.equal(0);
        });

        it('should throw an error if primary key value already exists in the table', async () => {
            try {
                await exampleTable.insert({ id: 1, name: 'Entry 1' });
            } catch (error) {
                expect(error).to.be.an.instanceOf(Error);
            }
        });

        after(closeTable);
    });

    describe('select()', () => {
        it('should return the row matching the specified primary key value', async () => {
            await createTable();

            // Inserting some test data
            await exampleTable.insert({ name: 'Entry 1' });
            await exampleTable.insert({ name: 'Entry 2' });
            await exampleTable.insert({ name: 'Entry 3' });

            // Selecting row by primary key value
            let row = await exampleTable.select({ id: 1 });
            expect(row).to.deep.equal({ id: 1, name: 'Entry 1' });

            // Selecting non-existent row by primary key value
            row = await exampleTable.select({ id: 999 });
            expect(row).to.be.undefined;

            await closeTable();
        });

        it('should return an array of rows matching the specified criteria', async () => {
            await createTable();

            // Inserting some test data
            await exampleTable.insert({ name: 'Entry 1' });
            await exampleTable.insert({ name: 'Entry 2' });
            await exampleTable.insert({ name: 'Entry 3' });

            // Selecting rows without any criteria
            let rows = await exampleTable.select();
            expect(rows).to.have.lengthOf(3);
            expect(rows[0]).to.deep.equal({ id: 1, name: 'Entry 1' });
            expect(rows[1]).to.deep.equal({ id: 2, name: 'Entry 2' });
            expect(rows[2]).to.deep.equal({ id: 3, name: 'Entry 3' });

            // Selecting rows by other criteria
            rows = await exampleTable.select({ fields: ['name'] });
            expect(rows).to.have.lengthOf(3);
            expect(rows[0]).to.deep.equal({ name: 'Entry 1' });
            expect(rows[1]).to.deep.equal({ name: 'Entry 2' });
            expect(rows[2]).to.deep.equal({ name: 'Entry 3' });

            await closeTable();
        });
    });

    describe('select({pkAsRowKey})', () => {
        before(async () => {
            await createTable();
            await exampleTable.insert({ name: 'Entry 1' });
            await exampleTable.insert({ name: 'Entry 2' });
            await exampleTable.insert({ name: 'Entry 3' });
        });

        it('should return an object with primary key as keys and row as values when pkAsRowKey is true', async () => {
            const result = await exampleTable.select({pkAsRowKey: true, debug: true});
            expect(result).to.eql({
                1: {id: 1, name: 'Entry 1'},
                2: {id: 2, name: 'Entry 2'},
                3: {id: 3, name: 'Entry 3'}
            });
        });

        it('should return an object with primary key as keys and field as values when pkAsRowKey is true and field is specified', async () => {
            const result = await exampleTable.select({field: 'name', pkAsRowKey: true});
            expect(result).to.eql({1: 'Entry 1', 2: 'Entry 2', 3: 'Entry 3'});
        });

        it('should return an array containing only field when field is specified', async () => {
            const result = await exampleTable.select({field: 'name'});
            expect(result).to.eql(['Entry 1', 'Entry 2', 'Entry 3']);
        });

        it('should return all rows when pkAsRowKey is false', async () => {
            const result = await exampleTable.select({pkAsRowKey: false});
            expect(result).to.have.lengthOf(3);
            expect(result[0]).to.eql({id: 1, name: 'Entry 1'});
            expect(result[1]).to.eql({id: 2, name: 'Entry 2'});
            expect(result[2]).to.eql({id: 3, name: 'Entry 3'});
        });

        after(async () => {
            await closeTable();
        });
    });

    describe('update()', () => {
        before(createTable);

        it('should update an existing row in the table', async () => {
            // Inserting some test data
            await exampleTable.insert({ name: 'Entry 1' });

            // Updating an existing row
            const result = await exampleTable.update({ id: 1, name: 'Updated Entry 1' });
            expect(result).to.equal(1);

            // Verifying the updated row
            const updatedRow = await exampleTable.select({ id: 1 });
            expect(updatedRow).to.deep.equal({ id: 1, name: 'Updated Entry 1' });
        });

        it('should return 0 if trying to update a non-existent row', async () => {
            // Trying to update a non-existent row
            const result = await exampleTable.update({ id: 999, name: 'Non-existent Entry' });
            expect(result).to.equal(0);
        });

        it('should return 0 if primary key value is not provided', async () => {
            // Trying to update without providing primary key value
            const result = await exampleTable.update({ name: 'New Entry' });
            expect(result).to.equal(0);
        });

        it('should return 0 if provided primary key value does not exist in the table', async () => {
            // Inserting some test data
            await exampleTable.insert({ name: 'Entry 1' });

            // Trying to update with a non-existent primary key value
            const result = await exampleTable.update({ id: 999, name: 'Non-existent Entry' });
            expect(result).to.equal(0);
        });

        after(closeTable);
    });

    describe('replace()', () => {
        before(createTable);

        it('should replace an existing row in the table', async () => {
            // Inserting some test data
            await exampleTable.insert({ name: 'Entry 1' });

            // Replacing an existing row
            await exampleTable.replace({ id: 1, name: 'Replaced Entry 1' });

            // Verifying the replaced row
            const replacedRow = await exampleTable.select({ id: 1 });
            expect(replacedRow).to.deep.equal({ id: 1, name: 'Replaced Entry 1' });
        });

        it('should insert a new row if the provided primary key value does not exist in the table', async () => {
            // Replacing a non-existent row
            await exampleTable.replace({ id: 999, name: 'New Entry' });

            // Verifying the replaced row
            const newEntry = await exampleTable.select({ id: 999 });
            expect(newEntry).to.deep.equal({ id: 999, name: 'New Entry' });
        });

        after(closeTable);
    });

    describe('delete()', () => {
        before(createTable);

        it('should delete an existing row from the table', async () => {
            // Inserting some test data
            await exampleTable.insert({ name: 'Entry to be deleted' });

            // Deleting an existing row
            await exampleTable.delete({ id: 1 });

            // Verifying the row has been deleted
            const deletedRow = await exampleTable.select({ id: 1 });
            expect(deletedRow).to.be.undefined;
        });

        it('should return 0 if trying to delete a non-existent row', async () => {
            // Deleting a non-existent row
            const result = await exampleTable.delete({ id: 999 });

            // Verifying the result
            expect(result).to.equal(0);
        });

        after(closeTable);
    });

    describe('count()', () => {
        before(createTable);

        it('should return the correct count of rows in the table', async () => {
            // Inserting some test data
            await exampleTable.inserts([{ name: 'Entry 1' }, { name: 'Entry 2' }, { name: 'Entry 3' }]);

            // Getting the count of rows
            const rowCount = await exampleTable.count();

            // Verifying the count
            expect(rowCount).to.equal(3);
        });

        it('should return 0 if the table is empty', async () => {
            // Deleting all rows from the table
            await db.exec('DELETE FROM example_table');

            // Getting the count of rows
            const rowCount = await exampleTable.count();

            // Verifying the count
            expect(rowCount).to.equal(0);
        });

        after(closeTable);
    });
});
