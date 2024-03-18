import {Db} from '../index.js';

class Table {
    constructor(db, name, fieldTypes, {pk, uk = ''} = {}) {
        if (!(pk in fieldTypes)) {
            throw new Error('fieldTypes must contains pk.');
        }
        this.db = db;
        this.name = Db.identifier(name);
        this.fieldTypes = {};
        for (const field in fieldTypes) {
            this.fieldTypes[Db.identifier(field)] = fieldTypes[field];
        }
        this.fieldsWithPk = Object.keys(fieldTypes);
        this.filedsSetWithPk = new Set(this.fieldsWithPk);
        delete fieldTypes[pk];
        this.fields = Object.keys(fieldTypes);
        this.filedsSet = new Set(this.fields);
        this.pk = pk;
        this.uk = uk in fieldTypes ? uk : '';
    }

    _getAvailableFieldsInObject(object) {
        return this.fieldsWithPk.filter(field => field in object);
    }

    _getAvailableFieldsInArray(arr) {
        return arr.filter(field => this.filedsSetWithPk.has(field));
    }

    getCreateTableSql() {
        return '';
    }

    getCreatePkUkTableSql() {
        if (this.fieldsWithPk.length === 2 && this.pk.length && this.uk.length) {
            const pkStmt = this.fieldTypes[this.pk] === 'int'
                ? `"${this.pk}" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT`
                : `"${this.pk}" TEXT NOT NULL COLLATE BINARY PRIMARY KEY`;
            const ukStmt = this.fieldTypes[this.uk] === 'int'
                ? `"${this.uk}" INTEGER NOT NULL`
                : `"${this.uk}" INTEGER NOT NULL COLLATE BINARY`;
            return `
                CREATE TABLE IF NOT EXISTS "${this.name}" (
                  ${pkStmt},
                  ${ukStmt}
                );
                CREATE UNIQUE INDEX IF NOT EXISTS "main"."${this.name}_${this.uk}"
                ON "${this.name}" (
                  "${this.uk}" ASC
                );
            `;
        }
        return '';
    }

    async createTable() {
        const sql = this.getCreateTableSql();
        if (sql.length) {
            return await this.db.exec(sql);
        }
    }

    async createPkUkTable() {
        const sql = this.getCreatePkUkTableSql();
        if (sql.length) {
            return await this.db.exec(sql);
        }
    }

    inserts(objects, {fields = [], chunkSize = 1000} = {}) {
        if (!objects.length) {
            return 0;
        }
        fields = fields.length ? this._getAvailableFieldsInArray(fields) : this._getAvailableFieldsInObject(objects[0]);
        if (!fields.length) {
            return 0;
        }
        return this.db.inserts(this.name, fields, objects, {chunkSize});
    }

    insert(object, {fields = [],immediate = false} = {}) {
        fields = fields.length ? this._getAvailableFieldsInArray(fields) : this._getAvailableFieldsInObject(object);
        if (!fields.length) {
            return 0;
        }
        return this.db.insert(this.name, fields, object, {immediate});
    }

    suspendInsert() {
        this.db.suspendInsert();
    }

    async commitInserts({chunkSize = 1000} = {}) {
        await this.db.commitInserts({chunkSize});
    }

    selects({
        distinct = false,
        fields = [],
        where = [],
        order = '',
        descending = false,
        limit = 0,
        page = 1,
        resultType = 'array',
        resultKey = '',
        resultField = '',
        pkAsResultKey = false,
        ukAsResultKey = false,
    } = {}) {
        if (!order.length) {
            order = this.pk;
        }
        if (pkAsResultKey) {
            resultKey = this.pk;
        } else if (ukAsResultKey && this.uk.length) {
            resultKey = this.uk;
        }
        return this.db.selects(this.name, {distinct, fields, where, order, descending, limit, page, resultType, resultKey, resultField, fieldTypes: this.fieldTypes});
    }

    select({
        fields = [],
        pk = 0,
        uk = 0,
        where = [],
        resultField = '',
    } = {}) {
        if (pk !== 0) {
            where = [[this.pk, pk]];
        } else if (uk !== 0 && this.uk.length) {
            where = [[this.uk, uk]];
        }
        return this.db.select(this.name, {fields, where, resultField, fieldTypes: this.fieldTypes})
    }

    update(object, {
        where = [],
        pk = 0,
    } = {}) {
        if (pk !== 0) {
            where = [[this.pk, pk]];
            delete object[this.pk];
        } else if (this.pk in object) {
            where = [[this.pk, object[this.pk]]];
            delete object[this.pk];
        } else if (!where.length) {
            return 0;
        }
        const fields = this._getAvailableFieldsInObject(object);
        if (!fields.length) {
            return 0;
        }
        return this.db.update(this.name, fields, object, {where});
    }

    replace(object) {
        const fields = this._getAvailableFieldsInObject(object);
        if (!fields.length) {
            return 0;
        }
        return this.db.replace(this.name, fields, object);
    }

    delete({
        where = [],
        pk = 0,
    }) {
        if (pk !== 0) {
            where = [[this.pk, pk]];
        } else if (!where.length) {
            return 0;
        }
        return this.db.delete(this.name, {where});
    }

    count() {
        return this.db.count(this.name);
    }
}

export default Table;
