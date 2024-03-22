import {Db} from '../index.js';

class Table {
    constructor(db, name, fieldTypes, {
        pk, uk = '',
        uks = [],
        createdTimeField = 't',
        updatedTimeField = 'tu',
        deletedTimeField = 'td',
    } = {}) {
        if (!(pk in fieldTypes)) {
            throw new Error('fieldTypes must contains pk.');
        }
        if (uk.length) {
            if (!(uk in fieldTypes)) {
                throw new Error('uk must be in fieldTypes.');
            }
            if (uk === pk) {
                throw new Error('uk cannot be equal to pk.');
            }
            if (uks.length) {
                if (!(uk in fieldTypes)) {
                    throw new Error('uk and uks cannot appear simultaneously.');
                }
            }
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
        this.uk = uk;
        if (uks.length) {
            uks = uks.filter(item => this.filedsSet.has(item));
            if (uks.length < 2) {
                throw new Error('uks must contain multiple unique keys.');
            }
        }
        this.uks = uks;
        this.t = createdTimeField;
        this.tu = updatedTimeField;
        this.td = deletedTimeField;
        this._hasT = this.filedsSet.has(createdTimeField);
        this._hasTu = this.filedsSet.has(updatedTimeField);
        this._hasTd = this.filedsSet.has(deletedTimeField);
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
                : `"${this.uk}" TEXT NOT NULL COLLATE BINARY`;
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

    getCreatePkUksTableSql() {
        if (this.fieldsWithPk.length === 2 && this.pk.length && this.uks.length) {
            const pkStmt = this.fieldTypes[this.pk] === 'int'
                ? `"${this.pk}" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT`
                : `"${this.pk}" TEXT NOT NULL COLLATE BINARY PRIMARY KEY`;
            const ukStmts = [];
            const ukIndexStmts = [];
            this.uks.forEach(uk => {
                ukStmts.push(this.fieldTypes[uk] === 'int'
                    ? `"${uk}" INTEGER NOT NULL`
                    : `"${uk}" TEXT NOT NULL COLLATE BINARY`
                )
                ukIndexStmts.push(`"${uk}" ASC`);
            });
            return `
                CREATE TABLE IF NOT EXISTS "${this.name}" (
                  ${pkStmt},
                  ${ukStmts.join(', ')}
                );
                CREATE UNIQUE INDEX IF NOT EXISTS "main"."${this.name}_${this.uks.join('_')}"
                ON "${this.name}" (
                  ${ukIndexStmts.join(', ')}
                );
            `;
        }
        return '';
    }

    getCreateIndexSql(field, {unique = false} = {}) {
        if (!field.length) {
            throw new Error(`Field cannot be empty.`);
        }
        if (typeof field === 'string') {
            field = [field];
        }
        const stmts = [];
        for (const fld of field) {
            if (!this.filedsSet.has(fld)) {
                throw new Error(`Invalid field '${fld}'`);
            }
            stmts.push(fld);
        }
        return `
            CREATE ${unique ? 'UNIQUE INDEX' : 'INDEX'} IF NOT EXISTS "main"."${this.name}_${field.join('_')}"
            ON "${this.name}" (
              ${stmts.join(',')}
            );
        `;
    }

    getCreateUkIndexSql() {
        return this.getCreateIndexSql(this.uk, {unique: true});
    }

    getCreateUksIndexSql() {
        return this.getCreateIndexSql(this.uks, {unique: true});
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

    async createPkUksTable() {
        const sql = this.getCreatePkUksTableSql();
        if (sql.length) {
            return await this.db.exec(sql);
        }
    }

    async createIndex(field) {
        const sql = this.getCreateIndexSql(field);
        return await this.db.exec(sql);
    }

    async createUkIndex() {
        const sql = this.getCreateUkIndexSql();
        return await this.db.exec(sql);
    }

    async createUksIndex() {
        const sql = this.getCreateUksIndexSql();
        return await this.db.exec(sql);
    }

    async inserts(objects, {fields = [], chunkSize = 1000} = {}) {
        if (!objects.length) {
            return 0;
        }
        fields = fields.length ? this._getAvailableFieldsInArray(fields) : this._getAvailableFieldsInObject(objects[0]);
        if (!fields.length) {
            return 0;
        }
        return await this.db.inserts(this.name, fields, objects, {chunkSize});
    }

    async insert(object, {fields = [], insertOrIgnore = false, immediate = false} = {}) {
        fields = fields.length ? this._getAvailableFieldsInArray(fields) : this._getAvailableFieldsInObject(object);
        if (!fields.length) {
            return 0;
        }
        if (this._hasT && !(this.t in object)) {
            object[this.t] = Db.timestamp(); // add timestamp to object
        }
        return await this.db.insert(this.name, fields, object, {pkField: this.pk, insertOrIgnore, immediate});
    }

    suspendInsert() {
        this.db.suspendInsert();
    }

    async commitInserts({chunkSize = 1000} = {}) {
        await this.db.commitInserts({chunkSize});
    }

    async selects({
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
        return await this.db.selects(this.name, {distinct, fields, where, order, descending, limit, page, resultType, resultKey, resultField, fieldTypes: this.fieldTypes});
    }

    async select({
        fields = [],
        pk = 0,
        uk = 0,
        where = [],
        order = '',
        descending = false,
        resultField = '',
    } = {}) {
        if (pk !== 0) {
            where = [[this.pk, pk]];
        } else if (uk !== 0 && this.uk.length) {
            where = [[this.uk, uk]];
        }
        return await this.db.select(this.name, {fields, where, order, descending, resultField, fieldTypes: this.fieldTypes})
    }

    lastId() {
        return this.db.lastId(this.name, this.pk);
    }

    async update(object, {
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
        if (this._hasTu && !(this.tu in object)) {
            object[this.tu] = Db.timestamp();
        }
        const fields = this._getAvailableFieldsInObject(object);
        if (!fields.length) {
            return 0;
        }
        return await this.db.update(this.name, fields, object, {where});
    }

    async updateByPk(pk, object) {
        return await this.update(object, {pk});
    }

    async replace(object) {
        const fields = this._getAvailableFieldsInObject(object);
        if (!fields.length) {
            return 0;
        }
        return await this.db.replace(this.name, fields, object);
    }

    async delete({
        where = [],
        pk = 0,
    }) {
        if (pk !== 0) {
            where = [[this.pk, pk]];
        } else if (!where.length) {
            return 0;
        }
        return await this.db.delete(this.name, {where});
    }

    async deleteByPk(pk) {
        return await this.delete({pk});
    }

    async softDelete({where = [], pk = 0}) {
        if (this._hasTd) {
            if (pk !== 0) {
                where = [[this.pk, pk]];
            } else if (!where.length) {
                return 0;
            }
            return await this.update({[this.td]: Db.timestamp()}, {where});
        }
    }

    async softDeleteByPk(pk) {
        return await this.softDelete({pk});
    }

    async unDelete({where = [], pk = 0}) {
        if (this._hasTd) {
            if (pk !== 0) {
                where = [[this.pk, pk]];
            } else if (!where.length) {
                return 0;
            }
            return await this.update({[this.td]: 0}, {where});
        }
    }

    async unDeleteByPk(pk) {
        return await this.unDelete({pk});
    }

    async count() {
        return await this.db.count(this.name);
    }

    async selectsPk() {
        return await this.selects({
            fields: [this.pk],
            resultField: this.pk,
        });
    }

    async selectsPkSet() {
        return await this.selects({
            fields: [this.pk],
            resultType: 'set',
            resultField: this.pk,
        });
    }

    async selectsUk() {
        return await this.selects({
            fields: [this.uk],
            resultField: this.uk,
        });
    }

    async selectsUks() {
        return await this.selects({
            fields: this.uks,
        });
    }

    async selectsUkSet() {
        return await this.selects({
            fields: [this.pk],
            resultType: 'set',
            resultField: this.uk,
        });
    }

    async selectsPkUk() {
        return await this.selects({
            fields: [this.pk, this.uk],
        });
    }

    async selectsPkUks() {
        return await this.selects({
            fields: [this.pk, ...this.uk],
        });
    }

    async selectsPkUkMap() {
        return await this.selects({
            fields: [this.pk, this.uk],
            resultType: 'map',
            resultKey: this.pk,
            resultField: this.uk,
        });
    }

    async selectsPkUkObject() {
        return await this.selects({
            fields: [this.pk, this.uk],
            resultType: 'object',
            resultKey: this.pk,
            resultField: this.uk,
        });
    }

    async selectsUkPkMap() {
        return await this.selects({
            fields: [this.pk, this.uk],
            resultType: 'map',
            resultKey: this.uk,
            resultField: this.pk,
        });
    }

    async selectsUkPkObject() {
        return await this.selects({
            fields: [this.pk, this.uk],
            resultType: 'object',
            resultKey: this.uk,
            resultField: this.pk,
        });
    }

    async selectsUksPkMap() {
        const result = await this.selects({
            fields: [this.pk, ...this.uks],
        });
        const map = new Map();
        const lastUkIndex = this.uks.length - 1;
        result.forEach(row => {
            let subMap = map;
            for (let i = 0; i < lastUkIndex; i++) {
                const ukValue = row[this.uks[i]];
                if (!subMap.has(ukValue)) {
                    subMap.set(ukValue, new Map());
                }
                subMap = subMap.get(ukValue);
            }
            subMap.set(row[this.uks[lastUkIndex]], row[this.pk]);
        });
        return map;
    }

    async selectsUksPkObject() {
        const result = await this.selects({
            fields: [this.pk, ...this.uks],
        });
        const object = {};
        const lastUkIndex = this.uks.length - 1;
        result.forEach(row => {
            let subObject = object;
            for (let i = 0; i < lastUkIndex; i++) {
                const ukValue = row[this.uks[i]];
                if (!(ukValue in subObject)) {
                    subObject[ukValue] = {};
                }
                subObject = subObject[ukValue];
            }
            subObject[row[this.uks[lastUkIndex]]] = row[this.pk];
        });
        return object;
    }

    async selectByPk(pk) {
        return await this.select({pk});
    }

    async selectByUk(uk) {
        return await this.select({uk});
    }
}

export default Table;
