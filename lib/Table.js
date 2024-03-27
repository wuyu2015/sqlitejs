import {Db} from '../index.js';

class Table {
    static t = 'created_at';
    static tu = 'updated_at';
    static td = 'deleted_at';

    static getTimeFieldTypes() {
        return {
            [this.t]: 'int',
            [this.tu]: 'int',
            [this.td]: 'int',
        };
    }

    static _vdPkUkUksKFieldTypes(pk, uk, uks, k, fieldTypes) {
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
        if (k.length) {
            if (!(k in fieldTypes)) {
                throw new Error('k must be in fieldTypes.');
            }
            if (k === pk) {
                throw new Error('k cannot be equal to pk.');
            }
            if (k === uk) {
                throw new Error('k cannot be equal to uk.');
            }
        }
    }

    static _vdField(field, fieldsSet) {
        if (!fieldsSet.has(field)) {
            throw new Error(`invalid field '${field}'.`);
        }
        return field;
    }

    static _getUniqueIndexes(arr, fieldsSet, uk, uks) {
        const uniqueIndexes = {};
        if (uk.length) {
            uniqueIndexes[uk] = {[uk]: {descending: false}};
        }
        if (uks.length) {
            uniqueIndexes[uks.join('_')] = uks.reduce((obj, field) => {
                obj[field] = {descending: false};
                return obj;
            }, {});
        }
        return {...uniqueIndexes, ...this._getIndexes(arr, fieldsSet, true)};
    }

    static _getIndexes(arr, fieldsSet) {
        if (typeof arr === 'string') {
            return {[this._vdField(arr)]: {descending: false}};
        }
        if (!Array.isArray(arr)) {
            throw new Error('indexes must be array.');
        }
        const indexes = {};
        arr.forEach(item1 => {
            const item1Type = typeof item1;
            if (item1Type === 'string') {
                indexes[this._vdField(item1, fieldsSet)] = {[item1]: {descending: false}};
            } else if (Array.isArray(item1) && item1.length) {
                indexes[item1.join('_')] = item1.reduce((obj, field) => {
                    obj[field] = {descending: false};
                    return obj;
                }, {});
            } else if (item1Type === 'object') {
                const fields = [];
                const index = {};
                for (const field in item1) {
                    this._vdField(field, fieldsSet);
                    index[field] = {
                        descending: !!item1[field].descending,
                    };
                    fields.push(field);
                }
                if (fields.length) {
                    indexes[fields.join('_')] = index;
                } else {
                    throw new Error(`invalid field '${item1}'.`);
                }
            } else {
                throw new Error(`invalid field '${item1}'.`);
            }
        });
        return indexes;
    }

    static _getIndexesDef(table, indexes, {
        unique = false,
        ifNotExists = true,
    } = {}) {
        const uniqueStmt = unique ? 'UNIQUE INDEX' : 'INDEX';
        const stmts = [];
        const ifNotExistsStmt = ifNotExists ? ' IF NOT EXISTS' : '';
        for (const fieldKey in indexes) {
            const index = indexes[fieldKey];
            const fieldStmts = [];
            for (const field in index) {
                fieldStmts.push(`  ${field} ${index[field].descending ? 'DESC' : 'ASC'}`);
            }
            const stmt = `CREATE ${uniqueStmt}${ifNotExistsStmt} "main"."${table}_${fieldKey}"\n` +
                `ON "${table}" (\n` +
                fieldStmts.join(',\n') +
                '\n);';
            stmts.push(stmt);
        }
        return stmts.join('\n');
    }

    constructor(db, name, fieldTypes, {
        pk, uk = '',
        uks = [],
        k = '',
        uniqueIndexes = [],
        indexes = [],
        createdTimeField = Table.t,
        updatedTimeField = Table.tu,
        deletedTimeField = Table.td,
    } = {}) {
        Table._vdPkUkUksKFieldTypes(pk, uk, uks, k, fieldTypes);
        this.db = db;
        this.name = Db.identifier(name);
        this.fieldTypes = {};
        this.fieldProps = {};
        for (const field in fieldTypes) {
            Db.identifier(field);
            const fieldType = fieldTypes[field];
            if (typeof fieldType === 'string') {
                this.fieldTypes[field] = fieldType;
            } else if (Array.isArray(fieldType)) {
                if (!fieldType.length) {
                    throw new Error('fieldType empty.');
                }
                this.fieldTypes[field] = fieldType[0];
                if (fieldType.length > 1) {
                    const prop = fieldType[1];
                    const propType = typeof prop;
                    this.fieldProps[field] = propType === 'object' && Object.keys(prop).length
                        ? prop
                        : {defaultValue: prop};
                }
            } else {
                throw new Error('fieldType must be string or array.');
            }
        }
        this.fieldsWithPk = Object.keys(fieldTypes);
        this.filedsSetWithPk = new Set(this.fieldsWithPk);
        delete fieldTypes[pk];
        this.fields = Object.keys(fieldTypes);
        this.fieldsSet = new Set(this.fields);
        this.pk = pk;
        if (!this.fieldProps[pk]) {
            this.fieldProps[pk] = {};
        }
        this.fieldProps[pk].pk = true;
        this.uk = uk;
        if (uks.length) {
            uks = uks.filter(item => this.fieldsSet.has(item));
            if (uks.length < 2) {
                throw new Error('uks must contain multiple unique keys.');
            }
        }
        this.uks = uks;
        this.k = k;
        this.uniqueIndexes = Table._getUniqueIndexes(uniqueIndexes, this.fieldsSet, this.uk, this.uks);
        this.indexes = Table._getIndexes(indexes, this.fieldsSet);
        this.t = createdTimeField;
        this.tu = updatedTimeField;
        this.td = deletedTimeField;
        this._hasT = this.fieldsSet.has(createdTimeField);
        this._hasTu = this.fieldsSet.has(updatedTimeField);
        this._hasTd = this.fieldsSet.has(deletedTimeField);
    }

    _getAvailableFieldsInObject(object) {
        return this.fieldsWithPk.filter(field => field in object);
    }

    _getAvailableFieldsInArray(arr) {
        return arr.filter(field => this.filedsSetWithPk.has(field));
    }

    _vdField(field) {
        if (!this.filedsSetWithPk.has(field)) {
            throw new Error(`field '${field}' not exist.`);
        }
        return field;
    }

    getCreateTableSql() {
        return this.getTableDef();
    }

    getFieldDef(field, {
        notNull = true,
        defaultValue = null,
        collate = '',
        onConflict = '',
        pk = false,
        autoIncrement = true,
    } = {}) {
        return Db.getFieldDef(this._vdField(field), this.fieldTypes[field], {
            notNull,
            defaultValue,
            collate,
            onConflict,
            pk,
            autoIncrement,
        });
    }

    getPkFieldDef({
        collate = '',
        onConflict = '',
        autoIncrement = true,
    } = {}) {
        return Db.getPkFieldDef(this.pk, this.fieldTypes[this.pk], {
            collate,
            onConflict,
            autoIncrement,
        });
    }

    getUkFieldDef({
        collate = '',
        onConflict = '',
    } = {}) {
        return Db.getFieldDef(this.uk, this.fieldTypes[this.uk], {
            notNull: true,
            defaultValue: null,
            collate,
            onConflict,
            pk: false,
            autoIncrement: false,
        });
    }

    getKFieldDef({
        notNull = true,
        defaultValue = null,
        collate = '',
    } = {}) {
        return Db.getFieldDef(this.k, this.fieldTypes[this.k], {
            notNull,
            defaultValue,
            collate,
            pk: false,
            autoIncrement: false,
        });
    }

    getTimeFieldsDef() {
        return `"${this.t}" INTEGER NOT NULL, ` +
            `"${this.tu}" INTEGER NOT NULL DEFAULT 0, ` +
            `"${this.td}" INTEGER NOT NULL DEFAULT 0`;
    }

    getTableDef({
        ifNotExists = true,
    } = {}) {
        const tableDef = Db.getTableDef(this.name, this.fieldTypes, this.fieldProps, {ifNotExists});
        const uniqueIndexesDef = Table._getIndexesDef(this.name, this.uniqueIndexes, {unique: true, ifNotExists});
        const indexesDef = Table._getIndexesDef(this.name, this.indexes, {ifNotExists});
        return `${tableDef}${uniqueIndexesDef.length ? '\n' + uniqueIndexesDef : ''}${indexesDef.length ? '\n' + indexesDef : ''}`;
    }

    async createTable() {
        const sql = this.getCreateTableSql();
        if (sql.length) {
            return await this.db.exec(sql);
        }
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

    async selectsK() {
        return await this.selects({
            distinct: true,
            fields: [this.k],
            resultField: this.k,
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

    async selectsPkK() {
        return await this.selects({
            fields: [this.pk, this.k],
        });
    }

    async selectsUkK() {
        return await this.selects({
            fields: [this.uk, this.k],
        });
    }

    async selectsUksK() {
        return await this.selects({
            fields: [...this.uks, this.k],
        });
    }

    async selectsPkUkK() {
        return await this.selects({
            fields: [this.pk, this.uk, this.k],
        });
    }

    async selectsPkUksK() {
        return await this.selects({
            fields: [this.pk, ...this.uk, this.k],
        });
    }

    async selectsPkSet() {
        return await this.selects({
            fields: [this.pk],
            resultType: 'set',
            resultField: this.pk,
        });
    }

    async selectsUkSet() {
        return await this.selects({
            fields: [this.pk],
            resultType: 'set',
            resultField: this.uk,
        });
    }

    async selectsKSet() {
        return await this.selects({
            distinct: true,
            fields: [this.k],
            resultType: 'set',
            resultField: this.k,
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

    async selectsPkKMap() {
        return await this.selects({
            fields: [this.pk, this.k],
            resultType: 'map',
            resultKey: this.pk,
            resultField: this.k,
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

    async selectsUkKMap() {
        return await this.selects({
            fields: [this.uk, this.k],
            resultType: 'map',
            resultKey: this.uk,
            resultField: this.k,
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

    async selectsUksKMap() {
        const result = await this.selects({
            fields: [...this.uks, this.k],
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
            subMap.set(row[this.uks[lastUkIndex]], row[this.k]);
        });
        return map;
    }

    async selectsPkUkObject() {
        return await this.selects({
            fields: [this.pk, this.uk],
            resultType: 'object',
            resultKey: this.pk,
            resultField: this.uk,
        });
    }

    async selectsPkKObject() {
        return await this.selects({
            fields: [this.pk, this.k],
            resultType: 'object',
            resultKey: this.pk,
            resultField: this.k,
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

    async selectsUkKObject() {
        return await this.selects({
            fields: [this.uk, this.k],
            resultType: 'object',
            resultKey: this.uk,
            resultField: this.k,
        });
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

    async selectsUksKObject() {
        const result = await this.selects({
            fields: [...this.uks, this.k],
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
            subObject[row[this.uks[lastUkIndex]]] = row[this.k];
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
