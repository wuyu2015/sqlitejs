import sqlite3 from 'sqlite3';

class Db {
    static timestamp(date = null) {
        return Math.floor((date instanceof Date ? date : (new Date())).getTime() / 1000);
    }

    static toSqlValue(value) {
        if (value === undefined || value === null) {
            return '';
        }
        switch(typeof value) {
            case 'object':
                switch(true) {
                    case value instanceof String:
                        return Db._stripInvisibleCharacters(value);
                    case value instanceof Boolean:
                        return value.valueOf() ? 1 : 0;
                    case value instanceof Number:
                        const num = value.valueOf();
                        return Number.isFinite(num) ? num : '';
                    case value instanceof BigInt:
                        return value.toString();
                    case value instanceof Date:
                        return Db.timestamp(value);
                    default:
                        const s = JSON.stringify(value);
                        return s === '[]' || s === '{}' ? '' : s;
                }
            case 'string':
                return Db._stripInvisibleCharacters(value);
            case 'boolean':
                return value.valueOf() ? 1 : 0;
            case 'number':
                return Number.isFinite(value) ? value : '';
            case 'bigint':
                return value.toString();
            default:
                return '';
        }
    }

    static escape(str) {
        return str.replace(/[\x00-\x1f\x27\x5c\x7f]/g, (char) => {
            switch (char.charCodeAt(0)) {
                case 0x09:
                    return '\\t';
                case 0x0a:
                    return '\\n';
                case 0x27:
                    return "''";
                case 0x5c:
                    return '\\\\';
                default:
                    return ''; // we don't like special characters, including \r
            }
        });
    }

    static quote(value) {
        const sqlValue = Db.toSqlValue(value);
        return typeof sqlValue === 'number' ? sqlValue : `'${Db.escape(sqlValue)}'`;
    }

    static identifier(str) {
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(str)) {
            throw new Error(`Identifier '${str}' invalid.`)
        }
        return str;
    }

    static _stripInvisibleCharacters(str) {
        return str.replace(/[\x00-\x08\x0b-\x1f\x7f]/g, ''); // we don't like special characters, including \r
    }

    static _getFieldExpression(fields) {
        return fields.length ? fields.join(',') : '*';
    }

    static _getParams(fields, objects) {
        return objects.flatMap(object => fields.map(field => Db.toSqlValue(object[field])));
    }

    static _getWhereStatement(where) {
        if (!Array.isArray(where) || !where.length) {
            return '';
        }
        const arr = [];
        for (const whereArr of where) {
            if (whereArr.length < 2) {
                throw new Error("The 'where' array must contain both a key and a value.");
            }
            const op = whereArr[2] ?? '=';
            arr.push(`${Db.identifier(whereArr[0])} ${op} ${Db.quote(whereArr[1])}`)
        }
        return ' WHERE ' + arr.join(' AND ');
    }

    static _getOrderStatement(order, descending) {
        return  order.length
            ? ` ORDER BY ${Db.identifier(order)}${descending ? ' DESC' : ''}`
            : '';
    }

    static _getLimitStatement(limit, page) {
        return limit > 0
            ? (page > 1 ? ` LIMIT ${(page - 1) * limit},${limit}` : ` LIMIT ${limit}`)
            : '';
    }

    static _getFieldValue(value, fieldType) {
        switch (fieldType) {
            case 'string':
                return value.toString();
            case 'int':
                return parseInt(value, 10);
            case 'float':
                return parseFloat(value);
            case 'boolean':
                return !!value;
            case 'array':
                if (typeof value !== 'string') {
                    if (value === null) {
                        return [];
                    }
                    throw new Error('The value must be a string.');
                }
                if (!value.length) {
                    return [];
                }
                if (!value.startsWith('[')) {
                    throw new Error('The value must be a JSON array string.');
                }
                return JSON.parse(value);
            case 'object':
                if (typeof value !== 'string') {
                    if (value === null) {
                        return {};
                    }
                    throw new Error('The value must be a string.');
                }
                if (!value.length) {
                    return {};
                }
                if (!value.startsWith('{')) {
                    throw new Error('The value must be a JSON object string.');
                }
                return JSON.parse(value);
            case 'date':
                if (typeof value !== 'number') {
                    if (value === null) {
                        return null;
                    }
                    throw new Error('The value must be a number.');
                }
                return Number.isFinite(value) ? new Date(value) : null;
            default:
                return value;
        }
    }

    static _getFieldValues(row, fieldTypes) {
        for (const field in fieldTypes) {
            if (field in row) {
                row[field] = Db._getFieldValue(row[field], fieldTypes[field]);
            }
        }
        return row;
    }

    constructor({file = ':memory:', isVerbose = true} = {}) {
        this.db = isVerbose
            ? new (sqlite3.verbose()).Database(file)
            : new sqlite3.Database(file);
        this._insertMap = new Map();
        this._insertPaused = false;
    }

    close() {
        return new Promise((resolve, reject) => {
            this.db.close((err) => {
                err ? reject(err) : resolve();
            });
        });
    }

    exec(sql) {
        return new Promise((resolve, reject) => {
            this.db.exec(sql, (err) => {
                err ? reject(err) : resolve();
            });
        });
    }

    run(sql, {params = []} = {}) {
        return new Promise((resolve, reject) => {
            this.db.run(sql, params, function (err) {
                err ? reject(err) : resolve({lastID: this.lastID, changes: this.changes});
            });
        });
    }

    all(sql, {params = [], resultType = 'array', resultKey = '', resultField = '', fieldTypes = {}} = {}) {
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    return reject(err);
                }
                if (!rows || !rows.length) {
                    switch (resultType) {
                        case 'set':
                        case 'map':
                        case 'object':
                            return resolve(null);
                        default:
                            return resolve([]);
                    }
                }
                if ((resultKey.length && !(resultKey in rows[0])) || (!resultKey.length && (resultType === 'map' || resultType === 'object'))) {
                    return reject(new Error(`resultKey '${resultKey}' does not exist.`));
                }
                if ((resultField.length && !(resultField in rows[0])) || (!resultField.length && resultType === 'set')) {
                    return reject(new Error(`resultField '${resultField}' does not exist.`));
                }
                try {
                    switch (resultType) {
                        case 'set':
                            const set = new Set();
                            if (resultField in fieldTypes) {
                                try {
                                    for (const row of rows) {
                                        set.add(Db._getFieldValue(row[resultField], fieldTypes[resultField]));
                                    }
                                } catch (valueError) {
                                    reject(valueError);
                                }
                            } else {
                                for (const row of rows) {
                                    set.add(row[resultField]);
                                }
                            }
                            return resolve(set);
                        case 'map':
                            const map = new Map();
                            if (resultField.length) {
                                if (resultField in fieldTypes) {
                                    try {
                                        for (const row of rows) {
                                            map.set(row[resultKey], Db._getFieldValue(row[resultField], fieldTypes[resultField]));
                                        }
                                    } catch (valueError) {
                                        reject(valueError);
                                    }
                                } else {
                                    for (const row of rows) {
                                        map.set(row[resultKey], row[resultField]);
                                    }
                                }
                            } else {
                                for (const row of rows) {
                                    map.set(row[resultKey], Db._getFieldValues(row, fieldTypes));
                                }
                            }
                            return resolve(map);
                        case 'object':
                            const object = {};
                            if (resultField.length) {
                                if (resultField in fieldTypes) {
                                    try {
                                        for (const row of rows) {
                                            object[row[resultKey]] = Db._getFieldValue(row[resultField], fieldTypes[resultField]);
                                        }
                                    } catch (valueError) {
                                        reject(valueError);
                                    }
                                } else {
                                    for (const row of rows) {
                                        object[row[resultKey]] = row[resultField];
                                    }
                                }
                            } else {
                                for (const row of rows) {
                                    object[row[resultKey]] = Db._getFieldValues(row, fieldTypes)
                                }
                            }
                            return resolve(object);
                        default:
                            const arr =[];
                            if (resultField.length) {
                                if (resultField in fieldTypes) {
                                    try {
                                        for (const row of rows) {
                                            arr.push(Db._getFieldValue(row[resultField], fieldTypes[resultField]));
                                        }
                                    } catch (valueError) {
                                        reject(valueError);
                                    }
                                } else {
                                    for (const row of rows) {
                                        arr.push(row[resultField]);
                                    }
                                }
                            } else {
                                for (const row of rows) {
                                    arr.push(Db._getFieldValues(row, fieldTypes));
                                }
                            }
                            return resolve(arr);
                    }
                } catch (valueError) {
                    return reject(valueError);
                }
            });
        });
    }

    get(sql, {params = [], resultField = '', fieldTypes = {}} = {}) {
        return new Promise((resolve, reject) => {
            this.db.get(sql, params, (err, row) => {
                if (err) {
                    return reject(err);
                }
                if (!row) {
                    return resolve(null);
                }
                if (!resultField.length) {
                    try {
                        return resolve(Db._getFieldValues(row, fieldTypes));
                    } catch (valueError) {
                        reject(valueError);
                    }
                }
                if (!(resultField in row)) {
                    return reject(new Error(`resultField '${resultField}' does not exist.`));
                }
                const value = row[resultField];
                if (!(resultField in fieldTypes)) {
                    return resolve(value);
                }
                try {
                    resolve(Db._getFieldValue(value, fieldTypes[resultField]));
                } catch (valueError) {
                    reject(valueError);
                }
            });
        });
    }

    each(sql, {params = [], fn = null} = {}) {
        return new Promise((resolve, reject) => {
            this.db.each(sql, params, fn, (err, count) => {
                err ? reject(err) : resolve(count);
            });
        });
    }

    selects(table, {
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
        fieldTypes = {},
    } = {}) {
        if (fields.length) {
            if (resultKey.length && !fields.includes(resultKey)) {
                throw new Error(`resultKey '${resultKey}' must be included in the 'fields' array.`);
            }
            if (resultField.length && !fields.includes(resultField)) {
                throw new Error(`resultField '${resultField}' must be included in the 'fields' array.`);
            }
        }
        table = Db.identifier(table);
        const distinctStmt = distinct ? ' DISTINCT ' : '';
        const fieldsExpr = Db._getFieldExpression(fields);
        const whereStmt = Db._getWhereStatement(where);
        const orderStmt = Db._getOrderStatement(order, descending);
        const limitStmt = Db._getLimitStatement(limit, page);
        const sql = `SELECT ${distinctStmt}${fieldsExpr} FROM ${table}${whereStmt}${orderStmt}${limitStmt};`;
        return this.all(sql, {resultType, resultKey, resultField, fieldTypes});
    }

    select(table, {
        fields = [],
        where = [],
        resultField = '',
        fieldTypes = {},
    } = {}) {
        table = Db.identifier(table);
        const fieldsExpr = resultField ? Db.identifier(resultField) : Db._getFieldExpression(fields);
        const whereStmt = Db._getWhereStatement(where);
        const sql = `SELECT ${fieldsExpr} FROM ${table}${whereStmt} LIMIT 1;`;
        return this.get(sql, {resultField, fieldTypes});
    }

    async inserts(table, fields, objects, {chunkSize = 1000} = {}) {
        if (!objects.length) {
            return 0;
        }
        const tableStr = Db.identifier(table);
        const fieldsStr = Db._getFieldExpression(fields);
        const fieldsPlaceholder = `(${Array(fields.length).fill('?').join(',')})`;
        let num = 0;
        for (let i = 0; i < objects.length; i += chunkSize) {
            const chunk = objects.slice(i, i + chunkSize);
            const valuesPlaceholder = Array(chunk.length).fill(fieldsPlaceholder).join(',');
            const sql = `INSERT INTO ${tableStr} (${fieldsStr}) VALUES ${valuesPlaceholder};`;
            const params = Db._getParams(fields, chunk);
            const {changes} = await this.run(sql, {params});
            num += changes;
        }
        return num;
    }

    async insert(table, fields, object, {immediate = false} = {}) {
        if (!immediate && this._insertPaused) {
            if (!this._insertMap.has(table)) {
                this._insertMap.set(table, {
                    fields: fields,
                    rowObjects: [],
                });
            }
            this._insertMap.get(table).rowObjects.push(object);
            return 0;
        }
        const params = fields.map(field => Db.toSqlValue(object[field]));
        const fieldsStr = Db._getFieldExpression(fields);
        const placeholder = Array(fields.length).fill('?').join(',');
        const sql = `INSERT INTO ${table} (${fieldsStr}) VALUES (${placeholder});`;
        return (await this.run(sql, {params})).lastID;
    }

    suspendInsert() {
        if (!this._insertPaused) {
            this._insertMap.clear();
            this._insertPaused = true;
        }
    }

    async commitInserts({chunkSize = 1000} = {}) {
        if (this._insertPaused) {
            for (const [table, tableObject] of this._insertMap) {
                await this.inserts(table, tableObject.fields, tableObject.rowObjects, {chunkSize});
            }
            this._insertMap.clear();
            this._insertPaused = false;
        }
    }

    async update(table, fields, object, {where}) {
        const tableStr = Db.identifier(table);
        const placeholder = fields.map(field => {
            return `${Db.identifier(field)}=?`;
        }).join(',');
        const whereStmt = Db._getWhereStatement(where);
        const sql = `UPDATE ${tableStr} SET ${placeholder}${whereStmt};`;
        const params = fields.map(field => Db.toSqlValue(object[field]));
        return (await this.run(sql, {params})).changes;
    }

    async replace(table, fields, object) {
        const tableStr = Db.identifier(table);
        const fieldsStr = Db._getFieldExpression(fields);
        const fieldsPlaceholder = Array(fields.length).fill('?').join(',');
        const sql = `INSERT OR REPLACE INTO ${tableStr} (${fieldsStr}) VALUES (${fieldsPlaceholder});`;
        const params = fields.map(field => Db.toSqlValue(object[field]));
        return (await this.run(sql, {params})).changes;
    }

    async delete(table, {where}) {
        const whereStmt = Db._getWhereStatement(where);
        const sql = `DELETE FROM ${Db.identifier(table)}${whereStmt};`;
        return (await this.run(sql)).changes;
    }

    count(table) {
        const sql = `SELECT COUNT(1) as num FROM ${Db.identifier(table)};`;
        return new Promise((resolve, reject) => {
            this.db.get(sql, (err, row) => {
                err ? reject(err) : resolve(row ? row.num : 0);
            });
        });
    }
}

export default Db;
