import sqlite3 from 'sqlite3';

class Db {
    static timestamp(date = null) {
        return Math.floor((date instanceof Date ? date : (new Date())).getTime() / 1000);
    }

    static identifier(str) {
        return str.replace(/[^a-zA-Z0-9_]/g, '');
    }

    static stripInvisibleCharacters(str) {
        return str.replace(/[\x00-\x08\x0b-\x1f\x7f]/g, ''); // we don't like special characters, including \r
    }

    static toSqlValue(value) {
        if (value === undefined || value === null) {
            return '';
        }
        switch(typeof value) {
            case 'object':
                switch(true) {
                    case value instanceof String:
                        return Db.stripInvisibleCharacters(value);
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
                return Db.stripInvisibleCharacters(value);
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
        return str.replace(/[\x00-\x1f\x25\x27\x5c\x7f]/g, (char) => {
            switch (char.charCodeAt(0)) {
                case 0x09:
                    return '\\t';
                case 0x0a:
                    return '\\n';
                case 0x25:
                    return '%%';
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

    static getFieldString(fields) {
        return fields.length ? fields.map(field => Db.identifier(field)).join(',') : '*';
    }

    static getParams(fields, objects) {
        return objects.flatMap(object => fields.map(field => Db.toSqlValue(object[field])));
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

    all(sql, {params = [], field = '', pk = ''} = {}) {
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    return reject(err);
                }
                if (!rows || !rows.length) {
                    return resolve(undefined);
                }
                const row0 = rows[0];
                if (field.length) {
                    if (!(field in row0)) {
                        return resolve(undefined);
                    }
                    if (pk.length) {
                        if (!(pk in row0)) {
                            return resolve(undefined);
                        }
                        // If pk and field exist, return an object with pk as keys and field as values
                        return resolve(rows.reduce((obj, row) => {
                            obj[row[pk]] = row[field];
                            return obj;
                        }, {}));
                    }
                    // If field exists, return an array containing only field
                    return resolve(rows.map(row => row[field]));
                }
                if (pk.length) {
                    if (!(pk in row0)) {
                        return resolve(undefined);
                    }
                    // If pk exists, return an object with pk as keys and row as values
                    return resolve(rows.reduce((obj, row) => {
                        obj[row[pk]] = row;
                        return obj;
                    }, {}));
                }
                return resolve(rows);
            });
        });
    }

    get(sql, {params = [], field = ''} = {}) {
        return new Promise((resolve, reject) => {
            this.db.get(sql, params, (err, row) => {
                err ? reject(err) : resolve(row ? (field.length ? row[field] : row) : undefined);
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

    async inserts(table, fields, objects, {chunkSize = 1000} = {}) {
        if (!objects.length) {
            return 0;
        }
        const tableStr = Db.identifier(table);
        const fieldsStr = Db.getFieldString(fields);
        const fieldsPlaceholder = `(${Array(fields.length).fill('?').join(',')})`;
        let num = 0;
        for (let i = 0; i < objects.length; i += chunkSize) {
            const chunk = objects.slice(i, i + chunkSize);
            const valuesPlaceholder = Array(chunk.length).fill(fieldsPlaceholder).join(',');
            const sql = `INSERT INTO ${tableStr} (${fieldsStr}) VALUES ${valuesPlaceholder};`;
            const params = Db.getParams(fields, chunk);
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
        const fieldsStr = Db.getFieldString(fields);
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

    select(table, {
        pk = '',
        id = 0,
        fields = [],
        field = '',
        order = '',
        descending = false,
        limit = 0,
        page = 1,
        pkAsRowKey = false,
    } = {}) {
        table = Db.identifier(table);
        pk = Db.identifier(pk);
        field = Db.identifier(field);
        const fieldsStr = field.length > 0
            ?
                (
                    pk.length > 0 && pkAsRowKey
                        ? Db.getFieldString([pk, field])
                        : field
                )
            : Db.getFieldString(fields);
        let sql;
        switch (true) {
            case pk.length > 0 && id !== 0:
                sql = `SELECT ${fieldsStr} FROM ${table} WHERE ${pk}=? LIMIT 1;`;
                return this.get(sql, { params: [id], field });
            default:
                if (order.length) {
                    order = ` ORDER BY ${Db.identifier(order)}${descending ? ' DESC' : ''}`;
                } else if (pk.length > 0) {
                    order = ` ORDER BY ${pk}${descending ? ' DESC' : ''}`;
                }
                const limitStr = limit > 0
                    ? (page > 1 ? ` LIMIT ${(page - 1) * limit},${limit}` : ` LIMIT ${limit}`)
                    : '';
                sql = `SELECT ${fieldsStr} FROM ${table}${order}${limitStr};`;
                return this.all(sql, {field, pk: pk.length > 0 && pkAsRowKey ? pk : ''});
        }
    }

    async update(table, fields, object, {pk, id}) {
        const tableStr = Db.identifier(table);
        const placeholder = fields.map(field => {
            return `${Db.identifier(field)}=?`;
        }).join(',');
        const sql = `UPDATE ${tableStr} SET ${placeholder} WHERE ${pk}=?;`;
        delete object[pk]; // disallow updating primary key
        const params = fields.map(field => Db.toSqlValue(object[field]));
        params.push(Db.toSqlValue(id));
        return (await this.run(sql, {params})).changes;
    }

    async replace(table, fields, object) {
        const tableStr = Db.identifier(table);
        const fieldsStr = Db.getFieldString(fields);
        const fieldsPlaceholder = Array(fields.length).fill('?').join(',');
        const sql = `INSERT OR REPLACE INTO ${tableStr} (${fieldsStr}) VALUES (${fieldsPlaceholder});`;
        const params = fields.map(field => Db.toSqlValue(object[field]));
        return (await this.run(sql, {params})).changes;
    }

    async delete(table, {pk, id}) {
        const sql = `DELETE FROM ${Db.identifier(table)} WHERE ${Db.identifier(pk)}=?;`;
        return (await this.run(sql, {params: [id]})).changes;
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
