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
        delete fieldTypes[pk];
        this.fields = Object.keys(fieldTypes);
        this.pk = pk;
        this.uk = uk in fieldTypes ? uk : '';
    }

    _getAvailableFields(object) {
        return this.fieldsWithPk.filter(field => field in object);
    }

    inserts(objects, {chunkSize = 1000} = {}) {
        if (!objects.length) {
            return 0;
        }
        const fields = this._getAvailableFields(objects[0]);
        if (!fields.length) {
            return 0;
        }
        return this.db.inserts(this.name, fields, objects, {chunkSize});
    }

    insert(object, {immediate = false} = {}) {
        const fields = this._getAvailableFields(object);
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
        const fields = this._getAvailableFields(object);
        if (!fields.length) {
            return 0;
        }
        return this.db.update(this.name, fields, object, {where});
    }

    replace(object) {
        const fields = this._getAvailableFields(object);
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
