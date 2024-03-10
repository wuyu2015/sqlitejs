class Table {
    constructor(db, name, {pk, fields}) {
        this.db = db;
        this.name = name;
        this.pk = pk;
        this.fieldsWithPk = [pk];
        this.fields = [];
        for (const field of fields) {
            if (!this.fieldsWithPk.includes(field)) {
                this.fieldsWithPk.push(field);
                this.fields.push(field);
            }
        }
    }

    _getAvailableFields(object) {
        const fields = [];
        if (this.pk in object) {
            fields.push(this.pk);
        }
        for (const field of this.fields) {
            if (field in object) {
                fields.push(field);
            }
        }
        return fields;
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

    select({
        id = 0,
        fields = [],
        field = '',
        order = '',
        descending = false,
        limit = 0,
        page = 1,
        pkAsRowKey = false,
    } = {}) {
        if (!fields.length) {
            fields = this.fieldsWithPk;
        }
        const pk = id !== 0
            ? this.pk
            : (pkAsRowKey ? this.pk : '');
        return this.db.select(this.name, {
            pk,
            id,
            fields,
            field,
            order,
            descending,
            limit,
            page,
            pkAsRowKey,
        });
    }

    update(object, {id = 0} = {}) {
        if (id !== 0) {
            delete object[this.pk];
        } else if (this.pk in object) {
            id = object[this.pk];
            delete object[this.pk];
        } else {
            return 0;
        }
        const fields = [];
        const rowObject = {}
        for (const field of this.fields) {
            if (field in object) {
                fields.push(field);
                rowObject[field] = object[field];
            }
        }
        if (!fields.length) {
            return 0;
        }
        return this.db.update(this.name, fields, rowObject, {pk: this.pk, id});
    }

    replace(object) {
        const fields = this._getAvailableFields(object);
        if (!fields.length) {
            return 0;
        }
        return this.db.replace(this.name, fields, object);
    }

    delete({id}) {
        return this.db.delete(this.name, {pk: this.pk, id});
    }

    count() {
        return this.db.count(this.name);
    }
}

export default Table;