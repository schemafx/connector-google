import { Connector } from 'schemafx';

export default class GoogleConnector extends Connector {
    async listTables() {
        return [];
    }

    async getTable(path: string[]) {
        return {
            id: '',
            name: '',
            connector: this.id,
            path: path,
            fields: [],
            actions: []
        };
    }
}
