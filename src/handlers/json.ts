import {
    type AppTable,
    type AppTableRow,
    DataSourceType,
    type DataSourceDefinition,
    inferTable
} from 'schemafx';

import type { FileHandler, GoogleClients, InferredTable } from './types.js';

import {
    findRowIndex,
    hasValidKeys,
    readFileContent,
    writeFileContent,
    getFileName,
    getKeyFields,
    safeJsonParse
} from './utils.js';

function parseJson(content: string): AppTableRow[] {
    const parsed = safeJsonParse(content);

    if (Array.isArray(parsed)) {
        for (const item of parsed) {
            if (item === null || typeof item !== 'object' || Array.isArray(item)) {
                throw new Error('JSON array must contain only objects');
            }
        }

        return parsed as AppTableRow[];
    }

    return [parsed as Record<string, unknown>];
}

export const jsonHandler: FileHandler = {
    async getTable(fileId: string, clients: GoogleClients): Promise<InferredTable> {
        const content = await readFileContent(fileId, clients);

        return inferTable(
            (await getFileName(fileId, clients)).replace(/\.json$/i, ''),
            parseJson(content)
        );
    },

    async getData(table: AppTable, clients: GoogleClients): Promise<DataSourceDefinition> {
        const [, fileId] = table.path;

        if (!fileId) return { type: DataSourceType.Inline, data: [] };
        const content = await readFileContent(fileId, clients);

        return {
            type: DataSourceType.Inline,
            data: parseJson(content).reduce((acc: AppTableRow[], row) => {
                if (hasValidKeys(row, getKeyFields(table))) acc.push(row as AppTableRow);
                return acc;
            }, [])
        };
    },

    async addRow(table: AppTable, row: AppTableRow, clients: GoogleClients): Promise<void> {
        const [, fileId] = table.path;
        if (!fileId) throw new Error('Invalid path for addRow. Expected ["json", fileId]');

        const content = await readFileContent(fileId, clients);
        const data = parseJson(content);

        data.push(row);

        await writeFileContent(fileId, JSON.stringify(data, null, 2), 'application/json', clients);
    },

    async updateRow(
        table: AppTable,
        key: Record<string, unknown>,
        row: AppTableRow,
        clients: GoogleClients
    ): Promise<void> {
        const [, fileId] = table.path;
        if (!fileId) throw new Error('Invalid path for updateRow. Expected ["json", fileId]');

        const content = await readFileContent(fileId, clients);
        const data = parseJson(content);

        const rowIndex = findRowIndex(data, key);
        if (rowIndex === -1) return;

        data[rowIndex] = { ...data[rowIndex], ...row };

        await writeFileContent(fileId, JSON.stringify(data, null, 2), 'application/json', clients);
    },

    async deleteRow(
        table: AppTable,
        key: Record<string, unknown>,
        clients: GoogleClients
    ): Promise<void> {
        const [, fileId] = table.path;
        if (!fileId) throw new Error('Invalid path for deleteRow. Expected ["json", fileId]');

        const content = await readFileContent(fileId, clients);
        const data = parseJson(content);

        const rowIndex = findRowIndex(data, key);
        if (rowIndex === -1) return;

        data.splice(rowIndex, 1);

        await writeFileContent(fileId, JSON.stringify(data, null, 2), 'application/json', clients);
    }
};
