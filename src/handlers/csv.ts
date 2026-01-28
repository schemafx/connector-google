import {
    type AppTable,
    type AppTableRow,
    DataSourceType,
    type DataSourceDefinition,
    inferTable
} from 'schemafx';

import Papa from 'papaparse';
import type { FileHandler, GoogleClients, InferredTable } from './types.js';

import {
    findRowIndex,
    hasValidKeys,
    readFileContent,
    writeFileContent,
    getFileName,
    getKeyFields,
    serializeFieldValue,
    deserializeFieldValue
} from './utils.js';

type Delimiter = ',' | '\t';

interface ParsedFileInfo {
    data: Record<string, unknown>[];
    delimiter: Delimiter;
}

function detectDelimiter(content: string): Delimiter {
    const firstLine = content.split(/\r?\n/)[0] || '';

    const tabCount = (firstLine.match(/\t/g) || []).length;
    const commaCount = (firstLine.match(/,/g) || []).length;

    return tabCount > commaCount ? '\t' : ',';
}

function parseDelimitedContent(content: string, delimiter?: Delimiter): ParsedFileInfo {
    const detectedDelimiter = delimiter ?? detectDelimiter(content);

    const result = Papa.parse<Record<string, unknown>>(content, {
        header: true,
        delimiter: detectedDelimiter,
        skipEmptyLines: true,
        dynamicTyping: false,
        transformHeader: (header: string) => header.trim()
    });

    return {
        data: result.data,
        delimiter: detectedDelimiter
    };
}

function toDelimitedString(
    data: Record<string, unknown>[],
    delimiter: Delimiter,
    headers?: string[]
): string {
    const allHeaders = headers || (data.length > 0 ? Object.keys(data[0]!) : []);

    const stringData = data.map(row => {
        const stringRow: Record<string, string> = {};
        for (const header of allHeaders) {
            const value = row[header];
            stringRow[header] = value === null || value === undefined ? '' : String(value);
        }

        return stringRow;
    });

    return Papa.unparse(stringData, {
        delimiter,
        header: true,
        columns: allHeaders,
        newline: '\n'
    });
}

function getMimeType(delimiter: Delimiter): string {
    return delimiter === '\t' ? 'text/tab-separated-values' : 'text/csv';
}

function processRowForStorage(
    row: AppTableRow,
    headers: string[],
    fields: AppTable['fields']
): Record<string, unknown> {
    const processedRow: Record<string, unknown> = {};

    for (const header of headers) {
        processedRow[header] = serializeFieldValue(
            row[header],
            fields.find(f => f.name === header)
        );
    }

    return processedRow;
}

export const csvHandler: FileHandler = {
    async getTable(fileId: string, clients: GoogleClients): Promise<InferredTable> {
        const content = await readFileContent(fileId, clients);
        const { data } = parseDelimitedContent(content);

        return inferTable((await getFileName(fileId, clients)).replace(/\.(csv|tsv)$/i, ''), data);
    },

    async getData(table: AppTable, clients: GoogleClients): Promise<DataSourceDefinition> {
        const [, fileId] = table.path;

        if (!fileId) return { type: DataSourceType.Inline, data: [] };

        const content = await readFileContent(fileId, clients);
        const { data: rawData } = parseDelimitedContent(content);

        return {
            type: DataSourceType.Inline,
            data: rawData.reduce((acc: AppTableRow[], row) => {
                const rowObj: AppTableRow = {};

                for (const [key, value] of Object.entries(row)) {
                    rowObj[key] = deserializeFieldValue(
                        value,
                        table.fields.find(f => f.name === key)
                    );
                }

                if (hasValidKeys(rowObj, getKeyFields(table))) acc.push(rowObj);
                return acc;
            }, [])
        };
    },

    async addRow(table: AppTable, row: AppTableRow, clients: GoogleClients): Promise<void> {
        const [, fileId] = table.path;
        if (!fileId) throw new Error('Invalid path for addRow. Expected ["csv", fileId]');

        const content = await readFileContent(fileId, clients);
        const { data: existingData, delimiter } = parseDelimitedContent(content);

        const allHeaders = [
            ...new Set([
                ...(existingData.length > 0 ? Object.keys(existingData[0]!) : []),
                ...Object.keys(row)
            ])
        ];

        existingData.push(processRowForStorage(row, allHeaders, table.fields));

        await writeFileContent(
            fileId,
            toDelimitedString(existingData, delimiter, allHeaders),
            getMimeType(delimiter),
            clients
        );
    },

    async updateRow(
        table: AppTable,
        key: Record<string, unknown>,
        row: AppTableRow,
        clients: GoogleClients
    ): Promise<void> {
        const [, fileId] = table.path;
        if (!fileId) throw new Error('Invalid path for updateRow. Expected ["csv", fileId]');

        const content = await readFileContent(fileId, clients);
        const { data, delimiter } = parseDelimitedContent(content);

        const rowIndex = findRowIndex(data, key);
        if (rowIndex === -1) return;

        const allHeaders = [...new Set([...Object.keys(data[0] || {}), ...Object.keys(row)])];

        data[rowIndex] = processRowForStorage(
            { ...data[rowIndex], ...row } as AppTableRow,
            allHeaders,
            table.fields
        );

        await writeFileContent(
            fileId,
            toDelimitedString(data, delimiter, allHeaders),
            getMimeType(delimiter),
            clients
        );
    },

    async deleteRow(
        table: AppTable,
        key: Record<string, unknown>,
        clients: GoogleClients
    ): Promise<void> {
        const [, fileId] = table.path;
        if (!fileId) throw new Error('Invalid path for deleteRow. Expected ["csv", fileId]');

        const content = await readFileContent(fileId, clients);
        const { data, delimiter } = parseDelimitedContent(content);

        const rowIndex = findRowIndex(data, key);
        if (rowIndex === -1) return;

        data.splice(rowIndex, 1);

        await writeFileContent(
            fileId,
            toDelimitedString(data, delimiter, data.length > 0 ? Object.keys(data[0]!) : []),
            getMimeType(delimiter),
            clients
        );
    }
};
