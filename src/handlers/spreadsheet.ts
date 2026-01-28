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
    getKeyFields,
    serializeFieldValue,
    deserializeFieldValue,
    escapeSheetName,
    validateFileId
} from './utils.js';

function getColumnLetter(colIndex: number): string {
    let temp;
    let letter = '';

    while (colIndex > 0) {
        temp = (colIndex - 1) % 26;
        letter = String.fromCharCode(temp + 65) + letter;
        colIndex = (colIndex - temp - 1) / 26;
    }

    return letter;
}

export const spreadsheetHandler: FileHandler = {
    async getTable(
        fileId: string,
        clients: GoogleClients,
        sheetName?: string
    ): Promise<InferredTable> {
        if (!sheetName) throw new Error('Sheet name is required for spreadsheet tables');
        validateFileId(fileId);

        const response = await clients.sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: escapeSheetName(sheetName)
        });

        const rows = response.data.values || [];

        if (rows.length === 0) return inferTable(sheetName, []);

        const headers = rows[0]!.map(h => String(h));

        return inferTable(
            sheetName,
            rows.slice(1).map((row: unknown[]) => {
                const rowObj: Record<string, unknown> = {};
                headers.forEach((header: string, index: number) => {
                    rowObj[header] = row[index];
                });

                return rowObj;
            })
        );
    },

    async getData(table: AppTable, clients: GoogleClients): Promise<DataSourceDefinition> {
        const [, fileId, sheetName] = table.path;

        if (!fileId || !sheetName) return { type: DataSourceType.Inline, data: [] };

        const response = await clients.sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: escapeSheetName(sheetName)
        });

        const rows = response.data.values || [];

        if (rows.length === 0) return { type: DataSourceType.Inline, data: [] };

        const headers = rows[0]!.map(h => String(h));

        return {
            type: DataSourceType.Inline,
            data: rows.slice(1).reduce((acc: AppTableRow[], row) => {
                const rowObj: AppTableRow = {};

                headers.forEach((header: string, index: number) => {
                    rowObj[header] = deserializeFieldValue(
                        row[index],
                        table.fields.find(f => f.name === header)
                    );
                });

                if (hasValidKeys(rowObj, getKeyFields(table))) acc.push(rowObj);
                return acc;
            }, [])
        };
    },

    async addRow(table: AppTable, row: AppTableRow, clients: GoogleClients): Promise<void> {
        const [, fileId, sheetName] = table.path;

        if (!fileId || !sheetName) {
            throw new Error('Invalid path for addRow. Expected ["file", fileId, sheetName]');
        }

        const headerResponse = await clients.sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: `${escapeSheetName(sheetName)}!A1:ZZ1`
        });

        let headers =
            headerResponse.data.values && headerResponse.data.values[0]
                ? headerResponse.data.values[0].map(h => String(h))
                : [];

        const rowKeys = Object.keys(row);
        const newColumns = rowKeys.filter(key => !headers.includes(key));

        if (newColumns.length > 0) {
            const startColIndex = headers.length;
            const range = `${escapeSheetName(sheetName)}!${getColumnLetter(startColIndex + 1)}1`;

            await clients.sheets.spreadsheets.values.update({
                spreadsheetId: fileId,
                range: range,
                valueInputOption: 'RAW',
                requestBody: {
                    values: [newColumns]
                }
            });

            headers = [...headers, ...newColumns];
        }

        await clients.sheets.spreadsheets.values.append({
            spreadsheetId: fileId,
            range: escapeSheetName(sheetName),
            valueInputOption: 'USER_ENTERED',
            requestBody: {
                values: [
                    headers.map(header =>
                        serializeFieldValue(
                            row[header],
                            table.fields.find(f => f.name === header)
                        )
                    )
                ]
            }
        });
    },

    async updateRow(
        table: AppTable,
        key: Record<string, unknown>,
        row: AppTableRow,
        clients: GoogleClients
    ): Promise<void> {
        const [, fileId, sheetName] = table.path;

        if (!fileId || !sheetName) {
            throw new Error('Invalid path for updateRow. Expected ["file", fileId, sheetName]');
        }

        const response = await clients.sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: escapeSheetName(sheetName)
        });

        const rows = response.data.values || [];
        if (rows.length === 0) return;

        const headers = rows[0]!.map(h => String(h));

        const dataRows = rows.slice(1).map((r: unknown[]) => {
            const obj: Record<string, unknown> = {};
            headers.forEach((h, idx) => {
                obj[h] = r[idx];
            });

            return obj;
        });

        const rowIndex = findRowIndex(dataRows, key);
        if (rowIndex === -1) return;

        const newColumns = Object.keys(row).filter(k => !headers.includes(k));
        let updatedHeaders = [...headers];

        if (newColumns.length > 0) {
            const startColIndex = headers.length;
            const range = `${escapeSheetName(sheetName)}!${getColumnLetter(startColIndex + 1)}1`;

            await clients.sheets.spreadsheets.values.update({
                spreadsheetId: fileId,
                range: range,
                valueInputOption: 'RAW',
                requestBody: {
                    values: [newColumns]
                }
            });

            updatedHeaders = [...headers, ...newColumns];
        }

        await clients.sheets.spreadsheets.values.update({
            spreadsheetId: fileId,
            range: `${escapeSheetName(sheetName)}!A${rowIndex + 2}`,
            valueInputOption: 'USER_ENTERED',
            requestBody: {
                values: [
                    updatedHeaders.map(header =>
                        serializeFieldValue(
                            { ...dataRows[rowIndex]!, ...row }[header],
                            table.fields.find(f => f.name === header)
                        )
                    )
                ]
            }
        });
    },

    async deleteRow(
        table: AppTable,
        key: Record<string, unknown>,
        clients: GoogleClients
    ): Promise<void> {
        const [, fileId, sheetName] = table.path;

        if (!fileId || !sheetName) {
            throw new Error('Invalid path for deleteRow. Expected ["file", fileId, sheetName]');
        }

        const response = await clients.sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: escapeSheetName(sheetName)
        });

        const rows = response.data.values || [];
        if (rows.length === 0) return;

        const headers = rows[0]!.map(h => String(h));
        const dataRows = rows.slice(1).map((r: unknown[]) => {
            const obj: Record<string, unknown> = {};
            headers.forEach((h, idx) => {
                obj[h] = r[idx];
            });

            return obj;
        });

        const rowIndex = findRowIndex(dataRows, key);
        if (rowIndex === -1) return;

        await clients.sheets.spreadsheets.values.clear({
            spreadsheetId: fileId,
            range: `${escapeSheetName(sheetName)}!A${rowIndex + 2}:ZZ${rowIndex + 2}`
        });
    }
};
