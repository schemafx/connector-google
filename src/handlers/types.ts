import type { AppTable, AppTableRow, DataSourceDefinition, inferTable } from 'schemafx';
import type { drive_v3, sheets_v4 } from 'googleapis';

export type InferredTable = ReturnType<typeof inferTable>;

export interface GoogleClients {
    drive: drive_v3.Drive;
    sheets: sheets_v4.Sheets;
}

export interface FileHandler {
    getTable(fileId: string, clients: GoogleClients, sheetName?: string): Promise<InferredTable>;
    getData(table: AppTable, clients: GoogleClients): Promise<DataSourceDefinition>;
    addRow(table: AppTable, row: AppTableRow, clients: GoogleClients): Promise<void>;
    updateRow(
        table: AppTable,
        key: Record<string, unknown>,
        row: AppTableRow,
        clients: GoogleClients
    ): Promise<void>;
    deleteRow(table: AppTable, key: Record<string, unknown>, clients: GoogleClients): Promise<void>;
}
