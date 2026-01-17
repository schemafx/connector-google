import {
    AppFieldType,
    type AppTable,
    type AppTableRow,
    Connector,
    ConnectorTableCapability,
    type ConnectorOptions,
    DataSourceType,
    type DataSourceDefinition,
    inferTable
} from 'schemafx';

import { google, drive_v3, sheets_v4 } from 'googleapis';

export type GoogleConnectorOptions = ConnectorOptions & {
    clientId: string;
    clientSecret: string;
    redirectUri: string;
};

export default class GoogleConnector extends Connector {
    clientId: string;
    clientSecret: string;
    redirectUri: string;

    constructor(options: GoogleConnectorOptions) {
        super(options);

        this.clientId = options.clientId;
        this.clientSecret = options.clientSecret;
        this.redirectUri = options.redirectUri;
    }

    private getOAuthClient() {
        return new google.auth.OAuth2({
            clientId: this.clientId,
            clientSecret: this.clientSecret,
            redirectUri: new URL(`api/connectors/${this.id}/auth/callback`, this.redirectUri).href
        });
    }

    private getAuth(auth?: string) {
        if (!auth) throw new Error('Unauthorized');

        const oauth2Client = this.getOAuthClient();
        oauth2Client.setCredentials({
            refresh_token: auth
        });

        return oauth2Client;
    }

    override async getAuthUrl(): Promise<string> {
        return this.getOAuthClient().generateAuthUrl({
            access_type: 'offline',
            prompt: 'consent',
            scope: [
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        });
    }

    override async authorize(body: Record<string, unknown>) {
        const oauth2Client = this.getOAuthClient();
        const { tokens } = await oauth2Client.getToken(body.code as string);

        if (!tokens.refresh_token) {
            throw new Error('No refresh_token returned.');
        }

        const email = tokens.access_token
            ? ((await oauth2Client.getTokenInfo(tokens.access_token)).email ?? '')
            : '';

        return {
            name: email,
            content: tokens.refresh_token,
            email
        };
    }

    override async listTables(path: string[], auth?: string) {
        const _auth = this.getAuth(auth);
        const drive = google.drive({ version: 'v3', auth: _auth });
        const sheets = google.sheets({ version: 'v4', auth: _auth });

        if (path.length === 0) {
            const tables = [
                {
                    name: 'Personal',
                    path: ['folder', 'root'],
                    capabilities: [ConnectorTableCapability.Explore]
                }
            ];

            try {
                const response = await drive.drives.list({
                    pageSize: 10,
                    useDomainAdminAccess: false
                });

                if (response.data.drives && response.data.drives.length > 0) {
                    tables.push({
                        name: 'Shared Drives',
                        path: ['drives'],
                        capabilities: [ConnectorTableCapability.Explore]
                    });
                }
            } catch (error) {
                console.warn('Could not list shared drives:', error);
            }

            return tables;
        }

        const [type, id] = path;

        if (type === 'drives') {
            const response = await drive.drives.list({
                pageSize: 100
            });

            return (response.data.drives || []).map((d: drive_v3.Schema$Drive) => ({
                name: d.name || 'Unknown Drive',
                path: ['drive', d.id!],
                capabilities: [ConnectorTableCapability.Explore]
            }));
        }

        if (type === 'folder' || type === 'drive') {
            const isSharedDrive = type === 'drive';
            const queryParams: drive_v3.Params$Resource$Files$List = {
                q: `'${id}' in parents and trashed = false`,
                fields: 'nextPageToken, files(id, name, mimeType)',
                pageSize: 100,
                includeItemsFromAllDrives: true,
                supportsAllDrives: true
            };

            if (isSharedDrive) {
                queryParams.corpora = 'drive';
                queryParams.driveId = id;
            }

            const response = await drive.files.list(queryParams);

            return (response.data.files || []).map((file: drive_v3.Schema$File) => {
                let capability = ConnectorTableCapability.Unavailable;
                let nextPath: string[] = [];
                const currentPath = ['file', file.id!];

                if (file.mimeType === 'application/vnd.google-apps.folder') {
                    capability = ConnectorTableCapability.Explore;
                    nextPath = ['folder', file.id!];
                } else if (file.mimeType === 'application/vnd.google-apps.spreadsheet') {
                    capability = ConnectorTableCapability.Explore;
                    nextPath = ['file', file.id!];
                }

                return {
                    name: file.name || 'Unknown File',
                    path: nextPath.length > 0 ? nextPath : currentPath,
                    capabilities: [capability]
                };
            });
        }

        if (type === 'file') {
            const response = await sheets.spreadsheets.get({
                spreadsheetId: id
            });

            return (response.data.sheets || []).map((sheet: sheets_v4.Schema$Sheet) => ({
                name: sheet.properties?.title || 'Unknown Sheet',
                path: ['file', id!, sheet.properties?.title || ''],
                capabilities: [ConnectorTableCapability.Connect]
            }));
        }

        return [];
    }

    override async getTable(path: string[], auth?: string) {
        const [type, fileId, sheetName] = path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for getTable. Expected ["file", fileId, sheetName]');
        }

        const _auth = this.getAuth(auth);
        const sheets = google.sheets({ version: 'v4', auth: _auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];

        if (rows.length === 0) {
            return inferTable(sheetName, path, [], this.id);
        }

        const headers = rows[0]!.map(h => String(h));
        const data = rows.slice(1).map((row: Record<string, unknown>[]) => {
            const rowObj: Record<string, unknown> = {};
            headers.forEach((header: string, index: number) => {
                rowObj[header] = row[index];
            });

            return rowObj;
        });

        return inferTable(sheetName, path, data, this.id);
    }

    override async getData(table: AppTable, auth?: string): Promise<DataSourceDefinition> {
        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            return { type: DataSourceType.Inline, data: [] };
        }

        const _auth = this.getAuth(auth);
        const sheets = google.sheets({ version: 'v4', auth: _auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];

        if (rows.length === 0) {
            return { type: DataSourceType.Inline, data: [] };
        }

        const headers = rows[0]!.map(h => String(h));
        const keyFields = table.fields.filter(f => f.isKey).map(f => f.name);

        const data = rows.slice(1).reduce((acc: AppTableRow[], row) => {
            const rowObj: AppTableRow = {};
            let hasValidKeys = true;

            headers.forEach((header: string, index: number) => {
                let value = row[index];

                const field = table.fields.find(f => f.name === header);
                if (field && field.type === AppFieldType.JSON && typeof value === 'string') {
                    try {
                        value = JSON.parse(value);
                    } catch (e) {
                        console.warn(`Failed to parse JSON for field ${header}:`, e);
                    }
                }

                rowObj[header] = value;
            });

            if (keyFields.length > 0) {
                for (const keyField of keyFields) {
                    const val = rowObj[keyField];
                    if (val === undefined || val === null || String(val).trim() === '') {
                        hasValidKeys = false;
                        break;
                    }
                }
            }

            if (hasValidKeys) {
                acc.push(rowObj);
            }

            return acc;
        }, []);

        return { type: DataSourceType.Inline, data };
    }

    override async addRow(table: AppTable, auth?: string, row?: AppTableRow) {
        if (!row) return;

        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for addRow. Expected ["file", fileId, sheetName]');
        }

        const _auth = this.getAuth(auth);
        const sheets = google.sheets({ version: 'v4', auth: _auth });

        const headerResponse = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: `${sheetName}!A1:ZZ1`
        });

        let headers =
            headerResponse.data.values && headerResponse.data.values[0]
                ? headerResponse.data.values[0].map(h => String(h))
                : [];

        const rowKeys = Object.keys(row);
        const newColumns = rowKeys.filter(key => !headers.includes(key));

        if (newColumns.length > 0) {
            const startColIndex = headers.length;
            const range = `${sheetName}!${this.getColumnLetter(startColIndex + 1)}1`;

            await sheets.spreadsheets.values.update({
                spreadsheetId: fileId,
                range: range,
                valueInputOption: 'RAW',
                requestBody: {
                    values: [newColumns]
                }
            });

            headers = [...headers, ...newColumns];
        }

        const values = headers.map(header => {
            let value = row[header];

            const field = table.fields.find(f => f.name === header);
            if (field && field.type === AppFieldType.JSON && typeof value === 'object') {
                value = JSON.stringify(value);
            }

            if (typeof value === 'object' && value !== null) {
                value = JSON.stringify(value);
            }

            return value ?? '';
        });

        await sheets.spreadsheets.values.append({
            spreadsheetId: fileId,
            range: sheetName,
            valueInputOption: 'USER_ENTERED',
            requestBody: {
                values: [values]
            }
        });
    }

    private getColumnLetter(colIndex: number) {
        let temp,
            letter = '';

        while (colIndex > 0) {
            temp = (colIndex - 1) % 26;
            letter = String.fromCharCode(temp + 65) + letter;
            colIndex = (colIndex - temp - 1) / 26;
        }

        return letter;
    }

    override async updateRow(
        table: AppTable,
        auth?: string,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ) {
        if (!key || !row) return;
        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for updateRow. Expected ["file", fileId, sheetName]');
        }

        const _auth = this.getAuth(auth);
        const sheets = google.sheets({ version: 'v4', auth: _auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];
        if (rows.length === 0) return;

        const headers = rows[0]!.map(h => String(h));

        let rowIndex = -1;
        let existingRowData: Record<string, unknown> = {};

        for (let i = 1; i < rows.length; i++) {
            const currentRow = rows[i]!;
            const currentRowObj: Record<string, unknown> = {};
            headers.forEach((h, idx) => {
                currentRowObj[h] = currentRow[idx];
            });

            let match = true;
            for (const k in key) {
                if (String(currentRowObj[k]) !== String(key[k])) {
                    match = false;
                    break;
                }
            }

            if (match) {
                rowIndex = i;
                existingRowData = currentRowObj;
                break;
            }
        }

        if (rowIndex === -1) {
            return;
        }

        const rowKeys = Object.keys(row);
        const newColumns = rowKeys.filter(k => !headers.includes(k));
        let updatedHeaders = [...headers];

        if (newColumns.length > 0) {
            const startColIndex = headers.length;
            const range = `${sheetName}!${this.getColumnLetter(startColIndex + 1)}1`;

            await sheets.spreadsheets.values.update({
                spreadsheetId: fileId,
                range: range,
                valueInputOption: 'RAW',
                requestBody: {
                    values: [newColumns]
                }
            });

            updatedHeaders = [...headers, ...newColumns];
        }

        const mergedData = { ...existingRowData, ...row };
        const values = updatedHeaders.map(header => {
            let value = mergedData[header];

            const field = table.fields.find(f => f.name === header);
            if (field && field.type === AppFieldType.JSON && typeof value === 'object') {
                value = JSON.stringify(value);
            }
            if (typeof value === 'object' && value !== null) {
                value = JSON.stringify(value);
            }

            return value ?? '';
        });

        const sheetRowNumber = rowIndex + 1;
        const range = `${sheetName}!A${sheetRowNumber}`;

        await sheets.spreadsheets.values.update({
            spreadsheetId: fileId,
            range: range,
            valueInputOption: 'USER_ENTERED',
            requestBody: {
                values: [values]
            }
        });
    }

    override async deleteRow(table: AppTable, auth?: string, key?: Record<string, unknown>) {
        if (!key) return;
        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for deleteRow. Expected ["file", fileId, sheetName]');
        }

        const _auth = this.getAuth(auth);
        const sheets = google.sheets({ version: 'v4', auth: _auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];
        if (rows.length === 0) return;

        const headers = rows[0]!.map(h => String(h));

        let rowIndex = -1;
        for (let i = 1; i < rows.length; i++) {
            const currentRow = rows[i]!;
            const currentRowObj: Record<string, unknown> = {};
            headers.forEach((h, idx) => {
                currentRowObj[h] = currentRow[idx];
            });

            let match = true;
            for (const k in key) {
                if (String(currentRowObj[k]) !== String(key[k])) {
                    match = false;
                    break;
                }
            }

            if (match) {
                rowIndex = i;
                break;
            }
        }

        if (rowIndex === -1) return;

        const sheetRowNumber = rowIndex + 1;
        const range = `${sheetName}!A${sheetRowNumber}:ZZ${sheetRowNumber}`;

        await sheets.spreadsheets.values.clear({
            spreadsheetId: fileId,
            range: range
        });
    }
}
