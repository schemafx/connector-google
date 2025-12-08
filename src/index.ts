import {
    Connector,
    ConnectorTableCapability,
    type AppTable,
    inferTable,
    type AppTableRow,
    AppFieldType
} from 'schemafx';
import { google, drive_v3, sheets_v4 } from 'googleapis';

export default class GoogleConnector extends Connector {
    private getOAuthClient() {
        const clientId = process.env.GOOGLE_CLIENT_ID;
        const clientSecret = process.env.GOOGLE_CLIENT_SECRET;
        const redirectUri = process.env.GOOGLE_REDIRECT_URI;

        if (!clientId || !clientSecret || !redirectUri) {
            throw new Error(
                'Missing GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, or GOOGLE_REDIRECT_URI environment variables'
            );
        }

        return new google.auth.OAuth2(clientId, clientSecret, redirectUri);
    }

    private getAuth() {
        const oauth2Client = this.getOAuthClient();
        const refreshToken = process.env.GOOGLE_REFRESH_TOKEN;

        if (!refreshToken) {
            const authUrl = oauth2Client.generateAuthUrl({
                access_type: 'offline',
                scope: [
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/spreadsheets'
                ]
            });
            throw new Error(
                `Missing GOOGLE_REFRESH_TOKEN. Authorize the app by visiting this url: ${authUrl}`
            );
        }

        oauth2Client.setCredentials({
            refresh_token: refreshToken
        });

        return oauth2Client;
    }

    async authorize(code: string) {
        const oauth2Client = this.getOAuthClient();
        const { tokens } = await oauth2Client.getToken(code);

        console.log('Successfully retrieved tokens.');
        console.log('Add the following to your environment variables:');
        console.log(`GOOGLE_REFRESH_TOKEN=${tokens.refresh_token}`);

        if (!tokens.refresh_token) {
            console.warn(
                'No refresh_token returned. Make sure you used access_type: "offline" and that the user approved access.'
            );
        }

        return tokens;
    }

    async listTables(path: string[]) {
        const auth = this.getAuth();
        const drive = google.drive({ version: 'v3', auth });
        const sheets = google.sheets({ version: 'v4', auth });

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
                path: ['file', id, sheet.properties?.title || ''],
                capabilities: [ConnectorTableCapability.Connect]
            }));
        }

        return [];
    }

    async getTable(path: string[]) {
        const [type, fileId, sheetName] = path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for getTable. Expected ["file", fileId, sheetName]');
        }

        const auth = this.getAuth();
        const sheets = google.sheets({ version: 'v4', auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];

        if (rows.length === 0) {
            return inferTable(sheetName, path, [], this.id);
        }

        const headers = rows[0].map(h => String(h));
        const data = rows.slice(1).map((row: Record<string, unknown>[]) => {
            const rowObj: Record<string, unknown> = {};
            headers.forEach((header: string, index: number) => {
                rowObj[header] = row[index];
            });
            return rowObj;
        });

        return inferTable(sheetName, path, data, this.id);
    }

    async getData(table: AppTable) {
        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for getData. Expected ["file", fileId, sheetName]');
        }

        const auth = this.getAuth();
        const sheets = google.sheets({ version: 'v4', auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];

        if (rows.length === 0) {
            return [];
        }

        const headers = rows[0].map(h => String(h));
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

        return data;
    }

    async addRow(table: AppTable, row?: AppTableRow) {
        if (!row) return [];
        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for addRow. Expected ["file", fileId, sheetName]');
        }

        const auth = this.getAuth();
        const sheets = google.sheets({ version: 'v4', auth });

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

        return this.getData(table);
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

    async updateRow(table: AppTable, key?: Record<string, unknown>, row?: AppTableRow) {
        if (!key || !row) return [];
        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for updateRow. Expected ["file", fileId, sheetName]');
        }

        const auth = this.getAuth();
        const sheets = google.sheets({ version: 'v4', auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];
        if (rows.length === 0) return [];

        const headers = rows[0].map(h => String(h));

        let rowIndex = -1;
        let existingRowData: Record<string, unknown> = {};

        for (let i = 1; i < rows.length; i++) {
            const currentRow = rows[i];
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
            return [];
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

        return this.getData(table);
    }

    async deleteRow(table: AppTable, key?: Record<string, unknown>) {
        if (!key) return [];
        const [type, fileId, sheetName] = table.path;

        if (type !== 'file' || !fileId || !sheetName) {
            throw new Error('Invalid path for deleteRow. Expected ["file", fileId, sheetName]');
        }

        const auth = this.getAuth();
        const sheets = google.sheets({ version: 'v4', auth });

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: fileId,
            range: sheetName
        });

        const rows = response.data.values || [];
        if (rows.length === 0) return [];

        const headers = rows[0].map(h => String(h));

        let rowIndex = -1;
        for (let i = 1; i < rows.length; i++) {
            const currentRow = rows[i];
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

        if (rowIndex === -1) {
            return this.getData(table);
        }

        const sheetRowNumber = rowIndex + 1;
        const range = `${sheetName}!A${sheetRowNumber}:ZZ${sheetRowNumber}`;

        await sheets.spreadsheets.values.clear({
            spreadsheetId: fileId,
            range: range
        });

        return this.getData(table);
    }
}
