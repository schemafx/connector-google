import {
    Connector,
    ConnectorTable,
    ConnectorTableCapability,
    AppTable,
    inferTable
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

    async listTables(path: string[]): Promise<ConnectorTable[]> {
        const auth = this.getAuth();
        const drive = google.drive({ version: 'v3', auth });
        const sheets = google.sheets({ version: 'v4', auth });

        // Root
        if (path.length === 0) {
            const tables: ConnectorTable[] = [
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
                // Ignore error if shared drives cannot be listed, just don't show the folder
                console.warn('Could not list shared drives:', error);
            }

            return tables;
        }

        const [type, id] = path;

        // List Shared Drives
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

        // List Folder (Personal or Shared Drive)
        if (type === 'folder' || type === 'drive') {
            // If it's a shared drive, the id is the drive id.
            // When listing a shared drive, we need specific query parameters.
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
                // By default explore files with same path in case we want to support other file types later
                // But for now only specific ones have capabilities
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

        // List Sheets in a Spreadsheet
        if (type === 'file') {
            // path is ['file', fileId]
            // We want to return list of sheets.
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

    async getTable(path: string[]): Promise<AppTable> {
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

        // Convert to AppTableRow objects
        // Assuming first row is header
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
}
