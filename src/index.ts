import {
    type AppTable,
    type AppTableRow,
    Connector,
    ConnectorTableCapability,
    type ConnectorOptions,
    type DataSourceDefinition
} from 'schemafx';

import { google, type drive_v3, type sheets_v4 } from 'googleapis';
import {
    spreadsheetHandler,
    csvHandler,
    jsonHandler,
    type GoogleClients
} from './handlers/index.js';

import { validateFileId } from './handlers/utils.js';

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

    private getClients(auth?: string): GoogleClients {
        const _auth = this.getAuth(auth);

        return {
            drive: google.drive({ version: 'v3', auth: _auth }),
            sheets: google.sheets({ version: 'v4', auth: _auth })
        };
    }

    private getHandler(type: string) {
        switch (type) {
            case 'file':
                return spreadsheetHandler;
            case 'csv':
                return csvHandler;
            case 'json':
                return jsonHandler;
            default:
                return null;
        }
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

        if (!tokens.refresh_token) throw new Error('No refresh_token returned.');

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
        const clients = this.getClients(auth);

        if (path.length === 0) {
            const tables = [
                {
                    name: 'Personal',
                    path: ['folder', 'root'],
                    capabilities: [ConnectorTableCapability.Explore]
                }
            ];

            // Just need to check if any shared drives exist
            const response = await clients.drive.drives.list({
                pageSize: 1,
                useDomainAdminAccess: false
            });

            if (response.data.drives && response.data.drives.length > 0) {
                tables.push({
                    name: 'Shared Drives',
                    path: ['drives'],
                    capabilities: [ConnectorTableCapability.Explore]
                });
            }

            return tables;
        }

        const [type, id] = path;

        if (type === 'drives') {
            const allDrives: drive_v3.Schema$Drive[] = [];
            let pageToken: string | undefined;

            do {
                const response = await clients.drive.drives.list({
                    pageSize: 100,
                    pageToken
                });

                if (response.data.drives) allDrives.push(...response.data.drives);
                pageToken = response.data.nextPageToken ?? undefined;
            } while (pageToken);

            return allDrives.map((d: drive_v3.Schema$Drive) => ({
                name: d.name || 'Unknown Drive',
                path: ['drive', d.id!],
                capabilities: [ConnectorTableCapability.Explore]
            }));
        }

        if (type === 'folder' || type === 'drive') {
            if (id && id !== 'root') validateFileId(id);

            const isSharedDrive = type === 'drive';
            const allFiles: drive_v3.Schema$File[] = [];
            let pageToken: string | undefined;

            do {
                const queryParams: drive_v3.Params$Resource$Files$List = {
                    q: `'${id}' in parents and trashed = false`,
                    fields: 'nextPageToken, files(id, name, mimeType, shortcutDetails)',
                    pageSize: 100,
                    includeItemsFromAllDrives: true,
                    supportsAllDrives: true,
                    pageToken
                };

                if (isSharedDrive) {
                    queryParams.corpora = 'drive';
                    queryParams.driveId = id;
                }

                const response = await clients.drive.files.list(queryParams);
                if (response.data.files) allFiles.push(...response.data.files);

                pageToken = response.data.nextPageToken ?? undefined;
            } while (pageToken);

            return allFiles.map((file: drive_v3.Schema$File) => {
                let capability = ConnectorTableCapability.Unavailable;
                let nextPath: string[] = [];

                const isShortcut = file.mimeType === 'application/vnd.google-apps.shortcut';
                const targetMimeType = isShortcut
                    ? file.shortcutDetails?.targetMimeType
                    : file.mimeType;

                const targetId = isShortcut ? file.shortcutDetails?.targetId : file.id;

                if (targetMimeType === 'application/vnd.google-apps.folder') {
                    capability = ConnectorTableCapability.Explore;
                    nextPath = ['folder', targetId!];
                } else if (targetMimeType === 'application/vnd.google-apps.spreadsheet') {
                    capability = ConnectorTableCapability.Explore;
                    nextPath = ['file', targetId!];
                } else if (
                    targetMimeType === 'text/csv' ||
                    targetMimeType === 'text/tab-separated-values' ||
                    file.name?.toLowerCase().endsWith('.csv') ||
                    file.name?.toLowerCase().endsWith('.tsv')
                ) {
                    capability = ConnectorTableCapability.Connect;
                    nextPath = ['csv', targetId!];
                } else if (
                    targetMimeType === 'application/json' ||
                    file.name?.toLowerCase().endsWith('.json')
                ) {
                    capability = ConnectorTableCapability.Connect;
                    nextPath = ['json', targetId!];
                }

                return {
                    name: file.name || 'Unknown File',
                    path: nextPath.length > 0 ? nextPath : ['file', file.id!],
                    capabilities: [capability]
                };
            });
        }

        if (type === 'file') {
            if (id) validateFileId(id);

            const response = await clients.sheets.spreadsheets.get({
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

        if (!type) throw new Error('Invalid path for getTable. Type is required');

        const handler = this.getHandler(type);
        if (!handler || !fileId) {
            throw new Error(`Invalid path for getTable. Unsupported type: ${type}`);
        }

        const clients = this.getClients(auth);
        return handler.getTable(fileId, clients, sheetName);
    }

    override async getData(table: AppTable, auth?: string): Promise<DataSourceDefinition> {
        const [type] = table.path;

        if (!type) throw new Error('Invalid path for getData. Type is required');

        const handler = this.getHandler(type);
        if (!handler) throw new Error(`Invalid path for getData. Unsupported type: ${type}`);

        const clients = this.getClients(auth);
        return handler.getData(table, clients);
    }

    override async addRow(table: AppTable, auth?: string, row?: AppTableRow) {
        if (!row) return;

        const [type] = table.path;

        if (!type) throw new Error('Invalid path for addRow. Type is required');

        const handler = this.getHandler(type);
        if (!handler) throw new Error(`Invalid path for addRow. Unsupported type: ${type}`);

        const clients = this.getClients(auth);
        return handler.addRow(table, row, clients);
    }

    override async updateRow(
        table: AppTable,
        auth?: string,
        key?: Record<string, unknown>,
        row?: AppTableRow
    ) {
        if (!key || !row) return;

        const [type] = table.path;

        if (!type) throw new Error('Invalid path for updateRow. Type is required');

        const handler = this.getHandler(type);
        if (!handler) throw new Error(`Invalid path for updateRow. Unsupported type: ${type}`);

        const clients = this.getClients(auth);
        return handler.updateRow(table, key, row, clients);
    }

    override async deleteRow(table: AppTable, auth?: string, key?: Record<string, unknown>) {
        if (!key) return;

        const [type] = table.path;

        if (!type) throw new Error('Invalid path for deleteRow. Type is required');

        const handler = this.getHandler(type);
        if (!handler) throw new Error(`Invalid path for deleteRow. Unsupported type: ${type}`);

        const clients = this.getClients(auth);
        return handler.deleteRow(table, key, clients);
    }
}
