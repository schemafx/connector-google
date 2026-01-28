import { Readable } from 'stream';
import type { GoogleClients } from './types.js';
import { AppFieldType, type AppTable } from 'schemafx';

export function validateFileId(fileId: string): void {
    if (!fileId || typeof fileId !== 'string') throw new Error('File ID is required');
    if (fileId.length > 100) throw new Error('File ID exceeds maximum length');
    if (!/^[a-zA-Z0-9_-]+$/.test(fileId)) throw new Error('Invalid file ID format');
}

export function escapeSheetName(sheetName: string): string {
    if (!sheetName || typeof sheetName !== 'string') throw new Error('Sheet name is required');

    const escaped = sheetName.replace(/'/g, "''");
    return `'${escaped}'`;
}

export function safeJsonParse(content: string, maxSize: number = 10 * 1024 * 1024): unknown {
    if (content.length > maxSize) {
        throw new Error(`JSON content exceeds maximum allowed size of ${maxSize} bytes`);
    }

    const parsed = JSON.parse(content);

    if (parsed === null || typeof parsed !== 'object') {
        throw new Error('JSON must be an object or array');
    }

    return parsed;
}

export async function readFileContent(fileId: string, clients: GoogleClients): Promise<string> {
    validateFileId(fileId);

    const response = await clients.drive.files.get(
        { fileId, alt: 'media' },
        { responseType: 'text' }
    );

    return response.data as string;
}

export async function writeFileContent(
    fileId: string,
    content: string,
    mimeType: string,
    clients: GoogleClients
): Promise<void> {
    validateFileId(fileId);

    await clients.drive.files.update({
        fileId,
        media: {
            mimeType,
            body: Readable.from([content])
        }
    });
}

export async function getFileName(fileId: string, clients: GoogleClients): Promise<string> {
    validateFileId(fileId);

    const response = await clients.drive.files.get({
        fileId,
        fields: 'name'
    });

    return response.data.name || 'Unknown';
}

export function findRowIndex(
    data: Record<string, unknown>[],
    key: Record<string, unknown>
): number {
    return data.findIndex(r => {
        for (const k in key) {
            if (String(r[k]) !== String(key[k])) return false;
        }

        return true;
    });
}

export function hasValidKeys(row: Record<string, unknown>, keyFields: string[]): boolean {
    if (keyFields.length === 0) return true;

    for (const keyField of keyFields) {
        const val = row[keyField];
        if (val === undefined || val === null || String(val).trim() === '') return false;
    }

    return true;
}

export function getKeyFields(table: AppTable): string[] {
    return table.fields.filter(f => f.isKey).map(f => f.name);
}

export function serializeFieldValue(
    value: unknown,
    field: AppTable['fields'][number] | undefined
): unknown {
    if (field && field.type === AppFieldType.JSON && typeof value === 'object') {
        return JSON.stringify(value);
    } else if (typeof value === 'object' && value !== null) {
        return JSON.stringify(value);
    }

    return value ?? '';
}

export function deserializeFieldValue(
    value: unknown,
    field: AppTable['fields'][number] | undefined
): unknown {
    if (field && field.type === AppFieldType.JSON && typeof value === 'string') {
        try {
            return JSON.parse(value);
        } catch {}
    }

    return value;
}
