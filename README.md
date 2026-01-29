# @schemafx/connector-google

A Google Drive and Sheets connector for [SchemaFX](https://github.com/schemafx/schemafx), enabling seamless integration with Google Sheets, CSV, TSV, and JSON files stored in Google Drive.

## Features

- 🔐 **OAuth2 Authentication** - Secure authentication
- 📊 **Google Sheets** - Full CRUD operations on spreadsheet data
- 📄 **CSV/TSV Files** - Read and write delimited files with auto-detection
- 📋 **JSON Files** - Support for JSON array data sources
- 📁 **Drive Navigation** - Browse personal and shared drives
- 🔗 **Shortcut Support** - Handles Google Drive shortcuts as normal folders

## Installation

```bash
npm install @schemafx/connector-google
```

## Prerequisites

### Google Cloud Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the following APIs:
    - Google Drive API
    - Google Sheets API
4. Configure the OAuth consent screen:
    - Add required scopes: `userinfo.email`, `drive`, `spreadsheets`
    - Add authorized test users (if in testing mode)
5. Create OAuth 2.0 credentials:
    - Application type: Web application
    - Add your redirect URI (e.g., `http://localhost:3000/api/connectors/{connector-id}/auth/callback`)
6. Note your **Client ID** and **Client Secret**

## Usage

### Basic Setup

```typescript
import SchemaFX from 'schemafx';
import GoogleConnector from '@schemafx/connector-google';

const connector = new GoogleConnector({
    id: 'google-drive',
    name: 'Google Drive',
    clientId: process.env.GOOGLE_CLIENT_ID,
    clientSecret: process.env.GOOGLE_CLIENT_SECRET,
    redirectUri: process.env.APP_URL
});

// Normal SchemaFX initialization.
const sfx = new SchemaFX({
    // ...
    dataServiceOpts: {
        // ...
        connectors: [
            // ...
            connector
        ]
    }
});
```

_Learn more on [Self Hosting SchemaFX through the official docs](https://docs.schemafx.com/advanced/self-host-schemafx)._

## Supported File Types

| Type          | Path Format                          |
| ------------- | ------------------------------------ |
| Google Sheets | `['file', spreadsheetId, sheetName]` |
| CSV Files     | `['csv', fileId]`                    |
| TSV Files     | `['csv', fileId]`                    |
| JSON Files    | `['json', fileId]`                   |

## API Reference

### `GoogleConnectorOptions`

| Property       | Type     | Description                         |
| -------------- | -------- | ----------------------------------- |
| `id`           | `string` | Unique identifier for the connector |
| `name`         | `string` | Display name for the connector      |
| `clientId`     | `string` | Google OAuth2 Client ID             |
| `clientSecret` | `string` | Google OAuth2 Client Secret         |
| `redirectUri`  | `string` | Base URL for OAuth callback         |

## 🤝 Contributing

- See [`CONTRIBUTING.md`](.github/CONTRIBUTING.md) for guidelines
- Issues, discussions, and PRs are welcome

## 📜 License

Apache 2.0 — see [LICENSE](LICENSE).

SchemaFX is community-driven. Contributions and new connectors are encouraged.
