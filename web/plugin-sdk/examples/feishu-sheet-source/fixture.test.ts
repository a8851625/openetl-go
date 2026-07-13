/**
 * Fixture expectations for feishu-sheet-source plugin sample.
 * These are documentation-style assertions used by certification kit
 * string checks and local review; they do not require a WASM runtime.
 */

export type FixtureCase = {
  name: string;
  config: Record<string, unknown>;
  expect: 'error' | 'eof' | 'records';
  errorIncludes?: string;
  minRecords?: number;
};

export const FIXTURE_CASES: FixtureCase[] = [
  {
    name: 'token/config missing app_id',
    config: {
      app_secret: 's',
      spreadsheet_token: 't',
      sheet_range: 'Sheet1!A1:B2',
    },
    expect: 'error',
    errorIncludes: 'app_id',
  },
  {
    name: 'token/config missing app_secret',
    config: {
      app_id: 'a',
      spreadsheet_token: 't',
      sheet_range: 'Sheet1!A1:B2',
    },
    expect: 'error',
    errorIncludes: 'app_secret',
  },
  {
    name: 'empty sheet rows -> EOF',
    config: {
      app_id: 'a',
      app_secret: 's',
      spreadsheet_token: 't',
      sheet_range: 'Sheet1!A1:B2',
      fixture_rows: [],
    },
    expect: 'eof',
  },
  {
    name: 'header error when first row is not strings',
    config: {
      app_id: 'a',
      app_secret: 's',
      spreadsheet_token: 't',
      sheet_range: 'Sheet1!A1:B2',
      fixture_rows: [[1, 2], [3, 4]],
    },
    expect: 'error',
    errorIncludes: 'header',
  },
  {
    name: 'happy path emits data rows',
    config: {
      app_id: 'a',
      app_secret: 's',
      spreadsheet_token: 't',
      sheet_range: 'Sheet1!A1:C3',
      fixture_rows: [
        ['id', 'name', 'amount'],
        ['1', 'alpha', '10'],
        ['2', 'beta', '20'],
      ],
    },
    expect: 'records',
    minRecords: 2,
  },
];

/** Registry name after install. */
export const REGISTRY_TYPE = 'plugin_feishu-sheet-source';

/** Manifest ABI constants that must stay aligned with Plugin ABI v1. */
export const ABI = 'openetl.plugin.abi/v1';
export const MIN_RUNTIME = 'openetl-runtime/v1';
