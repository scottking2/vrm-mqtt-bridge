# VRM Bridge V2 Runtime Note

## Runtime Config Contract

The bridge now supports two runtime modes:

1. Lakemates config-pull V2 with runtime decrypt
2. Static config fallback via `BRIDGE_CONFIG_FILE` / `BRIDGE_CONFIG_JSON` or legacy single-boat env vars

### Required shared runtime env for V2

- `VICTRON_INTERNAL_API_BASE_URL`
- `VICTRON_BRIDGE_MACHINE_TOKEN`
- `VICTRON_ENCRYPTION_KEY`
- `DATABASE_HOST`
- `DATABASE_NAME`
- `DATABASE_USER`
- `DATABASE_PASS`

Optional V2 env:

- `VICTRON_CONFIG_PULL_ENABLED`
- `VICTRON_INTERNAL_API_CONFIG_PATH`
- `VICTRON_INTERNAL_API_STATUS_PATH`
- `VICTRON_INTERNAL_API_TIMEOUT`
- `VICTRON_CONFIG_REFRESH_INTERVAL`
- `VICTRON_CONFIG_PULL_ALLOW_STATIC_FALLBACK`
- `LOCAL_MQTT_*`
- `DB_WRITE_INTERVAL`

### Expected Lakemates config response

`GET {VICTRON_INTERNAL_API_BASE_URL}{VICTRON_INTERNAL_API_CONFIG_PATH}`

Machine auth:

- `Authorization: Bearer {VICTRON_BRIDGE_MACHINE_TOKEN}`

Expected response shape:

```json
{
  "defaults": {
    "vrmApiBase": "https://vrmapi.victronenergy.com/v2",
    "pollInterval": 60,
    "requestTimeout": 20,
    "lakematesPushUrl": "https://stage.lakemates.com/api/victron/ingest",
    "lakematesPushTimeout": 10,
    "lakematesIncludeSummary": false
  },
  "integrations": [
    {
      "boatSlug": "tranquility",
      "boatName": "Tranquility",
      "enabled": true,
      "vrmPortalId": "c0619ab12345",
      "lakemates": {
        "siteKey": "stage-victron-tranquility",
        "ingestSecret": "push-secret"
      },
      "encryptedCredentials": {
        "algorithm": "aes-256-gcm",
        "iv": "base64",
        "ciphertext": "base64",
        "tag": "base64",
        "aad": "base64"
      }
    }
  ]
}
```

Notes:

- `vrmPortalId` remains plain-text metadata.
- Credentials may also arrive as a transitional plain `credentials` object or top-level `vrmToken`; the bridge still prefers `encryptedCredentials`.
- `VICTRON_ENCRYPTION_KEY` must decode to 16, 24, or 32 bytes. The intended contract is a 32-byte base64 value.
- Decrypted credentials stay in process memory only and are never persisted or logged.

### Expected status callback

`POST {VICTRON_INTERNAL_API_BASE_URL}{VICTRON_INTERNAL_API_STATUS_PATH}`

Payload shape:

```json
{
  "boatSlug": "tranquility",
  "boatName": "Tranquility",
  "siteKey": "stage-victron-tranquility",
  "enabled": true,
  "runtimeStatus": "healthy",
  "lastPollAt": "2026-04-11T17:00:00Z",
  "lastSuccessAt": "2026-04-11T17:00:00Z",
  "lastErrorAt": null,
  "lastErrorCode": null,
  "lastErrorMessage": null,
  "consecutiveFailures": 0,
  "lastMetricCount": 12,
  "lastPushAt": "2026-04-11T17:00:00Z"
}
```

No raw VRM credential material is ever included.

## Runtime Behavior

- The bridge starts from shared trust material only when V2 config-pull is enabled.
- One worker runs per enabled boat, and each worker has its own session, installation resolution, and failure state.
- Lakemates config is refreshed on a loop; changed integrations are restarted individually.
- A failed config refresh does not tear down the currently running workers.
- Static config fallback remains available for rollout safety and can coexist as the initial runtime set before Lakemates config is proven.

## Rollout Notes

1. Stage can deploy V2 with the shared machine secrets and keep a static fallback integration definition for the first rollout.
2. Once Lakemates Workstream B exposes the machine config API, the bridge will replace the fallback worker set on the first successful refresh.
3. If the Lakemates API is temporarily unavailable, the bridge keeps the last-good in-memory worker set running.
4. If Lakemates returns an explicit empty integration list, the bridge treats that as authoritative and stops all workers.

## Dependencies On Other Workstreams

- Workstream B must provide the machine-only config endpoint and status callback endpoint.
- Workstream B must encrypt credential payloads with AES-GCM-compatible fields matching the documented contract.
- Workstream F still needs to wire the new shared env vars into the deployment.
