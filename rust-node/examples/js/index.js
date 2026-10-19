/**
 * Tron Node — gRPC Subscribe Example (Node.js)
 *
 * Usage:
 *   npm install
 *   node index.js
 */

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PROTO_PATH = path.join(__dirname, '../../proto/node_api.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [path.join(__dirname, '../../proto')]
});

const apiProto = grpc.loadPackageDefinition(packageDefinition).api;
const client = new apiProto.NodeApi('127.0.0.1:50051', grpc.credentials.createInsecure());

function subscribeBatch(addresses) {
    console.log(`[Subscribe] Batch subscribing to: ${addresses.join(', ')}`);
    const stream = client.subscribe();
    stream.write({ action: 'ADD', addresses: addresses });
    return stream;
}

const stream = subscribeBatch([
    'TE9WMPdzVgAx8h6KFpxVRUPFVBaUYTest3',
    'TTtPqcSyJfbArskV3zKEZpMDAAdqiE6U7g',
]);

stream.on('data', (response) => {
    try {
        const tx = JSON.parse(response.transaction_json);
        const details = tx.details || {};
        const timestamp = details.timestamp || tx.timestamp;
        const timeStr = timestamp ? new Date(parseInt(timestamp)).toISOString() : 'N/A';
        console.log(JSON.stringify(tx, null, 2));
    } catch (e) {
        console.error('JSON Parsing Error:', e.message, response.transaction_json);
    }
});

stream.on('error', (err) => {
    if (err.code === grpc.status.CANCELLED) {
        console.log('[Stream] Connection closed by client.');
    } else {
        console.error('[Stream Error]', err.message);
    }
});

stream.on('end', () => {
    console.log('[Stream] Connection closed by server.');
});

console.log('[Info] Waiting for events. Press Ctrl+C to exit.\n');
