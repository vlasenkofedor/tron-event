/**
 * TRON Event Node — Node.js gRPC Subscriber Example
 *
 * This example demonstrates how to connect to a running tron-event node
 * via gRPC, subscribe to address filters, and stream transaction events.
 *
 * Usage:
 *   node index.js [address1] [address2] ...
 *
 * Example:
 *   node index.js TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t
 */

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// ── Configuration ──────────────────────────────────────────────────────────
const SERVER_ADDR = '127.0.0.1:50051';
const PROTO_PATH = path.join(__dirname, '../../rust-node/proto/node_api.proto');

// Parse command-line arguments (skip first two: node and script path)
const addresses = process.argv.slice(2);
const monitorAddresses = addresses.length > 0
    ? addresses
    : ['TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t']; // Default: USDT contract

// ── Load protobuf definitions ─────────────────────────────────────────────
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
});

const apiProto = grpc.loadPackageDefinition(packageDefinition).api;

// ── Main subscriber logic ─────────────────────────────────────────────────
async function main() {
    console.log('TRON Event Node — Node.js Subscriber');
    console.log('=====================================');
    console.log(`Monitoring ${monitorAddresses.length} address(es):`);
    monitorAddresses.forEach((addr) => console.log(`  - ${addr}`));
    console.log();

    // Create gRPC client
    const client = new apiProto.NodeApi(
        SERVER_ADDR,
        grpc.credentials.createInsecure(),
    );

    console.log(`Connecting to ${SERVER_ADDR}...`);

    // Open the bidirectional subscription stream
    const stream = client.subscribe();

    let eventCount = 0;
    const startTime = Date.now();

    // Handle incoming transaction events from the server
    stream.on('data', (event) => {
        eventCount++;

        // Parse the JSON payload from the event
        let json;
        try {
            json = JSON.parse(event.transaction_json);
        } catch (e) {
            console.error('Failed to parse event JSON:', e.message);
            return;
        }

        // Extract key fields for display
        const txid = json.txid || 'unknown';
        const txType = json.type || 'unknown';
        const status = json.status || 'unknown';
        const updateType = json.update_type || 'unknown';
        const details = json.details || {};
        const from = details.from || '-';
        const to = details.to || '-';
        const amount = details.amount || '-';
        const symbol = details.symbol || '-';

        console.log(
            `[EVENT #${eventCount}] ${txid} | ${txType} | ${status} | ${updateType}`
        );
        console.log(
            `  from=${from}, to=${to}, amount=${amount}, symbol=${symbol}`
        );

        // Print elapsed time every 10 events
        if (eventCount % 10 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            console.log(
                `\n  [STATS] ${eventCount} events in ${elapsed.toFixed(1)}s ` +
                `(${(eventCount / elapsed).toFixed(1)} events/s)\n`
            );
        }
    });

    // Handle stream errors
    stream.on('error', (err) => {
        console.error('gRPC stream error:', err.message);
        process.exit(1);
    });

    // Handle stream closure
    stream.on('end', () => {
        console.log('\nStream ended.');
        printSummary(eventCount, startTime);
        process.exit(0);
    });

    // ── Send initial ADD subscriptions ─────────────────────────────────────
    // Send one SubscriptionAction per address with Action::ADD
    for (const addr of monitorAddresses) {
        const action = {
            action: 'ADD', // proto enum: ADD=0, REMOVE=1
            addresses: [addr],
        };
        stream.write(action);
        console.log(`  [SENT] ADD: ${addr}`);
    }
    console.log('\nListening for transaction events...\n');

    // ── Graceful shutdown on SIGINT (Ctrl+C) ──────────────────────────────
    process.on('SIGINT', () => {
        console.log('\n\nReceived SIGINT, closing stream...');
        stream.end();
        printSummary(eventCount, startTime);
        process.exit(0);
    });
}

// ── Summary printer ───────────────────────────────────────────────────────
function printSummary(count, startTime) {
    const elapsed = (Date.now() - startTime) / 1000;
    console.log('\n=====================================');
    console.log('Stream closed.');
    console.log(`Total events: ${count}`);
    console.log(`Elapsed time: ${elapsed.toFixed(1)}s`);
    if (elapsed > 0) {
        console.log(`Average rate: ${(count / elapsed).toFixed(1)} events/s`);
    }
}

// ── Error handling ────────────────────────────────────────────────────────
main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
});
