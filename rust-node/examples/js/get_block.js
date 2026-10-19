const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const PROTO_PATH = path.join(__dirname, 'proto/api/api.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
    includeDirs: [path.join(__dirname, 'proto')]
});
const api = grpc.loadPackageDefinition(packageDefinition).protocol;
const client = new api.Wallet('127.0.0.1:50051', grpc.credentials.createInsecure());
client.GetBlockByNum({ num: 81783519 }, (err, response) => {
    if (err) { console.error('Error:', err.message); }
    else { console.log('Success: Block number', response.block_header.raw_data.number); }
    process.exit(0);
});
