#![allow(dead_code)]
use crate::protocol;
use bytes::{BufMut, BytesMut};
use prost::Message;

pub const MSG_LIBP2P_PONG: u8 = 0xfe;
pub const MSG_LIBP2P_HELLO: u8 = 0xfd;
pub const MSG_LIBP2P_STATUS: u8 = 0xfc;
pub const MSG_LIBP2P_DISCONNECT: u8 = 0xfb;
pub const MSG_LIBP2P_PING: u8 = 0xff;

// ─── Tron Layer Message IDs (Prefix >= 0) ──────────────────────────────────
pub const MSG_TRX: u8 = 0x01;
pub const MSG_BLOCK: u8 = 0x02;
pub const MSG_TRXS: u8 = 0x03;
pub const MSG_BLOCKS: u8 = 0x04;
pub const MSG_BLOCKHEADERS: u8 = 0x05;
pub const MSG_INVENTORY: u8 = 0x06;
pub const MSG_FETCH_INV_DATA: u8 = 0x07;
pub const MSG_SYNC_BLOCK_CHAIN: u8 = 0x08;
pub const MSG_BLOCK_CHAIN_INVENTORY: u8 = 0x09;
pub const MSG_ITEM_NOT_FOUND: u8 = 0x10;
pub const MSG_FETCH_BLOCK_HEADERS: u8 = 0x11;
pub const MSG_BLOCK_INVENTORY: u8 = 0x12;
pub const MSG_TRX_INVENTORY: u8 = 0x13;
pub const MSG_PBFT_COMMIT_MSG: u8 = 0x14;
pub const MSG_TRON_HELLO: u8 = 0x20;
pub const MSG_TRON_DISCONNECT: u8 = 0x21;
pub const MSG_TRON_PING: u8 = 0x22;
pub const MSG_TRON_PONG: u8 = 0x23;
pub const MSG_PBFT_MSG: u8 = 0x34;

#[derive(Debug, Clone)]
pub enum P2pMessage {
    // LibP2P Layer
    Libp2pHello(protocol::p2p::HelloMessage),
    Libp2pStatus(protocol::p2p::StatusMessage),
    Libp2pDisconnect(protocol::p2p::P2pDisconnectMessage),
    Libp2pPing(protocol::p2p::KeepAliveMessage),
    Libp2pPong(protocol::p2p::KeepAliveMessage),

    // Tron Layer
    TronHello(protocol::protocol::HelloMessage),
    TronDisconnect(protocol::protocol::DisconnectMessage),
    TronPing(Vec<u8>),
    TronPong(Vec<u8>),
    TronInventory(protocol::protocol::Inventory),
    TronFetchInvData(protocol::protocol::Inventory),
    TronSyncBlockChain(protocol::protocol::BlockInventory),
    TronBlockChainInventory(protocol::protocol::ChainInventory),
    TronTrx(protocol::protocol::Transaction),
    TronBlock(protocol::protocol::Block),
    TronTrxs(protocol::protocol::Transactions),
    TronBlockInventory(protocol::protocol::BlockInventory),
    TronTrxsInventory(protocol::protocol::BlockInventory),

    // Generic / Unknown
    Unknown(u8, Vec<u8>),
}

impl P2pMessage {
    pub fn get_type_id(&self) -> u8 {
        match self {
            P2pMessage::Libp2pHello(_) => MSG_LIBP2P_HELLO,
            P2pMessage::Libp2pStatus(_) => MSG_LIBP2P_STATUS,
            P2pMessage::Libp2pDisconnect(_) => MSG_LIBP2P_DISCONNECT,
            P2pMessage::Libp2pPing(_) => MSG_LIBP2P_PING,
            P2pMessage::Libp2pPong(_) => MSG_LIBP2P_PONG,
            P2pMessage::TronHello(_) => MSG_TRON_HELLO,
            P2pMessage::TronDisconnect(_) => MSG_TRON_DISCONNECT,
            P2pMessage::TronPing(_) => MSG_TRON_PING,
            P2pMessage::TronPong(_) => MSG_TRON_PONG,
            P2pMessage::TronInventory(_) => MSG_INVENTORY,
            P2pMessage::TronFetchInvData(_) => MSG_FETCH_INV_DATA,
            P2pMessage::TronSyncBlockChain(_) => MSG_SYNC_BLOCK_CHAIN,
            P2pMessage::TronBlockChainInventory(_) => MSG_BLOCK_CHAIN_INVENTORY,
            P2pMessage::TronTrx(_) => MSG_TRX,
            P2pMessage::TronBlock(_) => MSG_BLOCK,
            P2pMessage::TronTrxs(_) => MSG_TRXS,
            P2pMessage::TronBlockInventory(_) => MSG_BLOCK_INVENTORY,
            P2pMessage::TronTrxsInventory(_) => MSG_TRX_INVENTORY,
            P2pMessage::Unknown(tid, _) => *tid,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            P2pMessage::Libp2pHello(_) => "LibP2P Hello",
            P2pMessage::Libp2pStatus(_) => "LibP2P Status",
            P2pMessage::Libp2pDisconnect(_) => "LibP2P Disconnect",
            P2pMessage::Libp2pPing(_) => "LibP2P Ping",
            P2pMessage::Libp2pPong(_) => "LibP2P Pong",
            P2pMessage::TronHello(_) => "Tron Hello",
            P2pMessage::TronDisconnect(_) => "Tron Disconnect",
            P2pMessage::TronPing(_) => "Tron Ping",
            P2pMessage::TronPong(_) => "Tron Pong",
            P2pMessage::TronInventory(_) => "Inventory",
            P2pMessage::TronFetchInvData(_) => "FetchInvData",
            P2pMessage::TronSyncBlockChain(_) => "SyncBlockChain",
            P2pMessage::TronBlockChainInventory(_) => "BlockChainInventory",
            P2pMessage::TronTrx(_) => "Transaction",
            P2pMessage::TronBlock(_) => "Block",
            P2pMessage::TronTrxs(_) => "Transactions",
            P2pMessage::TronBlockInventory(_) => "BlockInventory",
            P2pMessage::TronTrxsInventory(_) => "TrxInventory",
            P2pMessage::Unknown(_, _) => "Unknown",
        }
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), prost::EncodeError> {
        match self {
            P2pMessage::Libp2pHello(m) => m.encode(buf),
            P2pMessage::Libp2pStatus(m) => m.encode(buf),
            P2pMessage::Libp2pDisconnect(m) => m.encode(buf),
            P2pMessage::Libp2pPing(m) => m.encode(buf),
            P2pMessage::Libp2pPong(m) => m.encode(buf),
            P2pMessage::TronHello(m) => m.encode(buf),
            P2pMessage::TronDisconnect(m) => m.encode(buf),
            P2pMessage::TronPing(d) => { buf.put_slice(d); Ok(()) }
            P2pMessage::TronPong(d) => { buf.put_slice(d); Ok(()) }
            P2pMessage::TronInventory(m) => m.encode(buf),
            P2pMessage::TronFetchInvData(m) => m.encode(buf),
            P2pMessage::TronSyncBlockChain(m) => m.encode(buf),
            P2pMessage::TronBlockChainInventory(m) => m.encode(buf),
            P2pMessage::TronTrx(m) => m.encode(buf),
            P2pMessage::TronBlock(m) => m.encode(buf),
            P2pMessage::TronTrxs(m) => m.encode(buf),
            P2pMessage::TronBlockInventory(m) => m.encode(buf),
            P2pMessage::TronTrxsInventory(m) => m.encode(buf),
            P2pMessage::Unknown(_, data) => { buf.put_slice(data); Ok(()) }
        }
    }

    pub fn decode(type_id: u8, data: &[u8]) -> Self {
        match type_id {
            MSG_LIBP2P_HELLO => protocol::p2p::HelloMessage::decode(data).map(P2pMessage::Libp2pHello).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_LIBP2P_STATUS => protocol::p2p::StatusMessage::decode(data).map(P2pMessage::Libp2pStatus).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_LIBP2P_DISCONNECT => protocol::p2p::P2pDisconnectMessage::decode(data).map(P2pMessage::Libp2pDisconnect).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_LIBP2P_PING => protocol::p2p::KeepAliveMessage::decode(data).map(P2pMessage::Libp2pPing).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_LIBP2P_PONG => protocol::p2p::KeepAliveMessage::decode(data).map(P2pMessage::Libp2pPong).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_TRON_HELLO => protocol::protocol::HelloMessage::decode(data).map(P2pMessage::TronHello).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_TRON_DISCONNECT => protocol::protocol::DisconnectMessage::decode(data).map(P2pMessage::TronDisconnect).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),

            MSG_TRON_PING => P2pMessage::TronPing(data.to_vec()),
            MSG_TRON_PONG => P2pMessage::TronPong(data.to_vec()),
            MSG_INVENTORY => protocol::protocol::Inventory::decode(data).map(P2pMessage::TronInventory).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_FETCH_INV_DATA => protocol::protocol::Inventory::decode(data).map(P2pMessage::TronFetchInvData).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_SYNC_BLOCK_CHAIN => protocol::protocol::BlockInventory::decode(data).map(P2pMessage::TronSyncBlockChain).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_BLOCK_CHAIN_INVENTORY => protocol::protocol::ChainInventory::decode(data).map(P2pMessage::TronBlockChainInventory).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_TRX => protocol::protocol::Transaction::decode(data).map(P2pMessage::TronTrx).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_BLOCK => protocol::protocol::Block::decode(data).map(P2pMessage::TronBlock).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_TRXS => protocol::protocol::Transactions::decode(data).map(P2pMessage::TronTrxs).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_BLOCK_INVENTORY => protocol::protocol::BlockInventory::decode(data).map(P2pMessage::TronBlockInventory).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            MSG_TRX_INVENTORY => protocol::protocol::BlockInventory::decode(data).map(P2pMessage::TronTrxsInventory).unwrap_or(P2pMessage::Unknown(type_id, data.to_vec())),
            _ => P2pMessage::Unknown(type_id, data.to_vec()),
        }
    }
}
