#!/usr/bin/env python3
"""Probe TRON P2P seed nodes and report which ones complete a full handshake.

A node is considered healthy when it:
  1. accepts a TCP connection on port 18888
  2. responds to the LibP2P hello (0xfd) with its own hello
  3. sends a Tron hello (0x20) announcing its chain head

Many public seeds accept the TCP connection but drop it during the
handshake (roughly 5 % complete it), so an empty result for a single node
is normal. Probing the whole list in parallel is the intended usage.

The output is a curated seed list ordered by observed head height.
"""

import socket
import sys
import time
import concurrent.futures
import re


# ── Protocol helpers ────────────────────────────────────────────────────────

def read_varint(buf, i):
    value = 0
    shift = 0
    while i < len(buf):
        byte = buf[i]
        i += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, i
        shift += 7
    raise ValueError("truncated varint")


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def decode_fields(buf):
    """Decode a protobuf message into a list of (field_number, value)."""
    fields = []
    i = 0
    while i < len(buf):
        tag, i = read_varint(buf, i)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value, i = read_varint(buf, i)
            fields.append((field, value))
        elif wire == 2:
            length, i = read_varint(buf, i)
            fields.append((field, buf[i:i + length]))
            i += length
        elif wire == 5:
            i += 4
        elif wire == 1:
            i += 8
        else:
            break
    return fields


def build_libp2p_hello():
    """Build a LibP2P hello (0xfd) with a 64-byte node id."""
    endpoint = b'\x0a\x09127.0.0.1' + b'\x10' + encode_varint(18888)
    endpoint += b'\x1a\x40' + bytes(range(64))
    payload = b'\x0a' + encode_varint(len(endpoint)) + endpoint
    payload += b'\x10' + encode_varint(11111)   # network_id
    payload += b'\x20' + encode_varint(int(time.time() * 1000))  # timestamp
    payload += b'\x28' + encode_varint(1)       # version
    return payload


def build_tron_hello():
    """Build a Tron hello (0x20) announcing the genesis block.

    Tron messages are wrapped in an EndpointMessage envelope:
        [0x12][varint len][type_id][protobuf payload]
    """
    # Endpoint{from}: address "127.0.0.1", port 18888, node_id 64 bytes
    endpoint = b'\x0a\x09127.0.0.1' + b'\x10' + encode_varint(18888)
    endpoint += b'\x1a\x40' + bytes(range(64))
    payload = b'\x0a' + encode_varint(len(endpoint)) + endpoint
    payload += b'\x10' + encode_varint(11111)   # version
    payload += b'\x18' + encode_varint(int(time.time() * 1000))  # timestamp
    # BlockId{hash, number} for genesis / solid / head
    genesis_hash = bytes.fromhex(
        '00000000000000001ebf88508a03865c71d452e25f4d51194196a1d22b6653dc'
    )
    block_id = b'\x0a' + encode_varint(len(genesis_hash)) + genesis_hash
    block_id += b'\x10' + encode_varint(0)
    for field in (4, 5, 6):  # genesisBlockId, solidBlockId, headBlockId
        payload += encode_varint(field << 3 | 2) + encode_varint(len(block_id)) + block_id
    payload += b'\x5a\x04' + b'4.8.1'  # codeVersion (field 11)

    # Wrap in EndpointMessage: field 2 (data) = [type_id][payload]
    inner = b'\x20' + payload
    envelope = b'\x12' + encode_varint(len(inner)) + inner
    return envelope


def probe(ip):
    """Return (ip, head_height) when the node answers with a Tron hello."""
    try:
        sock = socket.create_connection((ip, 18888), timeout=5)
    except Exception:
        return None

    try:
        sock.settimeout(5)
        hello = build_libp2p_hello()
        # LibP2P frames: [varint length][type_id][payload]
        sock.sendall(encode_varint(len(hello) + 1) + b'\xfd' + hello)

        buf = b''
        sent_tron_hello = False
        deadline = time.time() + 8
        while time.time() < deadline:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                break
            buf += chunk

            offset = 0
            while offset < len(buf):
                try:
                    length, pos = read_varint(buf, offset)
                except ValueError:
                    break
                if pos + length > len(buf):
                    break
                frame = buf[pos:pos + length]
                offset = pos + length
                if not frame:
                    continue
                type_id = frame[0]
                payload = frame[1:]

                # LibP2P frames (>= 0xfb) are [type_id][protobuf]. Tron frames
                # are wrapped in an EndpointMessage envelope whose field 2 is
                # [type_id][protobuf], so the type byte is nested one level.
                if type_id < 0xfb:
                    inner = next(
                        (v for f, v in decode_fields(frame) if f == 2 and isinstance(v, bytes)),
                        None,
                    )
                    if inner:
                        type_id = inner[0]
                        payload = inner[1:]

                if type_id == 0xfd and not sent_tron_hello:
                    # Our LibP2P hello was already sent before the loop. Once
                    # the peer answers with its own hello, the Tron hello must
                    # follow on the same connection.
                    tron = build_tron_hello()
                    sock.sendall(encode_varint(len(tron)) + tron)
                    sent_tron_hello = True
                    continue

                if type_id == 0x20:
                    for field, value in decode_fields(payload):
                        if field == 6:
                            for sub, sub_value in decode_fields(value):
                                if sub == 2:
                                    return (ip, sub_value)
        return None
    except Exception:
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else 'rust-node/src/seed_nodes.rs'
    text = open(path).read()
    ips = re.findall(r'"([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):18888"', text)
    print(f"probing {len(ips)} seed nodes...", file=sys.stderr)

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as pool:
        for outcome in pool.map(probe, ips):
            if outcome:
                results.append(outcome)
                print(f"  {outcome[0]} head={outcome[1]}", file=sys.stderr)

    results.sort(key=lambda item: item[1], reverse=True)
    print(f"\n{len(results)} healthy nodes", file=sys.stderr)
    for ip, _ in results:
        print(ip)
    print(f"HEALTHY={len(results)} TOTAL={len(ips)}", file=sys.stderr)


if __name__ == '__main__':
    main()
