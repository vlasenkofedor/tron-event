# Fork Detection & Re-org Protection

This document explains how the Tron Node handles blockchain reorganizations (forks) and ensures the integrity of transaction finality (`SOLID` status).

## The Re-org Problem

In a DPoS network like Tron, several Super Representatives (SRs) produce blocks. Occasionally, two blocks may be produced at the same height, causing a temporary split (fork). Most forks are resolved within 1-2 blocks, but they can technicaly be longer.

If a transaction is seen in a block that is later orphaned (discarded in favor of a longer chain), its status must be invalidated or updated.

## Chain-Aware Tracking

The node maintains a rolling history of the last 64 block headers (Number, Hash, and Parent Hash) in memory and persists them to `height.txt`.

### Fork Detection Algorithm

When a new block header arrives via P2P:

1.  **Extension**: If `parent_hash` matches our current head's `hash`, and the number is `N+1`, we simply append it.
2.  **Re-org**: If `parent_hash` does NOT match but the height is near our current tip:
    -   The node searches its history for a block matching the `parent_hash`.
    -   If found (e.g., at height `N-2`), it means a **2-block re-org** occurred.
    -   The node **rolls back** its internal tip to that point and follows the new branch.
3.  **Gaps**: If the incoming block is far ahead or behind and no parent is found in history, the node resets its tracking to start fresh from the new block.

## Solidity (Finality)

A transaction is considered `SOLID` (irreversible) in Tron when it is confirmed by 2/3 + 1 of the SRs. In practice, this happens when the block is **19 levels deep**.

The node's confirmation depth is configurable:

- `--solid-depth <N>` sets the number of blocks that must be built on top of the transaction's block.
- The default is **20** — one block above the 19-block protocol threshold, as a safety margin.
- The accepted range is `1`–`100000`; an invalid value prints usage and exits with status `2`.
- The value affects only the moment the `SOLID` event is emitted. Fork detection, rollback and `MINED` reporting are independent of it.

Because our node is now "Chain-Aware", the `SOLID` status is guaranteed to be on the **canonical main chain**.

### Persistence (`height.txt`)

The block chain is saved to `height.txt` in the following format:
```text
block_number:block_hash:parent_hash
...
```
This allows the node to maintain its fork-detection context even after a restart. If the file is missing or empty, the node initializes from the default configured values.

## Operational Impact

-   **Accuracy**: You can rely on the `SOLID` status as a final, non-reversible state.
-   **Logging**: Re-orgs are explicitly logged with warnings like `[FORK] Re-org detected! Rolling back X blocks`.
-   **Lite Mode Compatibility**: This logic is fully autonomous and does not require complex gRPC indexing, making it perfect for Lite FullNodes.
