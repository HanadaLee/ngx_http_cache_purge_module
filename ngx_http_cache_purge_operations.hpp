/* Copyright (C) Hanada */
#ifndef NGX_HTTP_CACHE_PURGE_OPERATIONS_HPP
#define NGX_HTTP_CACHE_PURGE_OPERATIONS_HPP

#include "ngx_http_cache_purge_index.hpp"

namespace ngx_http_cache_purge {

enum class PurgeMode : uint8_t { invalidate = 1, remove = 2 };
enum class OperationStage : uint8_t { preparing = 1, active = 2, draining = 3, materialized = 4 };

struct OperationInput {
    std::string id;
    Selector selector;
    PurgeMode mode = PurgeMode::invalidate;
    uint64_t issued_at_ms = 0;
    std::string binding; // Frozen configuration/authorization contract, supplied by admission.
};

struct Operation {
    OperationInput input;
    uint64_t sequence = 0; // Ordering of control operations; never changes a cache key.
    uint64_t revision = 0;
    OperationStage stage = OperationStage::preparing;
    uint64_t accepted_at_ms = 0, effective_at_ms = 0, propagation_until_ms = 0;
    uint32_t propagation_ms = 300000;
    bool propagate = false;
    std::string bookmark;
    bool scan_done = false;
    uint64_t next_run_ms = 0;
    uint64_t visits = 0; // Attempts, not a deduplicated object count.
    uint64_t pending = 0, failed = 0;
    uint64_t checkpoint_id = 0;
    std::string last_error;
};

struct OperationCheckpoint {
    uint64_t id = 0; // Strictly increasing per operation; replay of the last ID is idempotent.
    std::string bookmark;
    bool scan_done = false;
    uint64_t next_run_ms = 0;
    uint64_t visits_delta = 0;
    uint64_t pending = 0, failed = 0; // Absolute outstanding work counts.
    std::string last_error;
};

struct OperationLimits {
    uint32_t live_records = 4096;
    uint64_t live_selector_bytes = 4 * 1024 * 1024;
};

struct OperationPage {
    std::vector<Operation> items;
    std::string after;
    bool done = false;
};

struct OperationUsage {
    uint64_t live_records = 0, live_selector_bytes = 0, last_sequence = 0;
};

/* Durable ledger, called only by the single component executor. It does not
 * publish SHM, access files or prove that old readers/writers have drained.
 * activate follows real FENCE publication. materialize requires a verified
 * quiescence watermark from the future native-cache coordinator; EOF alone is
 * insufficient. Do not expose these transitions as client assertions in HTTP.
 * All accepted operations retain their idempotency record on disk, even after
 * their bounded resident-control slot is released. */
class Operations {
public:
    explicit Operations(ngx_http_cache_rocksdb_t *store, OperationLimits limits = {});
    Operation preview(const OperationInput &input, uint64_t now_ms, uint32_t propagation_ms = 300000);
    Operation accept(const OperationInput &input, uint64_t now_ms, uint32_t propagation_ms = 300000);
    Operation get(const std::string &id);
    OperationUsage usage(); // Recovery verifies this budget against paged live records.
    Operation activate(const std::string &id, uint64_t revision, uint64_t now_ms);
    Operation checkpoint(const std::string &id, uint64_t revision, const OperationCheckpoint &checkpoint);
    Operation materialize(const std::string &id, uint64_t revision, uint64_t quiescent_sequence);
    OperationPage live(const std::string &after = "", size_t limit = 32);
    OperationPage due(uint64_t now_ms, const std::string &after = "", size_t limit = 32);
    size_t expire_propagation(uint64_t now_ms, size_t limit = 32);
private:
    ngx_http_cache_rocksdb_t *store_;
    OperationLimits limits_;
};

uint64_t operation_control_bytes(const OperationInput &input);

} // namespace ngx_http_cache_purge
#endif
