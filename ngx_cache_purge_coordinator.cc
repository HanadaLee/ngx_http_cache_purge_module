/* Copyright (C) Hanada */
#include "ngx_cache_purge_coordinator.hpp"

namespace ngx_cache_purge {
namespace {
void require(bool ok, int code, const char *message) { if (!ok) throw IndexError(code, message); }
void checked(int code, const ngx_cache_purge_store_error_t &error) { if (code) throw IndexError(code, error.message); }
bool same(const OperationUsage &a, const OperationUsage &b) {
    return a.live_records == b.live_records && a.live_selector_bytes == b.live_selector_bytes && a.last_sequence == b.last_sequence;
}
Operation control(const Operation &source) {
    Operation result; result.input = source.input; result.sequence = source.sequence; result.revision = source.revision;
    result.stage = source.stage; result.accepted_at_ms = source.accepted_at_ms; result.effective_at_ms = source.effective_at_ms;
    result.propagation_until_ms = source.propagation_until_ms; result.propagation_ms = source.propagation_ms; result.propagate = source.propagate;
    // Do not retain per-operation bookmarks, error details or file statistics
    // in the resident control collection, including their string capacities.
    return result;
}
} // namespace

Coordinator::Coordinator(Operations &operations, void *shared, size_t shared_bytes)
    : operations_(operations), shared_(shared), shared_bytes_(shared_bytes) {
    require(shared && shared_bytes, NGX_PURGE_STORE_INVALID, "missing coordinator shared memory");
}
Coordinator::~Coordinator() {
    if (writer_) {
        ngx_cache_purge_store_error_t error;
        ngx_cache_purge_control_writer_close(&writer_, &error);
    }
}
void Coordinator::load_begin() {
    pending_.reset(); controls_.clear(); after_.clear(); loaded_bytes_ = 0;
    expected_ = operations_.usage();
    require(expected_.live_records <= 65536 && expected_.live_selector_bytes <= 64 * 1024 * 1024,
            NGX_PURGE_STORE_CORRUPT, "durable control usage exceeds supported bounds");
    controls_.reserve(static_cast<size_t>(expected_.live_records)); state_ = Coordination::loading;
}
void Coordinator::refresh() {
    require(state_ == Coordination::ready, NGX_PURGE_STORE_BUSY, "control synchronization already in progress");
    load_begin();
}
bool Coordinator::step() {
    ngx_cache_purge_store_error_t error;
    try {
        switch (state_) {
        case Coordination::claiming: {
            int rc = ngx_cache_purge_control_writer_open(shared_, shared_bytes_, &writer_, &error);
            if (rc == NGX_PURGE_STORE_BUSY) return false;
            checked(rc, error); load_begin(); return true;
        }
        case Coordination::loading: {
            auto page = operations_.live(after_, 32);
            for (const auto &op : page.items) {
                auto size = operation_control_bytes(op.input);
                require(controls_.size() < expected_.live_records && loaded_bytes_ <= expected_.live_selector_bytes
                        && size <= expected_.live_selector_bytes - loaded_bytes_
                        && op.sequence <= expected_.last_sequence,
                        NGX_PURGE_STORE_CORRUPT, "live control enumeration exceeds durable budget");
                require(controls_.empty() || controls_.back().sequence < op.sequence,
                        NGX_PURGE_STORE_CORRUPT, "live controls not strictly ordered");
                controls_.push_back(control(op)); loaded_bytes_ += size;
            }
            after_ = std::move(page.after);
            if (page.done) {
                require(controls_.size() == expected_.live_records && loaded_bytes_ == expected_.live_selector_bytes
                        && same(expected_, operations_.usage()), NGX_PURGE_STORE_CORRUPT, "live control recovery watermark mismatch");
                state_ = Coordination::building;
            }
            return true;
        }
        case Coordination::building:
            pending_ = std::make_unique<ControlImage>(prepare_controls(writer_, controls_));
            checked(ngx_cache_purge_control_gate_close(writer_, &error), error);
            state_ = Coordination::publishing; return true;
        case Coordination::publishing:
            publication_ = publish_controls(writer_, *pending_); pending_.reset(); state_ = Coordination::draining; return true;
        case Coordination::draining: {
            int rc = ngx_cache_purge_control_gate_open(writer_, &error);
            if (rc == NGX_PURGE_STORE_BUSY) return false;
            checked(rc, error); state_ = Coordination::ready; return true;
        }
        case Coordination::ready: return true;
        }
    } catch (const IndexError &e) {
        if (e.code == NGX_PURGE_STORE_BUSY) return false;
        // Even a failure while prebuilding a refresh must not let a caller
        // accidentally continue with unverified recovered state.
        if (writer_) ngx_cache_purge_control_gate_close(writer_, &error);
        throw;
    }
    return false;
}
Operation Coordinator::accept(const OperationInput &input, uint64_t now, uint32_t propagation_ms) {
    auto durable = operations_.usage();
    auto preview = operations_.preview(input, now, propagation_ms);
    if (preview.sequence <= durable.last_sequence) return preview; // Replay remains available while publication waits.
    require(state_ == Coordination::ready, NGX_PURGE_STORE_BUSY, "control publication is pending");
    require(expected_.last_sequence != UINT64_MAX && preview.sequence == expected_.last_sequence + 1,
            NGX_PURGE_STORE_CONFLICT, "operation sequence changed outside coordinator");
    controls_.push_back(control(preview));
    std::unique_ptr<ControlImage> image;
    try { image = std::make_unique<ControlImage>(prepare_controls(writer_, controls_)); }
    catch (...) { controls_.pop_back(); throw; }
    ngx_cache_purge_store_error_t error;
    try { checked(ngx_cache_purge_control_gate_close(writer_, &error), error); }
    catch (...) { controls_.pop_back(); throw; }
    Operation accepted;
    try {
        accepted = operations_.accept(input, now, propagation_ms);
        require(accepted.sequence == preview.sequence && accepted.revision == preview.revision,
                NGX_PURGE_STORE_CORRUPT, "accepted operation differs from prepared controls");
        expected_ = operations_.usage(); loaded_bytes_ = expected_.live_selector_bytes;
    } catch (...) {
        // A write error can be ambiguous. Keep the gate closed and recover
        // from the durable ledger instead of assuming no operation exists.
        load_begin(); throw;
    }
    pending_ = std::move(image); state_ = Coordination::publishing;
    return accepted; // Durable PREPARING; no claim of native cache effectiveness.
}

} // namespace ngx_cache_purge
