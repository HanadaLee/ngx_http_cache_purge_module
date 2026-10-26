/* Copyright (C) Hanada */
#ifndef NGX_HTTP_CACHE_PURGE_COORDINATOR_HPP
#define NGX_HTTP_CACHE_PURGE_COORDINATOR_HPP

#include "ngx_http_cache_purge_control.hpp"

namespace ngx_http_cache_purge {

enum class Coordination : uint32_t { claiming = 1, loading, building, publishing, draining, ready };

/* Single executor only, while that executor holds the DB owner lease.
 * A step advances at most one recovery page or one publication transition.
 * Controls may be readable while operations remain PREPARING. This class does
 * not attest native cache hooks, activate operations or complete file work. */
class Coordinator {
public:
    Coordinator(Operations &operations, void *shared, size_t shared_bytes);
    ~Coordinator();
    Coordinator(const Coordinator &) = delete;
    Coordinator &operator=(const Coordinator &) = delete;
    bool step(); // false means a live reader/registration still blocks progress.
    Operation accept(const OperationInput &input, uint64_t now_ms, uint32_t propagation_ms);
    void refresh(); // Call after durable control changes; old stronger constraints remain until publication.
    Coordination state() const { return state_; }
    uint64_t publication() const { return publication_; }
private:
    Operations &operations_;
    void *shared_;
    size_t shared_bytes_;
    ngx_http_cache_purge_control_writer_t *writer_ = nullptr;
    Coordination state_ = Coordination::claiming;
    OperationUsage expected_{};
    std::vector<Operation> controls_;
    std::string after_;
    uint64_t loaded_bytes_ = 0, publication_ = 0;
    std::unique_ptr<ControlImage> pending_;
    void load_begin();
};

} // namespace ngx_http_cache_purge
#endif
