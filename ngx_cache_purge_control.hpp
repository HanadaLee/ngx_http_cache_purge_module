/* Copyright (C) Hanada */
#ifndef NGX_CACHE_PURGE_CONTROL_HPP
#define NGX_CACHE_PURGE_CONTROL_HPP

#include "ngx_cache_purge_control.h"
#include "ngx_cache_purge_operations.hpp"

namespace ngx_cache_purge {

class ControlImage {
public:
    ControlImage(ControlImage &&) noexcept;
    ControlImage &operator=(ControlImage &&) noexcept;
    ~ControlImage();
    ControlImage(const ControlImage &) = delete;
    ControlImage &operator=(const ControlImage &) = delete;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    explicit ControlImage(std::unique_ptr<Impl> impl);
    friend ControlImage prepare_controls(ngx_cache_purge_control_writer_t *, const std::vector<Operation> &);
    friend uint64_t publish_controls(ngx_cache_purge_control_writer_t *, const ControlImage &);
};

/* Build before closing the serving gate when admission ordering permits it.
 * A prepared image binds its owner and prior publication; it cannot overwrite
 * an intervening update. BUSY publication can retry the same prepared image. */
ControlImage prepare_controls(ngx_cache_purge_control_writer_t *writer,
                              const std::vector<Operation> &operations);
uint64_t publish_controls(ngx_cache_purge_control_writer_t *writer, const ControlImage &image);

/* Rebuild an immutable, bounded image from live control operations only. The
 * full file index never enters this structure. Publication requires a closed
 * gate. BUSY means the inactive bank is still pinned; retry without opening
 * the gate or discarding the pending operation. No cached key is rewritten. */
uint64_t publish_controls(ngx_cache_purge_control_writer_t *writer,
                          const std::vector<Operation> &operations);

} // namespace ngx_cache_purge
#endif
