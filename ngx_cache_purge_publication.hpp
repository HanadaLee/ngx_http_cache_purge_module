/* Copyright (C) Hanada */
#ifndef NGX_CACHE_PURGE_PUBLICATION_HPP
#define NGX_CACHE_PURGE_PUBLICATION_HPP
#include "ngx_cache_purge_index.hpp"
namespace ngx_cache_purge {
struct RootSpec { std::string path, zone; unsigned kind; };
/* Executor-thread filesystem policy; pinned roots are configuration-sized,
 * never an in-memory copy of the file index. Construction may perform I/O. */
class Publications {
public:
    Publications(const std::vector<RootSpec> &roots, const std::string &database);
    ~Publications();
    Match prepare(Index &index, Fill fill, const std::string &temporary,
                  int anchor_fd, int lease_fd, int temporary_fd, uint32_t peer_uid);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}
#endif
