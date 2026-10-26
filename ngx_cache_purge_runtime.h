/* Copyright (C) Hanada */
#ifndef NGX_CACHE_PURGE_RUNTIME_H
#define NGX_CACHE_PURGE_RUNTIME_H

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_cache_purge_control.h"

typedef struct ngx_http_cache_purge_runtime_s ngx_http_cache_purge_runtime_t;

ngx_http_cache_purge_runtime_t *ngx_http_cache_purge_runtime_create(ngx_conf_t *cf);
char *ngx_http_cache_purge_store_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
char *ngx_http_cache_purge_state_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
char *ngx_http_cache_purge_propagation_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
ngx_int_t ngx_http_cache_purge_runtime_post(ngx_conf_t *cf);
ngx_int_t ngx_http_cache_purge_runtime_module_init(ngx_cycle_t *cycle);
ngx_int_t ngx_http_cache_purge_runtime_process_init(ngx_cycle_t *cycle);
void ngx_http_cache_purge_runtime_worker_exit(ngx_cycle_t *cycle);

/* Internal native-hook entry: lazily register this worker's reader. No DB I/O.
 * NOT_FOUND means the persistent runtime was not configured; BUSY must never
 * be interpreted as a cache miss or as permission to serve an unchecked HIT. */
int ngx_http_cache_purge_runtime_reader(ngx_http_request_t *r,
    ngx_cache_purge_control_reader_t **reader, ngx_cache_purge_store_error_t *error);

#endif
