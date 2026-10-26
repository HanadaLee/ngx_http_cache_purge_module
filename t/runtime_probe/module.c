#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_cache_purge_runtime.h"

extern ngx_module_t ngx_http_cache_purge_runtime_probe_module;
typedef struct { ngx_flag_t enabled; } probe_conf_t;
typedef struct { ngx_flag_t fail_agent; } probe_main_t;
typedef struct { unsigned flags; } probe_result_t;

static int
visit(void *data, const ngx_cache_purge_control_match_t *match)
{
    probe_result_t *result = data;
    result->flags |= match->flags;
    return 0;
}

static ngx_int_t
guard(ngx_http_request_t *r)
{
    probe_conf_t *conf;
    ngx_cache_purge_control_reader_t *reader;
    ngx_cache_purge_store_error_t error;
    ngx_cache_purge_control_context_t context;
    probe_result_t result = {0};
    uint64_t publication;
    int rc;

    conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_purge_runtime_probe_module);
    if (!conf->enabled) { return NGX_DECLINED; }
    rc = ngx_http_cache_purge_runtime_reader(r, &reader, &error);
    if (rc != 0) { return NGX_HTTP_SERVICE_UNAVAILABLE; }
    ngx_memzero(&context, sizeof(context));
    /* Deliberately test the barrier with URI bytes. This fixture is not the
     * production key/URL provider and does not attest native file hooks. */
    context.key.data = r->uri.data; context.key.len = r->uri.len;
    context.url = context.key;
    context.host.data = r->headers_in.server.data; context.host.len = r->headers_in.server.len;
    context.now_ms = (uint64_t) ngx_time() * 1000;
    context.proxy = 1;
    rc = ngx_cache_purge_control_match(reader, &context, 64, 65536,
                                       visit, &result, &publication, &error);
    if (rc != 0 || (result.flags & NGX_PURGE_CONTROL_FENCE)) {
        return NGX_HTTP_SERVICE_UNAVAILABLE;
    }
    return NGX_DECLINED;
}

static void *
create(ngx_conf_t *cf)
{
    probe_conf_t *conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf) { conf->enabled = NGX_CONF_UNSET; }
    return conf;
}

static void *
create_main(ngx_conf_t *cf)
{
    probe_main_t *conf = ngx_pcalloc(cf->pool, sizeof(*conf));
    if (conf) { conf->fail_agent = NGX_CONF_UNSET; }
    return conf;
}

static ngx_int_t
init_process(ngx_cycle_t *cycle)
{
#if (HAVE_PRIVILEGED_PROCESS_PATCH)
    probe_main_t *conf;
    if (!ngx_is_privileged_agent) { return NGX_OK; }
    conf = ngx_http_cycle_get_module_main_conf(cycle, ngx_http_cache_purge_runtime_probe_module);
    if (conf && conf->fail_agent == 1) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0, "probe rejects agent initialization");
        return NGX_ERROR;
    }
    ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0, "probe agent module initialized");
#endif
    return NGX_OK;
}

static char *
merge(ngx_conf_t *cf, void *parent, void *child)
{
    probe_conf_t *p = parent, *c = child;
    ngx_conf_merge_value(c->enabled, p->enabled, 0);
    return NGX_CONF_OK;
}

static ngx_int_t
post(ngx_conf_t *cf)
{
    ngx_http_core_main_conf_t *core;
    ngx_http_handler_pt *handler;
    core = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);
    handler = ngx_array_push(&core->phases[NGX_HTTP_PRECONTENT_PHASE].handlers);
    if (handler == NULL) { return NGX_ERROR; }
    *handler = guard;
    return NGX_OK;
}

static ngx_command_t commands[] = {
    { ngx_string("cache_purge_test_fail_agent"), NGX_HTTP_MAIN_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot, NGX_HTTP_MAIN_CONF_OFFSET, offsetof(probe_main_t, fail_agent), NULL },
    { ngx_string("cache_purge_test_reader"), NGX_HTTP_LOC_CONF|NGX_CONF_FLAG,
      ngx_conf_set_flag_slot, NGX_HTTP_LOC_CONF_OFFSET, offsetof(probe_conf_t, enabled), NULL },
    ngx_null_command
};
static ngx_http_module_t context = { NULL, post, create_main, NULL, NULL, NULL, create, merge };
ngx_module_t ngx_http_cache_purge_runtime_probe_module = {
    NGX_MODULE_V1, &context, commands, NGX_HTTP_MODULE,
    NULL, NULL, init_process, NULL, NULL, NULL, NULL, NGX_MODULE_V1_PADDING
};
