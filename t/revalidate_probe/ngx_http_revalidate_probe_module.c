
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>


static ngx_int_t ngx_http_revalidate_probe_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_http_revalidate_probe_preconfiguration(ngx_conf_t *cf);


static ngx_http_module_t  ngx_http_revalidate_probe_module_ctx = {
    ngx_http_revalidate_probe_preconfiguration,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
};


ngx_module_t  ngx_http_revalidate_probe_module = {
    NGX_MODULE_V1,
    &ngx_http_revalidate_probe_module_ctx,
    NULL,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NGX_MODULE_V1_PADDING
};


/* Request validation through cache_bypass without bypassing the validators. */

static ngx_int_t
ngx_http_revalidate_probe_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
#if (NGX_RESTY_EXT && NGX_HTTP_CACHE)
    ngx_str_t  value;

    if (r->cache != NULL
        && ngx_http_arg(r, (u_char *) "force", 5, &value) == NGX_OK
        && value.len == 1 && value.data[0] == '1')
    {
        r->cache->purged = 1;
    }
#endif

    *v = ngx_http_variable_null_value;
    v->no_cacheable = 1;

    return NGX_OK;
}


static ngx_int_t
ngx_http_revalidate_probe_preconfiguration(ngx_conf_t *cf)
{
    ngx_str_t             name = ngx_string("cache_revalidate_probe");
    ngx_http_variable_t  *v;

    v = ngx_http_add_variable(cf, &name, NGX_HTTP_VAR_NOCACHEABLE);
    if (v == NULL) {
        return NGX_ERROR;
    }

    v->get_handler = ngx_http_revalidate_probe_variable;

    return NGX_OK;
}
