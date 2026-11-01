
/*
 * Copyright (C) Hanada
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#if (NGX_EXPR)
#include <ngx_http_expr_module.h>

typedef ngx_array_t  ngx_http_cache_key_candidate_t;
#else
typedef ngx_http_complex_value_t  ngx_http_cache_key_candidate_t;
#endif


#define NGX_HTTP_CACHE_KEY_COMPONENTS  6

typedef enum {
    NGX_HTTP_CACHE_KEY_NAMESPACE = 0,
    NGX_HTTP_CACHE_KEY_SCHEME,
    NGX_HTTP_CACHE_KEY_HOST,
    NGX_HTTP_CACHE_KEY_URI,
    NGX_HTTP_CACHE_KEY_MISC,
    NGX_HTTP_CACHE_KEY_RANGE,
    NGX_HTTP_CACHE_KEY_URL,
    NGX_HTTP_CACHE_KEY_BASE,
    NGX_HTTP_CACHE_KEY_FULL,
    NGX_HTTP_CACHE_KEY_COUNT
} ngx_http_cache_key_field_e;


typedef struct {
    ngx_http_complex_value_t        *defaults[NGX_HTTP_CACHE_KEY_COMPONENTS];
} ngx_http_cache_key_main_conf_t;


typedef struct {
    ngx_http_cache_key_candidate_t  *field[NGX_HTTP_CACHE_KEY_COMPONENTS];
} ngx_http_cache_key_loc_conf_t;


typedef struct {
    ngx_uint_t                       valid;
    ngx_uint_t                       busy;
    ngx_str_t                        value[NGX_HTTP_CACHE_KEY_COUNT];
    unsigned                         failed:1;
} ngx_http_cache_key_ctx_t;


static ngx_int_t ngx_http_cache_key_validate_namespace(ngx_str_t *value);
static ngx_int_t ngx_http_cache_key_check_self_reference(ngx_str_t *value);
static char *ngx_http_cache_key_set_component(ngx_conf_t *cf,
    ngx_command_t *cmd, void *conf);
static void *ngx_http_cache_key_create_main_conf(ngx_conf_t *cf);
static void *ngx_http_cache_key_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_cache_key_merge_loc_conf(ngx_conf_t *cf, void *parent,
    void *child);
static ngx_int_t ngx_http_cache_key_join(ngx_http_request_t *r,
    ngx_http_cache_key_ctx_t *ctx, ngx_uint_t field);
static ngx_int_t ngx_http_cache_key_add_variables(ngx_conf_t *cf);
static ngx_int_t ngx_http_cache_key_get_value(ngx_http_request_t *r,
    ngx_http_cache_key_field_e field, ngx_str_t *value);
static ngx_int_t ngx_http_cache_key_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_http_cache_key_eval(ngx_http_request_t *r,
    ngx_http_cache_key_ctx_t *ctx, ngx_uint_t field);


static ngx_command_t  ngx_http_cache_key_commands[] = {

    { ngx_string("cache_key_namespace"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
      ngx_http_cache_key_set_component,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_key_loc_conf_t,
               field[NGX_HTTP_CACHE_KEY_NAMESPACE]),
      NULL },

    { ngx_string("cache_key_scheme"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
      ngx_http_cache_key_set_component,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_key_loc_conf_t,
               field[NGX_HTTP_CACHE_KEY_SCHEME]),
      NULL },

    { ngx_string("cache_key_host"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
      ngx_http_cache_key_set_component,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_key_loc_conf_t,
               field[NGX_HTTP_CACHE_KEY_HOST]),
      NULL },

    { ngx_string("cache_key_uri"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
      ngx_http_cache_key_set_component,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_key_loc_conf_t,
               field[NGX_HTTP_CACHE_KEY_URI]),
      NULL },

    { ngx_string("cache_key_misc"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
      ngx_http_cache_key_set_component,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_key_loc_conf_t,
               field[NGX_HTTP_CACHE_KEY_MISC]),
      NULL },

    { ngx_string("cache_key_range"),
      NGX_HTTP_MAIN_CONF|NGX_HTTP_SRV_CONF|NGX_HTTP_LOC_CONF
#if (NGX_EXPR)
                        |NGX_HTTP_MAIN_WHEN_CONF|NGX_HTTP_SRV_WHEN_CONF
                        |NGX_HTTP_LOC_WHEN_CONF
#endif
                        |NGX_CONF_TAKE1,
      ngx_http_cache_key_set_component,
      NGX_HTTP_LOC_CONF_OFFSET,
      offsetof(ngx_http_cache_key_loc_conf_t,
               field[NGX_HTTP_CACHE_KEY_RANGE]),
      NULL },

      ngx_null_command
};


static ngx_http_module_t  ngx_http_cache_key_module_ctx = {
    ngx_http_cache_key_add_variables,      /* preconfiguration */
    NULL,                                  /* postconfiguration */

    ngx_http_cache_key_create_main_conf,   /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_cache_key_create_loc_conf,    /* create location configuration */
    ngx_http_cache_key_merge_loc_conf      /* merge location configuration */
};


ngx_module_t  ngx_http_cache_key_module = {
    NGX_MODULE_V1,
    &ngx_http_cache_key_module_ctx,        /* module context */
    ngx_http_cache_key_commands,           /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_str_t  ngx_http_cache_key_names[] = {
    ngx_string("cache_key_namespace"),
    ngx_string("cache_key_scheme"),
    ngx_string("cache_key_host"),
    ngx_string("cache_key_uri"),
    ngx_string("cache_key_misc"),
    ngx_string("cache_key_range"),
    ngx_string("cache_key_url"),
    ngx_string("cache_key_base"),
    ngx_string("cache_key_full")
};


static ngx_str_t  ngx_http_cache_key_defaults[] = {
    ngx_string(""),                 /* namespace */
    ngx_string("$scheme"),          /* scheme */
    ngx_string("$host"),            /* host */
    ngx_string("$request_uri"),     /* URI */
    ngx_string(""),                 /* misc */
    ngx_string("")                  /* range */
};


static ngx_int_t
ngx_http_cache_key_validate_namespace(ngx_str_t *value)
{
    size_t  i;

    if (value->len > 255) {
        return NGX_ERROR;
    }

    for (i = 0; i < value->len; i++) {
        if (value->data[i] < 0x21 || value->data[i] > 0x7e
            || value->data[i] == ',')
        {
            return NGX_ERROR;
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_key_check_self_reference(ngx_str_t *value)
{
    size_t      i, start, n;
    u_char      c;
    ngx_uint_t  field;

    for (i = 0; i < value->len; i++) {
        if (value->data[i] != '$') {
            continue;
        }

        start = ++i;

        if (i < value->len && value->data[i] == '{') {
            start = ++i;
        }

        while (i < value->len) {
            c = value->data[i];

            if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                  || (c >= '0' && c <= '9') || c == '_'))
            {
                break;
            }

            i++;
        }

        n = i - start;

        for (field = 0; field < NGX_HTTP_CACHE_KEY_COUNT; field++) {
            if (n == ngx_http_cache_key_names[field].len
                && ngx_strncmp(value->data + start,
                               ngx_http_cache_key_names[field].data, n)
                   == 0)
            {
                return NGX_ERROR;
            }
        }

        if (i < value->len) {
            i--;
        }
    }

    return NGX_OK;
}


static char *
ngx_http_cache_key_set_component(ngx_conf_t *cf, ngx_command_t *cmd,
    void *conf)
{
    ngx_str_t  *value;
    ngx_uint_t  field;

    value = cf->args->elts;
    field = (cmd->offset - offsetof(ngx_http_cache_key_loc_conf_t, field))
            / sizeof(ngx_http_cache_key_candidate_t *);

    if (ngx_http_cache_key_check_self_reference(&value[1]) != NGX_OK) {
        ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
                           "%V cannot reference cache key output variables",
                           &cmd->name);

        return NGX_CONF_ERROR;
    }

    if (field == NGX_HTTP_CACHE_KEY_NAMESPACE
        && ngx_http_script_variables_count(&value[1]) == 0
        && ngx_http_cache_key_validate_namespace(&value[1]) != NGX_OK)
    {
        return "requires a namespace of 0-255 printable ASCII bytes without "
               "spaces or commas";
    }
#if (NGX_EXPR)
    return ngx_http_set_expr_complex_value_slot(cf, cmd, conf);
#else
    return ngx_http_set_complex_value_slot(cf, cmd, conf);
#endif
}


static void *
ngx_http_cache_key_create_main_conf(ngx_conf_t *cf)
{
    ngx_http_cache_key_main_conf_t  *mcf;

    mcf = ngx_pcalloc(cf->pool, sizeof(*mcf));

    if (mcf == NULL) {
        return NULL;
    }

    return mcf;
}


static void *
ngx_http_cache_key_create_loc_conf(ngx_conf_t *cf)
{
    ngx_uint_t                      i;
    ngx_http_cache_key_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_cache_key_loc_conf_t));

    if (conf == NULL) {
        return NULL;
    }

    for (i = 0; i < NGX_HTTP_CACHE_KEY_COMPONENTS; i++) {
        conf->field[i] = NGX_CONF_UNSET_PTR;
    }

    return conf;
}


static char *
ngx_http_cache_key_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_cache_key_loc_conf_t  *prev = parent;
    ngx_http_cache_key_loc_conf_t  *conf = child;

    ngx_uint_t                        i;
    ngx_http_cache_key_main_conf_t   *mcf;
    ngx_http_compile_complex_value_t  ccv;

    mcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_cache_key_module);

    for (i = 0; i < NGX_HTTP_CACHE_KEY_COMPONENTS; i++) {
        if (mcf->defaults[i] == NULL) {
            mcf->defaults[i] =
                ngx_pcalloc(cf->pool, sizeof(ngx_http_complex_value_t));

            if (mcf->defaults[i] == NULL) {
                return NGX_CONF_ERROR;
            }

            ngx_memzero(&ccv, sizeof(ccv));
            ccv.cf = cf;
            ccv.value = &ngx_http_cache_key_defaults[i];
            ccv.complex_value = mcf->defaults[i];

            if (ngx_http_compile_complex_value(&ccv) != NGX_OK) {
                return NGX_CONF_ERROR;
            }
        }
#if (NGX_EXPR)
        if (ngx_conf_merge_expr_ptr_value(cf, &conf->field[i], prev->field[i],
                                          mcf->defaults[i])
            != NGX_OK)
        {
            return NGX_CONF_ERROR;
        }
#else
        ngx_conf_merge_ptr_value(conf->field[i], prev->field[i],
                                 mcf->defaults[i]);
#endif
    }

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_http_cache_key_add_variables(ngx_conf_t *cf)
{
    ngx_uint_t            i;
    ngx_http_variable_t  *var;

    for (i = 0; i < NGX_HTTP_CACHE_KEY_COUNT; i++) {
        var = ngx_http_add_variable(cf, &ngx_http_cache_key_names[i],
                                    NGX_HTTP_VAR_NOCACHEABLE);

        if (var == NULL) {
            return NGX_ERROR;
        }

        var->get_handler = ngx_http_cache_key_variable;
        var->data = i;
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_key_join(ngx_http_request_t *r,
    ngx_http_cache_key_ctx_t *ctx, ngx_uint_t field)
{
    size_t      n, j, extra;
    u_char     *p, c;
    ngx_uint_t  i, first, last;

    if (field == NGX_HTTP_CACHE_KEY_URL) {
        first = NGX_HTTP_CACHE_KEY_SCHEME;
        last = NGX_HTTP_CACHE_KEY_URI;
        n = 3;

    } else {
        first = NGX_HTTP_CACHE_KEY_NAMESPACE;
        last = (field == NGX_HTTP_CACHE_KEY_FULL) ? NGX_HTTP_CACHE_KEY_RANGE
                                                : NGX_HTTP_CACHE_KEY_MISC;
        n = last;
    }

    for (i = first; i <= last; i++) {
        if (ngx_http_cache_key_eval(r, ctx, i) != NGX_OK) {
            return NGX_ERROR;
        }

        for (j = 0; j < ctx->value[i].len; j++) {
            c = ctx->value[i].data[j];
            extra = 1;

            if (field != NGX_HTTP_CACHE_KEY_URL) {
                if (c == 0x1f) {
                    extra = 4;

                } else if (c == '\\') {
                    extra = 2;
                }
            }

            if (n > NGX_MAX_SIZE_T_VALUE - extra) {
                return NGX_ERROR;
            }

            n += extra;
        }
    }

    p = ngx_pnalloc(r->pool, n);

    if (p == NULL) {
        return NGX_ERROR;
    }

    ctx->value[field].data = p;
    ctx->value[field].len = n;

    for (i = first; i <= last; i++) {
        if (i != first) {
            if (field != NGX_HTTP_CACHE_KEY_URL) {
                *p++ = 0x1f;
            } else if (i == NGX_HTTP_CACHE_KEY_HOST) {
                p = ngx_cpymem(p, "://", 3);
            }
        }

        for (j = 0; j < ctx->value[i].len; j++) {
            c = ctx->value[i].data[j];

            if (field != NGX_HTTP_CACHE_KEY_URL && c == 0x1f) {
                p = ngx_cpymem(p, "\\x1F", 4);
            } else {
                if (field != NGX_HTTP_CACHE_KEY_URL && c == '\\') {
                    *p++ = '\\';
                }

                *p++ = c;
            }
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_key_eval(ngx_http_request_t *r,
    ngx_http_cache_key_ctx_t *ctx, ngx_uint_t field)
{
    ngx_int_t                       rc;
    ngx_str_t                       value;
    ngx_uint_t                      bit;
    ngx_http_cache_key_loc_conf_t  *conf;

    bit = (ngx_uint_t) 1 << field;

    if (ctx->failed) {
        return NGX_ERROR;
    }

    if (ctx->valid & bit) {
        return NGX_OK;
    }

    if (ctx->busy & bit) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "cache key evaluation cycle at %V",
                      &ngx_http_cache_key_names[field]);
        ctx->failed = 1;

        return NGX_ERROR;
    }

    ctx->busy |= bit;

    if (field < NGX_HTTP_CACHE_KEY_COMPONENTS) {
        conf = ngx_http_get_module_loc_conf(r, ngx_http_cache_key_module);
#if (NGX_EXPR)
        rc = ngx_http_get_expr_complex_value(r, conf->field[field], &value);
#else
        rc = ngx_http_complex_value(r, conf->field[field], &value);
#endif
        if (rc == NGX_OK && field == NGX_HTTP_CACHE_KEY_NAMESPACE
            && ngx_http_cache_key_validate_namespace(&value) != NGX_OK)
        {
            ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                          "cache key namespace must contain 0-255 printable "
                          "ASCII bytes without spaces or commas");
            rc = NGX_ERROR;
        }

        if (rc == NGX_OK && !ctx->failed) {
            ctx->value[field].len = value.len;
            ctx->value[field].data = (u_char *) "";

            if (value.len) {
                ctx->value[field].data = ngx_pnalloc(r->pool, value.len);

                if (ctx->value[field].data == NULL) {
                    rc = NGX_ERROR;
                } else {
                    ngx_memcpy(ctx->value[field].data, value.data, value.len);
                }
            }
        }
    } else {
        rc = ngx_http_cache_key_join(r, ctx, field);
    }

    ctx->busy &= ~bit;

    if (rc != NGX_OK || ctx->failed) {
        ctx->failed = 1;

        return NGX_ERROR;
    }

    ctx->valid |= bit;

    return NGX_OK;
}


static ngx_int_t
ngx_http_cache_key_get_value(ngx_http_request_t *r,
    ngx_http_cache_key_field_e field, ngx_str_t *value)
{
    ngx_int_t                  rc;
    ngx_http_cache_key_ctx_t   local;
    ngx_http_cache_key_ctx_t  *ctx, *saved;

    saved = ngx_http_get_module_ctx(r, ngx_http_cache_key_module);
    ctx = saved;

    if (ctx == NULL) {
        ngx_memzero(&local, sizeof(ngx_http_cache_key_ctx_t));
        ctx = &local;
        ngx_http_set_ctx(r, ctx, ngx_http_cache_key_module);
    }

    rc = ngx_http_cache_key_eval(r, ctx, field);
    ngx_http_set_ctx(r, saved, ngx_http_cache_key_module);

    if (rc == NGX_OK) {
        *value = ctx->value[field];
    }

    return rc;
}


static ngx_int_t
ngx_http_cache_key_variable(ngx_http_request_t *r,
    ngx_http_variable_value_t *v, uintptr_t data)
{
    ngx_str_t  value;

    if (ngx_http_cache_key_get_value(r, (ngx_http_cache_key_field_e) data,
                                     &value)
        != NGX_OK)
    {
        return NGX_ERROR;
    }

    v->len = value.len;
    v->data = value.data;
    v->valid = 1;
    v->no_cacheable = 1;
    v->not_found = 0;

    return NGX_OK;
}
