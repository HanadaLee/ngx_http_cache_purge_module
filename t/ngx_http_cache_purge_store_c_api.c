#include "rocksdb/ngx_http_cache_rocksdb.h"
#include <string.h>

/* Compiled as C, so the test verifies the bridge's actual C ABI. */
int
ngx_http_cache_purge_store_c_probe(ngx_http_cache_rocksdb_t *store)
{
    static const unsigned char      key[] = "C:abi";
    unsigned char                   value[16];
    ngx_http_cache_rocksdb_bytes_t  input = { key, sizeof(key) - 1 };
    ngx_http_cache_rocksdb_buffer_t output = { value, sizeof(value), 0 };
    ngx_http_cache_rocksdb_error_t  error;
    int rc = ngx_http_cache_rocksdb_get(store, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                        input, &output, &error);
    return rc == NGX_HTTP_CACHE_ROCKSDB_OK && output.length == 5
                   && memcmp(value, "C ABI", 5) == 0 ?
               0 :
               1;
}
