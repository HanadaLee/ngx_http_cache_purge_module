#include "ngx_cache_purge_store.h"
#include <string.h>

/* Compiled as C, so the test verifies the bridge's actual C ABI. */
int cache_purge_store_c_probe(ngx_cache_purge_store_t *store)
{
    static const unsigned char key[] = "C:abi";
    unsigned char value[16];
    ngx_cache_purge_bytes_t input = { key, sizeof(key) - 1 };
    ngx_cache_purge_buffer_t output = { value, sizeof(value), 0 };
    ngx_cache_purge_store_error_t error;
    int rc = ngx_cache_purge_store_get(store, NGX_PURGE_STORE_DEFAULT,
                                     input, &output, &error);
    return rc == NGX_PURGE_STORE_OK && output.length == 5
           && memcmp(value, "C ABI", 5) == 0 ? 0 : 1;
}
