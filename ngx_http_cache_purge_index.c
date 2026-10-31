/* Copyright (C) Hanada */

#include "ngx_http_cache_purge_internal.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>


#define NGX_HTTP_CACHE_PURGE_ARENA  (1024 * 1024)
#define NGX_HTTP_CACHE_PURGE_ROWS   1024
#define NGX_HTTP_CACHE_PURGE_TEXT   32768


typedef ngx_http_cache_rocksdb_bytes_t  ngx_http_cache_purge_bytes_t;

struct ngx_http_cache_purge_catalog_s {
    ngx_http_cache_rocksdb_t           *store;
    ngx_http_cache_rocksdb_mutation_t   rows[NGX_HTTP_CACHE_PURGE_ROWS];
    unsigned char                      *arena;
    unsigned char                       scratch[65536];
    size_t                             used, count;
    uint64_t                            next;
    int                                 status;
    ngx_http_cache_rocksdb_error_t     *error;
};


typedef struct {
    uint64_t                       fields[6], tags[64];
    uint64_t                       uniq, created, retain_until;
    size_t                          tag_count;
    ngx_http_cache_purge_bytes_t    path;
} ngx_http_cache_purge_row_t;


struct ngx_http_cache_purge_scan_s {
    ngx_http_cache_purge_catalog_t  *catalog;
    ngx_http_cache_rocksdb_cursor_t *outer, *inner;
    ngx_http_cache_rocksdb_cf_e      cf;
    unsigned char                   outer_key[NGX_HTTP_CACHE_PURGE_TEXT + 2];
    unsigned char                   inner_key[49];
    unsigned char                   token[NGX_HTTP_CACHE_PURGE_TEXT + 54];
    size_t                          outer_length, inner_length;
    uint64_t                        now;
    int                             retained, nested;
};


static const unsigned char  ngx_http_cache_purge_kinds[] = "UHNMRZ";
static ngx_http_cache_purge_bytes_t  ngx_http_cache_purge_empty = { NULL, 0 };


static int
ngx_http_cache_purge_catalog_error(ngx_http_cache_purge_catalog_t *c,
    int code, const char *message)
{
    if (c->status == 0) {
        c->status = code;
        if (c->error != NULL) {
            c->error->code = code;
            snprintf(c->error->message, sizeof(c->error->message), "%s",
                     message);
        }
    }

    return c->status;
}


static void
ngx_http_cache_purge_catalog_begin(ngx_http_cache_purge_catalog_t *c,
    ngx_http_cache_rocksdb_error_t *error)
{
    c->used = 0;
    c->count = 0;
    c->status = 0;
    c->error = error;
    if (error != NULL) {
        memset(error, 0, sizeof(*error));
    }
}


static unsigned char *
ngx_http_cache_purge_catalog_alloc(ngx_http_cache_purge_catalog_t *c,
    size_t length)
{
    unsigned char  *p;

    if (c->status != 0) {
        return NULL;
    }

    if (length > NGX_HTTP_CACHE_PURGE_ARENA - c->used) {
        ngx_http_cache_purge_catalog_error(c, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                         "index batch exceeds its budget");
        return NULL;
    }

    p = c->arena + c->used;
    c->used += length;

    return p;
}


static uint64_t
ngx_http_cache_purge_catalog_number(const unsigned char *p, size_t length)
{
    uint64_t  value;

    value = 0;
    while (length--) {
        value = (value << 8) | *p++;
    }

    return value;
}


static unsigned char *
ngx_http_cache_purge_catalog_encode(unsigned char *p, uint64_t value,
    size_t length)
{
    while (length) {
        *p++ = (unsigned char) (value >> (--length * 8));
    }

    return p;
}


static ngx_http_cache_purge_bytes_t
ngx_http_cache_purge_catalog_copy(ngx_http_cache_purge_catalog_t *c,
    const void *data, size_t length)
{
    ngx_http_cache_purge_bytes_t  value;
    unsigned char                *p;

    p = ngx_http_cache_purge_catalog_alloc(c, length);
    if (p == NULL) {
        return ngx_http_cache_purge_empty;
    }

    if (length != 0) {
        memcpy(p, data, length);
    }

    value.data = p;
    value.len = length;

    return value;
}


static ngx_http_cache_purge_bytes_t
ngx_http_cache_purge_catalog_id(ngx_http_cache_purge_catalog_t *c,
    unsigned char kind, uint64_t id)
{
    unsigned char  key[9];

    key[0] = kind;
    ngx_http_cache_purge_catalog_encode(key + 1, id, 8);

    return ngx_http_cache_purge_catalog_copy(c, key, sizeof(key));
}


static int
ngx_http_cache_purge_catalog_equal(ngx_http_cache_purge_bytes_t a,
    ngx_http_cache_purge_bytes_t b)
{
    return a.len == b.len && (a.len == 0 || memcmp(a.data, b.data, a.len) == 0);
}


static ngx_http_cache_purge_bytes_t
ngx_http_cache_purge_catalog_read(ngx_http_cache_purge_catalog_t *c,
    ngx_http_cache_rocksdb_cursor_t *view, ngx_http_cache_rocksdb_cf_e cf,
    ngx_http_cache_purge_bytes_t key)
{
    ngx_http_cache_rocksdb_buffer_t  buffer;
    size_t                           i;
    int                              rc;

    if (c->status != 0) {
        return ngx_http_cache_purge_empty;
    }

    if (view == NULL) {
        for (i = c->count; i != 0; i--) {
            if (c->rows[i - 1].cf == cf
                && ngx_http_cache_purge_catalog_equal(c->rows[i - 1].key,
                                                       key))
            {
                return c->rows[i - 1].type == NGX_HTTP_CACHE_ROCKSDB_DELETE
                       ? ngx_http_cache_purge_empty : c->rows[i - 1].value;
            }
        }
    }

    buffer.data = c->scratch;
    buffer.capacity = sizeof(c->scratch);
    buffer.length = 0;
    if (view != NULL) {
        rc = ngx_http_cache_rocksdb_cursor_get(view, cf, key, &buffer,
                                               c->error);
    } else {
        rc = ngx_http_cache_rocksdb_get(c->store, cf, key, &buffer, c->error);
    }
    if (rc == NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND) {
        return ngx_http_cache_purge_empty;
    }

    if (rc != 0) {
        ngx_http_cache_purge_catalog_error(c, rc, "cannot read index row");
        return ngx_http_cache_purge_empty;
    }

    return ngx_http_cache_purge_catalog_copy(c, buffer.data, buffer.length);
}


static void
ngx_http_cache_purge_catalog_set(ngx_http_cache_purge_catalog_t *c,
    ngx_http_cache_rocksdb_cf_e cf, ngx_http_cache_purge_bytes_t key,
    ngx_http_cache_purge_bytes_t value, int remove)
{
    ngx_http_cache_rocksdb_mutation_t  *row;
    size_t                              i;

    if (c->status != 0) {
        return;
    }

    for (i = 0; i < c->count; i++) {
        if (c->rows[i].cf == cf
            && ngx_http_cache_purge_catalog_equal(c->rows[i].key, key))
        {
            break;
        }
    }

    if (i == NGX_HTTP_CACHE_PURGE_ROWS) {
        ngx_http_cache_purge_catalog_error(c, NGX_HTTP_CACHE_ROCKSDB_LIMIT,
                                         "too many index batch entries");
        return;
    }

    row = &c->rows[i];
    row->cf = cf;
    row->key = key;
    row->value = value;
    row->type = remove ? NGX_HTTP_CACHE_ROCKSDB_DELETE
                       : NGX_HTTP_CACHE_ROCKSDB_PUT;
    if (i == c->count) {
        c->count++;
    }
}


static int
ngx_http_cache_purge_catalog_commit(ngx_http_cache_purge_catalog_t *c,
    int sync)
{
    if (c->status != 0) {
        return c->status;
    }

    if (sync) {
        return ngx_http_cache_rocksdb_write(c->store, c->rows, c->count,
                                            NULL, 0, c->error);
    }

    return ngx_http_cache_rocksdb_write_deferred(c->store, c->rows, c->count,
                                                NULL, 0, c->error);
}


static ngx_http_cache_purge_bytes_t
ngx_http_cache_purge_catalog_word(ngx_http_cache_purge_catalog_t *c,
    unsigned char kind, ngx_http_cache_purge_bytes_t value)
{
    ngx_http_cache_purge_bytes_t  key;
    unsigned char                *p;

    p = ngx_http_cache_purge_catalog_alloc(c, value.len + 2);
    if (p == NULL) {
        return ngx_http_cache_purge_empty;
    }

    p[0] = 'd';
    p[1] = kind;
    if (value.len != 0) {
        memcpy(p + 2, value.data, value.len);
    }

    key.data = p;
    key.len = value.len + 2;

    return key;
}


static uint64_t
ngx_http_cache_purge_catalog_reference(ngx_http_cache_purge_catalog_t *c,
    unsigned char kind, ngx_http_cache_purge_bytes_t value, int add)
{
    ngx_http_cache_purge_bytes_t  key, old, encoded, reverse, number;
    ngx_http_cache_rocksdb_cf_e   cf;
    uint64_t                     id, count;
    unsigned char                raw[16];
    unsigned char               *p;

    cf = kind == 'U' ? NGX_HTTP_CACHE_ROCKSDB_URL
                     : NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    key = ngx_http_cache_purge_catalog_word(c, kind, value);
    old = ngx_http_cache_purge_catalog_read(c, NULL, cf, key);
    if (c->status != 0) {
        return 0;
    }

    if (old.data == NULL) {
        if (!add) {
            return 0;
        }

        if (c->next == UINT64_MAX) {
            ngx_http_cache_purge_catalog_error(c,
                NGX_HTTP_CACHE_ROCKSDB_LIMIT, "index ID limit reached");
            return 0;
        }

        id = ++c->next;
        count = 0;
        reverse = ngx_http_cache_purge_catalog_id(c, 'D', id);
        p = ngx_http_cache_purge_catalog_alloc(c, value.len + 1);
        if (p == NULL) {
            return 0;
        }

        *p = kind;
        if (value.len != 0) {
            memcpy(p + 1, value.data, value.len);
        }

        encoded.data = p;
        encoded.len = value.len + 1;
        ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                         reverse, encoded, 0);
        ngx_http_cache_purge_catalog_encode(raw, id, 8);
        number = ngx_http_cache_purge_catalog_copy(c, raw, 8);
        reverse = ngx_http_cache_purge_catalog_copy(c, "M:next", 6);
        ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                         reverse, number, 0);

    } else {
        if (old.len != 16) {
            ngx_http_cache_purge_catalog_error(c,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid dictionary row");
            return 0;
        }

        id = ngx_http_cache_purge_catalog_number(old.data, 8);
        count = ngx_http_cache_purge_catalog_number(old.data + 8, 8);
        if (id == 0 || count == 0 || (add && count == UINT64_MAX)) {
            ngx_http_cache_purge_catalog_error(c,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid dictionary reference");
            return 0;
        }
    }

    if (add) {
        ngx_http_cache_purge_catalog_encode(raw, id, 8);
        ngx_http_cache_purge_catalog_encode(raw + 8, count + 1, 8);
        encoded = ngx_http_cache_purge_catalog_copy(c, raw, sizeof(raw));
        ngx_http_cache_purge_catalog_set(c, cf, key, encoded, 0);
    }

    return id;
}


static void
ngx_http_cache_purge_catalog_release(ngx_http_cache_purge_catalog_t *c,
    uint64_t id)
{
    ngx_http_cache_purge_bytes_t  reverse, word, key, value, encoded;
    ngx_http_cache_rocksdb_cf_e   cf;
    uint64_t                     count;
    unsigned char                raw[16], kind;

    reverse = ngx_http_cache_purge_catalog_id(c, 'D', id);
    word = ngx_http_cache_purge_catalog_read(c, NULL,
                                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, reverse);
    if (word.len < 1) {
        ngx_http_cache_purge_catalog_error(c, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                         "missing dictionary value");
        return;
    }

    kind = *word.data++;
    word.len--;
    cf = kind == 'U' ? NGX_HTTP_CACHE_ROCKSDB_URL
                     : NGX_HTTP_CACHE_ROCKSDB_DEFAULT;
    key = ngx_http_cache_purge_catalog_word(c, kind, word);
    value = ngx_http_cache_purge_catalog_read(c, NULL, cf, key);
    if (value.len != 16
        || ngx_http_cache_purge_catalog_number(value.data, 8) != id)
    {
        ngx_http_cache_purge_catalog_error(c, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                         "dictionary reference mismatch");
        return;
    }

    count = ngx_http_cache_purge_catalog_number(value.data + 8, 8);
    if (count == 0) {
        ngx_http_cache_purge_catalog_error(c, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                         "empty dictionary reference");
        return;
    }

    if (count == 1) {
        ngx_http_cache_purge_catalog_set(c, cf, key,
                                         ngx_http_cache_purge_empty, 1);
        ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                              reverse, ngx_http_cache_purge_empty, 1);
        return;
    }

    ngx_http_cache_purge_catalog_encode(raw, id, 8);
    ngx_http_cache_purge_catalog_encode(raw + 8, count - 1, 8);
    encoded = ngx_http_cache_purge_catalog_copy(c, raw, sizeof(raw));
    ngx_http_cache_purge_catalog_set(c, cf, key, encoded, 0);
}


static int
ngx_http_cache_purge_catalog_decode(ngx_http_cache_purge_catalog_t *c,
    ngx_http_cache_purge_bytes_t value, ngx_http_cache_purge_row_t *row)
{
    const unsigned char  *p;
    size_t                i, length;

    memset(row, 0, sizeof(*row));
    if (value.len < 78 || value.data[0] != 1 || value.data[73] > 64) {
        return ngx_http_cache_purge_catalog_error(c,
                  NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid file index row");
    }

    p = value.data + 1;
    for (i = 0; i < 6; i++, p += 8) {
        row->fields[i] = ngx_http_cache_purge_catalog_number(p, 8);
        if (row->fields[i] == 0) {
            return ngx_http_cache_purge_catalog_error(c,
                    NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "empty file reference");
        }
    }

    row->uniq = ngx_http_cache_purge_catalog_number(p, 8);
    row->created = ngx_http_cache_purge_catalog_number(p + 8, 8);
    row->retain_until = ngx_http_cache_purge_catalog_number(p + 16, 8);
    p += 24;
    row->tag_count = *p++;
    if (value.len < 78 + row->tag_count * 8) {
        return ngx_http_cache_purge_catalog_error(c,
                  NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "truncated file TAG list");
    }

    for (i = 0; i < row->tag_count; i++, p += 8) {
        row->tags[i] = ngx_http_cache_purge_catalog_number(p, 8);
    }

    length = (size_t) ngx_http_cache_purge_catalog_number(p, 4);
    p += 4;
    if (length == 0 || length > NGX_HTTP_CACHE_PURGE_TEXT
        || value.len != 78 + row->tag_count * 8 + length)
    {
        return ngx_http_cache_purge_catalog_error(c,
                 NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid cached file path");
    }

    row->path.data = p;
    row->path.len = length;

    return 0;
}


static ngx_http_cache_purge_bytes_t
ngx_http_cache_purge_catalog_record(ngx_http_cache_purge_catalog_t *c,
    const ngx_http_cache_purge_row_t *row)
{
    ngx_http_cache_purge_bytes_t  value;
    unsigned char                *p;
    size_t                        i;

    value.len = 78 + row->tag_count * 8 + row->path.len;
    p = ngx_http_cache_purge_catalog_alloc(c, value.len);
    value.data = p;
    if (p == NULL) {
        return ngx_http_cache_purge_empty;
    }

    *p++ = 1;
    for (i = 0; i < 6; i++) {
        p = ngx_http_cache_purge_catalog_encode(p, row->fields[i], 8);
    }

    p = ngx_http_cache_purge_catalog_encode(p, row->uniq, 8);
    p = ngx_http_cache_purge_catalog_encode(p, row->created, 8);
    p = ngx_http_cache_purge_catalog_encode(p, row->retain_until, 8);
    *p++ = (unsigned char) row->tag_count;
    for (i = 0; i < row->tag_count; i++) {
        p = ngx_http_cache_purge_catalog_encode(p, row->tags[i], 8);
    }

    p = ngx_http_cache_purge_catalog_encode(p, row->path.len, 4);
    memcpy(p, row->path.data, row->path.len);

    return value;
}


static ngx_http_cache_purge_bytes_t
ngx_http_cache_purge_catalog_file_key(ngx_http_cache_purge_catalog_t *c,
    const unsigned char id[24])
{
    unsigned char  raw[25];

    raw[0] = 'F';
    memcpy(raw + 1, id, 24);

    return ngx_http_cache_purge_catalog_copy(c, raw, sizeof(raw));
}


static void
ngx_http_cache_purge_catalog_tag_reference(ngx_http_cache_purge_catalog_t *c,
    const ngx_http_cache_purge_row_t *row, uint64_t tag, int drop)
{
    unsigned char                raw[33], number[8], *p;
    ngx_http_cache_purge_bytes_t  key, value;
    uint64_t                     count;

    raw[0] = 'K';
    p = ngx_http_cache_purge_catalog_encode(raw + 1, row->fields[0], 8);
    p = ngx_http_cache_purge_catalog_encode(p, row->fields[2], 8);
    p = ngx_http_cache_purge_catalog_encode(p, row->fields[3], 8);
    ngx_http_cache_purge_catalog_encode(p, tag, 8);
    key = ngx_http_cache_purge_catalog_copy(c, raw, sizeof(raw));
    value = ngx_http_cache_purge_catalog_read(c, NULL,
                                             NGX_HTTP_CACHE_ROCKSDB_TAG, key);
    if (c->status != 0) {
        return;
    }
    count = value.data == NULL ? 0
            : ngx_http_cache_purge_catalog_number(value.data, value.len);
    if ((value.data != NULL && (value.len != 8 || count == 0))
        || (drop && count == 0) || (!drop && count == UINT64_MAX))
    {
        ngx_http_cache_purge_catalog_error(c, NGX_HTTP_CACHE_ROCKSDB_CORRUPT,
                                           "invalid key TAG reference");
        return;
    }
    count = drop ? count - 1 : count + 1;
    ngx_http_cache_purge_catalog_encode(number, count, 8);
    value = ngx_http_cache_purge_catalog_copy(c, number, sizeof(number));
    ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_TAG, key,
                     count ? value : ngx_http_cache_purge_empty, count == 0);
}


static void
ngx_http_cache_purge_catalog_post(ngx_http_cache_purge_catalog_t *c,
    const ngx_http_cache_purge_row_t *row, const unsigned char id[24], int drop)
{
    unsigned char                raw[49], *p;
    ngx_http_cache_purge_bytes_t  key;
    size_t                        i;

    raw[0] = 'A';
    p = ngx_http_cache_purge_catalog_encode(raw + 1, row->fields[0], 8);
    p = ngx_http_cache_purge_catalog_encode(p, row->fields[2], 8);
    p = ngx_http_cache_purge_catalog_encode(p, row->fields[3], 8);
    memcpy(p, id, 24);
    key = ngx_http_cache_purge_catalog_copy(c, raw, 49);
    ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_URL, key,
                                     ngx_http_cache_purge_empty, drop);

    raw[0] = 'H';
    p = ngx_http_cache_purge_catalog_encode(raw + 1, row->fields[1], 8);
    p = ngx_http_cache_purge_catalog_encode(p, row->fields[0], 8);
    memcpy(p, id, 24);
    key = ngx_http_cache_purge_catalog_copy(c, raw, 41);
    ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_URL, key,
                                     ngx_http_cache_purge_empty, drop);

    for (i = 0; i < row->tag_count; i++) {
        ngx_http_cache_purge_catalog_tag_reference(c, row, row->tags[i], drop);
        raw[0] = 'N';
        p = ngx_http_cache_purge_catalog_encode(raw + 1, row->fields[2], 8);
        p = ngx_http_cache_purge_catalog_encode(p, row->tags[i], 8);
        memcpy(p, id, 24);
        key = ngx_http_cache_purge_catalog_copy(c, raw, 41);
        ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_TAG, key,
                                         ngx_http_cache_purge_empty, drop);
        raw[0] = 'H';
        ngx_http_cache_purge_catalog_encode(raw + 1, row->fields[1], 8);
        key = ngx_http_cache_purge_catalog_copy(c, raw, 41);
        ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_TAG, key,
                                         ngx_http_cache_purge_empty, drop);
    }
}


/* A key's TAG union is independent of its slice, Vary and zone file count. */
int
ngx_http_cache_purge_catalog_tags(ngx_http_cache_purge_catalog_t *c,
    const ngx_http_cache_purge_entry_t *identity,
    ngx_http_cache_purge_tag_pt handler, void *data,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_rocksdb_cursor_t  *cursor;
    ngx_http_cache_rocksdb_buffer_t   key, value;
    ngx_http_cache_purge_bytes_t      fields[3], prefix, tag;
    unsigned char                    raw[33], count[8];
    const unsigned char              kinds[] = "UNM";
    uint64_t                         id;
    size_t                           i;
    int                              rc;

    ngx_http_cache_purge_catalog_begin(c, error);
    fields[0] = identity->url;
    fields[1] = identity->namespace_value;
    fields[2] = identity->misc;
    raw[0] = 'K';
    for (i = 0; i < 3; i++) {
        if (fields[i].len > NGX_HTTP_CACHE_PURGE_TEXT
            || (fields[i].len != 0 && fields[i].data == NULL))
        {
            return NGX_HTTP_CACHE_ROCKSDB_INVALID;
        }
        id = ngx_http_cache_purge_catalog_reference(c, kinds[i], fields[i], 0);
        if (id == 0) {
            return c->status;
        }
        ngx_http_cache_purge_catalog_encode(raw + 1 + i * 8, id, 8);
    }
    prefix.data = raw;
    prefix.len = 25;
    rc = ngx_http_cache_rocksdb_cursor_open_cached(c->store,
             NGX_HTTP_CACHE_ROCKSDB_TAG, prefix, ngx_http_cache_purge_empty,
             &cursor, error);
    if (rc != 0) {
        return rc;
    }
    key.data = raw;
    key.capacity = sizeof(raw);
    value.data = count;
    value.capacity = sizeof(count);
    for ( ;; ) {
        rc = ngx_http_cache_rocksdb_cursor_next(cursor, &key, &value, error);
        if (rc != 0) {
            rc = rc == NGX_HTTP_CACHE_ROCKSDB_DONE ? 0 : rc;
            break;
        }
        if (key.length != 33 || value.length != 8
            || ngx_http_cache_purge_catalog_number(count, 8) == 0)
        {
            rc = NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
            break;
        }
        ngx_http_cache_purge_catalog_begin(c, error);
        id = ngx_http_cache_purge_catalog_number(raw + 25, 8);
        tag = ngx_http_cache_purge_catalog_id(c, 'D', id);
        tag = ngx_http_cache_purge_catalog_read(c, cursor,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, tag);
        if (c->status != 0 || tag.len < 2 || tag.len > 1025
            || tag.data[0] != 'T')
        {
            rc = c->status ? c->status : NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
            break;
        }
        tag.data++;
        tag.len--;
        rc = handler(data, tag);
        if (rc != 0) {
            break;
        }
    }
    ngx_http_cache_rocksdb_cursor_close(&cursor, NULL);
    return rc;
}


static void
ngx_http_cache_purge_catalog_gc_row(ngx_http_cache_purge_catalog_t *c,
    uint64_t until, const unsigned char id[24], int remove)
{
    unsigned char                 raw[33];
    ngx_http_cache_purge_bytes_t  key;

    if (until == 0) {
        return;
    }

    raw[0] = 'G';
    ngx_http_cache_purge_catalog_encode(raw + 1, until, 8);
    memcpy(raw + 9, id, 24);
    key = ngx_http_cache_purge_catalog_copy(c, raw, sizeof(raw));
    ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key,
                                     ngx_http_cache_purge_empty, remove);
}


static void
ngx_http_cache_purge_catalog_drop(ngx_http_cache_purge_catalog_t *c,
    const ngx_http_cache_purge_row_t *row, const unsigned char id[24])
{
    size_t  i;

    ngx_http_cache_purge_catalog_post(c, row, id, 1);
    ngx_http_cache_purge_catalog_gc_row(c, row->retain_until, id, 1);
    for (i = 0; i < 6; i++) {
        ngx_http_cache_purge_catalog_release(c, row->fields[i]);
    }

    for (i = 0; i < row->tag_count; i++) {
        ngx_http_cache_purge_catalog_release(c, row->tags[i]);
    }
}


int
ngx_http_cache_purge_catalog_open(ngx_http_cache_rocksdb_t *store,
    ngx_http_cache_purge_catalog_t **catalog,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_catalog_t  *c;
    ngx_http_cache_purge_bytes_t     key, value;
    int                             rc;

    *catalog = NULL;
    c = calloc(1, sizeof(*c));
    if (c == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }

    c->arena = malloc(NGX_HTTP_CACHE_PURGE_ARENA);
    if (c->arena == NULL) {
        free(c);
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }

    c->store = store;
    ngx_http_cache_purge_catalog_begin(c, error);
    key = ngx_http_cache_purge_catalog_copy(c, "M:index", 7);
    value = ngx_http_cache_purge_catalog_read(c, NULL,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (value.data != NULL) {
        ngx_http_cache_purge_catalog_error(c, NGX_HTTP_CACHE_ROCKSDB_SCHEMA,
                           "legacy file-proof index needs explicit migration");
    }

    key = ngx_http_cache_purge_catalog_copy(c, "M:catalog", 9);
    value = ngx_http_cache_purge_catalog_read(c, NULL,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (value.data == NULL) {
        value = ngx_http_cache_purge_catalog_copy(c, "2", 1);
        ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                         key, value, 0);
        rc = ngx_http_cache_purge_catalog_commit(c, 1);

    } else if (value.len != 1 || value.data[0] != '2') {
        rc = ngx_http_cache_purge_catalog_error(c,
                   NGX_HTTP_CACHE_ROCKSDB_SCHEMA, "unsupported cache index");

    } else {
        rc = c->status;
    }

    if (rc != 0) {
        ngx_http_cache_purge_catalog_close(c);
        return rc;
    }

    *catalog = c;

    return 0;
}


void
ngx_http_cache_purge_catalog_close(ngx_http_cache_purge_catalog_t *catalog)
{
    if (catalog != NULL) {
        free(catalog->arena);
        free(catalog);
    }
}


int
ngx_http_cache_purge_catalog_put(ngx_http_cache_purge_catalog_t *c,
    const ngx_http_cache_purge_entry_t *entry,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_bytes_t  fields[6], key, value;
    ngx_http_cache_purge_row_t    row, old;
    unsigned char                id[24];
    size_t                       i, j;

    ngx_http_cache_purge_catalog_begin(c, error);
    if (entry->tag_count > 64 || entry->retain_until != 0) {
        return ngx_http_cache_purge_catalog_error(c,
                    NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache entry");
    }

    fields[0] = entry->url;
    fields[1] = entry->host;
    fields[2] = entry->namespace_value;
    fields[3] = entry->misc;
    fields[4] = entry->root;
    fields[5] = entry->zone;
    for (i = 0; i < 6; i++) {
        if (fields[i].len > NGX_HTTP_CACHE_PURGE_TEXT
            || (fields[i].len != 0 && fields[i].data == NULL)
            || (fields[i].len == 0 && i != 2 && i != 3))
        {
            return ngx_http_cache_purge_catalog_error(c,
                     NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache identity");
        }
    }

    if (entry->path.len == 0 || entry->path.len > NGX_HTTP_CACHE_PURGE_TEXT
        || entry->path.data == NULL || entry->path.data[0] == '/'
        || entry->path.data[entry->path.len - 1] == '/'
        || memchr(entry->path.data, 0, entry->path.len) != NULL
        || entry->root.data[0] != '/'
        || memchr(entry->root.data, 0, entry->root.len) != NULL)
    {
        return ngx_http_cache_purge_catalog_error(c,
                     NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache path");
    }

    for (i = 0; i < entry->path.len; i = j + 1) {
        for (j = i; j < entry->path.len && entry->path.data[j] != '/'; j++) {
            /* Find one relative path component. */
        }

        if (j == i || (j - i == 1 && entry->path.data[i] == '.')
            || (j - i == 2 && entry->path.data[i] == '.'
                && entry->path.data[i + 1] == '.'))
        {
            return ngx_http_cache_purge_catalog_error(c,
                       NGX_HTTP_CACHE_ROCKSDB_INVALID, "unsafe cache path");
        }
    }

    for (i = 0; i < entry->tag_count; i++) {
        if (entry->tags[i].len == 0 || entry->tags[i].len > 1024
            || entry->tags[i].data == NULL)
        {
            return ngx_http_cache_purge_catalog_error(c,
                         NGX_HTTP_CACHE_ROCKSDB_INVALID, "invalid cache TAG");
        }
    }

    memset(&row, 0, sizeof(row));
    key = ngx_http_cache_purge_catalog_copy(c, "M:next", 6);
    value = ngx_http_cache_purge_catalog_read(c, NULL,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    c->next = value.data == NULL ? 0 :
              value.len == 8 ? ngx_http_cache_purge_catalog_number(value.data,
                                                                  8) : 0;
    if (value.data != NULL && value.len != 8) {
        return ngx_http_cache_purge_catalog_error(c,
                  NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "invalid index allocator");
    }

    for (i = 0; i < 6; i++) {
        row.fields[i] = ngx_http_cache_purge_catalog_reference(c,
                             ngx_http_cache_purge_kinds[i], fields[i], 1);
    }

    for (i = 0; i < entry->tag_count; i++) {
        for (j = 0; j < i; j++) {
            if (ngx_http_cache_purge_catalog_equal(entry->tags[i],
                                                   entry->tags[j]))
            {
                break;
            }
        }

        if (j == i) {
            row.tags[row.tag_count++] = ngx_http_cache_purge_catalog_reference(
                                            c, 'T', entry->tags[i], 1);
        }
    }

    row.uniq = entry->uniq;
    row.created = entry->created;
    row.path = entry->path;
    ngx_http_cache_purge_catalog_encode(id, row.fields[4], 8);
    memcpy(id + 8, entry->hash, 16);
    key = ngx_http_cache_purge_catalog_file_key(c, id);
    value = ngx_http_cache_purge_catalog_read(c, NULL,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (value.data != NULL
        && ngx_http_cache_purge_catalog_decode(c, value, &old) == 0)
    {
        ngx_http_cache_purge_catalog_drop(c, &old, id);
    }

    ngx_http_cache_purge_catalog_post(c, &row, id, 0);
    value = ngx_http_cache_purge_catalog_record(c, &row);
    ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                     key, value, 0);

    return ngx_http_cache_purge_catalog_commit(c, 0);
}


static int
ngx_http_cache_purge_catalog_entry(ngx_http_cache_purge_catalog_t *c,
    ngx_http_cache_rocksdb_cursor_t *view, const unsigned char id[24],
    ngx_http_cache_purge_entry_t *entry)
{
    ngx_http_cache_purge_bytes_t  key, value, *fields[6];
    ngx_http_cache_purge_row_t    row;
    size_t                        i;

    memset(entry, 0, sizeof(*entry));
    key = ngx_http_cache_purge_catalog_file_key(c, id);
    value = ngx_http_cache_purge_catalog_read(c, view,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (c->status != 0) {
        return c->status;
    }

    if (value.data == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    }

    if (ngx_http_cache_purge_catalog_decode(c, value, &row) != 0) {
        return c->status;
    }

    if (ngx_http_cache_purge_catalog_number(id, 8) != row.fields[4]) {
        return ngx_http_cache_purge_catalog_error(c,
                NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "cache root identity mismatch");
    }

    fields[0] = &entry->url;
    fields[1] = &entry->host;
    fields[2] = &entry->namespace_value;
    fields[3] = &entry->misc;
    fields[4] = &entry->root;
    fields[5] = &entry->zone;
    for (i = 0; i < 6 + row.tag_count; i++) {
        key = ngx_http_cache_purge_catalog_id(c, 'D',
                                  i < 6 ? row.fields[i] : row.tags[i - 6]);
        value = ngx_http_cache_purge_catalog_read(c, view,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
        if (value.len < 1
            || value.data[0] != (i < 6 ? ngx_http_cache_purge_kinds[i] : 'T'))
        {
            return ngx_http_cache_purge_catalog_error(c,
                  NGX_HTTP_CACHE_ROCKSDB_CORRUPT, "missing cache dictionary");
        }

        value.data++;
        value.len--;
        if (i < 6) {
            *fields[i] = value;
        } else {
            entry->tags[i - 6] = value;
        }
    }

    memcpy(entry->id, id, 24);
    memcpy(entry->hash, id + 8, 16);
    entry->uniq = row.uniq;
    entry->created = row.created;
    entry->retain_until = row.retain_until;
    entry->tag_count = row.tag_count;
    entry->path = row.path;

    return c->status;
}


int
ngx_http_cache_purge_catalog_get(ngx_http_cache_purge_catalog_t *c,
    const unsigned char id[24], ngx_http_cache_purge_entry_t *entry,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_catalog_begin(c, error);

    return ngx_http_cache_purge_catalog_entry(c, NULL, id, entry);
}


int
ngx_http_cache_purge_catalog_remove(ngx_http_cache_purge_catalog_t *c,
    const unsigned char id[24], uint64_t expected,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_bytes_t  key, value;
    ngx_http_cache_purge_row_t    row;

    ngx_http_cache_purge_catalog_begin(c, error);
    key = ngx_http_cache_purge_catalog_file_key(c, id);
    value = ngx_http_cache_purge_catalog_read(c, NULL,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (c->status != 0) {
        return c->status;
    }

    if (value.data == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    }

    if (ngx_http_cache_purge_catalog_decode(c, value, &row) != 0) {
        return c->status;
    }

    if (expected != 0 && expected != row.uniq) {
        return NGX_HTTP_CACHE_ROCKSDB_CONFLICT;
    }

    ngx_http_cache_purge_catalog_drop(c, &row, id);
    ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                     key, ngx_http_cache_purge_empty, 1);

    return ngx_http_cache_purge_catalog_commit(c, 0);
}


int
ngx_http_cache_purge_catalog_retain(ngx_http_cache_purge_catalog_t *c,
    const unsigned char id[24], uint64_t expected, uint64_t until,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_bytes_t  key, value;
    ngx_http_cache_purge_row_t    row;

    ngx_http_cache_purge_catalog_begin(c, error);
    if (until == 0) {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }

    key = ngx_http_cache_purge_catalog_file_key(c, id);
    value = ngx_http_cache_purge_catalog_read(c, NULL,
                                        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, key);
    if (c->status != 0) {
        return c->status;
    }

    if (value.data == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_NOT_FOUND;
    }

    if (ngx_http_cache_purge_catalog_decode(c, value, &row) != 0) {
        return c->status;
    }

    if (expected != 0 && expected != row.uniq) {
        return NGX_HTTP_CACHE_ROCKSDB_CONFLICT;
    }

    ngx_http_cache_purge_catalog_gc_row(c, row.retain_until, id, 1);
    row.retain_until = until;
    ngx_http_cache_purge_catalog_gc_row(c, until, id, 0);
    value = ngx_http_cache_purge_catalog_record(c, &row);
    ngx_http_cache_purge_catalog_set(c, NGX_HTTP_CACHE_ROCKSDB_DEFAULT,
                                     key, value, 0);

    return ngx_http_cache_purge_catalog_commit(c, 0);
}


int
ngx_http_cache_purge_catalog_collect(ngx_http_cache_purge_catalog_t *c,
    uint64_t now, size_t limit, size_t *removed,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_rocksdb_cursor_t  *cursor;
    ngx_http_cache_rocksdb_buffer_t  key, value;
    ngx_http_cache_purge_bytes_t     prefix, stored, selected;
    ngx_http_cache_purge_row_t       row;
    unsigned char                    raw[33];
    size_t                           i;
    int                              rc;

    *removed = 0;
    cursor = NULL;
    prefix.data = (const unsigned char *) "G";
    prefix.len = 1;
    rc = ngx_http_cache_rocksdb_cursor_open(c->store,
        NGX_HTTP_CACHE_ROCKSDB_DEFAULT, prefix, ngx_http_cache_purge_empty,
        &cursor, error);
    if (rc != 0) {
        return rc;
    }

    key.data = raw;
    key.capacity = sizeof(raw);
    memset(&value, 0, sizeof(value));
    for (i = 0; i < limit; i++) {
        rc = ngx_http_cache_rocksdb_cursor_next(cursor, &key, &value, error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE) {
            rc = 0;
            break;
        }

        if (rc != 0) {
            break;
        }

        if (key.length != sizeof(raw)) {
            rc = NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
            break;
        }

        if (ngx_http_cache_purge_catalog_number(raw + 1, 8) > now) {
            break;
        }

        ngx_http_cache_purge_catalog_begin(c, error);
        selected = ngx_http_cache_purge_catalog_file_key(c, raw + 9);
        stored = ngx_http_cache_purge_catalog_read(c, NULL,
                                   NGX_HTTP_CACHE_ROCKSDB_DEFAULT, selected);
        if (stored.data != NULL
            && ngx_http_cache_purge_catalog_decode(c, stored, &row) == 0
            && row.retain_until != 0 && row.retain_until <= now)
        {
            rc = ngx_http_cache_purge_catalog_remove(c, raw + 9, row.uniq,
                                                      error);
            if (rc == 0) {
                (*removed)++;
            }

        } else {
            selected = ngx_http_cache_purge_catalog_copy(c, raw, sizeof(raw));
            ngx_http_cache_purge_catalog_set(c,
                NGX_HTTP_CACHE_ROCKSDB_DEFAULT, selected,
                ngx_http_cache_purge_empty, 1);
            rc = ngx_http_cache_purge_catalog_commit(c, 0);
        }

        if (rc != 0) {
            break;
        }
    }

    ngx_http_cache_rocksdb_cursor_close(&cursor, NULL);

    return rc;
}


static int
ngx_http_cache_purge_scan_inner(ngx_http_cache_purge_scan_t *scan,
    uint64_t url, ngx_http_cache_purge_bytes_t after,
    ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_bytes_t  prefix;
    unsigned char                 raw[9];

    raw[0] = 'A';
    ngx_http_cache_purge_catalog_encode(raw + 1, url, 8);
    prefix.data = raw;
    prefix.len = sizeof(raw);
    if (after.len != 0
        && (after.len < sizeof(raw)
            || memcmp(after.data, raw, sizeof(raw)) != 0))
    {
        after = ngx_http_cache_purge_empty;
    }

    return ngx_http_cache_rocksdb_cursor_fork(scan->outer,
                    NGX_HTTP_CACHE_ROCKSDB_URL, prefix, after,
                    &scan->inner, error);
}


int
ngx_http_cache_purge_scan_open(ngx_http_cache_purge_catalog_t *c,
    const ngx_http_cache_purge_select_t *selector,
    ngx_http_cache_purge_bytes_t after, int retained, uint64_t now,
    ngx_http_cache_purge_scan_t **out, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_scan_t  *scan;
    ngx_http_cache_purge_bytes_t  prefix, value, outer, inner;
    unsigned char                raw[25];
    uint64_t                     first, second, third;
    size_t                       length;
    int                          rc;

    *out = NULL;
    ngx_http_cache_purge_catalog_begin(c, error);
    if (selector->type > NGX_HTTP_CACHE_PURGE_SELECT_TAG
        || selector->value.len > NGX_HTTP_CACHE_PURGE_TEXT
        || (selector->value.len != 0 && selector->value.data == NULL)
        || selector->namespace_value.len > NGX_HTTP_CACHE_PURGE_TEXT
        || (selector->namespace_value.len != 0
            && selector->namespace_value.data == NULL)
        || selector->misc.len > NGX_HTTP_CACHE_PURGE_TEXT
        || (selector->misc.len != 0 && selector->misc.data == NULL))
    {
        return NGX_HTTP_CACHE_ROCKSDB_INVALID;
    }

    outer = inner = ngx_http_cache_purge_empty;
    if (after.len != 0) {
        if (after.len < 3 || after.data == NULL) {
            return NGX_HTTP_CACHE_ROCKSDB_INVALID;
        }

        length = (size_t) ngx_http_cache_purge_catalog_number(after.data, 2);
        if (length > NGX_HTTP_CACHE_PURGE_TEXT + 2
            || after.data[2] > 49 || after.len != length + after.data[2] + 3)
        {
            return NGX_HTTP_CACHE_ROCKSDB_INVALID;
        }

        outer.data = after.data + 3;
        outer.len = length;
        inner.data = outer.data + outer.len;
        inner.len = after.data[2];
    }

    scan = calloc(1, sizeof(*scan));
    if (scan == NULL) {
        return NGX_HTTP_CACHE_ROCKSDB_LIMIT;
    }

    scan->catalog = c;
    scan->retained = retained;
    scan->now = now;
    scan->cf = NGX_HTTP_CACHE_ROCKSDB_URL;
    rc = 0;
    if (selector->type == NGX_HTTP_CACHE_PURGE_SELECT_PREFIX) {
        scan->nested = 1;
        prefix = ngx_http_cache_purge_catalog_word(c, 'U', selector->value);
        if (c->status != 0) {
            rc = c->status;
            goto failed;
        }

        rc = ngx_http_cache_rocksdb_cursor_open(c->store, scan->cf, prefix,
                                               outer, &scan->outer, error);
        if (rc != 0) {
            goto failed;
        }

        if (outer.len != 0) {
            memcpy(scan->outer_key, outer.data, outer.len);
            scan->outer_length = outer.len;
            value = ngx_http_cache_purge_catalog_read(c, scan->outer,
                                           NGX_HTTP_CACHE_ROCKSDB_URL, outer);
            if (value.len == 16) {
                first = ngx_http_cache_purge_catalog_number(value.data, 8);
                rc = ngx_http_cache_purge_scan_inner(scan, first, inner, error);
            } else if (value.data != NULL) {
                rc = NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
            }
        }

        goto ready;
    }

    if (outer.len != 0) {
        rc = NGX_HTTP_CACHE_ROCKSDB_INVALID;
        goto failed;
    }

    first = second = third = 0;
    length = 9;
    if (selector->type == NGX_HTTP_CACHE_PURGE_SELECT_HOST) {
        raw[0] = 'H';
        first = ngx_http_cache_purge_catalog_reference(c, 'H', selector->value,
                                                       0);

    } else if (selector->type == NGX_HTTP_CACHE_PURGE_SELECT_TAG) {
        scan->cf = NGX_HTTP_CACHE_ROCKSDB_TAG;
        raw[0] = selector->host_scope ? 'H' : 'N';
        first = ngx_http_cache_purge_catalog_reference(c, raw[0],
                                            selector->namespace_value, 0);
        second = ngx_http_cache_purge_catalog_reference(c, 'T',
                                                         selector->value, 0);
        length = 17;
        if (second == 0) {
            goto ready;
        }

    } else {
        raw[0] = 'A';
        first = ngx_http_cache_purge_catalog_reference(c, 'U', selector->value,
                                                       0);
        if (selector->type == NGX_HTTP_CACHE_PURGE_SELECT_KEY) {
            second = ngx_http_cache_purge_catalog_reference(c, 'N',
                                                selector->namespace_value, 0);
            third = ngx_http_cache_purge_catalog_reference(c, 'M',
                                                            selector->misc, 0);
            length = 25;
            if (second == 0 || third == 0) {
                goto ready;
            }
        }
    }

    if (first == 0) {
        goto ready;
    }

    ngx_http_cache_purge_catalog_encode(raw + 1, first, 8);
    if (length >= 17) {
        ngx_http_cache_purge_catalog_encode(raw + 9, second, 8);
    }

    if (length == 25) {
        ngx_http_cache_purge_catalog_encode(raw + 17, third, 8);
    }

    prefix.data = raw;
    prefix.len = length;
    rc = ngx_http_cache_rocksdb_cursor_open(c->store, scan->cf, prefix, inner,
                                           &scan->inner, error);

ready:

    if (rc == 0) {
        rc = c->status;
    }

    if (rc == 0) {
        *out = scan;
        return 0;
    }

failed:

    ngx_http_cache_purge_scan_close(scan);

    return rc;
}


int
ngx_http_cache_purge_scan_next(ngx_http_cache_purge_scan_t *scan,
    ngx_http_cache_purge_entry_t *entry, ngx_http_cache_rocksdb_error_t *error)
{
    ngx_http_cache_purge_catalog_t  *c;
    ngx_http_cache_rocksdb_buffer_t  key, value;
    unsigned char                   number[16];
    uint64_t                        url;
    size_t                          visited;
    int                             rc;

    c = scan->catalog;
    ngx_http_cache_purge_catalog_begin(c, error);
    memset(entry, 0, sizeof(*entry));

    for (visited = 0; visited < 64; visited++) {
        if (scan->inner == NULL) {
            if (scan->outer == NULL) {
                return NGX_HTTP_CACHE_ROCKSDB_DONE;
            }

            key.data = scan->outer_key;
            key.capacity = sizeof(scan->outer_key);
            value.data = number;
            value.capacity = sizeof(number);
            rc = ngx_http_cache_rocksdb_cursor_next(scan->outer, &key, &value,
                                                     error);
            if (rc != 0) {
                return rc;
            }

            if (value.length != 16) {
                return NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
            }

            scan->outer_length = key.length;
            scan->inner_length = 0;
            url = ngx_http_cache_purge_catalog_number(number, 8);
            rc = ngx_http_cache_purge_scan_inner(scan, url,
                                         ngx_http_cache_purge_empty, error);
            if (rc != 0) {
                return rc;
            }
        }

        key.data = scan->inner_key;
        key.capacity = sizeof(scan->inner_key);
        memset(&value, 0, sizeof(value));
        rc = ngx_http_cache_rocksdb_cursor_next(scan->inner, &key, &value,
                                                 error);
        if (rc == NGX_HTTP_CACHE_ROCKSDB_DONE && scan->nested) {
            ngx_http_cache_rocksdb_cursor_close(&scan->inner, NULL);
            continue;
        }

        if (rc != 0) {
            return rc;
        }

        if (key.length != (scan->inner_key[0] == 'A' ? 49 : 41)) {
            return NGX_HTTP_CACHE_ROCKSDB_CORRUPT;
        }

        scan->inner_length = key.length;
        ngx_http_cache_purge_catalog_begin(c, error);
        rc = ngx_http_cache_purge_catalog_entry(c, scan->inner,
                           scan->inner_key + key.length - 24, entry);
        if (rc != 0) {
            return rc;
        }

        if (entry->retain_until != 0
            && (!scan->retained || entry->retain_until <= scan->now))
        {
            continue;
        }

        return 0;
    }

    return NGX_HTTP_CACHE_ROCKSDB_BUSY;
}


ngx_http_cache_purge_bytes_t
ngx_http_cache_purge_scan_bookmark(ngx_http_cache_purge_scan_t *scan)
{
    ngx_http_cache_purge_bytes_t  value;
    unsigned char                *p;

    p = ngx_http_cache_purge_catalog_encode(scan->token, scan->outer_length, 2);
    *p++ = (unsigned char) scan->inner_length;
    memcpy(p, scan->outer_key, scan->outer_length);
    p += scan->outer_length;
    memcpy(p, scan->inner_key, scan->inner_length);
    value.data = scan->token;
    value.len = 3 + scan->outer_length + scan->inner_length;

    return value;
}


void
ngx_http_cache_purge_scan_close(ngx_http_cache_purge_scan_t *scan)
{
    if (scan != NULL) {
        ngx_http_cache_rocksdb_cursor_close(&scan->inner, NULL);
        ngx_http_cache_rocksdb_cursor_close(&scan->outer, NULL);
        free(scan);
    }
}
