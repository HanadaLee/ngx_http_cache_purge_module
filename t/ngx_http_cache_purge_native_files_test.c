/* Copyright (C) Hanada */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include "ngx_http_cache_purge.h"


int
ngx_http_cache_purge_test_main(int argc, char **argv)
{
    ngx_http_file_cache_header_t          header;
    ngx_http_cache_purge_native_file_t    result;
    struct stat                           info;
    ngx_int_t                             rc;
    int                                   fd;
    static const char                    key[] = "\nKEY: native-test\n";
    static const char                    response[] =
        "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n"
        "Cache-Tag: first\r\n\r\nentity!";

    if (argc == 2 && strcmp(argv[1], "layout") == 0) {
        printf("{\"offset\":%zu,\"size\":%zu,\"header\":%zu}\n",
               offsetof(ngx_http_file_cache_header_t, valid_sec),
               sizeof(header.valid_sec), sizeof(header));
        return 0;
    }

    if (argc == 3 && strcmp(argv[1], "create") == 0) {
        memset(&header, 0, sizeof(header));
        header.version = NGX_HTTP_CACHE_VERSION;
        header.valid_sec = time(NULL) + 3600;
        header.updating_sec = 90;
        header.error_sec = 300;
        header.date = time(NULL);
        header.last_modified = header.date - 3600;
        header.valid_msec = 17;
        header.header_start = sizeof(header) + sizeof(key) - 1;
        header.body_start = header.header_start + sizeof(response) - 1 - 7;

        fd = open(argv[2], O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd == -1) {
            return 2;
        }

        if (write(fd, &header, sizeof(header)) != sizeof(header)
            || write(fd, key, sizeof(key) - 1) != sizeof(key) - 1
            || write(fd, response, sizeof(response) - 1)
               != sizeof(response) - 1
            || fstat(fd, &info) != 0 || close(fd) != 0)
        {
            return 2;
        }

        printf("{\"inode\":%llu}\n", (unsigned long long) info.st_ino);
        return 0;
    }

    if (argc != 5 || strcmp(argv[1], "apply") != 0) {
        return 2;
    }

    rc = ngx_http_cache_purge_native_file(argv[2], atoi(argv[3]),
                   (ngx_file_uniq_t) strtoull(argv[4], NULL, 10), &result);

    printf("{\"rc\":%ld,\"error\":%d,\"inode\":%llu,\"size\":%lld}\n",
           (long) rc, result.error, (unsigned long long) result.uniq,
           (long long) result.size);

    return 0;
}
