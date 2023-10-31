Jfio: a simple library to use `io_uring` for file I/O in Java
-------------------------------------------------------------

Currently very much WIP. This (obviously) only work on linux, and currently rely on
[liburing](https://github.com/axboe/liburing) to be installed.

At the time of this writing, there is 2 main API exposed:
1. a low level API, `IORing`, to create an `io_uring` ring, submit read/write through it, and check for completions.
   This is not thread-safe and require some care to be used.
2. an higher level API, `IOExecutor`, which starts 1 or more event loops that submit reads/writes to their underlying
   `IORing`. This is thread safe and a bit more user friendly.
