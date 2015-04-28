# Disque-py

A [disque](https://github.com/antirez/disque) Python client.

Under the hood, this used redis-py's HiRedis implementation and switches out
Redis commands with the disque ones.

## Usage

```
from disq import DisqueAlpha

c = DisqueAlpha()
c.addjob('queuename', 'body') # takes all ADDJOB arguments
# b'DI... job id ...SQ'
c.getjob('queuename')
# [[b'queue', b'DI3971f14a850d9e5b3ca5c881e3dd1ba2a34277b505a0SQ', b'body']]
```

## Status

- [x] Server commands (slowlog, clients, etc)
- [x] ADDJOB
- [x] GETJOB
- [x] ACKJOB
- [x] FASTACK
- [x] QLEN
- [x] QPEEK

## TODO

- [ ] Make connection pool support multiple nodes as specified [here][clients]
- [ ] Write tests
- [ ] Write benchmarks
- [ ] QSTAT
- [ ] DELJOB
- [ ] SHOW
- [ ] SCAN
- [ ] DEQUEUE
- [ ] ENQUEUE

## Thanks

Enormous thanks to Salvatore Sanfilippo (antirez) for writing
[disque][https://github.com/antirez/disque] and
[Andy McCurdy][https://github.com/andymccurdy], author of the
[redis-py][https://github.com/andymccurdy/redis-py] module.

[clients]: https://github.com/antirez/disque#client-libraries
