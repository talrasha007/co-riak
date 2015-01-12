# co-riak
  A riak(v2.x) client in [co](https://www.npmjs.com/package/co) style.
  *Note: Your bucket type should be created with {"props": {"allow_mult": false, "last_write_wins": true}} for example:*
```
riak-admin bucket-type create dmp '{"props": {"allow_mult": false, "last_write_wins": true}}'
```