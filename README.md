# MIT_6.824_Labs
Going through MIT 6.824 Distributed Systems Course Labs.

ToDo:
- encode intermediate files to json
encoding/json package
write:
 enc := json.NewEncoder(file)
  for _, kv := ... {
    err := enc.Encode(&kv)
And use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.

- lock on coordinator strucutres
- worker or RPC coordinator thread wait sleep
- coordinator thread monitor workers by check if 10secs pass   