# Gossip Glommers Challenges using Deno

## Prerequisites:
- [Maelstrom](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md)
- [Deno](https://deno.land/manual@v1.31.2/getting_started)

## Challenges:

### Echo:
```bash
./maelstrom test -w echo --bin ./echo.ts --node-count 1 --time-limit 10
```

### Unique Ids:
```bash
./maelstrom test -w unique-ids --bin ./unique-id.ts --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```