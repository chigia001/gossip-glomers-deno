#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIds, rpc, send } from "./Node.ts";
let counter = 0

handle<
    {}, 
    {id: string}
>("generate", (_src, _req, reply) => {
    reply({
        id: `${NodeId()}_${counter++}`
    })
})

await init()