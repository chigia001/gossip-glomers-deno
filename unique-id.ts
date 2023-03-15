#!/usr/bin/env -S deno run --allow-env
import { handle, init, nodeId, nodeIds, rpc, send } from "./Node.ts";
let counter = 0
handle<
    {type: "generate"}, 
    {type: "generate_ok", id: string}
>("generate", (_src, _req, reply) => {
    reply({
        type: "generate_ok",
        id: `${nodeId}_${counter++}`
    })
})

await init()