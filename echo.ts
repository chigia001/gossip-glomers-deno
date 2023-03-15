#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIds, rpc, send } from "./Node.ts";

handle<
    {echo: any}, 
    {echo: any}
>("echo", (_src, req, reply) => {
    reply({
        echo: req.echo
    })
})

await init()