#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIds, rpc, send } from "./Node.ts";

handle<
    {type: "echo", echo: any}, 
    {type: "echo_ok", echo: any}
>("echo", (_src, req, reply) => {
    reply({
        type: "echo_ok",
        echo: req.echo
    })
})

await init()