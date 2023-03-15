#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIds, NodeIdType, rpc, send } from "./Node.ts";
let topology: Record<NodeIdType, NodeIdType[]>
const messages = new Set<number>()

const handleBroadcast = async (message: number, ignoreNodes: string = '')  => {
    if (messages.has(message)) {
        return;
    }

    messages.add(message)
    const currentNodeId = NodeId()
    const sends = `${ignoreNodes}_${currentNodeId}`
    const peerSet = new Set((topology[currentNodeId] || []).filter(node => sends.indexOf(`_${node}`) === -1))
    
    const peers = [...peerSet.values()]

    await Promise.all(
        peers.map((dest) => rpc(dest, {
            type: "broadcast_peer",
            message,
            sends
        }))
    )
}

handle<
    {topology: Record<NodeIdType, NodeIdType[]>}, 
>("topology", (_src, req, reply) => {
    topology = req.topology
    reply()
})

handle<
    {message: number}
>("broadcast", async (_src, req, reply) => {
    await handleBroadcast(req.message)
    reply()
})

handle<
    {message: number, sends: string}
>("broadcast_peer", async (_src, req, reply) => {
    await handleBroadcast(req.message, req.sends)
    reply()
})

handle<{}, {messages: number[]}>("read", (_src, req, reply) => {
    reply({
        messages: [...messages.values()]
    })
})

await init()