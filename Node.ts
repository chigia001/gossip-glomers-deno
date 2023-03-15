type NodeId = `n${number}`

export let nodeId: NodeId
export let nodeIds: NodeId[]
let msgCounter = 0


interface MessageBody {
    type: string
    msg_id: number
    in_reply_to?: number
}

interface Message {
    src: string
    dest: string
    body: MessageBody
}

const rpcPromiseMap = new Map<number, {
    resolve: (result: any) => void,
    reject: (error: any) => void
}>()

export const send = <RequestBody extends Omit<MessageBody, "msg_id">>(dest: string, body: RequestBody): number => {
    const msgId = msgCounter++
    const serialize = JSON.stringify({
        src: nodeId,
        dest,
        body: {
            ...body,
            msg_id: msgId
        }
    } satisfies Message)
    console.log(serialize)
    console.warn(serialize)
    return msgId
}

export const rpc = async <RequestBody extends MessageBody, ResponseBody extends MessageBody>(dest: string, body: RequestBody): Promise<ResponseBody> => {
    const msgId = send(dest, body)
    try {
        return await new Promise<ResponseBody>((resolve, reject) => {
            rpcPromiseMap.set(msgId, {
                resolve, reject
            })
        })
    } finally {
        rpcPromiseMap.delete(msgId)
    }

}

type Handler<RequestBody extends Omit<MessageBody, "msg_id">, ResponseBody = never> = 
  ResponseBody extends Omit<MessageBody, "msg_id"> ? 
    (src: string,req: RequestBody, reply: (res: Omit<ResponseBody, "msg_id">)=> void) => void
    : (src: string,req: RequestBody) => void

const handlers = new Map<string, (src: string, req: any, reply: (data: any) => void) => void>()

export const handle = <RequestBody extends Omit<MessageBody, "msg_id">, ResponseBody>(event: string, callback: Handler<RequestBody, ResponseBody>) => {
    handlers.set(event, callback)
}

interface InitializeReqBody extends MessageBody{
    type     : "init",
    node_id  : NodeId,
    node_ids : NodeId[]
}

interface InitializeResBody extends MessageBody{
    type        : "init_ok"
}

export const init = async () => {
    handle<InitializeReqBody, InitializeResBody>("init", (_src, req, reply) => {
        nodeId = req.node_id
        reply({
            type: "init_ok"
        })
    })
    const decoder = new TextDecoder();
    for await (const chunk of Deno.stdin.readable) {
        const text = decoder.decode(chunk).trim();
        if (!text) {
            continue
        }
        const req = JSON.parse(text.trim()) as Message
        const src = req.src
        const type = req.body.type
        const msgId = req.body.msg_id
        const reply = (res:  Omit<MessageBody, "msg_id">): void => {
            send(src, {
                ...res,
                in_reply_to: msgId
            })
        }

        const handler = handlers.get(type)
        if (handler) {
            handler(src, req.body, reply)
        } else {
            send(src, {
                type: "error",
                in_reply_to: msgId,
                code: 10,
                text: `Don't have handler for event ${type}`
            })
        }
    }
}