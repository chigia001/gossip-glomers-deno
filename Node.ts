export type NodeIdType = `n${number}`;

let nodeId: NodeIdType;
let nodeIds: NodeIdType[] = [];
let msgCounter = 0;

export const NodeId = () => nodeId;
export const NodeIds = () => nodeIds;

interface MessageBody {
  type: string;
  msg_id: number;
  in_reply_to?: number;
}

interface Message {
  src: string;
  dest: string;
  body: MessageBody;
}

const rpcPromiseMap = new Map<number, {
  resolve: (result: any) => void;
  reject: (error: any) => void;
}>();

export const send = <RequestBody extends Omit<MessageBody, "msg_id">>(
  dest: string,
  body: RequestBody,
): number => {
  const msgId = msgCounter++;
  const serialize = JSON.stringify(
    {
      src: nodeId,
      dest,
      body: {
        ...body,
        msg_id: msgId,
      },
    } satisfies Message,
  );
  // send message to STDOUT
  console.log(serialize);
  return msgId;
};

export const rpc = async <
  RequestBody extends Omit<MessageBody, "msg_id">,
  ResponseBody extends Omit<MessageBody, "msg_id">,
>(dest: string, body: RequestBody): Promise<ResponseBody> => {
  while (true) {
    const msgId = send(dest, body);
    try {
      const resp = await new Promise<ResponseBody>((resolve, reject) => {
        rpcPromiseMap.set(msgId, {
          resolve,
          reject,
        });

        setTimeout(() => {
          reject();
        }, 10);
      });
      return resp;
    } catch {
        console.warn("retrying")
    }finally {
      rpcPromiseMap.delete(msgId);
    }
  }
};

type Handler<RequestBody, ResponseBody = undefined> = ResponseBody extends
  undefined ? (src: string, req: RequestBody, reply: () => void) => void
  : (src: string, req: RequestBody, reply: (res: ResponseBody) => void) => void;

const handlers = new Map<
  string,
  (src: string, req: any, reply: (res?: any) => void) => void
>();

export const handle = <RequestBody, ResponseBody = undefined>(
  event: string,
  callback: Handler<RequestBody, ResponseBody>,
) => {
  handlers.set(event, callback);
};

handle<{
  node_id: NodeIdType;
  node_ids: NodeIdType[];
}>("init", (_src, req, reply) => {
  nodeId = req.node_id;
  nodeIds = req.node_ids;
  reply();
});

export const init = async () => {
  const decoder = new TextDecoder();
  for await (const chunk of Deno.stdin.readable) {
    const input = decoder.decode(chunk).trim().split("\n");
    for (const text of input) {
      if (!text) {
        continue;
      }
      try {
        const req = JSON.parse(text.trim()) as Message;
        const src = req.src;
        const type = req.body.type;
        const msgId = req.body.msg_id;
        const inReplyTo = req.body.in_reply_to;

        if (inReplyTo !== undefined) {
          const defer = rpcPromiseMap.get(inReplyTo);
          if (type === "error") {
            defer?.reject(req.body);
          } else {
            defer?.resolve(req.body);
          }
          continue;
        }
        const reply = (res: Omit<MessageBody, "msg_id">): void => {
          send(src, {
            ...res,
            type: `${type}_ok`,
            in_reply_to: msgId,
          });
        };

        const handler = handlers.get(type);
        if (handler) {
          handler(src, req.body, reply);
        } else {
          send(src, {
            type: "error",
            in_reply_to: msgId,
            code: 10,
            text: `Don't have handler for event ${type}`,
          });
        }
      } catch {
        console.warn(`Error: ${text}`);
      }
    }
  }
};
