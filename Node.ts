import { batchWindow } from "./config.ts";

export type NodeIdType = `${"n" | "c"}${number}`;

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

interface MessageBatchBody extends MessageBody {
  type: "batch";
  messages: MessageBody[];
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

const standardMessageBody = <RequestBody extends Omit<MessageBody, "msg_id">>(
  body: RequestBody,
): [number, MessageBody] => {
  const msgId = msgCounter++;
  return [msgId, {
    ...body,
    msg_id: msgId,
  }];
};

const send_internal = (dest: string, body: MessageBody[]) => {
  const src = nodeId;
  // send message to STDOUT
  console.log(JSON.stringify({
    src,
    dest,
    body: body.length === 1 ? body[0] : standardMessageBody({
      type: "batch",
      messages: body,
    })[1] as MessageBatchBody,
  }));
};

const batchingQueue = new Map<NodeIdType, MessageBody[]>();

export const send = <RequestBody extends Omit<MessageBody, "msg_id">>(
  dest: NodeIdType,
  body: RequestBody,
): number => {
  const [msgId, standardizeBody] = standardMessageBody(body);
  if (dest.startsWith("c")) {
    // client don't understand batching so just send directly
    send_internal(dest, [standardizeBody]);
  } else {
    const queue = batchingQueue.get(dest);
    if (queue) {
      queue.push(standardizeBody);
    } else {
      const newQueue = [standardizeBody];
      batchingQueue.set(dest, newQueue);
      setTimeout(() => {
        batchingQueue.delete(dest);
        send_internal(dest, newQueue);
      }, batchWindow);
    }
  }

  return msgId;
};

export const rpc = <
  RequestBody extends Omit<MessageBody, "msg_id">,
  ResponseBody extends {},
>(
  dest: NodeIdType,
  body: RequestBody,
  abortSignal?: AbortSignal,
): [number, Promise<ResponseBody>] => {
  const msgId = send(dest, body);
  const promise = new Promise<ResponseBody>((resolve, reject) => {
    rpcPromiseMap.set(msgId, {
      resolve,
      reject,
    });

    if (abortSignal?.aborted) {
      reject(abortSignal.reason);
    }

    abortSignal?.addEventListener("abort", () => {
      reject(abortSignal.reason);
    });
  });

  promise.catch(() => {
    console.warn("ignore 1");
  }).finally(() => {
    rpcPromiseMap.delete(msgId);
  });
  return [msgId, promise];
};

const rpcFeedbackMap = new Map<number, (body: any) => void>();

export const rpcWithFeedback = <
  RequestBody extends Omit<MessageBody, "msg_id">,
  ResponseBody extends {},
  FeedbackBody extends {},
>(
  dest: NodeIdType,
  body: RequestBody,
  feedbackCallback: (feedbackBody: FeedbackBody) => void,
  { abortSignal, timeout = 500 }: {
    abortSignal?: AbortSignal;
    timeout?: number;
  },
): Promise<ResponseBody> => {
  const abortController = new AbortController();
  abortSignal?.addEventListener("abort", () => {
    abortController.abort(abortSignal.reason);
  });

  const [msgId, rpcPromise] = rpc<RequestBody, ResponseBody>(
    dest,
    body,
    abortController.signal,
  );

  let timeoutReturn: ReturnType<typeof setTimeout> | undefined;
  const resetTimeout = () => {
    if (timeoutReturn) {
      clearTimeout(timeoutReturn);
    }

    timeoutReturn = setTimeout(() => {
      abortController.abort({
        code: 0,
        text: "network timeout",
      });
    }, timeout);
  };

  const feedbackHandler = (feedbackBody: FeedbackBody) => {
    resetTimeout();
    feedbackCallback(feedbackBody);
  };
  resetTimeout();
  rpcFeedbackMap.set(msgId, feedbackHandler);
  rpcPromise.catch(() => {
    console.warn("ignore 2");
  }).finally(() => {
    rpcFeedbackMap.delete(msgId);
    clearTimeout(timeoutReturn);
  });
  return rpcPromise;
};

type Handler<RequestBody, ResponseBody = undefined, FeedbackBody = undefined> =
  (
    src: NodeIdType,
    req: RequestBody,
    reply: ResponseBody extends undefined ? () => void
      : (res: ResponseBody) => void,
    feedback: FeedbackBody extends undefined ? () => void
      : (res: FeedbackBody) => void,
  ) => void;

const handlers = new Map<
  string,
  Handler<any, any, any>
>();

export const handle = <
  RequestBody,
  ResponseBody = undefined,
  FeedbackBody = undefined,
>(
  event: string,
  callback: Handler<RequestBody, ResponseBody, FeedbackBody>,
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

const handleRequest = (src: NodeIdType, body: MessageBody) => {
  const type = body.type;
  const msgId = body.msg_id;
  const inReplyTo = body.in_reply_to;

  if (inReplyTo !== undefined) {
    Promise.resolve().then(() => {
      if (type === "error") {
        rpcPromiseMap.get(inReplyTo)?.reject(body);
      } else if (type.endsWith("_ok")) {
        rpcPromiseMap.get(inReplyTo)?.resolve(body);
      } else if (type.endsWith("_feedback")) {
        rpcFeedbackMap.get(inReplyTo)?.(body);
      }
    });
    return;
  }

  const reply = (res: Omit<MessageBody, "msg_id">): void => {
    send(src, {
      ...res,
      type: `${type}_ok`,
      in_reply_to: msgId,
    });
  };

  const feedback = (res: Omit<MessageBody, "msg_id">): void => {
    send(src, {
      ...res,
      type: `${type}_feedback`,
      in_reply_to: msgId,
    });
  };

  const error = (res: any): void => {
    send(src, {
      ...res,
      type: `error`,
      in_reply_to: msgId,
    });
  };

  const handler = handlers.get(type);
  if (handler) {
    Promise.resolve()
      .then(() => handler(src, body, reply, feedback))
      .catch((err) => {
        error(err);
      });
  } else {
    send(src, {
      type: "error",
      in_reply_to: msgId,
      code: 10,
      text: `Don't have handler for event ${type}`,
    });
  }
};

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
        const src = req.src as NodeIdType;
        const type = req.body.type;
        if (type === "batch") {
          const messages = (req.body as MessageBatchBody).messages;
          messages.forEach((message) => {
            handleRequest(src, message);
          });
        } else {
          handleRequest(src, req.body);
        }
      } catch {
        console.warn(`Error: ${text}`);
      }
    }
  }
};
