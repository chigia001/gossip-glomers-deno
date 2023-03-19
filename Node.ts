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
  in_feedback_to?: number;
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

export const rpc = <
  RequestBody extends Omit<MessageBody, "msg_id">,
  ResponseBody extends {},
>(
  dest: string,
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
    console.warn("ignore 1")
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
  dest: string,
  body: RequestBody,
  feedbackCallback: (feedbackBody: FeedbackBody) => void,
  { abortSignal, timeout = 500 }: {
    abortSignal?: AbortSignal;
    timeout?: number;
  },
): Promise<ResponseBody> => {
  const abortController = new AbortController();
  abortSignal?.addEventListener("abort", () => {
    abortController.abort("test 2");
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
      abortController.abort(abortController.signal.reason);
    }, timeout);
  };

  const feedbackHandler = (feedbackBody: FeedbackBody) => {
    resetTimeout();
    feedbackCallback(feedbackBody);
  };
  resetTimeout();
  rpcFeedbackMap.set(msgId, feedbackHandler);
  rpcPromise.catch(() => {
    console.warn("ignore 2")
  }).finally(() => {
    rpcFeedbackMap.delete(msgId);
    clearTimeout(timeoutReturn);
  })
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
        const msgId = req.body.msg_id;
        const inReplyTo = req.body.in_reply_to;
        const inFeedbackTo = req.body.in_feedback_to;

        if (inReplyTo !== undefined) {
          const defer = rpcPromiseMap.get(inReplyTo);
          if (type === "error") {
            defer?.reject(req.body);
          } else {
            defer?.resolve(req.body);
          }
          continue;
        } else if (inFeedbackTo !== undefined) {
          rpcFeedbackMap.get(inFeedbackTo)?.(req.body);
          continue;
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
            in_feedback_to: msgId,
          });
        };

        const handler = handlers.get(type);
        if (handler) {
          handler(src, req.body, reply, feedback);
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
