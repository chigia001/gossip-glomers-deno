#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIdType, rpcWithFeedback } from "./Node.ts";
let topology: Record<NodeIdType, NodeIdType[]>;
// message that successfully consumed and already broardcast to peer with confirmation
const completeMessages = new Set<number>();
const completedNodes = new Map<number, NodeIdType[]>();

interface IncompleteMessageInfo {
  haveUpdate: boolean
  path: Set<NodeIdType>;
  sendNodes: Set<NodeIdType>;
  timeout: number;
  unsendPeers: Set<NodeIdType>;
  inflightPromise: Promise<NodeIdType[]>;
  srcFeedback: Map<NodeIdType, (param: PeerInform) => void>;
  abortController: Map<NodeIdType, AbortController>;
}

const markComplete = (info: IncompleteMessageInfo, nodeId: NodeIdType) => {
  info.srcFeedback.delete(nodeId);
  info.unsendPeers.delete(nodeId);
  if (!info.sendNodes.has(nodeId)) {
    info.sendNodes.add(nodeId);
    info.haveUpdate = true
  }
};

const promiseWrapper = (info: IncompleteMessageInfo, nodeId: NodeIdType): Promise<NodeIdType[]> => {
  info.abortController.get(nodeId)?.abort()
  return new Promise((resolve, reject) => {
    const abortController = new AbortController()
    info.abortController.set(nodeId, abortController)
    abortController.signal.addEventListener("abort", () => {
      reject
    })
    info.inflightPromise.then(resolve);
  })
}
const incompleteMessages = new Map<number, IncompleteMessageInfo>();

interface Broadcast {
  message: number;
}

interface PeerBroadcast {
  type: "broadcast_peer";
  message: number;
  sendeds: NodeIdType[];
  path: NodeIdType[];
  timeout: number;
}

interface PeerInform {
  sendeds: NodeIdType[];
}

const sendBroadcastPeer = async (dest: NodeIdType, message: number) => {
  let counter = 3
  while (counter--) {
    try {
      const incompleteMessage = incompleteMessages.get(message);
      if (!incompleteMessage || !incompleteMessage.unsendPeers.has(dest)) {
        return;
      }

      const feedback = ({ sendeds }: PeerInform) => {
        sendeds.forEach((node) => {
          markComplete(incompleteMessage, node);
        });
      };
      const { sendeds } = await rpcWithFeedback<
        PeerBroadcast,
        PeerInform,
        PeerInform
      >(
        dest,
        {
          type: "broadcast_peer",
          message,
          sendeds: [...incompleteMessage.sendNodes.values()],
          path: [...incompleteMessage.path.values(), dest],
          timeout: incompleteMessage.timeout,
        },
        feedback,
        {
          timeout: incompleteMessage.timeout,
        },
      );
      sendeds.forEach((node) => {
        markComplete(incompleteMessage, node);
      });

      markComplete(incompleteMessage, dest);
      return;
    } catch {
      console.warn("retry");
    }
  }
};

const handleBroadcast = (
  src: NodeIdType,
  message: number,
  sendeds: NodeIdType[],
  path: NodeIdType[],
  timeout: number,
  feedback?: (param: PeerInform) => void,
): Promise<NodeIdType[]> => {
  const incompleteMessage = incompleteMessages.get(message);
  if (incompleteMessage) {
    sendeds.forEach((node) => {
      markComplete(incompleteMessage, node);
    });
    path.forEach((node) => {
      incompleteMessage.path.add(node);
      incompleteMessage.unsendPeers.delete(node);
    });
    if (feedback) {
      incompleteMessage.srcFeedback.set(src, feedback);
    }
    return promiseWrapper(incompleteMessage, src)
  }

  if (completeMessages.has(message)) {
    return Promise.resolve(completedNodes.get(message) ?? [NodeId()]);
  }

  completeMessages.add(message);
  const pathSet = new Set(path);
  const sendedSet = new Set(sendeds);
  const peerSet = new Set(
    (topology[NodeId()] || []).filter((node) =>
      !(sendedSet.has(node) || pathSet.has(node))
    ),
  );
  const remaining = [...peerSet.values()];
  if (remaining.length === 0) {
    return Promise.resolve([NodeId()]);
  }

  const newIncompleteMessage: IncompleteMessageInfo = {
    haveUpdate: false,
    path: pathSet,
    inflightPromise: Promise.resolve().then(async () => {
      await Promise.all(
        remaining.map((peer) => sendBroadcastPeer(peer, message)),
      );
      markComplete(newIncompleteMessage, NodeId());
      const sendNodes = [...newIncompleteMessage.sendNodes.values()];
      completedNodes.set(message, sendNodes);
      return sendNodes;
    }),
    sendNodes: sendedSet,
    unsendPeers: peerSet,
    timeout,
    srcFeedback: new Map(feedback ? [[src, feedback]] : []),
    abortController: new Map(),
  };

  incompleteMessages.set(message, newIncompleteMessage);
  const interval = setInterval(() => {
    if (newIncompleteMessage.haveUpdate) {
      const sendeds = [...newIncompleteMessage.sendNodes.values()];
      for (const feedback of newIncompleteMessage.srcFeedback.values()) {
        feedback({
          sendeds,
        });
      }
      newIncompleteMessage.haveUpdate = false
    }
  }, timeout);

  newIncompleteMessage.inflightPromise.finally(() => {
    incompleteMessages.delete(message);
    clearInterval(interval);
  });

  return promiseWrapper(newIncompleteMessage, src)
};

handle<
  { topology: Record<NodeIdType, NodeIdType[]> }
>("topology", (_src, req, reply) => {
  topology = req.topology;
  reply();
});

handle<
  Broadcast
>("broadcast", async (src, req, reply) => {
  await handleBroadcast(src, req.message, [], [NodeId()], 1000);
  reply();
});

handle<
  PeerBroadcast,
  { sendeds: NodeIdType[] },
  { sendeds: NodeIdType[] }
>("broadcast_peer", async (src, req, reply, feedback) => {
  const data = await handleBroadcast(
    src,
    req.message,
    req.sendeds,
    req.path,
    req.timeout,
    feedback,
  );
  reply({ sendeds: data });
});

handle<{}, { messages: number[] }>("read", (_src, req, reply) => {
  reply({
    messages: [...completeMessages.values()],
  });
});

await init();
