#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIdType, rpcWithFeedback } from "./Node.ts";
import { timeout } from "./config.ts";

let topology: Record<NodeIdType, NodeIdType[]>;
// message that successfully consumed and already broardcast to peer with confirmation
const completeMessages = new Map<number, NodeIdType[]>();
const completedNodes = new Map<number, NodeIdType[]>();

interface IncompleteMessageInfo {
  haveUpdate: boolean;
  path: Set<NodeIdType>;
  sendNodes: Set<NodeIdType>;
  unsendPeers: Set<NodeIdType>;
  inflightPromise: Promise<NodeIdType[]>;
  srcHearthbeat: Map<NodeIdType, (param: PeerInform) => void>;
  srcAbortController: Map<NodeIdType, AbortController>;
  peerAbortController: Map<NodeIdType, AbortController>;
}

const markComplete = (info: IncompleteMessageInfo, nodeId: NodeIdType) => {
  info.srcHearthbeat.delete(nodeId);
  info.unsendPeers.delete(nodeId);
  info.peerAbortController.get(nodeId)?.abort({
    code: 14,
    text: "aborted because already done"
  })
  if (!info.sendNodes.has(nodeId)) {
    info.sendNodes.add(nodeId);
    info.haveUpdate = true;
  }
  info.path.delete(nodeId)
};

const promiseWrapper = (
  info: IncompleteMessageInfo,
  nodeId: NodeIdType,
): Promise<NodeIdType[]> => {
  info.srcAbortController.get(nodeId)?.abort({
    code: 14,
    text: "new request from same src, abort old request"
  });
  return new Promise((resolve, reject) => {
    const abortController = new AbortController();
    info.srcAbortController.set(nodeId, abortController);
    abortController.signal.addEventListener("abort", () => {
      reject;
    });
    info.inflightPromise.then(resolve, reject);
  });
};
const incompleteMessages = new Map<number, IncompleteMessageInfo>();

interface Broadcast {
  message: number;
}

interface PeerBroadcast {
  type: "broadcast_peer";
  message: number;
  sendeds: NodeIdType[];
  path: NodeIdType[];
}

interface PeerInform {
  sendeds: NodeIdType[];
}

const sendBroadcastPeer = async (dest: NodeIdType, message: number) => {
  while (true) {
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
        },
        feedback,
        {
          timeout,
          abortSignal: incompleteMessage.peerAbortController.get(dest)?.signal
        },
      );
      sendeds.forEach((node) => {
        markComplete(incompleteMessage, node);
      });

      markComplete(incompleteMessage, dest);
      return;
    } catch (err) {
      if (err.code === 14) {
        return;
      } else if (err.code === 0) {
        console.warn("retry");
      } else {
        throw err
      }
    }
  }
};

const handleBroadcast = (
  src: NodeIdType,
  message: number,
  sendeds: NodeIdType[],
  path: NodeIdType[],
  hearthbeath?: (param: PeerInform) => void,
): Promise<NodeIdType[]> => {
  const incompleteMessage = incompleteMessages.get(message);
  if (incompleteMessage) {
    path.forEach((node) => {
      incompleteMessage.path.add(node);
      incompleteMessage.unsendPeers.delete(node);
      incompleteMessage.peerAbortController.get(node)?.abort({
        code: 14,
        text: "aborted"
      })
    });
    sendeds.forEach((node) => {
      markComplete(incompleteMessage, node);
    });
    if (hearthbeath) {
      incompleteMessage.srcHearthbeat.set(src, hearthbeath);
    }
    return promiseWrapper(incompleteMessage, src);
  }
  const completeNodes = completeMessages.get(message)
  if (completeNodes && completeNodes.length > 0) {
    return Promise.resolve(completeNodes);
  }

  completeMessages.set(message, []);
  const pathSet = new Set(path);
  const sendedSet = new Set(sendeds);
  const peerSet = new Set(
    (topology[NodeId()] || []).filter((node) =>
      !(sendedSet.has(node) || pathSet.has(node))
    ),
  );
  const remaining = [...peerSet.values()];
  if (remaining.length === 0) {
    sendedSet.add(NodeId())
    const newSendeds = [...sendedSet.values()]
    completeMessages.set(message, newSendeds)
    return Promise.resolve(newSendeds);
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
    srcHearthbeat: new Map(hearthbeath ? [[src, hearthbeath]] : []),
    srcAbortController: new Map(),
    peerAbortController: new Map(remaining.map((node) => [node, new AbortController()]))
  };

  incompleteMessages.set(message, newIncompleteMessage);
  const interval = setInterval(() => {
    if (newIncompleteMessage.haveUpdate) {
      const sendeds = [...newIncompleteMessage.sendNodes.values()];
      for (const hearthbeat of newIncompleteMessage.srcHearthbeat.values()) {
        hearthbeat({
          sendeds,
        });
      }
      newIncompleteMessage.haveUpdate = false;
    }
  }, timeout / 2);

  newIncompleteMessage.inflightPromise.then((value) => {
    completeMessages.set(message, value)
  }, () => {
    console.warn("ignore")
  }).finally(() => {
    incompleteMessages.delete(message);
    clearInterval(interval);
  });

  return promiseWrapper(newIncompleteMessage, src);
};

handle<
  { topology: Record<NodeIdType, NodeIdType[]> }
>("topology", (_src, req, reply) => {
  topology = req.topology;
  reply();
});

handle<
  Broadcast
>("broadcast", (src, req, ok) => {
  handleBroadcast(src, req.message, [], [NodeId()]);
  ok();
});

handle<
  PeerBroadcast,
  { sendeds: NodeIdType[] },
  { sendeds: NodeIdType[] }
>("broadcast_peer", async (src, req, ok, hearthbeath) => {
  try {
    const data = await handleBroadcast(
      src,
      req.message,
      req.sendeds,
      req.path,
      hearthbeath,
    );
    ok({ sendeds: data });
  } catch (e) {
    if (e.code === 14) {
      return
    }
    throw e
  }
});

handle<{}, { messages: number[] }>("read", (_src, req, reply) => {
  reply({
    messages: [...completeMessages.keys()],
  });
});

await init();
