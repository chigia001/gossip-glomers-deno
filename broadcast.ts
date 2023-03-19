#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIdType, rpcWithFeedback } from "./Node.ts";
let topology: Record<NodeIdType, NodeIdType[]>;
// message that successfully consumed and already broardcast to peer with confirmation
const completeMessages = new Set<number>();
const completedNodes = new Map<number, NodeIdType[]>();

interface IncompleteMessageInfo {
  path: Set<NodeIdType>;
  sendNodes: Set<NodeIdType>;
  timeout: number;
  unsendPeers: Set<NodeIdType>;
  inflightPromise: Promise<NodeIdType[]>;
  srcFeedback: Map<NodeIdType, (param: PeerInform) => void>;
}

const markComplete = (info: IncompleteMessageInfo, nodeId: NodeIdType) => {
  info.unsendPeers.delete(nodeId);
  info.sendNodes.add(nodeId);
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
  timeout: number;
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
      await rpcWithFeedback<PeerBroadcast, {}, PeerInform>(
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

      markComplete(incompleteMessage, dest);
      return;
    } catch {
      console.warn("retry");
    }
  }
};

const handleBroadcast = async (
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
      feedback({
        sendeds: [...incompleteMessage.sendNodes.values()],
      });
      incompleteMessage.srcFeedback.set(src, feedback);
    }
    return incompleteMessage.inflightPromise;
  }

  if (completeMessages.has(message)) {
    return completedNodes.get(message) ?? [NodeId()];
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
    return [NodeId()];
  }

  const newIncompleteMessage: IncompleteMessageInfo = {
    path: pathSet,
    inflightPromise: Promise.resolve().then(async () => {
      await Promise.all(
        remaining.map((peer) => sendBroadcastPeer(peer, message)),
      )
      markComplete(newIncompleteMessage, NodeId());
      const sendNodes = [...newIncompleteMessage.sendNodes.values()]
      completedNodes.set(message, sendNodes)
      return sendNodes
    }),
    sendNodes: sendedSet,
    unsendPeers: peerSet,
    timeout,
    srcFeedback: new Map(feedback ? [[src, feedback]] : []),
  };

  incompleteMessages.set(message, newIncompleteMessage);
  const interval = setInterval(() => {
    const sendeds = [...newIncompleteMessage.sendNodes.values()];
    for (const feedback of newIncompleteMessage.srcFeedback.values()) {
      feedback({
        sendeds,
      });
    }
  }, timeout);

  feedback?.({
    sendeds: [...newIncompleteMessage.sendNodes.values()],
  });

  try {
    return await newIncompleteMessage.inflightPromise;
  } finally {
    incompleteMessages.delete(message);
    clearInterval(interval);
  }
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
  await handleBroadcast(src, req.message, [], [NodeId()], 250);
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
  reply({sendeds: data});
});

handle<{}, { messages: number[] }>("read", (_src, req, reply) => {
  reply({
    messages: [...completeMessages.values()],
  });
});

await init();
