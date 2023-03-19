#!/usr/bin/env -S deno run --allow-env
import { handle, init, NodeId, NodeIdType, rpcWithFeedback } from "./Node.ts";
let topology: Record<NodeIdType, NodeIdType[]>;
// message that successfully consumed and already broardcast to peer with confirmation
const completeMessages = new Set<number>();

interface IncompleteMessageInfo {
  path: Set<NodeIdType>;
  sendNodes: Set<NodeIdType>;
  timeout: number;
  unsendPeers: Set<NodeIdType>;
  inflightPromise: Promise<void>;
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
  while (true) {
    try {
      const incompleteMessage = incompleteMessages.get(message);
      if (!incompleteMessage || !incompleteMessage.unsendPeers.has(dest)) {
        return;
      }

      const feedback = ({ sendeds }: PeerInform) => {
        sendeds.forEach((node) => {
          incompleteMessage.sendNodes.add(node);
          incompleteMessage.unsendPeers.delete(node);
        });
      };
      await rpcWithFeedback<PeerBroadcast, PeerInform, PeerInform>(
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
          timeout: incompleteMessage.timeout * 2,
        },
      );

      incompleteMessage.sendNodes.add(dest);
      incompleteMessage.unsendPeers.delete(dest);
      return;
    } catch {
    }
  }
};

const handleBroadcast = async (
  message: number,
  sendeds: NodeIdType[],
  path: NodeIdType[],
  timeout: number,
  feedback?: (param: PeerInform) => void,
) => {

  const incompleteMessage = incompleteMessages.get(message);
  if (incompleteMessage) {
    sendeds.forEach((node) => {
      incompleteMessage.sendNodes.add(node);
      incompleteMessage.unsendPeers.delete(node);
    });
    path.forEach((node) => {
      incompleteMessage.path.add(node);
      incompleteMessage.unsendPeers.delete(node);
    });
    return incompleteMessage.inflightPromise;
  }

  if (completeMessages.has(message)) {
    return;
  }

  completeMessages.add(message);
  const pathSet = new Set(path);
  const sendedSet = new Set(sendeds);
  const peerSet = new Set(
    (topology[NodeId()] || []).filter((node) =>
      !(sendedSet.has(node) || pathSet.has(node))
    ),
  );

  const inflightPromise = Promise.resolve([...peerSet.values()])
    .then(async (unsendPeers) => {
      await Promise.all(
        unsendPeers.map((peer) => sendBroadcastPeer(peer, message)),
      );
      return;
    });

  const newIncompleteMessage: IncompleteMessageInfo = {
    path: pathSet,
    inflightPromise,
    sendNodes: sendedSet,
    unsendPeers: peerSet,
    timeout,
  };

  incompleteMessages.set(message, newIncompleteMessage);
  const interval: ReturnType<typeof setInterval> | undefined = feedback
    ? setInterval(() => {
      feedback({
        sendeds: [...newIncompleteMessage.sendNodes.values()],
      });
    }, timeout / 2)
    : undefined;

  try {
    return await inflightPromise;
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
  { message: number }
>("broadcast", async (_src, req, reply) => {
  await handleBroadcast(req.message, [], [NodeId()], 500);
  reply();
});

handle<
  PeerBroadcast,
  undefined,
  { sendeds: NodeIdType[] }
>("broadcast_peer", async (_src, req, reply, feedback) => {
  await handleBroadcast(
    req.message,
    req.sendeds,
    req.path,
    req.timeout,
    feedback,
  );
  reply();
});

handle<{}, { messages: number[] }>("read", (_src, req, reply) => {
  reply({
    messages: [...completeMessages.values()],
  });
});

await init();
