package raft

import zio._

// PAXOS <- Leslie Lamport
// MongoDB and Redis
//
// N1
// available
// resiliency
// fault tolerant
//
// requests -> Single Node
// linearization
//  1 -> 2 -> 3 -> 4
//
// s x 10 ---> r x (10)
//
// RAFT consensus algorithm
// how do you agree on some state
// N1    N2    N3
// ------------------------
// 1. What it is? Motivation
// 2. Demo
// 3. High Level Algorithm
// 4. Low Level Implementation
//
// Distributed
//
// + Concurrency
// + Network Partition
// + Computers Crashing
// + Tidal Waves
//
// Consensus View -> State
//
// state.process(Message) -> State
trait Message
trait State

object StateMachine {
  // Log
  def process(state: State, message: Message): State = ???
}
// How do we develop a consensus?
//
//
// N1 (set x 3)
// N2 (set x 3)
// N3

// Replicated State Machine
//
// RAFT
// 1. Leader Election <--
// 2. Distributing Messages / AppendEntries

final case class NodeId(id: Int) extends AnyVal
final case class Term(int: Int)  extends AnyVal

final case class NodeState(
    term: Term,
    id: NodeId,
    votedFor: Option[NodeId] = None,
    receivedVotes: Set[NodeId] = Set.empty
) { self =>

  // deal with terms
  def handleVoteRequest(voteRequest: RaftMessage.VoteRequest): (NodeState, RaftMessage.VoteResponse) =
    if (votedFor.isEmpty) {
      (self.copy(votedFor = Some(voteRequest.candidateId)), RaftMessage.VoteResponse(id, term, true))
    } else {
      self -> RaftMessage.VoteResponse(id, term, false)
    }

  def handleVoteResponse(voteResponse: RaftMessage.VoteResponse): (NodeState, Boolean) =
    if (voteResponse.voteGranted) {
      val nextReceivedVotes = receivedVotes + voteResponse.fromId
      copy(receivedVotes = nextReceivedVotes) -> (nextReceivedVotes.size >= 2)
    } else {
      (self, false)
    }

  def becomeLeader: NodeState = ???
}

object NodeState {
  def empty(id: NodeId): NodeState =
    NodeState(Term(0), id)
}

sealed trait RaftMessage extends Product with Serializable
object RaftMessage {
  // RPC
  final case class VoteRequest(candidateId: NodeId, term: Term)                   extends RaftMessage
  final case class VoteResponse(fromId: NodeId, term: Term, voteGranted: Boolean) extends RaftMessage

  // log replication and heartbeat
  final case class AppendEntries(term: Term) extends RaftMessage

  // Internal
  case object StartElection extends RaftMessage
}

trait Cluster {
  def send(nodeId: NodeId, message: RaftMessage): Task[Unit]
  def sendAll(selfId: NodeId, message: RaftMessage): Task[Unit]
  def start: Task[Nothing]
}

final case class ClusterLive(var map: Map[NodeId, Node]) extends Cluster {
  override def send(nodeId: NodeId, message: RaftMessage): Task[Unit] =
    map(nodeId).inbox.offer(message).unit

  override def sendAll(selfId: NodeId, message: RaftMessage): Task[Unit] =
    ZIO.foreachParDiscard(map.values.filterNot(_.id == selfId)) { node =>
      node.inbox.offer(message)
    }

  override def start: Task[Nothing] =
    ZIO.foreachParDiscard(map.values)(_.start) *> ZIO.never
}

object Cluster {
  def make(size: Int): UIO[Cluster] = {
    val nodeIds = (1 to size).map(NodeId(_))
    val cluster = ClusterLive(Map.empty)
    for {
      nodes <- ZIO.foreachPar(nodeIds)(id => Node.make(id, cluster))
      _     <- ZIO.succeed { cluster.map = nodes.map(n => n.id -> n).toMap }
    } yield cluster
  }
}

final case class Node(
    id: NodeId,
    cluster: Cluster,
    inbox: Queue[RaftMessage]
) {

  private var state: NodeState                    = NodeState.empty(id)
  private var electionThunk: Fiber[Nothing, Unit] = Fiber.unit

  def start: Task[Nothing] =
    resetElectionTimer *>
      inbox.take.debug(s"$id >").flatMap(processMessage).forever

  private def resetElectionTimer: Task[Unit] =
    for {
      _     <- electionThunk.interrupt
      ms    <- Random.nextIntBetween(500, 1500)
      fiber <- inbox.offer(RaftMessage.StartElection).delay(ms.millis).unit.fork
      _     <- ZIO.succeed { electionThunk = fiber }
    } yield ()

  private def processMessage(raftMessage: RaftMessage): Task[Unit] =
    raftMessage match {
      case voteRequest: RaftMessage.VoteRequest =>
        val (nextState, response) = state.handleVoteRequest(voteRequest)
        state = nextState
        cluster.send(voteRequest.candidateId, response)

      case voteResponse: RaftMessage.VoteResponse =>
        val (nextState, becameLeader) = state.handleVoteResponse(voteResponse)
        if (becameLeader) {
          println(s"NODE $id BECAME LEADER")
        }
        state = nextState
        // we may have just been elected leader
        // if so, send heartbeats
        ZIO.unit

      case appendEntries: RaftMessage.AppendEntries =>
        resetElectionTimer

      case RaftMessage.StartElection =>
        cluster.sendAll(id, RaftMessage.VoteRequest(id, state.term))
    }

}

object Node {
  def make(id: NodeId, cluster: Cluster): ZIO[Any, Nothing, Node] =
    for {
      inbox <- Queue.unbounded[RaftMessage]
    } yield Node(id, cluster, inbox)
}

object RaftApp extends ZIOAppDefault {
  val run =
    for {
      cluster <- Cluster.make(3)
      _       <- cluster.start
    } yield ()
}
