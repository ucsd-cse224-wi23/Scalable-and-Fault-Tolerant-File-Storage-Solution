package surfstore

import (
	context "context"
	"log"
	"sort"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer

	// Taken from discussion
	id          int64
	peers       []string
	commitIndex int64
	lastApplied int64

	nextIndex  []int64
	matchIndex []int64
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	for {
		if s.CheckIfCrashed() {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.CheckIfLeader() {
			return nil, ERR_NOT_LEADER
		}

		// send heartbeat to all other servers
		hb, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		if s.CheckIfCrashed() {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.CheckIfLeader() {
			return nil, ERR_NOT_LEADER
		}

		if hb.Flag {
			// if successful, read from state machine
			return s.metaStore.GetFileInfoMap(ctx, empty)
		}

		// if not successful, retry
		time.Sleep(100 * time.Millisecond)
	}
	// return nil, errors.New("failed to find majority")
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	for {
		if s.CheckIfCrashed() {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.CheckIfLeader() {
			return nil, ERR_NOT_LEADER
		}

		// send heartbeat to all other servers
		hb, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		if s.CheckIfCrashed() {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.CheckIfLeader() {
			return nil, ERR_NOT_LEADER
		}

		if hb.Flag {
			// if successful, apply to state machine
			return s.metaStore.GetBlockStoreMap(ctx, hashes)
		}

		// if not successful, retry
		time.Sleep(100 * time.Millisecond)
	}
	// return nil, errors.New("failed to find majority")
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	for {
		if s.CheckIfCrashed() {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.CheckIfLeader() {
			return nil, ERR_NOT_LEADER
		}

		// send heartbeat to all other servers
		hb, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		if s.CheckIfCrashed() {
			return nil, ERR_SERVER_CRASHED
		}
		if !s.CheckIfLeader() {
			return nil, ERR_NOT_LEADER
		}

		if hb.Flag {
			// if successful, apply to state machine
			return s.metaStore.GetBlockStoreAddrs(ctx, empty)
		}

		// if not successful, retry
		time.Sleep(100 * time.Millisecond)
	}
	// return nil, errors.New("failed to find majority")
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// check if leader
	if !s.CheckIfLeader() {
		return nil, ERR_NOT_LEADER
	}
	// check if crashed
	if s.CheckIfCrashed() {
		return nil, ERR_SERVER_CRASHED
	}

	// append entry to log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	for {
		// check if leader
		if !s.CheckIfLeader() {
			return nil, ERR_NOT_LEADER
		}
		// check if crashed
		if s.CheckIfCrashed() {
			return nil, ERR_SERVER_CRASHED
		}
		// send AppendEntries RPC to all other servers
		_, err := s.SendToAllPeers(ctx)

		// if successful, apply to state machine
		if err != nil {
			return nil, err
		}
		log.Println("commitIndex: ", s.commitIndex, " lastApplied: ", s.lastApplied)
		if s.commitIndex > s.lastApplied {
			version, err := s.metaStore.UpdateFile(ctx, filemeta)
			s.lastApplied = s.commitIndex
			return &Version{Version: version.Version}, err
		}
		// if not successful, wait for next heartbeat
		time.Sleep(1000 * time.Millisecond)
	}
	// TODO change this to return the version of the file
	// return &Version{Version: filemeta.Version}, nil
}

func (s *RaftSurfstore) SendToAllPeers(ctx context.Context) (int, error) {
	responses := make(chan int, len(s.peers))
	responses <- len(s.log) - 1

	for idx := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.SendToPeer(ctx, idx, responses)
	}

	totalActive := 0
	matchIndexList := make([]int, len(s.peers))
	for i := 0; i < len(s.peers); i++ {
		matchIndexList[i] = <-responses
		if matchIndexList[i] != -2 {
			totalActive++
		}
	}

	if !s.CheckIfLeader() {
		return 0, ERR_NOT_LEADER
	}

	// sort matchIndexList in descending order
	sort.Slice(matchIndexList, func(i, j int) bool {
		return matchIndexList[i] > matchIndexList[j]
	})
	log.Println("matchIndexList: ", matchIndexList)
	medianMatchIndex := matchIndexList[len(s.peers)/2]
	if int64(medianMatchIndex) > s.commitIndex {
		for i := s.commitIndex + 1; i <= int64(medianMatchIndex); i++ {
			s.metaStore.UpdateFile(ctx, s.log[i].FileMetaData)
		}
		s.commitIndex = int64(medianMatchIndex)
	}
	return totalActive, nil
}

func (s *RaftSurfstore) SendToPeer(ctx context.Context, peer_id int, responses chan int) {
	peer := s.peers[peer_id]
	conn, err := grpc.Dial(peer, grpc.WithInsecure())
	if err != nil {
		log.Println("Failed to connect to peer: ", peer)
		responses <- -2
		return
	}
	defer conn.Close()

	client := NewRaftSurfstoreClient(conn)
	var appendInput *AppendEntryInput
	if s.nextIndex[peer_id] == 0 {
		appendInput = &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}
	} else {
		appendInput = &AppendEntryInput{
			Term:         s.term,
			PrevLogIndex: s.nextIndex[peer_id] - 1,
			PrevLogTerm:  s.log[s.nextIndex[peer_id]-1].Term,
			Entries:      s.log[s.nextIndex[peer_id]:],
			LeaderCommit: s.commitIndex,
		}
	}

	appendOut, err := client.AppendEntries(ctx, appendInput)
	if err != nil {
		log.Println(err)
		responses <- -2
		return
	}
	if appendOut.Term > s.term {
		s.term = appendOut.Term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		responses <- -1
		return
	}
	if appendOut.Success {
		s.nextIndex[peer_id] = appendOut.MatchedIndex + 1
		s.matchIndex[peer_id] = appendOut.MatchedIndex
		responses <- int(appendOut.MatchedIndex)
	} else {
		s.nextIndex[peer_id]--
		s.matchIndex[peer_id]--
		responses <- -1
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// TODO check if server is in crash mode
	if s.CheckIfCrashed() {
		return nil, ERR_SERVER_CRASHED
	}

	if input.Term > s.term {
		s.term = input.Term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}

	if input.Term < s.term {
		return &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      false,
			MatchedIndex: -1,
		}, nil
	}

	if input.PrevLogIndex >= 0 {

		// TODO check if log contains entry at prevLogIndex whose term matches prevLogTerm
		if input.PrevLogIndex >= int64(len(s.log)) || s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			return &AppendEntryOutput{
				ServerId:     s.id,
				Term:         s.term,
				Success:      false,
				MatchedIndex: -1,
			}, nil
		}
	}
	log.Println("AppendEntries: ", "Term", input.Term, "PrevLogIndex", input.PrevLogIndex, "PrevLogTerm", input.PrevLogTerm, "Entries", input.Entries, "LeaderCommit", input.LeaderCommit, s.peers[s.id])
	// TODO delete existing entries that conflict with new ones
	for i := 0; i < len(input.Entries); i++ {
		k := int(input.PrevLogIndex) + 1 + i
		if k < len(s.log) {
			if s.log[k].Term != input.Entries[i].Term {
				s.log = s.log[:k]
				break
			}
		}
	}

	// TODO append any new entries not already in the log
	for i := 0; i < len(input.Entries); i++ {
		k := int(input.PrevLogIndex) + 1 + i
		if k >= len(s.log) {
			s.log = append(s.log, input.Entries[i])
		}
	}

	// TODO if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		if input.LeaderCommit > int64(len(s.log)-1) {
			s.commitIndex = int64(len(s.log) - 1)
		} else {
			s.commitIndex = input.LeaderCommit
		}
	}

	// TODO apply to state machine
	for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		s.metaStore.UpdateFile(ctx, s.log[i].FileMetaData)
	}
	s.lastApplied = s.commitIndex

	return &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: int64(len(s.log) - 1),
	}, nil

}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// check if server is in crash mode
	if s.CheckIfCrashed() {
		return nil, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++
	// TODO update state
	s.nextIndex = make([]int64, len(s.peers))
	s.matchIndex = make([]int64, len(s.peers))

	for i := 0; i < len(s.peers); i++ {
		s.nextIndex[i] = int64(len(s.log))
		s.matchIndex[i] = int64(-1)
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// contact all followers, send AppendEntries RPC with empty entries
	if s.CheckIfCrashed() {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.CheckIfLeader() {
		return nil, ERR_NOT_LEADER
	}

	totalActive, err := s.SendToAllPeers(ctx)
	if err != nil {
		return &Success{Flag: false}, err
	}
	if totalActive > len(s.peers)/2 {
		return &Success{Flag: true}, nil
	}
	return &Success{Flag: false}, nil
}

func (s *RaftSurfstore) CheckIfLeader() bool {
	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()
	return s.isLeader
}
func (s *RaftSurfstore) CheckIfCrashed() bool {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
	return s.isCrashed
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
