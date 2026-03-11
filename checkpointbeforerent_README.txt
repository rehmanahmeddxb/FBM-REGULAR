Checkpoint: checkpointbeforerent
CreatedAt: 20260305_122623
Head: c276b41189658b20a80df926458595fae171f4a2
Tag: checkpointbeforerent
TrackedPatch: checkpointbeforerent_tracked.patch
StagedPatch: checkpointbeforerent_staged.patch
UntrackedList: checkpointbeforerent_untracked.txt
UntrackedZip: checkpointbeforerent_untracked.zip
Rollback:
1) git checkout checkpointbeforerent
2) git apply --index checkpointbeforerent_tracked.patch
3) restore untracked from checkpointbeforerent_untracked.zip if needed
