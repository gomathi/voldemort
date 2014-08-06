namespace java voldemort.hashtrees.thrift.generated

/**
* Contains nodeId and segment hash.
* 
**/
struct SegmentHash
{
	1: required i32 nodeId;
	2: required binary hash;
}

/**
* Contains key, digest of the value
*
**/
struct SegmentData
{
	1: required binary key;
	2: required binary digest;
}

service HashTreeSyncInterface
{
	string ping();
	
	/**
     * Adds the (key,value) pair to the original storage. Intended to be used
     * while synch operation.
     * 
     * @param keyValuePairs
     */
	void sPut(1:map<binary,binary> keyValuePairs);
	
	/**
     * Deletes the keys from the storage. While synching this function is used.
     * 
     * @param keys
     */
	void sRemove(1:list<binary> keys);
	
	/**
     * Hash tree internal nodes store the hash of their children nodes. Given a
     * set of internal node ids, this returns the hashes that are stored on the
     * internal node.
     * 
     * @param treeId
     * @param nodeIds, internal tree node ids.
     * @return
     */
	list<SegmentHash> getSegmentHashes(1:i32 treeId, 2:list<i32> nodeIds);
	
	/**
     * Returns the segment hash that is stored on the tree.
     * 
     * @param treeId, hash tree id.
     * @param nodeId, node id
     * @return
     */
	SegmentHash getSegmentHash(1:i32 treeId, 2:i32 nodeId);
	
	/**
     * Hash tree data is stored on the leaf blocks. Given a segment id this
     * method is supposed to return (key,hash) pairs.
     * 
     * @param treeId
     * @param segId, id of the segment block.
     * @return
     */
	list<SegmentData> getSegment(1:i32 treeId, 2:i32 segId);
	
	/**
     * Returns the (key,digest) for the given key in the given segment.
     * 
     */
	SegmentData getSegmentData(1:i32 treeId, 2:i32 segId, 3:binary key);
	
	/**
     * If the HashTree is getting initialized now, then this function returns
     * false. Otherwise returns true.
     * 
     */
	bool isReadyForSynch(1:i32 treeId);
	
	/**
     * Deletes tree nodes from the hash tree, and the corresponding segments.
     * 
     */
	void deleteTreeNodes(1:i32 treeId, 2:list<i32> nodeIds);
}
