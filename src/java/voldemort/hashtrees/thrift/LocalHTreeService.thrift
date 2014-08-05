namespace java voldemort.hashtrees.thrift

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

service HashTreeService
{
	string ping();
	void sPut(1:map<binary,binary> keyValuePairs);
	void sRemove(1:list<binary> keys);
	list<SegmentHash> getSegmentHashes(1:i32 treeId, 2:list<i32> nodeIds);
	SegmentHash getSegmentHash(1:i32 treeId, 2:i32 nodeId);
	list<SegmentData> getSegment(1:i32 treeId, 2:i32 segId);
	SegmentData getSegmentData(1:i32 treeId, 2:i32 segId, 3:binary key);
	bool isReadyForSynch(1:i32 treeId);
	void deleteTreeNodes(1:i32 treeId, 2:list<i32> nodeIds);
}
