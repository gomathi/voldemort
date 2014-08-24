namespace java voldemort.hashtrees.thrift.generated

/**
 * The data which is published to the client through publish/subscribe api.
 *
 */
struct VersionedData
{
	1: required i64 versionNo;
	2: required i32 treeId;
	3: required bool addedOrRemoved;
	4: required binary key;
	5: optional binary value;
}
	
	