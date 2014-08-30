namespace java voldemort.hashtrees.thrift.generated

/**
 * The data which is published to the client through publish/subscribe api.
 *
 */
struct VersionedData
{
	1: required i64 versionNo;
	2: required bool addedOrRemoved;
	3: required binary key;
	4: optional binary value;
}
	
/**
 * A client is supposed to run locally a VersionedDataListenerService to which each node in the voldemort notifies the changes. 
 *
 */
 	
service VersionedDataListenerService
{
	void post(1:list<VersionedData> vDataList);
}
	