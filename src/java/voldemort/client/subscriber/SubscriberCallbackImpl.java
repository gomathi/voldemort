/*
 * Copyright 2008-2014 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.client.subscriber;

import java.util.List;

import voldemort.hashtrees.thrift.generated.VersionedData;

public interface SubscriberCallbackImpl {

    /**
     * When the local subscriber receives changes from Voldemort node, the
     * server makes a call to this server. The client has to implement what to
     * do on receiving the data.
     * 
     * @param vDataList
     */
    public void onPost(List<VersionedData> vDataList);
}