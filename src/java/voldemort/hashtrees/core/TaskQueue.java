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
package voldemort.hashtrees.core;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class TaskQueue<T> implements Iterator<Future<T>> {

    private final CompletionService<T> completionService;
    private final Iterator<Callable<T>> tasksItr;
    private volatile int noOfTasksSubmitted;

    public TaskQueue(final ExecutorService fixedExecutors,
                     Iterator<Callable<T>> tasksItr,
                     int initTasksToExecute) {
        this.completionService = new ExecutorCompletionService<T>(fixedExecutors);
        this.tasksItr = tasksItr;
        while(initTasksToExecute > 0 && tasksItr.hasNext()) {
            completionService.submit(tasksItr.next());
            initTasksToExecute--;
            noOfTasksSubmitted++;
        }
    }

    @Override
    public boolean hasNext() {
        if(noOfTasksSubmitted > 0)
            return true;
        return false;
    }

    @Override
    public Future<T> next() {
        if(!hasNext())
            throw new NoSuchElementException();
        try {
            noOfTasksSubmitted--;
            Future<T> result = completionService.take();
            if(tasksItr.hasNext()) {
                completionService.submit(tasksItr.next());
                noOfTasksSubmitted++;
            }
            return result;
        } catch(InterruptedException e) {
            throw new RuntimeException("Exception occurred while waiting to remove the element from the queue.");
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
