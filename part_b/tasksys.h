#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <queue>
#include <unordered_map>
#include <set>
#include <iostream>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class TaskState {
    public:
        IRunnable* runnable;
        std::mutex* mutex; // lock for dependency variables
        std::mutex* mutex_count; // lock for count
        std::mutex* mutex_barrier; // lock for block
        std::condition_variable* cv_barrier; // cv for block

        std::vector<TaskID> vecDependentOn; // the current task is dependent on:
        int num_total_tasks;
        int counter;
        int taskID;
        int numThreads;
        int num_finished_threads_wait; // for block
        int num_finished_threads; // for block

        TaskState(IRunnable* runnable, int num_total_tasks, int taskID, const std::vector<TaskID>& deps, int numThreads) {
            this->mutex = new std::mutex();
            this->mutex_count = new std::mutex();
            this->mutex_barrier = new std::mutex();
            this->cv_barrier = new std::condition_variable();
            this->vecDependentOn =deps;
            this->runnable = runnable;
            this->counter = -1;
            this->num_total_tasks = num_total_tasks;
            this->taskID = taskID;
            this->numThreads = numThreads;
            this->num_finished_threads_wait = 0;
            this->num_finished_threads = 0;
        }
        ~TaskState() {
            delete mutex;
            delete mutex_count;
            delete mutex_barrier;
            delete cv_barrier;
            delete runnable;
        }

        void block(){
            std::unique_lock<std::mutex> lk(*mutex_barrier);
            ++num_finished_threads;
            ++num_finished_threads_wait;
            cv_barrier->wait(lk, [&]{return (num_finished_threads >= numThreads);});
            cv_barrier->notify_one(); // this will release other block
            --num_finished_threads_wait;
            if(num_finished_threads_wait == 0)
            {
               // reset barrier
               num_finished_threads = 0;
            }
            lk.unlock();
        }
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
        void workers(); // worker thread function
        void block(); // barrier

        int num_threads;
        std::thread* threads;
        bool spinning; // control while loop of workers
        std::mutex* mutex_at_sync; // lock for sync function
        std::mutex* mutex_working; // lock for working list
        std::mutex* mutex_waiting; // lock for waiting list
        std::mutex* mutex_removing; // lock for removed list
        std::mutex* mutex_map; // lock for dependency map
        std::mutex* mutex_shared_task; // lock for tasks
        std::mutex* mutex_barrier; // lock for barrier (block)
        std::condition_variable* cv_at_sync; // cv for sync function
        std::condition_variable* cv_barrier; // cv for barrier

        // For dependency
        std::vector<TaskState*> vecTask;
        TaskState* task_current;
        std::set<TaskID> set_waiting_ID; // waiting list
        std::set<TaskID> set_removed_ID; // removed list
        std::vector<TaskID> v_working_ID; // working list
        std::unordered_map<TaskID, std::vector<TaskID>> map_indep_to_dep; // dependency map
        int num_finished_threads; // for block
        int num_finished_threads_wait; // for block
        bool isAtSyncStatus = false; // indicating if in sync status
        bool isSyncRun = false; // indicating if the current test is synchro or not.

};
#endif
