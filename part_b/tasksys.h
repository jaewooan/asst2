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

class ThreadState{
public:
    IRunnable* runnable_;
    std::mutex* mutex_;
    int num_total_tasks_;
    int num_remaining_tasks;
    int counter_;
    ThreadState(IRunnable* runnable, int num_total_tasks) {
        mutex_ = new std::mutex();
        runnable_ = runnable;
        counter_ = -1;
        num_total_tasks_ = num_total_tasks;
        num_remaining_tasks = num_total_tasks;
    }
    ~ThreadState() {
        delete mutex_;
    }
};


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
        std::mutex* mutex;
        std::mutex* mutex_count;
        std::condition_variable* cv;
        int num_total_tasks;
        int num_remaining_tasks;
        int counter;
        int taskID;
        int nFinishedThread;
        int nWaitingThread;
        int numThreads;
        std::vector<TaskID> vecDependentOn; // the current task is dependent on:


        std::mutex* mutex_barrier;
        std::condition_variable* cv_barrier;
        int num_finished_threads_wait;
        int num_finished_threads;
        TaskState(IRunnable* runnable, int num_total_tasks, int taskID, const std::vector<TaskID>& deps, int numThreads) {
            this->mutex = new std::mutex();
            this->mutex_count = new std::mutex();
            this->cv = new std::condition_variable();
            this->runnable = runnable;
            this->counter = -1;
            this->num_total_tasks = num_total_tasks;
            this->num_remaining_tasks = num_total_tasks;
            this->taskID = taskID;
            this->vecDependentOn =deps;
            this->nFinishedThread = 0;
            this->nWaitingThread = 0;
            this->numThreads = numThreads;

            this->mutex_barrier = new std::mutex();
            this->cv_barrier = new std::condition_variable();
            this->num_finished_threads_wait = 0;
            this->num_finished_threads = 0;
        }
        ~TaskState() {
            delete mutex;
            delete mutex_count;
            delete cv;
            delete runnable;
            delete mutex_barrier;
            delete cv_barrier;
        }


        void block(int iThread, int taskID_local){
            std::unique_lock<std::mutex> lk(*mutex_barrier);
            ++num_finished_threads;
            ++num_finished_threads_wait;
            cv_barrier->wait(lk, [&]{return (num_finished_threads >= numThreads);});
            cv_barrier->notify_one();
            --num_finished_threads_wait;
            if(num_finished_threads_wait == 0)
            {
               //reset barrier
               num_finished_threads = 0;
            }
            lk.unlock();
        }

        void stop(int iThread, int taskID_local){
            std::unique_lock<std::mutex> lk(*mutex_barrier);
            ++num_finished_threads;
            ++num_finished_threads_wait;
            cv_barrier->wait(lk,[]{return false;});
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
        int num_threads;
        int num_finished_tasks = 0;
        std::vector<int> num_finished_tasks_threads;
        std::vector<int> num_idle_threads;
        int num_idle_init;
        std::thread* threads;
        ThreadState* thread_state;
        bool spinning;
        std::mutex* mutex_main;
        std::mutex* mutex_working;
        std::mutex* mutex_waiting;
        std::mutex* mutex_removing;
        std::condition_variable* cv_main;
        std::condition_variable* cv_barrier;
        std::mutex* mutex_thread_tot;
        std::mutex* mutex_thread_share;
        std::mutex* mutex_barrier;
        std::condition_variable* cv_thread_tot;
        std::vector<std::mutex*> mutex_thread;
        std::vector<std::condition_variable*> cv_thread;
        std::vector<bool> isWait;
        std::vector<bool> isInterateDone;
        void waitTask(int iThread);

        // For dependency
        std::vector<TaskState*> vecTask;
        TaskState* task_current;
        std::set<TaskID> set_waiting_ID;
        std::set<TaskID> set_removed_ID;
        std::vector<TaskID> q_working_ID;
        std::queue<TaskState*> q_task_waiting;
        std::queue<TaskState*> q_task_working;
        std::unordered_map<TaskID, std::vector<TaskID>> map_indep_to_dep;
        std::unordered_map<TaskID, std::vector<TaskID>> map_to_dep;
        std::unordered_map<TaskID, IRunnable*> map_runnable;
        std::unordered_map<TaskID, int> map_n_tot_taskID;
        int num_finished_threads;
        int num_finished_threads_wait;
        bool isAtSync = false;
        bool isSync = false;
        void runFunction(int iThread);

        void printqueue(std::queue<TaskID> Q){
            std::cout<<"Queue element are..."<< std::endl;
            while(!Q.empty()){
              std::cout<<" "<<Q.front()<<", ";
              Q.pop();
             }
            std::cout<<std::endl;
        };


        void block(int iThread, int taskID_local){
            std::unique_lock<std::mutex> lk(*mutex_barrier);
            ++num_finished_threads;
            ++num_finished_threads_wait;
            cv_barrier->wait(lk, [&]{return (num_finished_threads >= num_threads);});
            cv_barrier->notify_one();
            --num_finished_threads_wait;
            if(num_finished_threads_wait == 0)
            {
               //reset barrier
               num_finished_threads = 0;
            }
            lk.unlock();
        };

        void block_final(int iThread, int taskID_local){
            std::unique_lock<std::mutex> lk(*mutex_barrier);
            ++num_finished_threads;
            ++num_finished_threads_wait;
            cv_barrier->wait(lk, [&]{return (num_finished_threads >= num_threads);});
            cv_barrier->notify_one();
            --num_finished_threads_wait;
            if(num_finished_threads_wait == 0)
            {
               //reset barrier
               num_finished_threads = 0;
            }
            lk.unlock();
        };

};
#endif
