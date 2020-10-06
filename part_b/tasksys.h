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
        std::condition_variable* cv;
        int num_total_tasks;
        int num_remaining_tasks;
        int counter;
        int taskID;
        int nFinishedThread;
        std::vector<TaskID> vecDependentOn; // the current task is dependent on:
        TaskState(IRunnable* runnable, int num_total_tasks, int taskID, const std::vector<TaskID>& deps) {
            this->mutex = new std::mutex();
            this->cv = new std::condition_variable();
            this->runnable = runnable;
            this->counter = -1;
            this->num_total_tasks = num_total_tasks;
            this->num_remaining_tasks = num_total_tasks;
            this->taskID = taskID;
            this->vecDependentOn =deps;
            this->nFinishedThread = 0;
        }
        ~TaskState() {
            delete mutex;
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
        //std::vector<int> nFinishedTasks;
        int num_idle_init;
        int num_idle_wait;
        //int num_idle_result;
        //std::vector<int> num_idle;
        //std::vector<int> num_idle2;
        //int num_idle3;
        //bool isFullyIdle = false;
        std::thread* threads;
        ThreadState* thread_state;
        bool spinning;
        std::mutex* mutex_main;
        //std::mutex* mutex_main2;
        std::condition_variable* cv_main;
        //std::condition_variable* cv_main2;
        std::mutex* mutex_thread_tot;
        std::mutex* mutex_thread_share;
        //std::mutex* mutex_thread_main;
        //std::mutex* mutex_signal;
        //std::condition_variable* cv_signal;
        //std::condition_variable* cv_thread_main;
        std::condition_variable* cv_thread_tot;
        //std::condition_variable* cv_thread_share;
        std::vector<std::mutex*> mutex_thread;
        std::vector<std::condition_variable*> cv_thread;
        //bool on_thread_tot_wait = false;
        //bool on_thread_share_wait = false;
        //bool isAllWait = false;
        //std::vector<bool> isAllReleased;
        //std::vector<bool> isReady = {false,false};
        //bool isInitialized = false;
        //bool isRun = false;
        //bool isAllReleasedInit = false;
        std::vector<bool> isWait;
        std::vector<bool> isInterateDone;
        //int iRun = 0;;
        void waitTask(int iThread);
        //std::vector<std::unordered_set<int>> wait_thread;
        //std::unordered_set<int> wait_thread_init;

        // For dependency
        std::vector<TaskState*> vecTask;

        std::set<TaskID> set_waiting_ID;
        std::queue<TaskID> q_working_ID;
        std::queue<TaskState*> q_task_waiting;
        std::queue<TaskState*> q_task_working;
        std::unordered_map<TaskID, std::vector<TaskID>> map_indep_to_dep;
        std::unordered_map<TaskID, std::vector<TaskID>> map_to_dep;
        std::unordered_map<TaskID, IRunnable*> map_runnable;
        std::unordered_map<TaskID, int> map_n_tot_taskID;
        bool isAtSync = false;
        void runFunction(int iThread);

};
#endif
