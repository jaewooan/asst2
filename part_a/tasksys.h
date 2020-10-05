#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_set>
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



struct ThreadState {
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

        int num_threads;
        void runTask(ThreadState* thread_state);
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
        int num_threads;
        int num_remaining;
        std::thread* threads;
        ThreadState* thread_state;
        bool spinning;
        void spinningTask();
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
        std::vector<int> nFinishedTasks;
        int num_idle_init;
        std::vector<int> num_idle;
        std::vector<int> num_idle2;
        int num_idle3;
        bool isFullyIdle = false;
        std::thread* threads;
        ThreadState* thread_state;
        bool spinning;
        std::mutex* mutex_main;
        std::condition_variable* cv_main;
        std::mutex* mutex_thread_tot;
        std::mutex* mutex_thread_share;
        std::mutex* mutex_thread_main;
        std::mutex* mutex_signal;
        std::condition_variable* cv_signal;
        std::condition_variable* cv_thread_main;
        std::condition_variable* cv_thread_tot;
        std::condition_variable* cv_thread_share;
        std::vector<std::mutex*> mutex_thread;
        std::vector<std::condition_variable*> cv_thread;
        bool on_thread_tot_wait = false;
        bool on_thread_share_wait = false;
        bool isAllWait = false;
        std::vector<bool> isAllReleased;
        std::vector<bool> isReady = {false,false};
        bool isInitialized = false;
        bool isRun = false;
        bool isAllReleasedInit = false;
        std::vector<bool> isWait;
        std::vector<bool> isInterateDone;
        int iRun = 0;;
        void waitTask(int iThread);
        void signalTask(int iThread);
        std::vector<std::unordered_set<int>> wait_thread;
        std::unordered_set<int> wait_thread_init;
};

#endif
