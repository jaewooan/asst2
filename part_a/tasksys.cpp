#include "tasksys.h"
#include <condition_variable>
#include <mutex>
#include <thread>
#include <queue>
#include <map>
#include <algorithm>    // std::find

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

void TaskSystemParallelSpawn::runTask(ThreadState* thread_state){
    int iTask = 0;
    bool done = false;
    while(!done){
        thread_state->mutex_->lock();
        iTask = ++thread_state->counter_;
        thread_state->mutex_->unlock();
        if(iTask < thread_state->num_total_tasks_){
            thread_state->runnable_->runTask(iTask, thread_state->num_total_tasks_);
        } else{
            done = true;
        }
    }
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::thread* threads = new std::thread[num_threads];
    ThreadState* thread_state = new ThreadState(runnable, num_total_tasks);
    for(int i = 0; i < num_threads; i++){
        threads[i] = std::thread(&TaskSystemParallelSpawn::runTask, this, thread_state);
    }
    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

void TaskSystemParallelThreadPoolSpinning::spinningTask(){
    int iTask = 0;
    int nTotTask = 0;
    while(spinning){
        thread_state->mutex_->lock();
        iTask = ++thread_state->counter_;
        if(iTask < thread_state->num_total_tasks_ ){
            nTotTask = thread_state->num_total_tasks_;
            thread_state->mutex_->unlock();
            thread_state->runnable_->runTask(iTask, nTotTask);

            thread_state->mutex_->lock();
            thread_state->num_remaining_tasks--;
        }
        thread_state->mutex_->unlock();
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    spinning = true;
    thread_state = new ThreadState(nullptr, 0);
    threads = new std::thread[num_threads];
    for(int i = 0; i < num_threads; i++){
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spinningTask, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->spinning = false;
    for (int i = 0; i < this->num_threads; i++) {
        this->threads[i].join();
    }
    delete thread_state;
    delete[] threads;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //


    thread_state->mutex_->lock();
    thread_state->runnable_ = runnable;
    thread_state->num_total_tasks_ = num_total_tasks;
    thread_state->num_remaining_tasks = num_total_tasks;
    num_remaining = num_total_tasks;
    thread_state->counter_ = -1;
    thread_state->mutex_->unlock();
    while(true){
        thread_state->mutex_->lock();
        if(thread_state->num_remaining_tasks <= 0) {
            thread_state->mutex_->unlock();
            break;
        }
        thread_state->mutex_->unlock();
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    spinning = true;
    threads = new std::thread[num_threads];
    cv_barrier = new std::condition_variable();
    mutex_at_sync = new std::mutex();
    mutex_shared_task = new std::mutex();
    mutex_working = new std::mutex();
    mutex_waiting = new std::mutex();
    mutex_removing = new std::mutex();
    mutex_barrier = new std::mutex();

    cv_at_sync = new std::condition_variable();
    mutex_map = new std::mutex();
    vecTask = {};
    set_removed_ID = {};
    v_working_ID = {};
    map_indep_to_dep = {};
    isSyncRun = true;

    num_finished_threads = 0;
    num_finished_threads_wait = 0;
    for(int i = 0; i < num_threads; i++){
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::workers, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->spinning = false;
    for (int i = 0; i < this->num_threads; i++) {
        vecTask[vecTask.size()-1]->cv_barrier->notify_all();
        this->threads[i].join();
    }

    for(int i = 0; i < vecTask.size(); i++){
        free(vecTask[i]);
    }
    vecTask.clear();

    delete mutex_at_sync;
    delete cv_at_sync;
    delete cv_barrier;
    delete mutex_working;
    delete mutex_waiting;
    delete mutex_map;
    delete mutex_shared_task;
    delete mutex_barrier;
    delete[] threads;
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // Define task
    mutex_shared_task->lock();
    isSyncRun = true;
    int taskID_current = vecTask.size();
    vecTask.push_back(new TaskState(runnable, num_total_tasks, taskID_current, {}, num_threads));
    mutex_shared_task->unlock();

    // Insert task into working list
    mutex_working->lock();
    v_working_ID.push_back(taskID_current);
    mutex_working->unlock();

    // Wait until the current job is finished.
    std::unique_lock<std::mutex> lk(*mutex_at_sync);
    isAtSyncStatus = true; // indicating the main thread is at wait of sync function.
    cv_at_sync->wait(lk, [this]{return v_working_ID.empty() && set_waiting_ID.empty();});
    lk.unlock();

    return;
}

void TaskSystemParallelThreadPoolSleeping::block(){
    std::unique_lock<std::mutex> lk(*mutex_barrier);
    ++num_finished_threads;
    ++num_finished_threads_wait;
    cv_barrier->wait(lk, [&]{return (num_finished_threads >= num_threads);}); // released when all threads become waiting
    cv_barrier->notify_one();
    --num_finished_threads_wait;
    if(num_finished_threads_wait == 0)
    {
       //reset barrier
       num_finished_threads = 0;
    }
    lk.unlock();
};

void TaskSystemParallelThreadPoolSleeping::workers(){
    bool isSyncRun_ = isSyncRun; // Indicating if the current run is syncrho
    block(); // Start all threads together
    while(spinning){
        mutex_working->lock();
        isSyncRun_ = isSyncRun;// Indicating if the current run is syncrho
        bool isEmpty = v_working_ID.empty();
        if(!isEmpty){ // if tasks are in working list, run the task.
            // 1. Extract task data
            TaskID taskID_current = v_working_ID.front();
            mutex_working->unlock();
            mutex_shared_task->lock();
            task_current = vecTask[taskID_current];
            mutex_shared_task->unlock();
            int iTask = 0;

            // 2. Run task
            while(spinning){
                task_current->mutex_count->lock();
                iTask = ++task_current->counter;
                int nTotTask = task_current->num_total_tasks;
                task_current->mutex_count->unlock();
                if(iTask < nTotTask){
                    task_current->runnable->runTask(iTask, nTotTask);
                } else{
                    task_current->block(); // if total task is finished, wait for all threads reaching here.
                    break;
                }
            }

            // 3. Remove the finished task from the working list
            mutex_working->lock();
            bool isNotDeleted = (!v_working_ID.empty() && v_working_ID[0] == taskID_current); // check if it is not deleted
            mutex_working->unlock();
            if(isNotDeleted){
                mutex_working->lock();
                v_working_ID.erase(v_working_ID.begin());
                mutex_working->unlock();

                mutex_removing->lock();
                set_removed_ID.insert(taskID_current);
                mutex_removing->unlock();
            }
        } else{ // if no task, check if main thread is on sync function
            mutex_working->unlock();

            // Check if it is prepared for exiting sync function
            mutex_waiting->lock();
            bool isSyncPrepare = isAtSyncStatus && set_waiting_ID.empty();
            mutex_waiting->unlock();

            if(isSyncPrepare){ // at synchronization
                cv_at_sync->notify_all(); // release wait at sync function
                if(!isSyncRun_) break; // for asyncrho, finish this worker
            }
        }
    }
    block(); // waiting for all threads finishing together for the next run.
}


TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
