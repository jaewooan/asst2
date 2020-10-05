#include "tasksys.h"
#include <condition_variable>
#include <mutex>
#include <thread>

#include <stdio.h>

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
    thread_state = new ThreadState(nullptr, 0);
    num_idle_init = num_threads - 1;
    threads = new std::thread[num_threads];
    cv_thread = std::vector<std::condition_variable*>(num_threads);
    mutex_main = new std::mutex();
    mutex_thread_share = new std::mutex();
    mutex_thread = std::vector<std::mutex*>(num_threads);
    for(int i=0; i < num_threads; i++){
        isWait.push_back(true);
        isInterateDone.push_back(true);
        num_finished_tasks_threads.push_back(0);
        num_idle_threads.push_back(0);
        cv_thread[i] = new std::condition_variable();
        mutex_thread[i] = new std::mutex();
    }

    cv_thread_tot = new std::condition_variable();
    cv_main = new std::condition_variable();
    mutex_thread_tot = new std::mutex();

    for(int i = 0; i < num_threads; i++){
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::waitTask, this, i);
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
    //printf("Deletion\n");
    for (int i = 0; i < this->num_threads; i++) {
        thread_state->mutex_->lock();
        thread_state->counter_ = -1;
        isInterateDone[i] = false;
        cv_thread_tot->notify_all(); // release wait
        thread_state->mutex_->unlock();
        this->threads[i].join();
    }
    delete thread_state;
    delete[] threads;
}


void TaskSystemParallelThreadPoolSleeping::waitTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    int nFinishTask = 0;
    int nIdleLocal = 0;

    while(spinning){
        // Stop before initialization(When initialization is finished, isInitialized is true, when calculation is finished, this becomes false at cv_main->wait)

        //printf("100000. wait reached %d \n", iThread);
        std::unique_lock<std::mutex> lk(*mutex_thread_tot);
        isWait[iThread] = true;
        cv_thread_tot->wait(lk, [&]{ return (!isInterateDone[iThread] && (num_idle_threads[iThread]==0)) || !spinning;});
        num_idle_threads[iThread] = 0;
        num_finished_tasks_threads[iThread] = 0;
        isWait[iThread] = false;
        lk.unlock();

        nFinishTask = 0;
        nIdleLocal = 0;
        //printf("6. initializng worker %d with num_idle_init %d\n", iThread, num_idle_init);
        while(spinning){ // run simulation
            mutex_thread_share->lock();
            iTask = ++thread_state->counter_;
            nTotTask = thread_state->num_total_tasks_;
            mutex_thread_share->unlock();
            if(iTask < nTotTask){
                thread_state->runnable_->runTask(iTask, nTotTask);
                nFinishTask++;
            }
            else{
                mutex_thread[iThread]->lock();
                num_idle_threads[iThread] = ++nIdleLocal;
                num_finished_tasks_threads[iThread] = nFinishTask;
                isInterateDone[iThread] = true;
                //printf("7. finish worker %d with finished task %d and idles %d\n", iThread, num_finished_tasks_threads[iThread], num_idle_threads[iThread]);
                mutex_thread[iThread]->unlock();
                cv_main->notify_all();
                break;
            }
        } // finish simulation for the given case
    }
    //printf("delete %d thread\n", iThread);

}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    //printf("/////////////////////////////////////////////////////////////\n");
    mutex_thread_share->lock();
    thread_state->runnable_ = runnable;
    thread_state->num_total_tasks_ = num_total_tasks;
    thread_state->counter_ = -1;
    num_finished_tasks = 0;
    num_idle_init = 0;
    for(int i=0; i<num_threads; i++){
        num_finished_tasks_threads[i] = 0;
        num_idle_threads[i] = 0;
    }
    mutex_thread_share->unlock();

    //printf("1. Notifying initialization\n");
    while(true){
        bool isAllWait = true;
        for(int i = 0; i < num_threads; i++){
            mutex_thread_tot->lock();
            isAllWait = isAllWait && isWait[i];
            mutex_thread_tot->unlock();
            if(!isAllWait) break;
        }
        if(isAllWait){
            for(int i = 0; i < num_threads; i++){
                mutex_thread_tot->lock();
                isInterateDone[i] = false;
                mutex_thread_tot->unlock();
            }
            //printf("2. releasing threads \n");
            cv_thread_tot->notify_all();
            break;
        }
    }

    mutex_thread_tot->lock();
    bool isRun = (thread_state->counter_ < thread_state->num_total_tasks_ / num_threads);
    mutex_thread_tot->unlock();
    if(isRun){
        //printf("3. wait in \n");
        std::unique_lock<std::mutex> lk(*mutex_main);
        cv_main->wait(lk);
        lk.unlock();
        //printf("3. wait unitl work is done\n");
    }

    //printf("4. waiting operation\n");
    while(true){
        bool isAllWait = true;
        num_finished_tasks = 0;
        num_idle_init = 0;
        for(int i=0; i<num_threads; i++){
            num_idle_init += num_idle_threads[i];
            num_finished_tasks += num_finished_tasks_threads[i];
            isAllWait = isAllWait && isWait[i];
            if(!isAllWait) break;
        }
        bool isFinished = isAllWait && (num_idle_init == num_threads) && (num_finished_tasks == thread_state->num_total_tasks_);
        if(isFinished) {
            //printf("5. ALl are stop again\n");
            break;
        }
    }
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
