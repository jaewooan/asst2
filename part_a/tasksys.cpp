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


void TaskSystemParallelThreadPoolSleeping::signalTask(){
    while(spinning){
        thread_state->mutex_->lock();
        if(thread_state->num_remaining_tasks > 0){
            //printf("sign: 2 numReamin, numthread: %d %d \n", thread_state->num_remaining_tasks, num_threads);
            thread_state->condition_variable_->notify_all();
        }
        else{
            //printf("sign: 3 numReamin, numthread: %d %d \n", thread_state->num_remaining_tasks, num_threads);
            cv_main->notify_one();
        }
        thread_state->mutex_->unlock();
    }
}

void TaskSystemParallelThreadPoolSleeping::waitTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    std::unique_lock<std::mutex> lk(*mutex_thread[iThread]);

    printf("num_idle in 1: %d \n", thread_state->num_idle);
    thread_state->mutex_->lock();
    thread_state->num_idle--;
    printf("num_idle in 12: %d \n", thread_state->num_idle);
    thread_state->mutex_->unlock();
    //std::unique_lock<std::mutex> lk(*mutex_threads);
    while(spinning){
        thread_state->mutex_->lock();
        iTask = ++thread_state->counter_;
        printf("%d run starts : numReamin, itask: %d %d\n", iThread, thread_state->num_remaining_tasks, iTask);
        nTotTask = thread_state->num_total_tasks_;
        thread_state->mutex_->unlock();
        if(iTask < nTotTask){
            thread_state->runnable_->runTask(iTask, nTotTask);
            thread_state->mutex_->lock();
            nFinishedTasks++;
            printf("%d run ends: numReamin, itask: %d %d\n", iThread, thread_state->num_remaining_tasks, iTask);
            thread_state->mutex_->unlock();
        }
        else{
            // if all threads are completed, notify the main run.
            thread_state->mutex_->lock();
            thread_state->num_idle++;
            printf("num_finished, num_tot: %d %d\n", nFinishedTasks, thread_state->num_total_tasks_);
            thread_state->mutex_->unlock();
            cv_main->notify_all();
            // then sleep
            //printf("waited %d\n", iThread);
            thread_state->mutex_->lock();
            thread_state->num_idle--;
            printf("num_idle in 3: %d \n", thread_state->num_idle);
            printf("counter in 4: %d \n", thread_state->counter_);
            thread_state->mutex_->unlock();
            cv_thread[iThread]->wait(lk, [this]{return thread_state->counter_ < thread_state->num_total_tasks_- 1;});
            printf("release %d \n", iThread);


            //cv_threads->wait(lk, [=]{return (thread_state->num_idle == 0);});

        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    //std::unique_lock<std::mutex> lk(*mutex_main);
    //cv_main->wait(lk);
    thread_state->mutex_->lock();
    printf("run allocate starts \n");
    thread_state->runnable_ = runnable;
    thread_state->num_total_tasks_ = num_total_tasks;
    thread_state->num_remaining_tasks = num_total_tasks;
    thread_state->counter_ = -1;
    nFinishedTasks = 0;
    printf("run allocate ends \n");
    thread_state->mutex_->unlock();


    for(int i = 0; i < num_threads; i++){
        cv_thread[i]->notify_all();
    }
    /*while(true){
        mutex_main->lock();
        printf("num_idle in while: %d\n", thread_state->num_idle);
        if(thread_state->num_idle > 0){
            isIdle = true;
            mutex_main->unlock();
        }
        if(thread_state->num_idle == 0){
            isIdle = false;
            mutex_main->unlock();
            break;
        }
    }*/
    //do{
    //    cv_threads->notify_all();
    //} while(thread_state->num_idle > 0);
    /*do{
        for(int i = 0; i < num_threads; i++){
            cv_thread[i]->notify_all();
        }
    } while(thread_state->num_idle > 0);*/
    //thread_state->condition_variable_->notify_all();

    std::unique_lock<std::mutex> lk(*mutex_main);
    printf("before\n");
    cv_main->wait(lk, [this]{return (nFinishedTasks == thread_state->num_total_tasks_);});
    printf("after\n");
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
    thread_state->num_idle = num_threads;
    isIdle = false;
    num_Idle = 0;
    nFinishedTasks = 0;
    threads = new std::thread[num_threads];
    cv_main = new std::condition_variable();
    cv_threads = new std::condition_variable();
    cv_thread = std::vector<std::condition_variable*>(num_threads);
    mutex_main = new std::mutex();
    mutex_threads = new std::mutex();
    mutex_thread = std::vector<std::mutex*>(num_threads);
    for(int i=0; i < num_threads; i++){
        cv_thread[i] = new std::condition_variable();
        mutex_thread[i] = new std::mutex();
    }

    //this->threads[0] = std::thread(&TaskSystemParallelThreadPoolSleeping::signalTask, this);
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

    /*while(thread_state->num_idle > 0){
        //for(int i = 0; i < num_threads; i++){
        //    cv_thread[i]->notify_all();
        //}
        thread_state->condition_variable_->notify_all();
    }*/
    do{
        for(int i = 0; i < num_threads; i++){
            cv_thread[i]->notify_all();
        }
    } while(thread_state->num_idle > 0);

    for (int i = 0; i < this->num_threads; i++) {
        this->threads[i].join();
    }
    delete thread_state;
    delete[] threads;
}


TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
