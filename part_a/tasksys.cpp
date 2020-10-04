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

/*void TaskSystemParallelThreadPoolSleeping::waitTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    std::unique_lock<std::mutex> lk(*mutex_thread[iThread]);

    thread_state->mutex_->lock();
    num_idle--;
    thread_state->mutex_->unlock();
    while(spinning){
        thread_state->mutex_->lock();
        iTask = ++thread_state->counter_;
        nTotTask = thread_state->num_total_tasks_;
        thread_state->mutex_->unlock();
        if(iTask < nTotTask){
            thread_state->runnable_->runTask(iTask, nTotTask);
            thread_state->mutex_->lock();
            nFinishedTasks++;
            thread_state->mutex_->unlock();
        }
        else{
            // if all threads are completed, notify the main run.
            thread_state->mutex_->lock();
            num_idle++;
            thread_state->mutex_->unlock();
            cv_main->notify_all();
            // then sleep
            printf("%d wait\n", iThread);
            cv_thread[iThread]->wait(lk, [this]{return thread_state->counter_ < thread_state->num_total_tasks_- 1;});
            printf("%d release\n", iThread);

            //thread_state->mutex_->lock();
            //num_idle--;
            //thread_state->mutex_->unlock();
        }
    }
}*/
void TaskSystemParallelThreadPoolSleeping::signalTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    std::unique_lock<std::mutex> lk(*mutex_thread[iThread]);
    printf("%d wait\n", iThread);
    cv_thread[iThread]->wait(lk);
    printf("%d wait off\n", iThread);
    while(spinning){
        cv_thread_tot->notify_all();
        thread_state->mutex_->lock();
        iTask = ++thread_state->counter_;
        nTotTask = thread_state->num_total_tasks_;
        thread_state->mutex_->unlock();

        if(iTask < nTotTask){
            printf("%d task doing task %d\n", iThread, iTask);
            thread_state->runnable_->runTask(iTask, nTotTask);
            thread_state->mutex_->lock();
            nFinishedTasks++;
            thread_state->mutex_->unlock();
        } else{
            printf("%d wait2\n", iThread);
            cv_thread[iThread]->wait(lk);
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::waitTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    std::unique_lock<std::mutex> lk(*mutex_thread_tot);
    printf("%d wait\n", iThread);
    cv_thread_tot->wait(lk);
    num_idle++;
    printf("%d wait off\n", iThread);
    while(spinning){
        if(num_idle == num_threads){
            thread_state->mutex_->lock();
            iTask = ++thread_state->counter_;
            nTotTask = thread_state->num_total_tasks_;
            thread_state->mutex_->unlock();
            if(iTask < nTotTask){
                printf("%d task doing task %d\n", iThread, iTask);
                thread_state->runnable_->runTask(iTask, nTotTask);
                thread_state->mutex_->lock();
                nFinishedTasks++;
                thread_state->mutex_->unlock();
            } else{
                printf("%d wait2\n", iThread);
                cv_thread_tot->wait(lk);
            }
        } else{
            cv_thread_tot->notify_all();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // make all threads wait before starting assignment
    thread_state->mutex_->lock();
    thread_state->runnable_ = runnable;
    thread_state->num_total_tasks_ = num_total_tasks;
    thread_state->num_remaining_tasks = num_total_tasks;
    thread_state->counter_ = -1;
    nFinishedTasks = 0;
    num_idle2 = 0;
    thread_state->mutex_->unlock();

    std::unique_lock<std::mutex> lk(*mutex_thread_tot);
    //cv_thread_tot->notify_all();


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
    num_idle = 0;
    nFinishedTasks = 0;
    threads = new std::thread[num_threads];
    cv_main = new std::condition_variable();
    cv_thread_share = new std::condition_variable();
    cv_thread_tot = new std::condition_variable();
    cv_thread = std::vector<std::condition_variable*>(num_threads);
    mutex_main = new std::mutex();
    mutex_thread_share = new std::mutex();
    mutex_thread_tot = new std::mutex();
    mutex_thread = std::vector<std::mutex*>(num_threads);
    for(int i=0; i < num_threads; i++){
        cv_thread[i] = new std::condition_variable();
        mutex_thread[i] = new std::mutex();
    }

    //this->threads[0] = std::thread(&TaskSystemParallelThreadPoolSleeping::signalTask, this, 0);
    for(int i = 0; i < num_threads; i++){
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::waitTask, this, i);
    }
}

/*
void TaskSystemParallelThreadPoolSleeping::waitTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    isFullyIdle = false;
    std::unique_lock<std::mutex> lk(*mutex_thread[iThread]);
    while(spinning){
        thread_state->mutex_->lock();
        if(num_idle < num_threads && thread_state->num_total_tasks_ > 0){

            printf("%d other if\n", iThread);
            iTask = ++thread_state->counter_;
            nTotTask = thread_state->num_total_tasks_;
            thread_state->mutex_->unlock();

            if(iTask < nTotTask){
                //cv_thread[iThread]->wait(lk, []{return false;});
                printf("%d task doing task %d\n", iThread, iTask);
                thread_state->runnable_->runTask(iTask, nTotTask);
                thread_state->mutex_->lock();
                nFinishedTasks++;
                thread_state->mutex_->unlock();
            }
            else{
                // if all threads are completed, notify the main run.
                //thread_state->mutex_->lock();
                //num_idle++;
                //thread_state->mutex_->unlock();
                cv_main->notify_all();
                // then sleep
                thread_state->mutex_->lock();
                printf("%d wait with idel num  %d and task %d\n", iThread, num_idle, thread_state->num_total_tasks_);
                thread_state->mutex_->unlock();
                cv_thread[iThread]->wait(lk, [this]{
                    thread_state->mutex_->lock();
                    if(num_idle < num_threads && !isFullyIdle){
                        num_idle++;
                        printf("thread increase inside wait current num_idle %d with total task %d\n", num_idle, thread_state->num_total_tasks_);
                    } else{
                        num_idle--;
                        printf("release inside wait with num_idle %d \n", num_idle);
                    }

                    if(num_idle == num_threads && !isFullyIdle){
                        cv_thread_tot->notify_all();
                        isFullyIdle = true;
                    }

                    thread_state->mutex_->unlock();
                    return isFullyIdle && (num_idle < num_threads);});//thread_state->counter_ < thread_state->num_total_tasks_- 1;});
            }
        } else{
            thread_state->mutex_->unlock();
            //original
            cv_thread[iThread]->wait(lk, [this]{
                //thread_state->mutex_->lock();
                if(num_idle < num_threads && !isFullyIdle){
                    num_idle++;
                    //printf("thread increase inside wait current num_idle %d with total task %d\n", num_idle, thread_state->num_total_tasks_);
                } else{
                    num_idle--;
                    //printf("release inside wait with num_idle %d \n", num_idle);
                }

                if(num_idle == num_threads && !isFullyIdle){
                    // if 8 threads all stopped, releaset tot->wait.
                    while(true){
                        if(on_thread_tot_wait){
                            cv_thread_tot->notify_all();
                            isFullyIdle = true;
                            on_thread_tot_wait = false;
                            break;
                        }
                    }
                }
                //thread_state->mutex_->unlock();
                return isFullyIdle && (num_idle < num_threads);});
            // original end


            //cv_thread[iThread]->wait(lk, [this]{
                //thread_state->mutex_->lock();

               // if(num_idle < num_threads && !isFullyIdle){
                    //printf("thread increase inside wait current num_idle %d with total task %d\n", num_idle, thread_state->num_total_tasks_);
                //}

            num_idle++;
            if(num_idle == num_threads && !isFullyIdle){
                printf("%d in 8\n", iThread);
                // if 8 threads all stopped, releaset tot->wait.
                isFullyIdle = true;
                while(true){
                    if(on_thread_tot_wait){
                        printf("thread wait is okay \n");
                    }
                    if(isFullyIdle && on_thread_tot_wait){
                        cv_thread_tot->notify_all();
                        break;
                    }
                }
             }
             mutex_thread_share->unlock();
            //    return isFullyIdle;});

            //on_thread_tot_wait = false;
            //num_idle = 0;
            std::unique_lock<std::mutex> lk2(*mutex_thread_share);

            printf("%d out\n", iThread);
            cv_thread_share->wait(lk2, [this]{
                on_thread_share_wait = true;
                num_idle2++;
                return num_idle2 == num_threads;});
            printf("%d out2\n", iThread);
            on_thread_share_wait = false;
            on_thread_tot_wait = false;
                        num_idle = 0;
            //printf("%d out\n", iThread);

        }
    }
}*/
/*
void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // make all threads wait before starting assignment
    std::unique_lock<std::mutex> lk2(*mutex_thread_tot);
   // printf("thread_tot start \n");
    on_thread_tot_wait = true;
    cv_thread_tot->wait(lk2, [this]{
        printf("wait is changed\n");
        return isFullyIdle;});

    thread_state->mutex_->lock();
    thread_state->runnable_ = runnable;
    thread_state->num_total_tasks_ = num_total_tasks;
    thread_state->num_remaining_tasks = num_total_tasks;
    thread_state->counter_ = -1;
    nFinishedTasks = 0;
    num_idle2 = 0;
    thread_state->mutex_->unlock();


    //printf("start releasing with mum idle %d \n", num_idle);
    for(int i = 0; i < num_threads; i++){
        cv_thread[i]->notify_all();
    }
    //printf("finish releasing with mum idle %d \n", num_idle);

    std::unique_lock<std::mutex> lk(*mutex_main);
    printf("before\n");
    cv_main->wait(lk, [this]{return (nFinishedTasks == thread_state->num_total_tasks_);});
    printf("after\n");
}*/

/*
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
    num_idle = 0;
    nFinishedTasks = 0;
    threads = new std::thread[num_threads];
    cv_main = new std::condition_variable();
    cv_thread_share = new std::condition_variable();
    cv_thread_tot = new std::condition_variable();
    cv_thread = std::vector<std::condition_variable*>(num_threads);
    mutex_main = new std::mutex();
    mutex_thread_share = new std::mutex();
    mutex_thread_tot = new std::mutex();
    mutex_thread = std::vector<std::mutex*>(num_threads);
    for(int i=0; i < num_threads; i++){
        cv_thread[i] = new std::condition_variable();
        mutex_thread[i] = new std::mutex();
    }
    for(int i = 0; i < num_threads; i++){
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::waitTask, this, i);
    }
}*/

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->spinning = false;
    for (int i = 0; i < this->num_threads; i++) {
        thread_state->mutex_->lock();
        thread_state->counter_ = -1;
        cv_thread[i]->notify_all(); // release wait
        thread_state->mutex_->unlock();
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
