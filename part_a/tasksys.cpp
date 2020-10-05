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
    cv_main1 = new std::condition_variable();
    cv_main2 = new std::condition_variable();
    cv_thread = std::vector<std::condition_variable*>(num_threads);
    cv_signal = new std::condition_variable();
    mutex_main1 = new std::mutex();
    mutex_main2 = new std::mutex();
    mutex_thread_share = new std::mutex();
    mutex_signal = new std::mutex();
    mutex_thread = std::vector<std::mutex*>(num_threads);
    for(int i=0; i < num_threads; i++){
        isWait.push_back(true);
        isInterateDone.push_back(false);
        num_finished_tasks_threads.push_back(0);
        num_idle_threads.push_back(0);
        cv_thread[i] = new std::condition_variable();
        mutex_thread[i] = new std::mutex();
    }

    cv_thread_tot = new std::condition_variable();
    mutex_thread_tot = new std::mutex();
    /*
    num_idle = {0,0};
    num_idle2 = {0,0};
    nFinishedTasks = {0,0};
    cv_thread_share = new std::condition_variable();
    cv_thread_main = new std::condition_variable();
    mutex_thread_main = new std::mutex();
    */
    //for(int i = 1; i < num_threads; i++){
    //    wait_thread_init.insert(i);
    //}
    //for(int i=0; i<2; i++)
    //    wait_thread.push_back(wait_thread_init);

    //isAllReleased = {false, false};

    this->threads[0] = std::thread(&TaskSystemParallelThreadPoolSleeping::signalTask, this, 0);
    for(int i = 1; i < num_threads; i++){
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
        isInitialized = true;
        isInterateDone[i] = false;
        cv_thread[i]->notify_all(); // release wait
        thread_state->mutex_->unlock();
        this->threads[i].join();
    }
    delete thread_state;
    delete[] threads;
}


void TaskSystemParallelThreadPoolSleeping::signalTask(int iThread){
    //int iTask = 0;
    //int nTotTask = 0;
    //int iRunCurrent = 1;
    while(spinning){
        // Start run
        //printf("1. Notifying initialization\n");
        while(true){
            mutex_thread_share->lock();
            bool isInitialized_ = isInitialized;
            mutex_thread_share->unlock();
            if(isInitialized_){
                break;
            }
            cv_main1->notify_all();
        }
        // end initialization (right before the second main wait)

        // Start each working thread
        //printf("5. Notifying workers\n");
       /* for(int i = 1; i < num_threads; i++){
            while(true){
                mutex_thread[i]->lock();
                bool isWait_ = isWait[i] && (num_idle_threads[i] == 0);
                mutex_thread[i]->unlock();
                if(!isWait_) break;
                isInterateDone[i] = false;
                cv_thread[i]->notify_all();
            }
        }*/

        while(true){
            bool isAllWait = true;
            for(int i = 1; i < num_threads; i++){
                mutex_thread_tot->lock();
                isAllWait = isAllWait && isWait[i];
                isInterateDone[i] = false;
                mutex_thread_tot->unlock();
            }
            if(isAllWait){
                cv_thread_tot->notify_all();
                break;
            }
        }

        // wait until the working is finished
        //printf("5-1. Stop sIGNAL with %d %d\n", num_idle_init, num_finished_tasks);
        /*cv_signal->wait(lk, [this]{
            mutex_thread_share->lock();
            bool isSkip =  ((num_idle_init == num_threads - 1) || !spinning);
                //printf("inside signal: %d\n", num_idle_init);
                mutex_thread_share->unlock();
             return ((num_idle_init == num_threads - 1) || !spinning);});*/

        //std::unique_lock<std::mutex> lk(*mutex_signal);
        while(true){
            num_idle_init = 0;
            for(int i=0; i<num_threads; i++){
                num_idle_init += num_idle_threads[i];
            }
            //cv_signal->wait(lk, [this]{
            //    return !spinning || (num_idle_init == num_threads - 1);});
            if(!spinning || (num_idle_init == num_threads - 1)){
                //printf("5-1. Stop sIGNAL with %d %d\n", num_idle_init, num_finished_tasks);
                break;
            }
        }
        //lk.unlock();
        /*while(true){
            mutex_thread_share->lock();
            bool isEscape = ((num_idle_init == num_threads - 1) || !spinning);
            mutex_thread_share->unlock();
            if(isEscape) break;
        }*/
        //printf("8. Signal is Reactivated by workers. Activate run again\n");


        // Notify the second main
        while(true){
            mutex_thread_share->lock();
            bool isInitialized_ = isInitialized;
            mutex_thread_share->unlock();
            if(!isInitialized_ || !spinning) break;
            cv_main2->notify_all();
        }
    }

}


void TaskSystemParallelThreadPoolSleeping::waitTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    //int iRunCurrent = 1;

    while(spinning){
        // Stop before initialization(When initialization is finished, isInitialized is true, when calculation is finished, this becomes false at cv_main->wait)

        //printf("100000. wait reached %d \n", iThread);
        //std::unique_lock<std::mutex> lk(*mutex_thread[iThread]);
        std::unique_lock<std::mutex> lk(*mutex_thread_tot);
        isWait[iThread] = true;
        //cv_thread[iThread]->wait(lk, [&]{
            //bool isInterate = isInterateDone[iThread];
        //    return (isInitialized && !isInterateDone[iThread]) || !spinning;});
        cv_thread_tot->wait(lk, [&]{
            //bool isInterate = isInterateDone[iThread];
            return (isInitialized && !isInterateDone[iThread]) || !spinning;});
        isWait[iThread] = false;
        lk.unlock();

        //mutex_thread_share->lock();
        //num_idle_init = 0;
        //printf("6. initializng worker %d with num_idle_init %d\n", iThread, num_idle_init);
        //mutex_thread_share->unlock();


        while(spinning){ // run simulation
            mutex_thread_share->lock();
            iTask = ++thread_state->counter_;
            nTotTask = thread_state->num_total_tasks_;
            num_finished_tasks_threads[iThread]++;
            num_idle_threads[iThread] = 0;
            mutex_thread_share->unlock();
            if(iTask < nTotTask){
                thread_state->runnable_->runTask(iTask, nTotTask);
            }
            else{
                mutex_thread[iThread]->lock();
                num_idle_threads[iThread]++;
                num_finished_tasks_threads[iThread]--;
                //printf("7. finish worker %d with finished task %d and idles %d\n", iThread, num_finished_tasks_threads[iThread], num_idle_threads[iThread]);
                isInterateDone[iThread] = true;
                //num_finished_tasks += num_finished_tasks_threads[iThread];
                //num_idle_init += num_idle_threads[iThread];
                mutex_thread[iThread]->unlock();
                cv_signal->notify_all();
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

    // make all threads wait before starting assignment
    std::unique_lock<std::mutex> lk(*mutex_main1);
    //printf("2. start of run\n");
    cv_main1->wait(lk);
    //printf("3. start of initialization\n");
    lk.unlock();


    mutex_thread_share->lock();
    //iRun++;
    thread_state->runnable_ = runnable;
    thread_state->num_total_tasks_ = num_total_tasks;
    thread_state->counter_ = -1;
    num_finished_tasks = 0;
    num_idle_init = 0;
    //nFinishedTasks[iRun%2] = 0;
    //isReady[iRun%2] = true;
    //wait_thread[iRun%2].clear();
    for(int i=1; i<num_threads; i++){
        num_finished_tasks_threads[i] = 0;
        num_idle_threads[i] = 0;
        //wait_thread[iRun%2].insert(i);
    }
    //isAllReleased[iRun%2] = true;
    isInitialized = true;
    mutex_thread_share->unlock();

    std::unique_lock<std::mutex> lk2(*mutex_main2);
    //printf("4. end of initialization\n");
    if(num_threads > 1){
        cv_main2->wait(lk2, [this]{
            num_finished_tasks = 0;
            //num_idle_init = 0;
            for(int i=0; i<num_threads; i++){
                mutex_thread[i]->lock();
                num_finished_tasks += num_finished_tasks_threads[i];
                //num_idle_init  += num_idle_threads[i];
                mutex_thread[i]->unlock();
            }
            return (num_finished_tasks == thread_state->num_total_tasks_) || !spinning;});
    } else{
        cv_main2->wait(lk2);
    }
    //printf("9. end of run\n");
    lk2.unlock();

    mutex_thread_share->lock();
    isInitialized = false;
    mutex_thread_share->unlock();
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
