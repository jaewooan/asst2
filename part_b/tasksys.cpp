#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    num_idle = {0,0};
    num_idle2 = {0,0};
    nFinishedTasks = {0,0};
    threads = new std::thread[num_threads];
    cv_main = new std::condition_variable();
    cv_thread_share = new std::condition_variable();
    cv_thread_tot = new std::condition_variable();
    cv_thread = std::vector<std::condition_variable*>(num_threads);
    cv_thread_main = new std::condition_variable();
    mutex_main = new std::mutex();
    mutex_thread_main = new std::mutex();
    mutex_thread_share = new std::mutex();
    mutex_thread_tot = new std::mutex();
    mutex_thread = std::vector<std::mutex*>(num_threads);
    for(int i=0; i < num_threads; i++){
        cv_thread[i] = new std::condition_variable();
        mutex_thread[i] = new std::mutex();
    }

    for(int i = 1; i < num_threads; i++){
        wait_thread_init.insert(i);
    }
    for(int i=0; i<2; i++)
        wait_thread.push_back(wait_thread_init);

    isAllReleased = {false, false};

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


void TaskSystemParallelThreadPoolSleeping::signalTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    int iRunCurrent = 1;
    printf("wait in %d\n", iThread);
    while(true){
        mutex_thread_tot->lock();
        std::unordered_set<int> wait_list =  wait_thread_init;
        if(wait_list.size() == 0 || isAllReleasedInit){
            isAllReleasedInit = true;
            mutex_thread_tot->unlock();
            printf("wait out %d\n", iThread);
            break;
        }
        mutex_thread_tot->unlock();

        for(int i:wait_list){
            cv_thread[i]->notify_all();
        }
    }

    //cv_thread_tot->wait(lk, [this]{return num_idle == num_threads;});
    while(spinning){
        //if(num_idle == num_threads){
            mutex_thread_share->lock();
            iTask = ++thread_state->counter_;
            nTotTask = thread_state->num_total_tasks_;
            mutex_thread_share->unlock();

            if(iTask < nTotTask){
                thread_state->runnable_->runTask(iTask, nTotTask);
                mutex_thread_share->lock();
                nFinishedTasks[iRunCurrent % 2]++;
                printf("%d task doing task %d and finished %d\n", iThread, iTask, nFinishedTasks[iRunCurrent % 2]);
                mutex_thread_share->unlock();
            } else{
                mutex_thread_share->lock();
                num_idle[iRunCurrent % 2]++;
                mutex_thread_share->unlock();

                while(true){
                    mutex_thread_share->lock();
                    int iTask2 = thread_state->counter_;
                    int nTotTask2 = thread_state->num_total_tasks_;
                    mutex_thread_share->unlock();
                    if(iTask2 < nTotTask2){
                        break;
                    }
                    cv_main->notify_all();
                }

                while(true){
                    mutex_thread_tot->lock();
                    std::unordered_set<int> wait_list = wait_thread[iRunCurrent%2];
                    if(wait_list.size() == 0 && isAllReleased[iRunCurrent%2]){
                        printf("wait out %d with idle %d and finished task %d out of total task %d\n", iThread,
                               num_idle[iRunCurrent % 2], nFinishedTasks[iRunCurrent % 2], thread_state->num_total_tasks_);
                        isAllReleased[iRunCurrent%2] = true;
                        iRunCurrent = iRun;
                        mutex_thread_tot->unlock();
                        break;
                    }
                    mutex_thread_tot->unlock();

                    for(int i:wait_list){
                        cv_thread[i]->notify_all();
                    }

                }
                printf("wait out %d\n", iThread);
            }
    }
}

void TaskSystemParallelThreadPoolSleeping::waitTask(int iThread){
    int iTask = 0;
    int nTotTask = 0;
    int iRunCurrent = 1;
    std::unique_lock<std::mutex> lk(*mutex_thread[iThread]);
    printf("wait in %d\n", iThread);
    num_idle_init++;
    if(num_idle_init == num_threads - 1){
        isAllWait = true;
    }
    cv_thread[iThread]->wait(lk, [this]{return (isAllWait && thread_state->num_total_tasks_ > 0);});
    lk.unlock();

    mutex_thread_share->lock();
    wait_thread_init.erase(iThread);
    mutex_thread_share->unlock();

    while(true){
        mutex_thread_tot->lock();
        std::unordered_set<int> wait_list = wait_thread_init;
        if(wait_list.size() == 0 || isAllReleasedInit){
            isAllReleasedInit = true;
            mutex_thread_tot->unlock();
            break;
        }
        mutex_thread_tot->unlock();
        for(int i:wait_list){
            cv_thread[i]->notify_all();
        }
    }

    printf("wait out %d \n", iThread);
    //cv_thread_tot->wait(lk, [this]{return num_idle == num_threads;});
    while(spinning){
        //if(num_idle == num_threads){
            mutex_thread_share->lock();
            iTask = ++thread_state->counter_;
            nTotTask = thread_state->num_total_tasks_;
            mutex_thread_share->unlock();

            if(iTask < nTotTask){
                thread_state->runnable_->runTask(iTask, nTotTask);
                mutex_thread_share->lock();
                nFinishedTasks[iRunCurrent%2]++;
                printf("%d task doing task %d and finished %d with iRun %d\n", iThread, iTask, nFinishedTasks[iRunCurrent%2], iRunCurrent);
                mutex_thread_share->unlock();
            } else{
                mutex_thread_share->lock();
                num_idle[iRunCurrent%2]++;
                printf("wait out %d with idle %d before with waitlist size %d with irun %d\n",
                       iThread, num_idle[iRunCurrent%2], wait_thread[iRunCurrent%2].size(), iRunCurrent);
                mutex_thread_share->unlock();
                std::unique_lock<std::mutex> lk2(*mutex_thread[iThread]);
                cv_thread[iThread]->wait(lk2, [&]{ return (num_idle[iRunCurrent%2] == num_threads && isReady[(iRunCurrent + 1) % 2]);});
                lk2.unlock();

                mutex_thread_share->lock();
                wait_thread[iRunCurrent%2].erase(iThread);
                num_idle2[iRunCurrent%2]++;
                mutex_thread_share->unlock();

                printf("wait out %d with idle %d and new irun000 %d\n", iThread, num_idle[iRunCurrent%2], iRunCurrent);
                while(true){
                    mutex_thread_tot->lock();
                    std::unordered_set<int> wait_list = wait_thread[iRunCurrent%2];
                    if(wait_list.size() == 0 || isAllReleased[iRunCurrent%2]){
                        isAllReleased[iRunCurrent%2] = true;
                        isReady[(iRunCurrent + 1) % 2] = false;
                        iRunCurrent = iRun;
                        printf("wait out %d with idle %d and new irun %d\n", iThread, num_idle[iRunCurrent%2], iRunCurrent);
                        mutex_thread_tot->unlock();
                        break;
                    }
                    mutex_thread_tot->unlock();

                    for(int i:wait_list){
                        cv_thread[i]->notify_all();
                    }
                }
            }
        //}
    }
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // make all threads wait before starting assignment
    mutex_thread_share->lock();
    iRun++;
    thread_state->runnable_ = runnable;
    thread_state->num_total_tasks_ = num_total_tasks;
    thread_state->num_remaining_tasks = num_total_tasks;
    thread_state->counter_ = -1;
    printf("start with irun %d\n", iRun);
    nFinishedTasks[iRun%2] = 0;
    isReady[iRun%2] = true;
    wait_thread[iRun%2].clear();
    for(int i=1; i<num_threads; i++){
        wait_thread[iRun%2].insert(i);
    }
    isAllReleased[iRun%2] = true;
    mutex_thread_share->unlock();

    //cv_thread_tot->notify_all();
    //

    //for(int i=0; i<num_threads; i++){
    //    cv_thread_tot->notify_all();
    //}

    printf("before\n");
    //std::unique_lock<std::mutex> lk(*mutex_main);



    std::unique_lock<std::mutex> lk(*mutex_main);
    if(num_threads > 1){
        cv_main->wait(lk, [this]{return (nFinishedTasks[iRun%2] == thread_state->num_total_tasks_);});
    } else{
        cv_main->wait(lk);
    }
    lk.unlock();
    printf("after\n");
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
