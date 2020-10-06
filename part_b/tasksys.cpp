#include "tasksys.h"
#include <queue>
#include <map>
#include <algorithm>    // std::find
#include <iostream>

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
    vecTask = {};

    for(int i = 0; i < num_threads; i++){
        //this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::waitTask, this, i);
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runFunction, this, i);
    }
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
            std::unique_lock<std::mutex> lk(*mutex_main);
            cv_thread_tot->notify_all();
            cv_main->wait(lk);
            lk.unlock();
            //printf("3. wait unitl work is done\n");
            break;
        }
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

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->spinning = false;
    printf("Deletion\n");
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

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //
    // run and make resutls

    // set current task's id: maybe the maximum number of existing ids.
    printf("/////////////////////////////////////////////\n");
    int taskID_local = vecTask.size();

    // Define task
    printf("1. Define task %d with total task %d\n", taskID_local, num_total_tasks);
    vecTask.push_back(new TaskState(runnable, num_total_tasks, taskID_local, deps));

    // save this id into waiting queue if deps is not vacant and save map from this id to deps. qWaiting, map_dependency

    // save maps between dependent tasks
    printf("2. Save Maps\n");
    for(TaskID dep:deps){
        mutex_thread_share->lock();
        if(map_indep_to_dep.find(dep) != map_indep_to_dep.end()){
            map_indep_to_dep[dep].push_back(taskID_local);
        } else{
            map_indep_to_dep.insert(std::pair<TaskID, std::vector<TaskID>>(dep, {taskID_local}));
        }
        mutex_thread_share->unlock();
        std::cout << "The " << dep << " task is affection on following tasks: " ;
        printvector(map_indep_to_dep[dep]);


        mutex_thread_share->lock();
        bool isExist = set_removed_ID.find(dep) != set_removed_ID.end();
        std::vector<TaskID> vecDependent = map_indep_to_dep[dep];
        mutex_thread_share->unlock();
        // if current dependenct task is already implemented, move the current task into working groups.
        if(isExist){
            for(TaskID taskID:vecDependent){
                vecTask[taskID]->mutex->lock();
                std::vector<TaskID>::iterator it  = std::find(vecTask[taskID]->vecDependentOn.begin(), vecTask[taskID]->vecDependentOn.end(), dep);
                if(it != vecTask[taskID]->vecDependentOn.end())
                    vecTask[taskID]->vecDependentOn.erase(it);
                vecTask[taskID]->mutex->unlock();
            }
        }
    }

    printf("3. Save Queues\n");
    if(vecTask[taskID_local]->vecDependentOn.empty()){
        q_working_ID.push(taskID_local);
    } else{
        set_waiting_ID.insert(taskID_local);
    }
    printqueue(q_working_ID);
    printset(set_waiting_ID);

    cv_thread_tot->notify_all();

    return taskID_local;
}

void TaskSystemParallelThreadPoolSleeping::runFunction(int iThread){
    // Start when the first input is coming
    std::unique_lock<std::mutex> lk(*mutex_thread_tot);
    printf("Wait on running %d the first task\n", iThread);
    cv_thread_tot->wait(lk, [this]{return !q_working_ID.empty();});
    printf("Wait release on running the first task with %d\n", iThread);
    lk.unlock();

    while(spinning){
        // If working queue is not empty, run task
        if(!q_working_ID.empty()){
            TaskID taskID_local = q_working_ID.front();
            int iTask = 0;
            int nTotTask = 0;
            int nFinishTask = 0;

            // Run
            printf("4. Run simulation task %d with %d\n", taskID_local, iThread);
            while(spinning){
                vecTask[taskID_local]->mutex->lock();
                iTask = ++vecTask[taskID_local]->counter;
                nTotTask = vecTask[taskID_local]->num_total_tasks;
                vecTask[taskID_local]->mutex->unlock();
                if(iTask < nTotTask){
                    vecTask[taskID_local]->runnable->runTask(iTask, nTotTask);
                    nFinishTask++;
                    printf("4. Running simulation %d with %d\n", iThread, nFinishTask);
                } else{
                    vecTask[taskID_local]->nFinishedThread++;
                    printf("4. Fnish simulation %d with %d\n", iThread, vecTask[taskID_local]->num_total_tasks);
                    break;
                }
            }

            // Remove the current task from the working queue
            printf("5. Remove current ID from Queue %d\n", iThread);
            //if(vecTask[taskID_local]->nFinishedThread == num_threads){
            mutex_thread_share->lock();
            q_working_ID.pop();
            set_removed_ID.insert(taskID_local);
            bool isExist = map_indep_to_dep.find(taskID_local) != map_indep_to_dep.end();
            std::vector<TaskID> vecDependent = map_indep_to_dep[taskID_local];
            mutex_thread_share->unlock();
            printf("5-1. Removed current ID %d with size %d\n", iThread, q_working_ID.size());

            // Remove dependency from others
            if(isExist){
                for(TaskID IDtask:vecDependent){
                    vecTask[IDtask]->mutex->lock();

                    printf("in1\n");
                    std::vector<TaskID>::iterator it  = std::find(vecTask[IDtask]->vecDependentOn.begin(), vecTask[IDtask]->vecDependentOn.end(), taskID_local);
                    printf("in2\n");
                    if(it!= vecTask[IDtask]->vecDependentOn.end())
                        vecTask[IDtask]->vecDependentOn.erase(it);
                    printf("in3\n");
                    if(vecTask[IDtask] -> vecDependentOn.empty()){
                        printf("in4\n");
                        set_waiting_ID.erase(IDtask);
                        printf("in5\n");
                        q_working_ID.push(IDtask);
                        printf("in6\n");
                    }
                    vecTask[IDtask]->mutex->unlock();
                    printf("in7\n");
                }
            }
            printf("6. Reorgnaize map %d with working ID size %d and %d\n", iThread, q_working_ID.size(), set_waiting_ID.size());
            //}


            if(isAtSync && q_working_ID.empty()){ // at synchronization
                if(set_waiting_ID.size() != 0){
                    printf(" remaining waiting ");
                    printset(set_waiting_ID);
                }
                printf("7. Release sync\n");
                cv_main->notify_all();
            }
        } else{
            std::unique_lock<std::mutex> lk2(*mutex_thread_tot);
            printf("Wait on running %d with size %d\n", iThread);
            cv_thread_tot->wait(lk2, [this]{return !q_working_ID.empty() || !spinning;});
            printf("Wait release on running %d\n", iThread);
            lk2.unlock();
        }
    }
    std::cout << "run func out at thread " << iThread << std::endl;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    std::unique_lock<std::mutex> lk(*mutex_main);
    printf("7. At sync\n");
    isAtSync = true;
    cv_thread_tot->notify_all();
    cv_main->wait(lk, [this]{return q_working_ID.empty() && set_waiting_ID.empty();});
    lk.unlock();
    printf("7. At sync, it is released\n");

    return;
}



