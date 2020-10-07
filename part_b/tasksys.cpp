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
    cv_barrier = new std::condition_variable();
    mutex_main = new std::mutex();
    mutex_thread_share = new std::mutex();
    mutex_thread = std::vector<std::mutex*>(num_threads);
    mutex_working = new std::mutex();
    mutex_waiting = new std::mutex();
    mutex_removing = new std::mutex();
    mutex_barrier = new std::mutex();
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
    set_removed_ID = {};
    q_working_ID = {};
    map_indep_to_dep = {};
    isSync = true;

    num_finished_threads = 0;
    num_finished_threads_wait = 0;
    for(int i = 0; i < num_threads; i++){
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runFunction, this, i);
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // Define task
    mutex_thread_share->lock();
    isSync = true;
    int taskID_local = vecTask.size();
    vecTask.push_back(new TaskState(runnable, num_total_tasks, taskID_local, {}, num_threads));
    mutex_thread_share->unlock();

    // save this id into waiting queue if deps is not vacant and save map from this id to deps. qWaiting, map_dependency
    mutex_working->lock();
    q_working_ID.push_back(taskID_local);
    mutex_working->unlock();

    sync();
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
        isInterateDone[i] = false;
        cv_thread_tot->notify_all(); // release wait
        vecTask[vecTask.size()-1]->cv_barrier->notify_all();
        thread_state->mutex_->unlock();
        this->threads[i].join();
    }

    for(int i = 0; i < vecTask.size(); i++){
        free(vecTask[i]);
    }
    vecTask.clear();

    for (int i = 0; i < this->num_threads; i++) {
        delete mutex_thread[i];
        delete cv_thread[i];
    }


    delete mutex_main;
    delete cv_main;
    delete cv_barrier;
    delete mutex_working;
    delete mutex_waiting;
    delete mutex_thread_tot;
    delete mutex_thread_share;
    delete mutex_barrier;
    delete cv_thread_tot;
    delete thread_state;
    delete[] threads;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // Define task
    mutex_thread_share->lock();
    isSync = false;
    int taskID_local = vecTask.size();
    vecTask.push_back(new TaskState(runnable, num_total_tasks, taskID_local, deps, num_threads));
    mutex_thread_share->unlock();
    // save maps between dependent tasks
    for(TaskID dep:deps){
        mutex_thread_tot->lock();
        if(map_indep_to_dep.find(dep) != map_indep_to_dep.end()){
            map_indep_to_dep[dep].push_back(taskID_local);
        } else{
            map_indep_to_dep.insert(std::pair<TaskID, std::vector<TaskID>>(dep, {taskID_local}));
        }

        std::vector<TaskID> vecDependent = map_indep_to_dep[dep];
        mutex_thread_tot->unlock();

        mutex_removing->lock();
        bool isExist = set_removed_ID.empty()? false : (set_removed_ID.find(dep) != set_removed_ID.end());
        mutex_removing->unlock();


        // if current dependenct task is already implemented, move the current task into working groups.
        if(isExist){
            for(TaskID taskID:vecDependent){
                if(taskID < vecTask.size()){
                    vecTask[taskID]->mutex->lock();
                    std::vector<TaskID>::iterator it  = std::find(vecTask[taskID]->vecDependentOn.begin(), vecTask[taskID]->vecDependentOn.end(), dep);
                    if(it != vecTask[taskID]->vecDependentOn.end())
                        vecTask[taskID]->vecDependentOn.erase(it);
                    vecTask[taskID]->mutex->unlock();
                }
            }
        }
    }
    bool isEmpty = deps.empty();

    mutex_removing->lock();
    if(!isEmpty && !set_removed_ID.empty()){
        isEmpty = true;
        for(int i:deps){
            isEmpty = isEmpty && (set_removed_ID.find(i) != set_removed_ID.end());
        }
    }
    mutex_removing->unlock();

    if(isEmpty){
        mutex_working->lock();
        q_working_ID.push_back(taskID_local);
        mutex_working->unlock();
    } else{
        mutex_waiting->lock();
        set_waiting_ID.insert(taskID_local);
        mutex_waiting->unlock();
    }
    return taskID_local;
}

void TaskSystemParallelThreadPoolSleeping::runFunction(int iThread){
    // Start when the first input is coming
    bool isSync_ = isSync;
    block(iThread, -1);
    while(spinning){
        mutex_working->lock();
        isSync_ = isSync;
        bool isEmpty = q_working_ID.empty();
        if(!isEmpty){
            TaskID taskID_local = q_working_ID.front();
            mutex_working->unlock();
            mutex_thread_share->lock();
            task_current = vecTask[taskID_local];
            mutex_thread_share->unlock();
            int iTask = 0;
            int nFinishTask = 0;

            // Run
            while(spinning){
                task_current->mutex_count->lock();
                iTask = ++task_current->counter;
                int nTotTask = task_current->num_total_tasks;
                task_current->mutex_count->unlock();
                if(iTask < nTotTask){
                    task_current->runnable->runTask(iTask, nTotTask);
                    nFinishTask++;
                } else{
                    task_current->block(iThread, taskID_local);

                    break;
                }
            }

            bool isExist = false;
            std::vector<TaskID> vecDependent = {};

            if(isSync_){
                mutex_working->lock();
                bool isExistID = (!q_working_ID.empty() && q_working_ID[0] == taskID_local);
                mutex_working->unlock();
                if(isExistID){
                    mutex_working->lock();
                    q_working_ID.erase(q_working_ID.begin());
                    mutex_working->unlock();

                    mutex_removing->lock();
                    set_removed_ID.insert(taskID_local);
                    mutex_removing->unlock();
                }
            }
            else{
                mutex_working->lock();
                bool isExistID = (!q_working_ID.empty() && q_working_ID[0] == taskID_local);
                mutex_working->unlock();
                if(isExistID){
                    mutex_working->lock();
                    q_working_ID.erase(q_working_ID.begin());
                    mutex_working->unlock();

                    mutex_removing->lock();
                    set_removed_ID.insert(taskID_local);
                    mutex_removing->unlock();
                }

                mutex_thread_tot->lock();
                if(map_indep_to_dep.find(taskID_local) != map_indep_to_dep.end()){
                    isExist = true;
                    vecDependent = map_indep_to_dep[taskID_local];
                }
                mutex_thread_tot->unlock();

                // Remove dependency from others
                if(isExist){
                    for(TaskID IDtask:vecDependent){
                        if(IDtask < vecTask.size()){
                            mutex_thread_share->lock();
                            TaskState* task_dependent = vecTask[IDtask];
                            mutex_thread_share->unlock();

                            task_dependent->mutex->lock();
                            std::vector<TaskID>::iterator it  = std::find(task_dependent->vecDependentOn.begin(), task_dependent->vecDependentOn.end(), taskID_local);
                            if(it!= task_dependent->vecDependentOn.end())
                                task_dependent->vecDependentOn.erase(it);
                            bool isEmpty = task_dependent->vecDependentOn.empty();
                            task_dependent->mutex->unlock();
                            if(isEmpty){
                                mutex_waiting->lock();
                                bool isInsertedData = (set_waiting_ID.end() != set_waiting_ID.find(IDtask));
                                mutex_waiting->unlock();

                                if(isInsertedData){
                                    mutex_waiting->lock();
                                    set_waiting_ID.erase(IDtask);
                                    mutex_waiting->unlock();

                                    mutex_working->lock();
                                    q_working_ID.push_back(IDtask);
                                    mutex_working->unlock();
                                }
                            }
                        }
                    }
                }
            }
        } else{
            mutex_working->unlock();

            mutex_waiting->lock();
            bool isSyncPrepare = isAtSync && set_waiting_ID.empty();
            mutex_waiting->unlock();

            if(isSyncPrepare){ // at synchronization
                //printf("7. Release sync at thread %d\n", iThread);
                cv_main->notify_all();
                if(!isSync_) {
                    //printf("Run out! %d\n", iThread);
                    break;
                }
            }
        }
    }
    block_final(iThread, -1);
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    std::unique_lock<std::mutex> lk(*mutex_main);
    isAtSync = true;
    cv_thread_tot->notify_all();

    cv_main->wait(lk, [this]{return q_working_ID.empty() && set_waiting_ID.empty();});
    lk.unlock();

    return;
}
