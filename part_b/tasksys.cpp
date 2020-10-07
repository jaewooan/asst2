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
        this->threads[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::workers, this, i);
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // Define task
    mutex_shared_task->lock();
    isSyncRun = true;
    int taskID_local = vecTask.size();
    vecTask.push_back(new TaskState(runnable, num_total_tasks, taskID_local, {}, num_threads));
    mutex_shared_task->unlock();

    // save this id into waiting queue if deps is not vacant and save map from this id to deps. qWaiting, map_dependency
    mutex_working->lock();
    v_working_ID.push_back(taskID_local);
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

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // Define task
    mutex_shared_task->lock();
    isSyncRun = false;
    int taskID_local = vecTask.size();
    vecTask.push_back(new TaskState(runnable, num_total_tasks, taskID_local, deps, num_threads));
    mutex_shared_task->unlock();
    // save maps between dependent tasks
    for(TaskID dep:deps){
        mutex_map->lock();
        if(map_indep_to_dep.find(dep) != map_indep_to_dep.end()){
            map_indep_to_dep[dep].push_back(taskID_local);
        } else{
            map_indep_to_dep.insert(std::pair<TaskID, std::vector<TaskID>>(dep, {taskID_local}));
        }

        std::vector<TaskID> vecDependent = map_indep_to_dep[dep];
        mutex_map->unlock();

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
        v_working_ID.push_back(taskID_local);
        mutex_working->unlock();
    } else{
        mutex_waiting->lock();
        set_waiting_ID.insert(taskID_local);
        mutex_waiting->unlock();
    }
    return taskID_local;
}

void TaskSystemParallelThreadPoolSleeping::workers(int iThread){
    // Start when the first input is coming
    bool isSyncRun_ = isSyncRun;
    block();
    while(spinning){
        mutex_working->lock();
        isSyncRun_ = isSyncRun;
        bool isEmpty = v_working_ID.empty();
        if(!isEmpty){
            TaskID taskID_local = v_working_ID.front();
            mutex_working->unlock();
            mutex_shared_task->lock();
            task_current = vecTask[taskID_local];
            mutex_shared_task->unlock();
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
                    task_current->block();

                    break;
                }
            }

            bool isExist = false;
            std::vector<TaskID> vecDependent = {};

            if(isSyncRun_){
                mutex_working->lock();
                bool isExistID = (!v_working_ID.empty() && v_working_ID[0] == taskID_local);
                mutex_working->unlock();
                if(isExistID){
                    mutex_working->lock();
                    v_working_ID.erase(v_working_ID.begin());
                    mutex_working->unlock();

                    mutex_removing->lock();
                    set_removed_ID.insert(taskID_local);
                    mutex_removing->unlock();
                }
            }
            else{
                mutex_working->lock();
                bool isExistID = (!v_working_ID.empty() && v_working_ID[0] == taskID_local);
                mutex_working->unlock();
                if(isExistID){
                    mutex_working->lock();
                    v_working_ID.erase(v_working_ID.begin());
                    mutex_working->unlock();

                    mutex_removing->lock();
                    set_removed_ID.insert(taskID_local);
                    mutex_removing->unlock();
                }

                mutex_map->lock();
                if(map_indep_to_dep.find(taskID_local) != map_indep_to_dep.end()){
                    isExist = true;
                    vecDependent = map_indep_to_dep[taskID_local];
                }
                mutex_map->unlock();

                // Remove dependency from others
                if(isExist){
                    for(TaskID IDtask:vecDependent){
                        if(IDtask < vecTask.size()){
                            mutex_shared_task->lock();
                            TaskState* task_dependent = vecTask[IDtask];
                            mutex_shared_task->unlock();

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
                                    v_working_ID.push_back(IDtask);
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
            bool isSyncPrepare = isAtSyncStatus && set_waiting_ID.empty();
            mutex_waiting->unlock();

            if(isSyncPrepare){ // at synchronization
                //printf("7. Release sync at thread %d\n", iThread);
                cv_at_sync->notify_all();
                if(!isSyncRun_) {
                    //printf("Run out! %d\n", iThread);
                    break;
                }
            }
        }
    }
    block();
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    std::unique_lock<std::mutex> lk(*mutex_at_sync);
    isAtSyncStatus = true;

    cv_at_sync->wait(lk, [this]{return v_working_ID.empty() && set_waiting_ID.empty();});
    lk.unlock();

    return;
}
