"""
Tools for defining, running and terminating Cosmos workflows.
"""

import atexit
import copy
import datetime
import getpass
import os
import re
import sys
import time
import types

import funcsigs

from collections import defaultdict
from sqlalchemy import orm
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.schema import Column
from sqlalchemy.types import Boolean, Integer, String, DateTime, VARCHAR
from sqlalchemy.orm import validates, synonym, relationship
from flask import url_for
import networkx as nx
from networkx.algorithms.dag import descendants, topological_sort

from cosmos.util.iterstuff import only_one
from cosmos.util.helpers import duplicates, get_logger, mkdir
from cosmos.util.sqla import Enum_ColumnType, MutableDict, JSONEncodedDict
from cosmos.db import Base
from cosmos.core.cmd_fxn import signature

from cosmos import (
    TaskStatus,
    StageStatus,
    WorkflowStatus,
    signal_workflow_status_change,
    signal_processed_finished_tasks,
    signal_workflow_run_ended,
    COMPLETED_TASK_STATUSES
)
from cosmos.models.Task import Task, GetOutputError

opj = os.path.join


def default_task_log_output_dir(task, subdir=''):
    """The default function for computing Task.log_output_dir"""
    return os.path.abspath(opj('log', subdir, task.stage.name, str(task.uid)))


@signal_workflow_status_change.connect
def _workflow_status_changed(workflow):
    if workflow.status in [WorkflowStatus.successful, WorkflowStatus.failed, WorkflowStatus.killed]:
        logfunc = workflow.log.warning if workflow.status in [WorkflowStatus.failed, WorkflowStatus.killed] else workflow.log.info
        logfunc('%s %s (%s/%s Tasks completed)' % (workflow, workflow.status, sum(t.successful for t in workflow.tasks), len(workflow.tasks)))
        workflow.finished_on = datetime.datetime.now()

    if workflow.status == WorkflowStatus.successful:
        workflow.successful = True
        workflow.finished_on = datetime.datetime.now()


class Workflow(Base):
    """
    An collection Stages and Tasks encoded as a DAG
    """
    __tablename__ = 'workflow'

    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(200), unique=True, nullable=False)
    successful = Column(Boolean, nullable=False)
    created_on = Column(DateTime)
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    primary_log_path = Column(String(255))
    _log = None

    info = Column(MutableDict.as_mutable(JSONEncodedDict))
    _status = Column(Enum_ColumnType(WorkflowStatus, length=255), default=WorkflowStatus.no_attempt)
    stages = relationship("Stage", cascade="all, merge, delete-orphan", order_by="Stage.number", passive_deletes=True,
                          backref='workflow')

    exclude_from_dict = ['info']
    dont_garbage_collect = None
    termination_signal = None

    @declared_attr
    def status(cls):
        def get_status(self):
            return self._status

        def set_status(self, value):
            if self._status != value:
                self._status = value
                signal_workflow_status_change.send(self)

        return synonym('_status', descriptor=property(get_status, set_status))

    @validates('name')
    def validate_name(self, key, name):
        assert re.match(r"^[\w-]+$", name), 'Invalid workflow name, characters are limited to letters, numbers, ' \
                                            'hyphens and underscores'
        return name

    @orm.reconstructor
    def constructor(self):
        self.__init__(manual_instantiation=False)

    def __init__(self, manual_instantiation=True, *args, **kwargs):
        # FIXME provide the cosmos_app instance?

        if manual_instantiation:
            raise TypeError('Do not instantiate an Workflow manually.  Use the Cosmos.start method.')
        super(Workflow, self).__init__(*args, **kwargs)
        # assert self.output_dir is not None, 'output_dir cannot be None'
        if self.info is None:
            # mutable dict column defaults to None
            self.info = dict()
        self.jobmanager = None
        if not self.created_on:
            self.created_on = datetime.datetime.now()
        self.dont_garbage_collect = []
        self.max_cores = None

    @property
    def log(self):
        if self._log is None:
            self._log = get_logger('%s' % self, self.primary_log_path)
        return self._log

    def make_output_dirs(self):
        """
        Create directory paths of all output files
        """
        dirs = set()

        for task in self.tasks:
            for out_name, v in task.output_map.iteritems():
                dirname = lambda p: p if out_name.endswith('dir') or p is None else os.path.dirname(p)

                if isinstance(v, (tuple, list)):
                    dirs.update(map(dirname, v))
                elif isinstance(v, dict):
                    raise NotImplemented()
                else:
                    dirs.add(dirname(v))

        for d in dirs:
            if d is not None and '://' not in d:
                mkdir(d)

    def add_task(self, func, params=None, parents=None, stage_name=None, uid=None, drm=None,
                 queue=None, must_succeed=True, time_req=None, core_req=None, mem_req=None,
                 max_attempts=None, noop=False, job_class=None, drm_options=None):
        """
        Adds a new Task to the Workflow.  If the Task already exists (and was successful), return the successful Task stored in the database

        :param callable func: A function which returns a string which will get converted to a shell script to be executed.  `func` will not get called until
          all of its dependencies have completed.
        :param dict params: Parameters to `func`.  Must be jsonable so that it can be stored in the database.  Any Dependency objects will get resolved into
            a string, and the Dependency.task will be added to this Task's parents.
        :param list[Tasks] parents: A list of dependent Tasks.
        :param str uid: A unique identifier for this Task, primarily used for skipping  previously successful Tasks.
            If a Task with this stage_name and uid already exists in the database (and was successful), the
            database version will be returned and a new one will not be created.
        :param str stage_name: The name of the Stage to add this Task to.  Defaults to `func.__name__`.
        :param str drm: The drm to use for this Task (example 'local', 'ge' or 'drmaa:lsf').  Defaults to the `default_drm` parameter of :meth:`Cosmos.start`
        :param job_class: The name of a job_class to submit to; defaults to the `default_job_class` parameter of :meth:`Cosmos.start`
        :param queue: The name of a queue to submit to; defaults to the `default_queue` parameter of :meth:`Cosmos.start`
        :param bool must_succeed: Default True.  If False, the Workflow will not fail if this Task does not succeed.  Dependent Jobs will not be executed.
        :param bool time_req: The time requirement; will set the Task.time_req attribute which is intended to be used by :func:`get_submit_args` to request resources.
        :param int cpu_req: Number of cpus required for this Task.  Can also be set in the `params` dict or the default value of the Task function signature, but this value takes precedence.
            Warning!  In future versions, this will be the only way to set it.
        :param int mem_req: Number of MB of RAM required for this Task.   Can also be set in the `params` dict or the default value of the Task function signature, but this value takes predence.
            Warning!  In future versions, this will be the only way to set it.
        :param int max_attempts: The maximum number of times to retry a failed job.  Defaults to the `default_max_attempts` parameter of :meth:`Cosmos.start`
        :rtype: cosmos.api.Task
        """
        # Avoid cyclical import dependencies
        from cosmos.job.drm.DRM_Base import DRM
        from cosmos.models.Stage import Stage
        from cosmos import recursive_resolve_dependency

        # parents
        if parents is None:
            parents = []
        elif isinstance(parents, Task):
            parents = [parents]
        else:
            parents = list(parents)

        # params
        if params is None:
            params = dict()
        for k, v in params.iteritems():
            # decompose `Dependency` objects to values and parents
            new_val, parent_tasks = recursive_resolve_dependency(v)

            params[k] = new_val
            parents.extend(parent_tasks - set(parents))

        # uid
        if uid is None:
            raise AssertionError('uid parameter must be specified')
            # Fix me assert params are all JSONable
            # uid = str(params)
        else:
            assert isinstance(uid, basestring), 'uid must be a string'

        if stage_name is None:
            stage_name = str(func.__name__)

        # Get the right Stage
        stage = only_one((s for s in self.stages if s.name == stage_name), None)
        if stage is None:
            stage = Stage(workflow=self, name=stage_name, status=StageStatus.no_attempt)
            self.session.add(stage)

        # Check if task is already in stage
        task = stage.get_task(uid, None)

        if task is not None:
            # if task is already in stage, but unsuccessful, raise an error (duplicate params) since unsuccessful tasks
            # were already removed on workflow load
            if task.successful:
                # If the user manually edited the dag and this a resume, parents might need to be-readded
                task.parents.extend(set(parents).difference(set(task.parents)))

                for p in parents:
                    if p.stage not in stage.parents:
                        stage.parents.append(p.stage)

                return task
            else:
                # TODO check for duplicate params here?  would be a lot faster at Workflow.run
                raise ValueError('Duplicate uid, you have added a Task to Stage %s with the uid (unique identifier) `%s` twice.  '
                                 'Task uids must be unique within the same Stage.' % (stage_name, uid))
        else:
            # Create Task
            sig = funcsigs.signature(func)

            def params_or_signature_default_or(name, default):
                if name in params:
                    return params[name]
                if name in sig.parameters:
                    param_default = sig.parameters[name].default
                    if param_default is funcsigs._empty:
                        return default
                    else:
                        return param_default
                return default

            input_map = dict()
            output_map = dict()

            for keyword, param in sig.parameters.iteritems():
                if keyword.startswith('in_'):
                    v = params.get(keyword, param.default)
                    assert v != funcsigs._empty, 'parameter %s for %s is required' % (param, func)
                    input_map[keyword] = v
                elif keyword.startswith('out_'):
                    v = params.get(keyword, param.default)
                    assert v != funcsigs._empty, 'parameter %s for %s is required' % (param, func)
                    output_map[keyword] = v

            task = Task(stage=stage,
                        params=params,
                        parents=parents,
                        input_map=input_map,
                        output_map=output_map,
                        uid=uid,
                        drm=drm if drm is not None else self.cosmos_app.default_drm,
                        job_class=job_class if job_class is not None else self.cosmos_app.default_job_class,
                        queue=queue if queue is not None else self.cosmos_app.default_queue,
                        must_succeed=must_succeed,
                        core_req=core_req if core_req is not None else params_or_signature_default_or('core_req', 1),
                        mem_req=mem_req if mem_req is not None else params_or_signature_default_or('mem_req', None),
                        time_req=time_req if time_req is not None else self.cosmos_app.default_time_req,
                        successful=False,
                        max_attempts=max_attempts if max_attempts is not None else self.cosmos_app.default_max_attempts,
                        attempt=1,
                        NOOP=noop
                        )

            task.cmd_fxn = func

            # Set task DRM options
            if drm_options is None:
                task.drm_options = self.cosmos_app.default_drm_options
            else:
                final_drm_options = copy.deepcopy(
                    self.cosmos_app.default_drm_options)
                final_drm_options.update(drm_options)
                task.drm_options = final_drm_options
            DRM.validate_drm_options(task.drm, task.drm_options)

            # Initialize task DRM state
            task.drm_state = {}

        # Add Stage Dependencies
        for p in parents:
            if p.stage not in stage.parents:
                stage.parents.append(p.stage)

        self.dont_garbage_collect.append(task)

        return task

    def run(self, max_cores=None, dry=False, set_successful=True,
            cmd_wrapper=signature.default_cmd_fxn_wrapper,
            log_out_dir_func=default_task_log_output_dir):
        """
        Runs this Workflow's DAG

        :param Union[Dict[str, float], float] max_cores: Maximum number of cores to use at once.  May be a dict of DRMs to their respective core limits or a float indicating a core limit for the default DRM. A value of None indicates no user-specified core limits. Default DRM-specific core limits may still be enforced.
        :param int max_attempts: The maximum number of times to retry a failed job.
             Can be overridden with on a per-Task basis with Workflow.add_task(..., max_attempts=N, ...)
        :param callable log_out_dir_func: A function that returns a Task's logging directory (must be unique).
             It receives one parameter: the Task instance.
             By default a Task's log output is stored in log/stage_name/task_id.
             See _default_task_log_output_dir for more info.
        :param callable cmd_wrapper: A decorator which will be applied to every Task's cmd_fxn.
        :param bool dry: If True, do not actually run any jobs.
        :param bool set_successful: Sets this workflow as successful if all tasks finish without a failure.  You might set this to False if you intend to add and
            run more tasks in this workflow later.

        Returns True if all tasks in the workflow ran successfully, False otherwise.
        If dry is specified, returns None.
        """
        try:
            assert os.path.exists(os.getcwd()), 'current working dir does not exist! %s' % os.getcwd()

            assert hasattr(self, 'cosmos_app'), 'Workflow was not initialized using the Workflow.start method'
            assert hasattr(log_out_dir_func, '__call__'), 'log_out_dir_func must be a function'
            assert self.session, 'Workflow must be part of a sqlalchemy session'

            session = self.session
            self.log.info("Preparing to run %s using DRM `%s`, cwd is `%s`",
                self, self.cosmos_app.default_drm, os.getcwd())
            try:
                user = getpass.getuser()
            except:
                # fallback to uid if we can't respove a user name
                user = os.getuid()

            self.log.info('Running as %s@%s, pid %s',
                          user, os.uname()[1], os.getpid())

            from ..job.JobManager import JobManager

            if self.jobmanager is None:
                self.jobmanager = JobManager(get_submit_args=self.cosmos_app.get_submit_args,
                                             cmd_wrapper=cmd_wrapper,
                                             log_out_dir_func=log_out_dir_func)

            # Validate DRMs passed in max_cores
            if isinstance(max_cores, dict):
                invalid_drms_in_max_cores = {
                    d for d in max_cores
                    if d not in self.jobmanager.drms
                }
                if invalid_drms_in_max_cores:
                    raise ValueError(
                        f"Unknown DRM(s) specified in max_cores argument to "
                        "Workflow.run method: "
                        f"{', '.join(invalid_drms_in_max_cores)}"
                    )
                self.max_cores = max_cores
            # If a single numeric value is passed, assume that this is the
            # core limit for the default DRM.
            elif isinstance(max_cores, (int, float)):
                self.max_cores = {
                    self.cosmos_app.default_drm: float(max_cores)}
            elif max_cores is not None:
                raise ValueError(
                    "Expected an integer, float, or dict of core limits for "
                    "the respective DRMs as the max_cores argument, but got "
                    f"value of type {type(max_cores)}: {max_cores}"
                )

            self.status = WorkflowStatus.running
            self.successful = False

            if self.started_on is None:
                self.started_on = datetime.datetime.now()

            task_graph = self.task_graph()
            stage_graph = self.stage_graph()

            assert len(set(self.stages)) == len(self.stages), 'duplicate stage name detected: %s' % (
                next(duplicates(self.stages)))

            # renumber stages
            stage_graph_no_cycles = nx.DiGraph()
            stage_graph_no_cycles.add_nodes_from(stage_graph.nodes())
            stage_graph_no_cycles.add_edges_from(stage_graph.edges())
            for cycle in nx.simple_cycles(stage_graph):
                stage_graph_no_cycles.remove_edge(cycle[-1], cycle[0])
            for i, s in enumerate(topological_sort(stage_graph_no_cycles)):
                s.number = i + 1
                if s.status != StageStatus.successful:
                    s.status = StageStatus.no_attempt

            # Make sure everything is in the sqlalchemy session
            session.add(self)
            successful = filter(lambda t: t.successful, task_graph.nodes())

            # print stages
            for s in sorted(self.stages, key=lambda s: s.number):
                self.log.info('%s %s' % (s, s.status))

            # Create Task Queue
            task_queue = _copy_graph(task_graph)
            self.log.info('Skipping %s successful tasks...' % len(successful))
            task_queue.remove_nodes_from(successful)

            handle_exits(self)

            # make sure we've got enough cores
            if self.max_cores is not None:
                self.log.info('Ensuring there are enough cores...')
                for t in task_queue:
                    req = int(t.core_req)
                    # Determine the user-specified limit or the default limit
                    # for the DRM, if they exist
                    limit = self.max_cores.get(
                        t.drm,
                        self.jobmanager.get_max_cores_for_drm(t.drm)
                    )
                    # Only check if limit is not 0 or None. Both
                    # of these imply infinite cores.
                    if limit and req > limit:
                        raise RuntimeError(
                            f"Not enough cores for task: {t}. "
                            f"Requires {req} cores but only got {limit}."
                        )

            # Run this thing!
            self.log.info('Committing to SQL db...')
            session.commit()
            if not dry:
                _run(self, session, task_queue)

                # set status
                if self.status == WorkflowStatus.failed_but_running:
                    self.status = WorkflowStatus.failed
                    # set stage status to failed
                    for s in self.stages:
                        if s.status == StageStatus.running_but_failed:
                            s.status = StageStatus.failed
                    session.commit()
                    return False
                elif self.status == WorkflowStatus.running:
                    if set_successful:
                        self.status = WorkflowStatus.successful
                    session.commit()
                    return True
                else:
                    self.log.warning('%s exited with status "%s"', self, self.status)
                    session.commit()
                    return False
            else:
                self.log.info('Workflow dry run is complete')
                return None
        except Exception as ex:
            self.log.fatal(ex, exc_info=True)
            raise
        finally:
            signal_workflow_run_ended.send(self)

    def terminate(self, due_to_failure=True):
        self.log.warning('Terminating %s!' % self)
        if self.jobmanager:
            self.log.info(
                'Terminating {num_running_tasks} running tasks'.format(
                    num_running_tasks=len(self.jobmanager.running_tasks),
                )
            )
            self.jobmanager.terminate()

        if due_to_failure:
            self.status = WorkflowStatus.failed
        else:
            self.status = WorkflowStatus.killed

        self.session.commit()

    def cleanup(self):
        if self.jobmanager:
            self.jobmanager.cleanup()

    @property
    def tasks(self):
        return [t for s in self.stages for t in s.tasks]
        # return session.query(Task).join(Stage).filter(Stage.workflow == ex).all()

    def stage_graph(self):
        """
        :return: (networkx.DiGraph) a DAG of the stages
        """
        g = nx.DiGraph()
        g.add_nodes_from(self.stages)
        g.add_edges_from((s, c) for s in self.stages for c in s.children if c)
        return g

    def task_graph(self):
        """
        :return: (networkx.DiGraph) a DAG of the tasks
        """
        g = nx.DiGraph()
        g.add_nodes_from(self.tasks)
        g.add_edges_from([(t, c) for t in self.tasks for c in t.children])
        return g

    def get_stage(self, name_or_id):
        if isinstance(name_or_id, int):
            f = lambda s: s.id == name_or_id
        else:
            f = lambda s: s.name == name_or_id

        for stage in self.stages:
            if f(stage):
                return stage

        raise ValueError('Stage with name %s does not exist' % name_or_id)

    @property
    def url(self):
        return url_for('cosmos.workflow', name=self.name)

    def __repr__(self):
        return '<Workflow[%s] %s>' % (self.id or '', self.name)

    def __unicode__(self):
        return self.__repr__()

    def delete(self, delete_files=False):
        """
        :param delete_files: (bool) If True, delete :attr:`output_dir` directory and all contents on the filesystem
        """
        if hasattr(self, 'log'):
            self.log.info('Deleting %s, delete_files=%s' % (self, delete_files))
            for h in self.log.handlers:
                h.flush()
                h.close()
                self.log.removeHandler(h)

        if delete_files:
            raise NotImplementedError('This should delete all Task.output_files')

        print >> sys.stderr, '%s Deleting from SQL...' % self
        self.session.delete(self)
        self.session.commit()
        print >> sys.stderr, '%s Deleted' % self

    def get_first_failed_task(self, key=lambda t: t.finished_on):
        """
        Return the first failed Task (chronologically).

        If no Task failed, return None.
        """
        for t in sorted([t for t in self.tasks if key(t) is not None], key=key):
            if t.exit_status:
                return t
        return None


# @event.listens_for(Workflow, 'before_delete')
# def before_delete(mapper, connection, target):
# print 'before_delete %s ' % target

def _run(workflow, session, task_queue):
    """
    Do the workflow!
    """
    workflow.log.info('Executing TaskGraph')

    # Initially all cores should be available
    available_cores = True

    # Free up available cores by processing finished tasks
    while len(task_queue) > 0:
        # Flag to indicate that workflow should be terminated at the end of
        # this loop if any irrecoverable task failures are observed
        terminate_due_to_failure = False

        # Placeholder for finished tasks
        finished_tasks_by_status = defaultdict(set)

        for task in _process_finished_tasks(workflow.jobmanager):
            # Collect and track completed tasks
            if task.status in COMPLETED_TASK_STATUSES:
                finished_tasks_by_status[task.status].add(task)

            if task.status == TaskStatus.failed:
                if not task.must_succeed:
                    # pop the task, it's ok if the task failed
                    task_queue.remove_node(task)
                else:
                    if workflow.info['fail_fast']:
                        workflow.log.info(
                            'Task %s failed with exit status: %s. '
                            '%s will exit run loop at next available '
                            'opportunity.',
                            task,
                            task.exit_status,
                            workflow
                        )
                        terminate_due_to_failure = True
                        continue

                    # pop all descendents when a task fails; the rest of the
                    # graph can still execute
                    remove_nodes = descendants(task_queue, task).union({task})
                    # graph_failed.add_edges(task_queue.subgraph(remove_nodes).edges())

                    task_queue.remove_nodes_from(remove_nodes)
                    workflow.status = WorkflowStatus.failed_but_running
                    workflow.log.info(
                        '%s tasks left in the queue' % len(task_queue))
            elif task.status == TaskStatus.successful:
                # just pop this task
                task_queue.remove_node(task)
            elif task.status == TaskStatus.no_attempt:
                # the task must have failed, and is being reattempted
                pass
            else:
                raise AssertionError(
                    'Unexpected finished task status %s for %s' % (
                        task.status, task)
                )
            available_cores = True

        # only commit Task changes after processing a batch of finished ones
        session.commit()

        # If cores are available and this workflow is not scheduled to be
        # terminated, submit more jobs
        if available_cores and not terminate_due_to_failure:
            _run_queued_and_ready_tasks(task_queue, workflow)
            available_cores = False

        # Signal that finished tasks were processed in this iteration
        if finished_tasks_by_status:
            signal_processed_finished_tasks.send(finished_tasks_by_status)

        # If a task failure should result in workflow termination,
        # end here
        if terminate_due_to_failure:
            workflow.terminate(due_to_failure=True)
            return

        # conveniently, this returns early if we catch a signal
        time.sleep(workflow.jobmanager.poll_interval)

        if workflow.termination_signal:
            workflow.log.info(
                '%s Early termination requested (%d): stopping workflow',
                workflow,
                workflow.termination_signal
            )
            workflow.terminate(due_to_failure=False)
            return


def _run_queued_and_ready_tasks(task_queue, workflow):
    # Collect tasks that are ready to run
    ready_tasks = defaultdict(set)
    for task, degree in task_queue.in_degree():
        if degree == 0 and task.status == TaskStatus.no_attempt:
            ready_tasks[task.drm].add(task)

    # No ready tasks. Nothing to do here!
    if not ready_tasks:
        return

    # Core limits for workflow
    core_limits = workflow.max_cores or {}

    # Number of cores used
    cores_used = defaultdict(int)
    for task in workflow.jobmanager.running_tasks:
        cores_used[task.drm] += task.core_req

    # Collect as many tasks to launch as possible while respecting core limits
    submittable_tasks = set()
    for drm, tasks in ready_tasks.items():
        # Maximum allowed cores for this DRM. May be a user-specified limit or
        # one defined by the DRM.
        drm_core_limit = core_limits.get(
            drm,
            workflow.jobmanager.get_max_cores_for_drm(drm)
        )
        # No core limit (None or 0). All is well.
        if not drm_core_limit:
            submittable_tasks.update(tasks)
        # If a core limit exists, figure out how many free cores we have
        # and pack them with jobs!
        else:
            available_cores = drm_core_limit - cores_used.get(drm, 0)
            # TODO (jeev): ideally we should sort by the number of downstream
            # tasks that a given task can unblock (size of the subgraph of the
            # given node in the DAG)
            for task in sorted(
                    tasks,
                    key=lambda t: (t.core_req, t.id),
                    reverse=True
            ):
                # TODO (jeev): Do we want to greedily pack more small tasks
                # into the DRM?
                if task.core_req > available_cores:
                    workflow.log.info(
                        f"Reached core limit of {drm_core_limit} for "
                        f"DRM: {drm}. Waiting for a task to finish..."
                    )
                    break

                submittable_tasks.add(task)
                available_cores -= task.core_req

    # submit in a batch for speed
    workflow.jobmanager.run_tasks(submittable_tasks)

    # only commit submitted Tasks after submitting a batch
    workflow.session.commit()


def _process_finished_tasks(jobmanager):
    for task in jobmanager.get_finished_tasks():
        if task.NOOP:
            task.status = TaskStatus.successful
        # An exit status of None is indicative of a task that was lost
        # by the DRM layer.
        elif task.exit_status is None:
            task.status = TaskStatus.lost
        # Task completed with exit status. Extract logs for this task and
        # set the appropriate status
        else:
            try:
                jobmanager.get_drm(task.drm).populate_logs(task)
            except GetOutputError as e:
                task.log.warning(str(e))
                # DRM could not fetch the logs for this task. Consider this
                # an irrecoverable error and flag the task for requeuing
                task.status = TaskStatus.lost
            else:
                task.status = (
                    TaskStatus.successful
                    if task.exit_status == 0 else
                    TaskStatus.failed
                )

        yield task


def handle_exits(workflow, do_atexit=True):
    if do_atexit:
        @atexit.register
        def cleanup_check():
            try:
                try:
                    if workflow is not None and workflow.status in \
                       {WorkflowStatus.running, WorkflowStatus.failed_but_running}:
                        workflow.log.error(
                            '%s Still running when atexit() was called, terminating' % workflow)
                        workflow.terminate(due_to_failure=True)
                except SQLAlchemyError:
                    workflow.log.error(
                        '%s Unknown status when atexit() was called (SQL error), terminating' % workflow)
                    workflow.terminate(due_to_failure=True)
            finally:
                workflow.cleanup()
                workflow.log.info('%s Ceased work: this is its final log message', workflow)


def _copy_graph(graph):
    graph2 = nx.DiGraph()
    graph2.add_edges_from(graph.edges())
    graph2.add_nodes_from(graph.nodes())
    return graph2
