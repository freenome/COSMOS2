import base64
import copy
import functools
import os
import random
import re
import shutil
import string

from abc import abstractmethod, ABCMeta
from datetime import datetime, timedelta
from cosmos.api import TaskStatus
from cosmos.job.drm.DRM_Base import DRM
from cosmos.models.Task import GetOutputError, Task, TIMED_OUT_EXIT_STATUS
from cosmos.util.helpers import groupby2
from enum import auto, Enum
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from sqlalchemy import inspect as sqlalchemy_inspect
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception
)
from typing import Dict, List, NamedTuple, Optional, TypeVar, Union
from urllib3.exceptions import MaxRetryError, TimeoutError

CONTAINER_NAME = 'main'
DEFAULT_NAME_PREFIX = 'cosmos-job'
POD_ANNOTATIONS = {
    'cluster-autoscaler.kubernetes.io/safe-to-evict': 'false'
}
COSMOS_IDENTIFIER_LABEL = 'owned-by-cosmos'
COSMOS_TASK_STAGE_LABEL = 'cosmos-task-stage'
COSMOS_TASK_UID_LABEL = 'cosmos-task-uid'
COSMOS_CONTROLLER_NAME_LABEL = 'cosmos-controller-name'
COSMOS_CONTROLLER_UID_LABEL = 'cosmos-controller-uid'
POD_JOB_ID_LABEL = 'cosmos-job-id'
VOLUME_NAME_PREFIX = 'cosmos-vol'
KUBERNETES_LABEL_RE = re.compile(
    r'^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')
KUBERNETES_INVALID_LABEL = 'INVALID'
KUBERNETES_MAX_NAME_LENGTH = 63
KUBERNETES_POD_CHECK_ENVVAR = 'KUBERNETES_SERVICE_HOST'
PARENT_WORKFLOW_POD_NAME_ENVVAR = '__POD_NAME__'
PARENT_WORKFLOW_POD_UID_ENVVAR = '__POD_UID__'
GKE_POD_UNKNOWN_STATUS = 'Unknown'
DEFAULT_PV_STORAGE_CLASS = 'ssd'
SCRATCH_VOLUME_MOUNT_PATH = '/scratch'
LOGS_STREAM_CHUNK_SIZE = 128 * 1024  # 128 KB
LOGS_CONNECTION_TIMEOUT = timedelta(seconds=3)
LOGS_READ_TIMEOUT = timedelta(seconds=1)
PREEMPTIBLE_NODE_LABEL = 'cloud.google.com/gke-preemptible'
PREEMPTIBLE_START_GRACE_PERIOD = timedelta(hours=2)
LONG_RUNNING_JOB_THRESHOLD = timedelta(hours=4)
CONTAINER_READY_GRACE_PERIOD = timedelta(minutes=20)
NODE_PARTITION_LABEL_KEY = 'partition'


def _remove_null_dict(**kwargs):
    return dict(filter(lambda x: x[1] is not None, kwargs.items()))


def _random_str(length):
    chars = f'{string.ascii_lowercase}{string.digits}'
    return ''.join(random.choice(chars) for i in range(length))


def _generate_name(prefix, suffix_length=6):
    suffix = f'-{_random_str(length=suffix_length)}'
    return f'{prefix[:KUBERNETES_MAX_NAME_LENGTH - len(suffix)]}{suffix}'


def _kube_label(string, strict=False):
    # Check length of string
    if strict and len(string) > KUBERNETES_MAX_NAME_LENGTH:
        raise ValueError(
            f"Invalid value: '{string}': a valid label "
            f"must be no more than {KUBERNETES_MAX_NAME_LENGTH} characters"
        )

    value = string[:KUBERNETES_MAX_NAME_LENGTH]

    # Match the pattern
    match = KUBERNETES_LABEL_RE.match(value)
    if strict and (match is None or match.endpos != len(value)):
        raise ValueError(
            f"Invalid value: '{value}': a valid label must be an "
            "empty string or consist of alphanumeric characters, "
            "'-', '_' or '.', and must start and end with an alphanumeric "
            "character (e.g. 'MyValue', or 'my_value', or '12345', "
            f"regex used for validation is '{KUBERNETES_LABEL_RE.pattern}'"
        )

    # Extract value from match
    return match.group() if match is not None else KUBERNETES_INVALID_LABEL


def _k8s_api_wrapper(*codes_to_ignore, logger=None):
    def decorator(func):
        def _should_retry(e):
            # Retry on response truncation. This happens sometimes when fetching logs.
            is_response_truncated = (e.status == 500 and isinstance(e.body, bytes) and b'EOF' in e.body)

            # Retry on conflict with current cluster state. Workaround for:
            # HTTP response headers:
            # <CIMultiDictProxy('Audit-Id': '01fab989-6ce8-439a-bc55-b61145a0cf36', 'Content-Type': 'application/json',
            # 'Date': 'Sat, 11 Jan 2020 17:09:22 GMT', 'Content-Length': '342')>HTTP
            # response body: {"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",
            # "message":"Operation cannot be fulfilled on resourcequotas \"gke-resource-quotas\": the object has been modified;
            # please apply your changes to the latest version and try again","reason":"Conflict","details":{"name":"gke-resource-quotas","kind":"resourcequotas"},"code":409}
            # TODO (jeev): This is a bandaid fix. We need a more reliable
            #  solution.
            is_gke_quota_conflict = (e.status == 409)

            # Retry on all 50X errors
            is_50X_error = e.status > 500
            is_known_api_exception = (isinstance(e, ApiException) and
                                      (is_50X_error or is_response_truncated or is_gke_quota_conflict)
                                      )
            retry_cond = (is_known_api_exception or isinstance(e, TimeoutError))
            if retry_cond:
                if logger is not None:
                    logger.warning(f'Retrying API call due to error: {str(e)}')
                return True
            return False

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                retry_decorator = retry(
                    stop=stop_after_attempt(5),
                    wait=wait_exponential(multiplier=1, min=1, max=10),
                    retry=retry_if_exception(_should_retry)
                )
                result = retry_decorator(func)(*args, **kwargs)
            except ApiException as e:
                if e.status in codes_to_ignore:
                    if logger is not None:
                        logger.debug(
                            f'Ignoring error in API response: {str(e)}')
                else:
                    raise
            else:
                return result
        return wrapper
    return decorator


ENVVAR_TYPE = TypeVar('ENVVAR_TYPE', bound='GenericEnvVar')
VOL_TYPE = TypeVar('VOL_TYPE', bound='GenericVolume')


class GkeDRMOptions(NamedTuple):
    image: str
    image_pull_policy: str = 'Always'
    name: Optional[str] = None
    name_prefix: Optional[str] = None
    namespace: str = 'default'
    labels: Optional[Dict[str, str]] = None
    cpu: Optional[str] = None
    cpu_limit: Optional[str] = None
    memory: Optional[str] = None
    memory_limit: Optional[str] = None
    # TODO (jeev): Add support for disk requirements for tasks
    # disk: Optional[str] = None
    timeout: Optional[timedelta] = None
    environment: Optional[Dict[str, Union[str, ENVVAR_TYPE]]] = None
    volumes: Optional[List[VOL_TYPE]] = None
    partition: Optional[str] = None
    preemptible: bool = False
    node_selectors: Optional[Dict[str, str]] = None
    tolerations: Optional[List['PodToleration']] = None
    dns_policy: str = 'Default'
    host_network: bool = False
    service_account: Optional[str] = None
    collect_logs: bool = True


OPTIONAL_FIELDS = set(GkeDRMOptions._field_defaults.keys())
REQUIRED_FIELDS = set(GkeDRMOptions._fields) - OPTIONAL_FIELDS


class NonPreemptibleCondition(Enum):
    LONG_RUNNING_JOB = auto()
    OUT_OF_START_GRACE_PERIOD = auto()


TEMPORARY_NON_PREEMPTIBLE_CONDS = (
    NonPreemptibleCondition.OUT_OF_START_GRACE_PERIOD,
)


class PodTolerationEffect(Enum):
    NO_SCHEDULE = 'NoSchedule'
    NO_EXECUTE = 'NoExecute'


class PodToleration(NamedTuple):
    key: str
    value: Optional[str] = None
    effect: PodTolerationEffect = PodTolerationEffect.NO_SCHEDULE


class GenericEnvVar(object, metaclass=ABCMeta):
    @property
    @abstractmethod
    def value(self):
        raise NotImplementedError


class ValueFromSecret(GenericEnvVar):
    def __init__(self, name, key):
        super().__init__()
        self._name = name
        self._key = key

    @property
    def value(self):
        return client.V1EnvVarSource(
            secret_key_ref=client.V1SecretKeySelector(
                name=self._name,
                key=self._key
            )
        )


class ValueFromConfigMap(GenericEnvVar):
    def __init__(self, name, key):
        super().__init__()
        self._name = name
        self._key = key

    @property
    def value(self):
        return client.V1EnvVarSource(
            config_map_key_ref=client.V1ConfigMapKeySelector(
                name=self._name,
                key=self._key
            )
        )


class GenericVolume(object, metaclass=ABCMeta):
    def __init__(self, mount_path, read_only=False):
        super().__init__()
        self._mount_path = mount_path
        self._read_only = read_only

        # Flag indicating that volume is set up and active
        self._active = False

        # Placeholder for unique identifier of volume
        self.__name = None

        # Placeholder for parent task owning this volume
        self.__parent = None

    def __hash__(self):
        return hash(self._slots)

    def __eq__(self, other):
        return hash(self) == hash(other)

    @property
    def _slots(self):
        return (self.__class__.__name__, self._mount_path, self._read_only)

    @property
    def mount_path(self):
        return self._mount_path

    @property
    def _attached(self):
        return self.__name is not None and self.__parent is not None

    def _require_attached(self):
        assert self._attached, (
            f"GKE volume '{self.__class__.__name__}' is not attached")

    @property
    def _name(self):
        self._require_attached()
        return self.__name

    @property
    def _parent(self):
        self._require_attached()
        return self.__parent

    @property
    @abstractmethod
    def volume(self):
        raise NotImplementedError

    @property
    def mount(self):
        return client.V1VolumeMount(
            name=self._name,
            mount_path=self._mount_path,
            read_only=self._read_only
        )

    def attach(self, wrapped_task):
        # Check to see if volume is already attached
        if self._attached:
            # Sanity check to make sure that volume is attached to correct
            # parent task at the correct mount path. We should really never
            # fail this check.
            assert (
                self._mount_path in self._parent.attached_volumes and
                self._parent.attached_volumes[self._mount_path]._name ==
                self._name
            ), (
                f"Volume '{self._name}' expected to be attached to task "
                f"'{self._parent.pod_name}' at '{self._mount_path}', but "
                f"attachment not found"
            )
            return self

        # Check to see if a volume with the same definition is already attached
        existing_attached_volume = wrapped_task.attached_volumes.get(
            self._mount_path)
        if existing_attached_volume and existing_attached_volume == self:
            return existing_attached_volume

        # Sanity check to make sure that there is not already an attached
        # volume at the specified mount path
        assert existing_attached_volume is None, (
            f"Found existing volume '{existing_attached_volume._name}' at "
            f"mount path: {self._mount_path}"
        )

        # Create a copy of this volume object for the task
        new_instance = copy.deepcopy(self)
        # Set the unique name of newly copied volume
        new_instance.__name = _generate_name(VOLUME_NAME_PREFIX)
        # Set the parent task of this newly copied volume
        new_instance.__parent = wrapped_task
        # And add it to the task's state
        wrapped_task.attached_volumes[new_instance._mount_path] = new_instance
        return new_instance

    def setup(self, api_client=None):
        self._active = True

    def teardown(self, api_client=None):
        self._active = False


class MountableK8SResource(GenericVolume):
    """
    Base class for mounting Kubernetes Secret or ConfigMap objects as volumes
    onto pods.
    """
    def __init__(
            self,
            name,
            mount_path,
            mode='0644'
    ):
        super().__init__(mount_path, read_only=True)
        self._referent_name = name
        self._mode = int(mode, 8)


class SecretVolume(MountableK8SResource):
    @property
    def volume(self):
        return client.V1Volume(
            name=self._name,
            secret=client.V1SecretVolumeSource(
                secret_name=self._referent_name,
                default_mode=self._mode
            )
        )


class ConfigMapVolume(MountableK8SResource):
    @property
    def volume(self):
        return client.V1Volume(
            name=self._name,
            config_map=client.V1ConfigMapVolumeSource(
                name=self._referent_name,
                default_mode=self._mode
            )
        )


class EmptyDirVolume(GenericVolume):
    @property
    def volume(self):
        return client.V1Volume(
            name=self._name,
            empty_dir=client.V1EmptyDirVolumeSource()
        )


class HostPathVolume(GenericVolume):
    def __init__(
            self,
            mount_path,
            read_only=False,
            host_path=None,
            path_type='DirectoryOrCreate'
    ):
        super().__init__(mount_path, read_only)
        self._host_path = host_path or self._mount_path
        self._path_type = path_type

    @property
    def _slots(self):
        return super()._slots + (self._host_path, self._path_type)

    @property
    def volume(self):
        return client.V1Volume(
            name=self._name,
            host_path=client.V1HostPathVolumeSource(
                path=self._host_path,
                type=self._path_type
            )
        )


class PersistentVolume(GenericVolume):
    def __init__(
            self,
            mount_path,
            size,
            read_only=False,
            storage_class_name=DEFAULT_PV_STORAGE_CLASS,
            access_modes=None
    ):
        super().__init__(mount_path, read_only)
        self._size = size
        self._storage_class_name = storage_class_name
        self._access_modes = access_modes or ['ReadWriteOnce']

    @property
    def _slots(self):
        return super()._slots + (
            self._size,
            self._storage_class_name
        )

    @property
    def volume(self):
        return client.V1Volume(
            name=self._name,
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=self._name,
                read_only=self._read_only
            )
        )

    @property
    def _persistent_volume_claim(self):
        # Add some labels to make this PVC easy to find
        labels = {
            COSMOS_IDENTIFIER_LABEL: 'true',
            POD_JOB_ID_LABEL: self._parent.pod_name
        }

        # Construct PVC metadata
        metadata = client.V1ObjectMeta(
            name=self._name,
            labels=labels,
            owner_references=list(self._parent.owner_ref)
        )

        # Construct PVC spec
        spec = client.V1PersistentVolumeClaimSpec(
            access_modes=self._access_modes,
            resources=client.V1ResourceRequirements(
                requests={'storage': self._size}),
            storage_class_name=self._storage_class_name
        )

        # Compile PVC object
        return client.V1PersistentVolumeClaim(
            api_version='v1',
            kind='PersistentVolumeClaim',
            metadata=metadata,
            spec=spec
        )

    def setup(self, api_client=None):
        if not self._active:
            api = client.CoreV1Api(api_client=api_client)
            _k8s_api_wrapper(logger=self._parent.logger)(
                api.create_namespaced_persistent_volume_claim)(
                    namespace=self._parent.options.namespace,
                    body=self._persistent_volume_claim)
        super().setup(api_client)

    def teardown(self, api_client=None):
        if self._active:
            api = client.CoreV1Api(api_client=api_client)
            _k8s_api_wrapper(404, logger=self._parent.logger)(
                api.delete_namespaced_persistent_volume_claim)(
                name=self._name,
                namespace=self._parent.options.namespace
            )
        super().teardown(api_client)


class GcePersistentDisk(GenericVolume):
    def __init__(
            self,
            name,
            mount_path,
            read_only=False,
            fs_type='ext4',
    ):
        super().__init__(mount_path, read_only)
        self._pd_name = name
        self._fs_type = fs_type

    @property
    def _slots(self):
        return super()._slots + (
            self._pd_name,
            self._fs_type
        )

    @property
    def volume(self):
        return client.V1Volume(
            name=self._name,
            gce_persistent_disk=client.V1GCEPersistentDiskVolumeSource(
                pd_name=self._pd_name,
                fs_type=self._fs_type,
                read_only=self._read_only
            )
        )


class FlexVolume(GenericVolume):
    def __init__(
            self,
            driver,
            mount_path,
            read_only=False,
            fs_type='ext4',
            options=None,
            secret_name=None,
    ):
        super().__init__(mount_path, read_only)
        # Name of driver to use for this volume
        self._driver = driver
        # File system type (e.g. ext4)
        self._fs_type = fs_type
        # Options to be passed as params to driver
        self._options = options or {}
        # Name of secret containing values to be passed to driver
        self._secret_name = secret_name

    @property
    def _slots(self):
        return super()._slots + (
            self._driver,
            self._fs_type,
            # Extract values from Dict[str, str] into a tuple to make
            # slots hashable
            tuple(sorted(self._options.items())),
            self._secret_name
        )

    @property
    def volume(self):
        return client.V1Volume(
            name=self._name,
            flex_volume=client.V1FlexVolumeSource(
                driver=self._driver,
                fs_type=self._fs_type,
                options=self._options,
                read_only=self._read_only,
                secret_ref=(
                    client.V1LocalObjectReference(name=self._secret_name)
                    if self._secret_name
                    else None
                )
            )
        )


class WrappedTaskStateVariable(object):
    def __init__(self, name, default=None):
        super().__init__()
        self._name = name
        self._default = default

    def __get__(self, instance, owner):
        value = instance.raw.drm_state.get(self._name)
        if value is None:
            default = (
                self._default()
                if callable(self._default) else
                self._default
            )
            value = instance.raw.drm_state[self._name] = default
        return value

    def __set__(self, instance, value):
        instance.raw.drm_state[self._name] = value


class WrappedTask(object):
    attached_volumes = WrappedTaskStateVariable('attached_volumes', dict)
    scheduled_time = WrappedTaskStateVariable('scheduled_time')
    non_preemptible_conds = WrappedTaskStateVariable(
        'non_preemptible_conds', set)

    def __init__(self, task):
        super().__init__()
        self._raw = task

        # Placeholder for cached options
        self._options = None

    def __repr__(self):
        return repr(self._raw)

    def __str__(self):
        return str(self._raw)

    @property
    def state(self):
        return self._raw.drm_state

    @property
    def logger(self):
        return self._raw.log

    @property
    def raw(self):
        return self._raw

    @property
    def preemptible(self):
        return self.options.preemptible and not self.non_preemptible_conds

    @property
    def options(self):
        return self.compile_options()

    def compile_options(self, validate=False):
        if self._options is not None:
            return self._options

        drm = DRM_Gke

        # Compile a list of DRM options for task
        option_names = drm.required_drm_options | drm.optional_drm_options
        # We probably don't need to do this if the options are correctly
        # specified in the class definition, but it can't hurt.
        option_names.update(drm.drm_options_from_task_properties.keys())

        # Extract usable DRM options from task data
        options = {
            key: value
            for key, value in self._raw.drm_options.items()
            if key in option_names
        }

        # Override DRM options from task properties if necessary
        task_state = sqlalchemy_inspect(self._raw)
        for option_name, task_mapping in (
                drm.drm_options_from_task_properties.items()):
            override_value = (
                task_mapping(self._raw)
                if callable(task_mapping) else
                task_state.dict[task_mapping.key]
            )
            if override_value is not None:
                options[option_name] = override_value

        # Run validations on DRM options, if necessary
        if validate:
            # Warn on unrecognized options
            unrecognized_options = \
                set(self._raw.drm_options.keys()) - option_names
            if unrecognized_options:
                self.logger.warning(
                    f"Ignored {len(unrecognized_options)} unrecognized DRM "
                    f"option(s) for {self._raw}: "
                    f"{', '.join(unrecognized_options)}"
                )

            # Error on missing required options. We may not need to do
            # this since Cosmos should already verify that required options
            # are specified.
            missing_required_options = (
                drm.required_drm_options - set(options.keys()))
            if missing_required_options:
                raise ValueError(
                    f"{len(missing_required_options)} required option(s) not "
                    f"found for {self._raw}: "
                    f"{', '.join(missing_required_options)}"
                )

        self._options = GkeDRMOptions(**options)
        return self._options

    @property
    def volume_defs(self):
        seen_mount_paths = set()

        # Temporarily disabled this given that there is no support in CA for
        # scaling up node pools for pods with "WaitingforFirstConsumer" PVCs.
        # We can get around this by writing our own autoscaler.
        # Scratch disk
        # if self.options.disk is not None:
        #     # TODO (jeev): Add ability to use local SSDs
        #     seen_mount_paths.add(SCRATCH_VOLUME_MOUNT_PATH)
        #     yield PersistentVolume(
        #         SCRATCH_VOLUME_MOUNT_PATH,
        #         self.options.disk
        #     )

        # Additional user-specified volumes
        for v in (self.options.volumes or []):
            if v.mount_path in seen_mount_paths:
                raise ValueError(
                    f"Found volume for {self._raw} with non-unique "
                    f"mount path: {v.mount_path}"
                )
            seen_mount_paths.add(v.mount_path)
            yield v

    @property
    def container_resource_requirements(self):
        # Build resource requirements
        requests = _remove_null_dict(
            cpu=self.options.cpu,
            memory=self.options.memory
        )
        limits = _remove_null_dict(
            cpu=self.options.cpu_limit,
            memory=self.options.memory_limit
        )

        # TODO (jeev): Simplify this to using one set of resource requirements
        # for both requests and limits to achieve Guaranteed QOS for task pods
        # and better predictability of resource usage/management.
        return client.V1ResourceRequirements(requests=requests, limits=limits)

    @property
    def container_volume_mounts(self):
        yield from map(lambda d: d.mount, self.attached_volumes.values())

    @property
    def container_script(self):
        with open(self._raw.output_command_script_path, 'rb') as handle:
            contents = base64.b64encode(handle.read()).decode('utf-8')
        return f'date; echo {contents} | base64 --decode | bash'

    @property
    def container_environment_variables(self):
        # Default environment variables to pass to every container
        # This pod's name
        yield client.V1EnvVar(
            name=PARENT_WORKFLOW_POD_NAME_ENVVAR,
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(
                    field_path='metadata.name')
            )
        )

        # This pod's UID
        yield client.V1EnvVar(
            name=PARENT_WORKFLOW_POD_UID_ENVVAR,
            value_from=client.V1EnvVarSource(
                field_ref=client.V1ObjectFieldSelector(
                    field_path='metadata.uid')
            )
        )

        envvars = self.options.environment or {}
        for envvar_key, envvar_value in envvars.items():
            if isinstance(envvar_value, GenericEnvVar):
                yield client.V1EnvVar(
                    name=envvar_key,
                    value_from=envvar_value.value
                )
            else:
                yield client.V1EnvVar(name=envvar_key, value=envvar_value)

    @property
    def container(self):
        return client.V1Container(
            name=CONTAINER_NAME,
            image=self.options.image,
            image_pull_policy=self.options.image_pull_policy,
            command=['/bin/sh'],
            args=['-c', self.container_script],
            env=list(self.container_environment_variables),
            resources=self.container_resource_requirements,
            volume_mounts=list(self.container_volume_mounts)
        )

    @property
    def owner_ref(self):
        # Check if this process is currently running within a pod
        if os.getenv(KUBERNETES_POD_CHECK_ENVVAR) is None:
            return

        # We need both the name and UID of this pod, if set, to use as the
        # owner reference
        this_pod_name = os.getenv(PARENT_WORKFLOW_POD_NAME_ENVVAR)
        this_pod_uid = os.getenv(PARENT_WORKFLOW_POD_UID_ENVVAR)
        if this_pod_name and this_pod_uid:
            yield client.V1OwnerReference(
                api_version='v1',
                block_owner_deletion=True,
                controller=True,
                kind='Pod',
                name=this_pod_name,
                uid=this_pod_uid
            )

    @property
    def pod_name(self):
        return self._raw.drm_jobID

    def init_pod_name(self):
        self._raw.drm_jobID = self._generate_name()

    def _generate_name(self):
        # Use the user-specified name if provided
        if self.options.name:
            return self.options.name

        # Use the user-specified name prefix if provided. Otherwise, use the
        # default pod name prefix with the task stage name, if possible.
        prefix = self.options.name_prefix
        if not prefix:
            parts = [DEFAULT_NAME_PREFIX]
            if self._raw.stage:
                parts.append(self._raw.stage.name.lower().replace('_', '-'))
            prefix = '-'.join(parts)

        return _generate_name(prefix)

    @property
    def pod_metadata(self):
        pod_labels = self.options.labels or {}

        # Add cosmos parent/controller labels if they exist
        this_pod_name = os.getenv(PARENT_WORKFLOW_POD_NAME_ENVVAR)
        this_pod_uid = os.getenv(PARENT_WORKFLOW_POD_UID_ENVVAR)
        if this_pod_name and this_pod_uid:
            pod_labels[COSMOS_CONTROLLER_NAME_LABEL] = this_pod_name
            pod_labels[COSMOS_CONTROLLER_UID_LABEL] = this_pod_uid

        # We set a label to indicate that the created pod is associated with a
        # cosmos task
        pod_labels[COSMOS_IDENTIFIER_LABEL] = 'true'
        # Extract some usable labels from the task object. Labels that are
        # not compliant are marked as invalid.
        # Set the task stage name as a label, if possible
        if self._raw.stage:
            pod_labels[COSMOS_TASK_STAGE_LABEL] = _kube_label(
                self._raw.stage.name)
        # Set the task UID as a label
        pod_labels[COSMOS_TASK_UID_LABEL] = _kube_label(self._raw.uid)
        # We set this to make filtering pods by name easier. There are more
        # powerful operators for label selectors than field selectors.
        pod_labels[POD_JOB_ID_LABEL] = self.pod_name

        return client.V1ObjectMeta(
            annotations=POD_ANNOTATIONS,
            name=self.pod_name,
            labels=pod_labels,
            owner_references=list(self.owner_ref)
        )

    @property
    def pod_volumes(self):
        yield from map(lambda d: d.volume, self.attached_volumes.values())

    @property
    def pod_node_selectors(self):
        # Hard requirement to schedule non-preemptible pods on non-preemptible
        # nodes.
        if not self.preemptible:
            yield client.V1NodeSelectorRequirement(
                key=PREEMPTIBLE_NODE_LABEL,
                operator='DoesNotExist'
            )

        # Handle remaining node selectors
        data = self.options.node_selectors or {}
        # Set the node partition for this task, if necessary
        if self.options.partition:
            data[NODE_PARTITION_LABEL_KEY] = self.options.partition

        for key, value in data.items():
            yield client.V1NodeSelectorRequirement(
                key=key,
                operator='In' if value is not None else 'Exists',
                values=[value] if value is not None else []
            )

    @property
    def pod_scheduling_preferences(self):
        # Preemptible pods are preferred to be scheduled on preemptible
        # nodes but may be scheduled on non-preemptible nodes if the former is
        # not available.
        # See https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#node-affinity
        if self.preemptible:
            yield client.V1PreferredSchedulingTerm(
                preference=client.V1NodeSelectorTerm(
                    match_expressions=[
                        client.V1NodeSelectorRequirement(
                            key=PREEMPTIBLE_NODE_LABEL,
                            operator='Exists'
                        )
                    ]
                ),
                weight=100
            )

    @property
    def pod_affinity(self):
        # Compile pod scheduling requirements
        node_selectors = list(self.pod_node_selectors)
        pod_scheduling_requirements = (
            client.V1NodeSelector(
                node_selector_terms=[
                    client.V1NodeSelectorTerm(match_expressions=node_selectors)
                ]
            ) if node_selectors else None
        )

        return client.V1Affinity(
            node_affinity=client.V1NodeAffinity(
                preferred_during_scheduling_ignored_during_execution=list(
                    self.pod_scheduling_preferences),
                required_during_scheduling_ignored_during_execution=
                pod_scheduling_requirements
            )
        )

    @property
    def pod_tolerations(self):
        # User-specified tolerations
        user_specified = self.options.tolerations or []
        for toleration in user_specified:
            yield client.V1Toleration(
                effect=toleration.effect.value,
                key=toleration.key,
                operator='Equal' if toleration.value is not None else 'Exists',
                value=toleration.value
            )

    @property
    def pod_spec(self):
        return client.V1PodSpec(
            restart_policy='Never',
            containers=[self.container],
            volumes=list(self.pod_volumes),
            affinity=self.pod_affinity,
            tolerations=list(self.pod_tolerations),
            dns_policy=self.options.dns_policy,
            host_network=self.options.host_network,
            service_account_name=self.options.service_account
        )

    @property
    def pod(self):
        return client.V1Pod(
            api_version='v1',
            kind='Pod',
            metadata=self.pod_metadata,
            spec=self.pod_spec
        )

    def submit(self, api_client=None):
        # Validate DRM options
        self.compile_options(validate=True)

        # Set the time that the pod was scheduled on the cluster,
        # if necessary
        now = datetime.utcnow()
        if self.scheduled_time is None:
            self.scheduled_time = now
        # Determine if this task should be scheduled on a non-preemptible
        # node. If a task was previously scheduled and is being resubmitted,
        # we assume that it was probably lost by the DRM and needs to be
        # requeued. If it has been more than PREEMPTIBLE_START_GRACE_PERIOD
        # since it was last scheduled, then we set the
        # OUT_OF_START_GRACE_PERIOD condition that forces it to be scheduled
        # on a non-preemptible node to maximize possibility of completion. This
        # condition is a temporary one, and is reset when the task completes or
        # times out.
        # Note: scheduled_time state variable is set when creating a pod
        # object on the cluster and unset when the pod runs to completion
        # or times out.
        elif (
                self.options.preemptible and (
                    (now - self.scheduled_time) >
                    PREEMPTIBLE_START_GRACE_PERIOD
                )
        ):
            self.non_preemptible_conds.add(
                NonPreemptibleCondition.OUT_OF_START_GRACE_PERIOD)

        # (Re)set pod name
        self.init_pod_name()

        # Setup volumes here
        for v in self.volume_defs:
            v.attach(self).setup(api_client=api_client)

        # Finally submit the pod to the cluster
        api = client.CoreV1Api(api_client=api_client)
        _k8s_api_wrapper(logger=self.logger)(api.create_namespaced_pod)(
            namespace=self.options.namespace,
            body=self.pod
        )
        self._raw.status = TaskStatus.submitted

    def heartbeat(self, cnt_status):
        # If a preemptible job is determined to be long running based on its
        # last known runtime, we set the LONG_RUNNING_JOB condition such that
        # the next time the pod needs to be requeued, it will be scheduled on
        # a non-preemptible node. This is a permanent condition and does not
        # reset when the pod completes. As such, if it fails and needs to be
        # retried, the DRM already knows that this pod will take a long time
        # to run, and proactively schedules it on a non-preemptible node.
        if (
                self.options.preemptible and
                NonPreemptibleCondition.LONG_RUNNING_JOB not in
                self.non_preemptible_conds and
                cnt_status.wall_time is not None and
                cnt_status.wall_time >
                LONG_RUNNING_JOB_THRESHOLD
        ):
            self.non_preemptible_conds.add(
                NonPreemptibleCondition.LONG_RUNNING_JOB)

    def complete(self):
        # Unset the time any associated resources were scheduled if the task
        # ran to completion.
        self.scheduled_time = None

        # Reset temporary non-preemptible condition
        for cond in TEMPORARY_NON_PREEMPTIBLE_CONDS:
            self.non_preemptible_conds.discard(cond)

    def populate_logs(self, output, api_client=None):
        # Check if we actually need to collect logs for this task
        if not self.options.collect_logs:
            # Create an empty file at the appropriate path, since cosmos
            # might expect one there.
            open(output, 'w').close()
            return

        # Try to retrieve the pod name associated with this task. If it has
        # not been set, then we cannot proceed.
        if self.pod_name is None:
            return

        # Fetch logs for the associated pod
        api = client.CoreV1Api(api_client=api_client)

        # Make sure that the logs destination directory exists
        os.makedirs(os.path.dirname(output), exist_ok=True)

        # Wrapper for collecting task logs
        @_k8s_api_wrapper(logger=self.logger)
        def _inner():
            resp = api.read_namespaced_pod_log(
                name=self.pod_name,
                namespace=self.options.namespace,
                _preload_content=False,
                _request_timeout=(
                    LOGS_CONNECTION_TIMEOUT.total_seconds(),
                    LOGS_READ_TIMEOUT.total_seconds()
                )
            )

            # Write out logs to file. Unfortunately, we cannot split log stream
            # into stdout and stderr. Therefore, we will write all logs to the
            # same path
            with open(output, 'wb') as out_handle:
                shutil.copyfileobj(resp, out_handle, LOGS_STREAM_CHUNK_SIZE)

            # Release the connection back to the pool
            resp.release_conn()

        # Redirect HTTP exceptions while fetching logs as GetOutputError to be
        # properly handled by Cosmos
        try:
            _inner()
        except Exception as e:
            if isinstance(e, (ApiException, MaxRetryError, TimeoutError)):
                raise GetOutputError(
                    f'Error fetching logs for {self.pod_name}: {str(e)}')
            else:
                raise

    def cleanup(self, api_client=None):
        # Try to retrieve the pod name associated with this task. If it has
        # not been set, then we cannot proceed.
        if self.pod_name is None:
            return

        # Delete the pod associated with this task
        api = client.CoreV1Api(api_client=api_client)
        _k8s_api_wrapper(404, logger=self.logger)(api.delete_namespaced_pod)(
            name=self.pod_name,
            namespace=self.options.namespace
        )

        # Teardown and clean up attached volumes
        for v in self.attached_volumes.values():
            v.teardown(api_client=api_client)
        self.attached_volumes.clear()


class ContainerStatus(NamedTuple):
    exit_status: Optional[int] = None
    wall_time: Optional[timedelta] = None


class ContainerReadyCondition(NamedTuple):
    # Flag indicating if container is ready
    status: bool
    # Time spent in current state
    duration: timedelta


class DRM_Gke(DRM):
    """
    DRM to dispatch Cosmos tasks on a GKE Kubernetes cluster.
    """

    name = 'gke'

    # Interval at which to poll Kubernetes API server for task statuses
    poll_interval = 5

    # DRM options for running a task
    required_drm_options = REQUIRED_FIELDS
    optional_drm_options = OPTIONAL_FIELDS

    # DRM options to be overridden from task properties
    drm_options_from_task_properties = {
        # Translate cosmos memory requirements (in Megabytes)
        'memory': lambda task: (
            f'{task.mem_req}M'
            if task.mem_req is not None
            else None
        ),
        'cpu': Task.core_req,
        # Translate time requirements from minutes in Cosmos to a timedelta
        # object
        'timeout': lambda task: (
            timedelta(minutes=task.time_req)
            if task.time_req is not None
            else None
        ),
        'partition': Task.queue
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Placeholder for Kubernetes API client object
        self._api_client = None

    @property
    def api_client(self):
        if self._api_client is None:
            # Initialize Kubernetes API client
            # Try loading an in-cluster configuration, and fall back to a local
            # configuration
            try:
                config.load_incluster_config()
                self._api_client = client.ApiClient()
            except config.ConfigException:
                client_config = type.__call__(client.Configuration)
                config.load_kube_config(client_configuration=client_config)
                self._api_client = client.ApiClient(
                    configuration=client_config)
            finally:
                assert self._api_client is not None, (
                    'Error loading Kubernetes configuration.')

        return self._api_client

    def _group_by_namespace(self, *tasks):
        wrapped_tasks = [WrappedTask(t) for t in tasks]
        for namespace, subset in groupby2(
                wrapped_tasks, lambda t: t.options.namespace):
            yield namespace, list(subset)

    def _fetch_pod_statuses(self, *tasks):
        for namespace, wrapped_tasks in self._group_by_namespace(*tasks):
            # Validate that tasks have pods associated with them
            errors = list(
                filter(lambda t: t.pod_name is None, wrapped_tasks))
            assert not errors, (
                "Error trying to fetch status for tasks with no "
                f"associated pods: {', '.join(errors)}"
            )

            # Fetch statuses for desired tasks
            api = client.CoreV1Api(api_client=self.api_client)
            pods = {
                p.metadata.name: p.status
                for p in _k8s_api_wrapper()(api.list_namespaced_pod)(
                    namespace=namespace,
                    label_selector=(
                        f"{POD_JOB_ID_LABEL} in "
                        f"({','.join(t.pod_name for t in wrapped_tasks)})"
                    )
                ).items
            }

            for t in wrapped_tasks:
                yield t, pods.get(t.pod_name)

    @staticmethod
    def _parse_cnt_ready_cond(pod_status):
        # Container conditions not updated yet. Skip for now...
        if pod_status.conditions is None:
            return

        # Find the container ready condition
        for condition in pod_status.conditions:
            if not condition.type == 'ContainersReady':
                continue

            # Skip handling of Unknown status - wait for the cluster to resolve
            # this.
            if condition.status == 'Unknown':
                return

            # Parse the amount of time spent in current status
            elapsed_since_last_transition = (
                datetime.utcnow() -
                condition.last_transition_time.replace(tzinfo=None)
            )
            return ContainerReadyCondition(
                status=condition.status == 'True',
                duration=elapsed_since_last_transition
            )

    @staticmethod
    def _parse_cnt_status(pod_status):
        # Container status not updated yet. Skip for now...
        if pod_status.container_statuses is None:
            return

        cnt_state = pod_status.container_statuses[0].state

        # Parse container termination status
        done = cnt_state.terminated
        if done is not None and done.finished_at is not None:
            wall_time = done.finished_at - done.started_at
            return ContainerStatus(
                exit_status=done.exit_code,
                wall_time=wall_time
            )

        # Parse running container status
        running = cnt_state.running
        if running is not None and running.started_at is not None:
            # Container does not have an exit code, just report how long
            # it has been running for
            wall_time = (
                datetime.utcnow() - running.started_at.replace(tzinfo=None))
            return ContainerStatus(wall_time=wall_time)

    def _fetch_completed_tasks(self, *tasks):
        for wrapped_task, pod_status in self._fetch_pod_statuses(*tasks):
            # Handle case where pod was lost due to node failure or similar
            # problems.
            if pod_status is None:
                wrapped_task.logger.debug(
                    f"Could not find pod '{wrapped_task.pod_name}' associated "
                    f"with {wrapped_task}"
                )
                # We yield a null exit status to indicate to Cosmos to
                # simply requeue the job
                yield wrapped_task, {'exit_status': None}
                continue

            # Parse container ready condition
            cnt_ready_cond = self._parse_cnt_ready_cond(pod_status)

            # Parse completion status from pod container status
            cnt_status = self._parse_cnt_status(pod_status)

            # Run hooks associated with task heartbeat
            if cnt_status is not None:
                wrapped_task.heartbeat(cnt_status)

            # Pod ran to completion
            if cnt_status and cnt_status.exit_status is not None:
                wrapped_task.complete()
                yield wrapped_task, {
                    'exit_status': cnt_status.exit_status,
                    'wall_time': cnt_status.wall_time.total_seconds()
                }

            # Pod timed out
            elif (
                    wrapped_task.options.timeout is not None and
                    cnt_status and
                    cnt_status.wall_time is not None and
                    cnt_status.wall_time > wrapped_task.options.timeout
            ):
                wrapped_task.logger.warning(
                    f"Pod '{wrapped_task.pod_name}' associated with "
                    f"{wrapped_task} has exceeded deadline of "
                    f"{wrapped_task.options.timeout}. Timing out..."
                )
                wrapped_task.complete()
                # Cosmos expects a specific exit code to mark the job as
                # timed out
                yield wrapped_task, {'exit_status': TIMED_OUT_EXIT_STATUS}

            # Pod failed for some other k8s or infrastructure-specific
            # reason
            elif pod_status.phase == 'Failed':
                wrapped_task.logger.warning(
                    f"Pod '{wrapped_task.pod_name}' associated with "
                    f"{wrapped_task} failed as {pod_status.reason}: "
                    f"{pod_status.message}"
                )
                yield wrapped_task, {'exit_status': None}

            # Pod container stuck in not ready status for too long
            # We are observing pods being stuck in ContainerCreating for
            # many hours with the following error:
            # RunPodSandbox from runtime service failed: rpc error: code = Unknown desc = failed to create a sandbox for pod...:operation timeout: context deadline exceeded
            # Sometimes, this may resolve within 10 minutes, but other times
            # remains stuck until pods are manually deleted. In the following,
            # we give the pod container 20 minutes to start, or kill and
            # requeue the job.
            elif (
                cnt_ready_cond and
                not cnt_ready_cond.status and
                cnt_ready_cond.duration > CONTAINER_READY_GRACE_PERIOD
            ):
                wrapped_task.logger.warning(
                    f"Container in pod '{wrapped_task.pod_name}' associated "
                    f"with {wrapped_task} not ready after "
                    f"{cnt_ready_cond.duration}"
                )
                yield wrapped_task, {'exit_status': None}


    def submit_job(self, task):
        WrappedTask(task).submit(api_client=self.api_client)

    def filter_is_done(self, tasks):
        for wrapped_task, status in self._fetch_completed_tasks(*tasks):
            yield wrapped_task.raw, status

    def drm_statuses(self, tasks):
        return {
            wrapped_task.pod_name: (
                pod_status.reason
                if pod_status else
                GKE_POD_UNKNOWN_STATUS
            )
            for wrapped_task, pod_status in self._fetch_pod_statuses(*tasks)
        }

    def populate_logs(self, task):
        WrappedTask(task).populate_logs(
            output=task.output_stderr_path,
            api_client=self.api_client
        )

    def kill(self, task):
        self.cleanup_task(task)

    def cleanup_task(self, task):
        WrappedTask(task).cleanup(api_client=self.api_client)
