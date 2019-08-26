#!/usr/bin/env python3

import random

from cosmos.api import Cosmos
from cosmos.job.drm.drm_gke import (
    EmptyDirVolume,
    SecretVolume,
    ConfigMapVolume,
    ValueFromSecret,
    ValueFromConfigMap,
    PodToleration,
    PodTolerationEffect
)


def say(text):
    return r"""
        echo "{text}"
    """.format(text=text)


def long_running(time):
    return r"""
        sleep {time}
    """.format(time=time)


def random_failure(value):
    return """
        [ $((1 + RANDOM % 10)) -ge {value} ]
    """.format(value=value)


if __name__ == '__main__':
    cosmos = Cosmos(
        'sqlite:///sqlite.db',
        default_drm='gke',
        default_max_attempts=3,
        default_drm_options={
            'image': 'frolvlad/alpine-bash:latest',
            'environment': {
                'SLACK_TOKEN': ValueFromSecret('balrog', 'slack-token'),
                'SLACK_CHANNEL': ValueFromConfigMap(
                    'balrog',
                    'balrog-notifications-slack-channel'
                )
            },
            'volumes': [
                EmptyDirVolume('/scratch'),
                SecretVolume(
                    'balrog-gcp-serviceaccount',
                    '/var/google/secret'
                ),
                ConfigMapVolume('balrog', '/var/google/config')
            ],
            'preemptible': True,
            # 'partition': 'test',
            'tolerations': [
                PodToleration('test', 'true', PodTolerationEffect.NO_SCHEDULE)
            ]
        }
    )
    cosmos.initdb()
    workflow = cosmos.start('Example1', restart=True, skip_confirm=True)

    for idx in range(50):
        workflow.add_task(
            func=say,
            params=dict(text=f'Hello World from local worker {idx+1}'),
            uid=f'local_say_task_{idx}',
            core_req=1,
            drm='local'
        )

    workflow.add_task(
        func=say,
        params=dict(text='Hello World from gke',),
        uid='gke_say_task',
        core_req=1,
    )

    b_tasks = []
    for idx in range(3):
        b = workflow.add_task(
            func=long_running,
            params=dict(time=random.randint(0, 120),),
            uid=f'long_running_task_{idx}',
            core_req=0.1,
            mem_req=256,
            time_req=1,
            drm_options={
                'cpu_limit': 0.2
            }
        )
        b_tasks.append(b)

    c = workflow.add_task(
        func=say,
        params=dict(text='foobar',),
        parents=b_tasks,
        uid='noop_say',
        core_req=0.1,
        mem_req=256,
        noop=True
    )

    d = workflow.add_task(
        func=random_failure,
        params=dict(value=7,),
        parents=b_tasks,
        uid='random_failure',
        core_req=0.1,
        mem_req=256,
        max_attempts=3
    )

    workflow.run(max_cores={'local': 4})
