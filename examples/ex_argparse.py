"""
Tested with
Python 3.6 only

py_call() allows you to use python functions as Tasks functions.  It will autogenerate a python script
which imports the functions, and calls it with the params and use that for the command_script.
"""

import argparse
import os

from cosmos.api import Cosmos, py_call, add_workflow_args


def say(text, out_file):
    with open(out_file, 'w') as fp:
        print(text, file=fp)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    add_workflow_args(p)
    args = p.parse_args()

    cosmos = Cosmos('sqlite:///%s/sqlite.db' % os.path.dirname(os.path.abspath(__file__)),
                    default_drm='local')
    cosmos.initdb()
    workflow = cosmos.start(
        args.name,
        restart=args.restart,
        skip_confirm=args.skip_confirm
    )

    t = workflow.add_task(func=py_call(say),
                          params=dict(text='Hello World', out_file='out.txt'),
                          uid='my_task')

    workflow.make_output_dirs()
    print(args.max_cores)
    workflow.run(max_cores=args.max_cores)
    print(workflow.max_cores)
