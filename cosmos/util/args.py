import sys

from cosmos.job.drm.DRM_Base import DRM


def get_last_cmd_executed():
    cmd_args = [a if ' ' not in a else "'" + a + "'" for a in sys.argv[1:]]
    return ' '.join([sys.argv[0]] + cmd_args)


def max_cores(value):
    try:
        return float(value)
    except ValueError:
        pass

    valid_drms = {DRM_sub_cls.name for DRM_sub_cls in DRM.__subclasses__()}
    max_cores_dict = {}
    for pair in value.strip().split(','):
        split_pair = pair.split('=')
        if len(split_pair) != 2:
            raise ValueError
        drm, limit = split_pair
        if drm not in valid_drms:
            raise ValueError
        max_cores_dict[drm] = float(limit)
    return max_cores_dict


def add_workflow_args(p, require_name=True):
    p.add_argument('--name', '-n', help="A name for this workflow", required=require_name)
    p.add_argument('--max_cores', '--max-cores', '-c', type=max_cores,
                   help="Maximum number of cores to use. A single float value may be provided as the core limit for the default DRM. Core limits may also be specified for a specific DRM (e.g. local=2) or for multiple DRMs as a comma-delimited string (e.g. local=2,gke=1000)", default=None)
    p.add_argument('--restart', '-r', action='store_true',
                   help="Completely restart the workflow.  Note this will delete all record of the workflow in the database")
    p.add_argument('--skip_confirm', '--skip-confirm', '-y', action='store_true',
                   help="Do not use confirmation prompts before restarting or deleting, and assume answer is always yes")
    p.add_argument('--fail-fast', '--fail_fast', action='store_true',
                   help="terminate the entire workflow the first time a Task fails")
