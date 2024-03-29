export const METRIC_GROUPS = {
    'Host Metrics': [
        'total_overcommitted_burst',
        'total_power_draw',
        'total_failure_vm_slices',
        'total_granted_burst',
        'total_interfered_burst',
        'total_requested_burst',
        'mean_cpu_usage',
        'mean_cpu_demand',
        'mean_num_deployed_images',
        'max_num_deployed_images',
    ],
    'Compute Service Metrics': ['total_vms_submitted', 'total_vms_queued', 'total_vms_finished', 'total_vms_failed'],
}

export const AVAILABLE_METRICS = [
    'mean_cpu_usage',
    'mean_cpu_demand',
    'total_requested_burst',
    'total_granted_burst',
    'total_overcommitted_burst',
    'total_interfered_burst',
    'total_power_draw',
    'total_failure_vm_slices',
    'mean_num_deployed_images',
    'max_num_deployed_images',
    'total_vms_submitted',
    'total_vms_queued',
    'total_vms_finished',
    'total_vms_failed',
]

export const METRIC_NAMES_SHORT = {
    total_overcommitted_burst: 'Overcomm. CPU Cycles',
    total_granted_burst: 'Granted CPU Cycles',
    total_requested_burst: 'Requested CPU Cycles',
    total_interfered_burst: 'Interfered CPU Cycles',
    total_power_draw: 'Total Power Consumption',
    mean_cpu_usage: 'Mean Host CPU Usage',
    mean_cpu_demand: 'Mean Host CPU Demand',
    mean_num_deployed_images: 'Mean Num. Deployed Images Per Host',
    max_num_deployed_images: 'Max. Num. Deployed Images Per Host',
    total_failure_vm_slices: 'Total Num. Failed VM Slices',
    total_vms_submitted: 'Total Num. VMs Submitted',
    total_vms_queued: 'Max. Num. VMs Queued',
    total_vms_finished: 'Max. Num. VMs Finished',
    total_vms_failed: 'Max. Num. VMs Failed',
}

export const METRIC_NAMES = {
    total_overcommitted_burst: 'Overcommitted CPU Cycles',
    total_granted_burst: 'Granted CPU Cycles',
    total_requested_burst: 'Requested CPU Cycles',
    total_interfered_burst: 'Interfered CPU Cycles',
    total_power_draw: 'Total Power Consumption',
    mean_cpu_usage: 'Mean Host CPU Usage',
    mean_cpu_demand: 'Mean Host CPU Demand',
    mean_num_deployed_images: 'Mean Number of Deployed Images Per Host',
    max_num_deployed_images: 'Maximum Number Deployed Images Per Host',
    total_failure_vm_slices: 'Total Number Failed VM Slices',
    total_vms_submitted: 'Total Number VMs Submitted',
    total_vms_queued: 'Maximum Number VMs Queued',
    total_vms_finished: 'Maximum Number VMs Finished',
    total_vms_failed: 'Maximum Number VMs Failed',
}

export const METRIC_UNITS = {
    total_overcommitted_burst: 'MFLOP',
    total_granted_burst: 'MFLOP',
    total_requested_burst: 'MFLOP',
    total_interfered_burst: 'MFLOP',
    total_power_draw: 'Wh',
    mean_cpu_usage: 'MHz',
    mean_cpu_demand: 'MHz',
    mean_num_deployed_images: 'VMs',
    max_num_deployed_images: 'VMs',
    total_failure_vm_slices: 'VM Slices',
    total_vms_submitted: 'VMs',
    total_vms_queued: 'VMs',
    total_vms_finished: 'VMs',
    total_vms_failed: 'VMs',
}

export const METRIC_DESCRIPTIONS = {
    total_overcommitted_burst:
        'The total CPU clock cycles lost due to overcommitting of resources. This metric is an indicator for resource overload.',
    total_requested_burst: 'The total CPU clock cycles that were requested by all virtual machines.',
    total_granted_burst: 'The total CPU clock cycles executed by the hosts.',
    total_interfered_burst: 'The total CPU clock cycles lost due to resource interference between virtual machines.',
    total_power_draw: 'The average power usage in watts.',
    mean_cpu_usage: 'The average amount of CPU clock cycles consumed by all virtual machines on a host.',
    mean_cpu_demand: 'The average amount of CPU clock cycles requested by all powered on virtual machines on a host.',
    mean_num_deployed_images: 'The average number of virtual machines deployed on a host.',
    max_num_deployed_images: 'The maximum number of virtual machines deployed at any time.',
    total_failure_vm_slices: 'The total amount of CPU clock cycles lost due to failure.',
    total_vms_submitted: 'The total number of virtual machines scheduled by the compute service.',
    total_vms_queued:
        'The maximum number of virtual machines waiting to be scheduled by the compute service at any point.',
    total_vms_finished: 'The total number of virtual machines that completed successfully.',
    total_vms_failed: 'The total number of virtual machines that failed during execution.',
}
