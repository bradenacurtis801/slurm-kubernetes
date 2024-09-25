# config/slurm_config.py

import logging
import os



logger = logging.getLogger(__name__)

class SlurmConfig:
    def __init__(self):
        self.cluster_name = os.getenv("SLURM_CLUSTER_NAME", "dev_cluster")
        self.slurmctld_host = os.getenv("SLURMCTLD_HOST", "localhost")
        self.proctrack_type = os.getenv("SLURM_PROCTRACK_TYPE", "proctrack/linuxproc")
        self.prolog_flags = os.getenv("SLURM_PROLOG_FLAGS", "Contain")
        self.return_to_service = int(os.getenv("SLURM_RETURN_TO_SERVICE", 1))
        self.slurmctld_pid_file = os.getenv("SLURMCTLD_PID_FILE", "/var/run/slurmctld.pid")
        self.slurmd_pid_file = os.getenv("SLURMD_PID_FILE", "/var/run/slurmd.pid")
        self.slurmctld_port = int(os.getenv("SLURMCTLD_PORT", 6817))
        self.slurmd_port = int(os.getenv("SLURMD_PORT", 6818))
        self.slurmd_spool_dir = os.getenv("SLURMD_SPOOL_DIR", "/var/spool/slurmd")
        self.slurmd_user = os.getenv("SLURMD_USER", "root")
        self.state_save_location = os.getenv("SLURM_STATE_SAVE_LOCATION", "/var/spool/slurmctld")
        self.task_plugin = os.getenv("SLURM_TASK_PLUGIN", "task/none")
        self.auth_type = os.getenv("SLURM_AUTH_TYPE", "auth/munge")
        self.gres_types = os.getenv("SLURM_GRES_TYPES", "gpu")
        self.partition_name = os.getenv("SLURM_PARTITION_NAME", "debug")
        self.partition_nodes = os.getenv("SLURM_PARTITION_NODES", "ALL")
        self.partition_default = os.getenv("SLURM_PARTITION_DEFAULT", "YES")
        self.partition_max_time = os.getenv("SLURM_PARTITION_MAX_TIME", "INFINITE")
        self.partition_state = os.getenv("SLURM_PARTITION_STATE", "UP")
        self.nodes = []

    def add_node(
        self,
        node_name,
        node_addr,
        sockets,
        cores_per_socket,
        threads_per_core,
        real_memory,
        gres,
    ):
        self.remove_node(node_name)  # Ensure no duplicate nodes
        node_entry = (
            f"NodeName={node_name} "
            f"NodeAddr={node_addr} "
            f"Sockets={sockets} "
            f"CoresPerSocket={cores_per_socket} "
            f"ThreadsPerCore={threads_per_core} "
            f"RealMemory={real_memory} "
            f"State=UNKNOWN "
            f"Gres={gres}"
        )
        self.nodes.append(node_entry)
        logger.info(f"Node added: {node_entry}")

    def remove_node(self, node_name):
        self.nodes = [
            node for node in self.nodes if not node.startswith(f"NodeName={node_name}")
        ]
        logger.info(f"Node removed: {node_name}")

    def generate_conf(self):
        conf = (
            f"ClusterName={self.cluster_name}\n"
            f"SlurmctldHost={self.slurmctld_host}\n"
            f"ProctrackType={self.proctrack_type}\n"
            f"PrologFlags={self.prolog_flags}\n"
            f"ReturnToService={self.return_to_service}\n"
            f"SlurmctldPidFile={self.slurmctld_pid_file}\n"
            f"SlurmdPidFile={self.slurmd_pid_file}\n"
            f"SlurmctldPort={self.slurmctld_port}\n"
            f"SlurmdPort={self.slurmd_port}\n"
            f"SlurmdSpoolDir={self.slurmd_spool_dir}\n"
            f"SlurmdUser={self.slurmd_user}\n"
            f"StateSaveLocation={self.state_save_location}\n"
            f"TaskPlugin={self.task_plugin}\n"
            f"AuthType={self.auth_type}\n"
            f"GresTypes={self.gres_types}\n"
            f"PartitionName={self.partition_name} Nodes={self.partition_nodes} Default={self.partition_default} MaxTime={self.partition_max_time} State={self.partition_state}\n"
        )
        for node in self.nodes:
            conf += f"{node}\n"
        return conf
