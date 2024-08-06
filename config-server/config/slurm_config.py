# config/slurm_config.py

import logging

logger = logging.getLogger(__name__)

class SlurmConfig:
    def __init__(self, cluster_name="dev_cluster", slurmctld_host="localhost"):
        self.cluster_name = cluster_name
        self.slurmctld_host = slurmctld_host
        self.proctrack_type = "proctrack/linuxproc"
        self.prolog_flags = "Contain"
        self.return_to_service = 1
        self.slurmctld_pid_file = "/var/run/slurmctld.pid"
        self.slurmd_pid_file = "/var/run/slurmd.pid"
        self.slurmctld_port = 6817
        self.slurmd_port = 6818
        self.slurmd_spool_dir = "/var/spool/slurmd"
        self.slurmd_user = "root"
        self.state_save_location = "/var/spool/slurmctld"
        self.task_plugin = "task/none"
        self.auth_type = "auth/munge"
        self.gres_types = "gpu"
        self.partition_name = "debug"
        self.partition_nodes = "ALL"
        self.partition_default = "YES"
        self.partition_max_time = "INFINITE"
        self.partition_state = "UP"
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
