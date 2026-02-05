"""
GPU Datacenter Topology Generator.

Generates realistic spine-leaf GPU cluster topology data for TurboBulk examples.
Designed to scale to ~200K cables for performance testing.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Any
import json


@dataclass
class GPUDatacenterTopology:
    """
    GPU datacenter spine-leaf topology configuration.

    This class generates deterministic device and cable data for
    spine-leaf GPU cluster architectures. The topology is designed
    to represent realistic AI/ML datacenter deployments.

    Architecture:
        - Spine switches: High-radix switches at the top of the fabric
        - Leaf switches: ToR switches connecting to GPU servers
        - GPU servers: High-density GPU compute nodes (like DGX)
        - Each GPU server has multiple high-speed NICs

    Example topology (default):
        8 pods × 4 spines = 32 spine switches
        8 pods × 32 leaves = 256 leaf switches
        256 leaves × 16 servers = 4,096 GPU servers
        4,096 servers × 8 NICs = 32,768 server connections

    Scaling to ~200K cables:
        - Increase pods to 16 or more
        - Or increase nics_per_gpu_server
        - Or add management/console cabling
    """

    # Topology dimensions
    pods: int = 8
    spines_per_pod: int = 4
    leaves_per_pod: int = 32
    gpu_servers_per_leaf: int = 16
    nics_per_gpu_server: int = 8

    # Port counts
    spine_ports: int = 64  # Total ports per spine
    leaf_ports: int = 64   # Total ports per leaf

    # Naming prefix for filtering
    prefix: str = 'gpu-dc'

    # Interface types
    spine_interface_type: str = '400gbase-x-qsfpdd'
    leaf_interface_type: str = '400gbase-x-qsfpdd'
    server_interface_type: str = '400gbase-x-qsfpdd'

    # Cable types
    fabric_cable_type: str = 'mmf-om4'  # Multi-mode fiber OM4
    fabric_cable_status: str = 'connected'

    @property
    def total_spines(self) -> int:
        """Total spine switches."""
        return self.pods * self.spines_per_pod

    @property
    def total_leaves(self) -> int:
        """Total leaf switches."""
        return self.pods * self.leaves_per_pod

    @property
    def total_gpu_servers(self) -> int:
        """Total GPU servers."""
        return self.total_leaves * self.gpu_servers_per_leaf

    @property
    def total_devices(self) -> int:
        """Total devices."""
        return self.total_spines + self.total_leaves + self.total_gpu_servers

    @property
    def spine_downlinks(self) -> int:
        """Downlinks per spine to leaves."""
        return self.spine_ports  # All ports connect to leaves

    @property
    def leaf_uplinks(self) -> int:
        """Uplinks per leaf to spines."""
        return self.spines_per_pod * 2  # Redundant connections to each spine

    @property
    def leaf_downlinks(self) -> int:
        """Downlinks per leaf to servers."""
        return self.leaf_ports - self.leaf_uplinks

    @property
    def estimated_cables(self) -> int:
        """Estimated total cable count."""
        # Spine-to-leaf cables
        spine_leaf = self.total_leaves * self.leaf_uplinks

        # Leaf-to-server cables
        leaf_server = self.total_gpu_servers * self.nics_per_gpu_server

        return spine_leaf + leaf_server

    def generate_device_types(self) -> List[Dict[str, Any]]:
        """
        Generate device type definitions.

        Returns list of device type dicts for spine, leaf, and GPU server.
        """
        return [
            {
                'name': f'{self.prefix}-spine-64x400g',
                'manufacturer': self.prefix,  # Will need to resolve to ID
                'model': 'Spine-64x400G',
                'slug': f'{self.prefix}-spine-64x400g',
                'u_height': 2,
            },
            {
                'name': f'{self.prefix}-leaf-64x400g',
                'manufacturer': self.prefix,
                'model': 'Leaf-64x400G',
                'slug': f'{self.prefix}-leaf-64x400g',
                'u_height': 1,
            },
            {
                'name': f'{self.prefix}-gpu-8x400g',
                'manufacturer': self.prefix,
                'model': 'GPU-8x400G',
                'slug': f'{self.prefix}-gpu-8x400g',
                'u_height': 6,  # Typical GPU server height
            },
        ]

    def generate_device_roles(self) -> List[Dict[str, Any]]:
        """Generate device role definitions."""
        return [
            {
                'name': f'{self.prefix}-spine',
                'slug': f'{self.prefix}-spine',
                'color': 'ff0000',  # Red
            },
            {
                'name': f'{self.prefix}-leaf',
                'slug': f'{self.prefix}-leaf',
                'color': '00ff00',  # Green
            },
            {
                'name': f'{self.prefix}-gpu-server',
                'slug': f'{self.prefix}-gpu-server',
                'color': '0000ff',  # Blue
            },
        ]

    def generate_devices(
        self,
        site_id: int,
        device_type_ids: Dict[str, int],
        device_role_ids: Dict[str, int],
        rack_ids: Dict[str, int] = None,
    ) -> Dict[str, List[Any]]:
        """
        Generate device records.

        Args:
            site_id: FK to site
            device_type_ids: Map of type slug to ID
            device_role_ids: Map of role slug to ID
            rack_ids: Optional map of rack name to ID

        Returns:
            Column-oriented dict for Parquet creation
        """
        # FK columns use DB column names with _id suffix
        devices = {
            'name': [],
            'device_type_id': [],  # FK to device type
            'role_id': [],  # FK to device role
            'site_id': [],  # FK to site
            'status': [],
            'serial': [],
        }

        # Spine switches
        spine_type_id = device_type_ids.get(f'{self.prefix}-spine-64x400g')
        spine_role_id = device_role_ids.get(f'{self.prefix}-spine')

        for pod in range(self.pods):
            for s in range(self.spines_per_pod):
                name = f'{self.prefix}-spine-p{pod:02d}-s{s:02d}'
                devices['name'].append(name)
                devices['device_type_id'].append(spine_type_id)
                devices['role_id'].append(spine_role_id)
                devices['site_id'].append(site_id)
                devices['status'].append('active')
                devices['serial'].append(f'SPN-{pod:02d}{s:02d}')

        # Leaf switches
        leaf_type_id = device_type_ids.get(f'{self.prefix}-leaf-64x400g')
        leaf_role_id = device_role_ids.get(f'{self.prefix}-leaf')

        for pod in range(self.pods):
            for l in range(self.leaves_per_pod):
                name = f'{self.prefix}-leaf-p{pod:02d}-r{l:02d}'
                devices['name'].append(name)
                devices['device_type_id'].append(leaf_type_id)
                devices['role_id'].append(leaf_role_id)
                devices['site_id'].append(site_id)
                devices['status'].append('active')
                devices['serial'].append(f'LF-{pod:02d}{l:02d}')

        # GPU servers
        gpu_type_id = device_type_ids.get(f'{self.prefix}-gpu-8x400g')
        gpu_role_id = device_role_ids.get(f'{self.prefix}-gpu-server')

        server_num = 0
        for pod in range(self.pods):
            for l in range(self.leaves_per_pod):
                for g in range(self.gpu_servers_per_leaf):
                    name = f'{self.prefix}-gpu-p{pod:02d}-r{l:02d}-u{g:02d}'
                    devices['name'].append(name)
                    devices['device_type_id'].append(gpu_type_id)
                    devices['role_id'].append(gpu_role_id)
                    devices['site_id'].append(site_id)
                    devices['status'].append('active')
                    devices['serial'].append(f'GPU-{server_num:06d}')
                    server_num += 1

        return devices

    def generate_interfaces(self, device_id_map: Dict[str, int]) -> Dict[str, List[Any]]:
        """
        Generate interface records for all devices.

        Args:
            device_id_map: Map of device name to device ID

        Returns:
            Column-oriented dict for Parquet creation
        """
        # FK columns use DB column names with _id suffix
        interfaces = {
            'device_id': [],  # FK to device
            'name': [],
            'type': [],
            'enabled': [],
            'description': [],
        }

        # Spine interfaces
        for pod in range(self.pods):
            for s in range(self.spines_per_pod):
                device_name = f'{self.prefix}-spine-p{pod:02d}-s{s:02d}'
                device_id = device_id_map.get(device_name)
                if device_id is None:
                    continue

                for port in range(self.spine_ports):
                    interfaces['device_id'].append(device_id)
                    interfaces['name'].append(f'eth{port + 1}')
                    interfaces['type'].append(self.spine_interface_type)
                    interfaces['enabled'].append(True)
                    interfaces['description'].append(f'Spine port {port + 1}')

        # Leaf interfaces
        for pod in range(self.pods):
            for l in range(self.leaves_per_pod):
                device_name = f'{self.prefix}-leaf-p{pod:02d}-r{l:02d}'
                device_id = device_id_map.get(device_name)
                if device_id is None:
                    continue

                for port in range(self.leaf_ports):
                    if port < self.leaf_uplinks:
                        desc = f'Uplink to spine {port + 1}'
                    else:
                        desc = f'Downlink to server {port - self.leaf_uplinks + 1}'

                    interfaces['device_id'].append(device_id)
                    interfaces['name'].append(f'eth{port + 1}')
                    interfaces['type'].append(self.leaf_interface_type)
                    interfaces['enabled'].append(True)
                    interfaces['description'].append(desc)

        # GPU server interfaces
        for pod in range(self.pods):
            for l in range(self.leaves_per_pod):
                for g in range(self.gpu_servers_per_leaf):
                    device_name = f'{self.prefix}-gpu-p{pod:02d}-r{l:02d}-u{g:02d}'
                    device_id = device_id_map.get(device_name)
                    if device_id is None:
                        continue

                    for nic in range(self.nics_per_gpu_server):
                        interfaces['device_id'].append(device_id)
                        interfaces['name'].append(f'eth{nic + 1}')
                        interfaces['type'].append(self.server_interface_type)
                        interfaces['enabled'].append(True)
                        interfaces['description'].append(f'Fabric NIC {nic + 1}')

        return interfaces

    def generate_cables(
        self,
        interface_map: Dict[str, int],
        interface_content_type_id: int,
    ) -> Tuple[Dict[str, List[Any]], Dict[str, List[Any]]]:
        """
        Generate cable and cable termination records.

        Args:
            interface_map: Map of "device_name:interface_name" to interface ID
            interface_content_type_id: ContentType ID for dcim.interface

        Returns:
            Tuple of (cable_data, termination_data) dicts
        """
        cables = {
            'type': [],
            'status': [],
            'label': [],
            'color': [],
        }

        # We'll store termination info indexed by cable label
        # After loading cables, we'll update with cable IDs
        termination_staging = []  # List of (label, a_iface_key, b_iface_key)

        cable_idx = 0

        # Spine-to-Leaf cables
        # Each leaf connects to all spines in its pod with redundant links
        for pod in range(self.pods):
            for l in range(self.leaves_per_pod):
                leaf_name = f'{self.prefix}-leaf-p{pod:02d}-r{l:02d}'
                uplink_port = 1

                for s in range(self.spines_per_pod):
                    spine_name = f'{self.prefix}-spine-p{pod:02d}-s{s:02d}'

                    # 2 redundant connections per spine
                    for redundant in range(2):
                        # Find the next available port on leaf
                        leaf_iface = f'{leaf_name}:eth{uplink_port}'
                        # Distribute across spine ports
                        spine_port = (l * 2 + redundant) % self.spine_ports + 1
                        spine_iface = f'{spine_name}:eth{spine_port}'

                        label = f'{self.prefix}-fab-{cable_idx:06d}'
                        cables['type'].append(self.fabric_cable_type)
                        cables['status'].append(self.fabric_cable_status)
                        cables['label'].append(label)
                        cables['color'].append('00ff00')  # Green for fabric

                        termination_staging.append((label, leaf_iface, spine_iface))
                        cable_idx += 1
                        uplink_port += 1

        # Leaf-to-Server cables
        for pod in range(self.pods):
            for l in range(self.leaves_per_pod):
                leaf_name = f'{self.prefix}-leaf-p{pod:02d}-r{l:02d}'
                # Start from after uplinks
                downlink_port = self.leaf_uplinks + 1

                for g in range(self.gpu_servers_per_leaf):
                    gpu_name = f'{self.prefix}-gpu-p{pod:02d}-r{l:02d}-u{g:02d}'

                    for nic in range(self.nics_per_gpu_server):
                        leaf_iface = f'{leaf_name}:eth{downlink_port}'
                        gpu_iface = f'{gpu_name}:eth{nic + 1}'

                        label = f'{self.prefix}-srv-{cable_idx:06d}'
                        cables['type'].append(self.fabric_cable_type)
                        cables['status'].append(self.fabric_cable_status)
                        cables['label'].append(label)
                        cables['color'].append('0000ff')  # Blue for server

                        termination_staging.append((label, leaf_iface, gpu_iface))
                        cable_idx += 1
                        downlink_port += 1

        # Build termination data using interface_map
        # FK columns use DB column names with _id suffix
        terminations = {
            'cable_id': [],  # Will be populated after cable load
            'cable_end': [],
            'termination_type_id': [],  # FK to ContentType
            'termination_id': [],  # ID of the terminated object
            '_label': [],  # Temp field to match with cables
        }

        for label, a_key, b_key in termination_staging:
            a_id = interface_map.get(a_key)
            b_id = interface_map.get(b_key)

            if a_id is None or b_id is None:
                continue

            # A-side
            terminations['cable_id'].append(0)  # Placeholder
            terminations['cable_end'].append('A')
            terminations['termination_type_id'].append(interface_content_type_id)
            terminations['termination_id'].append(a_id)
            terminations['_label'].append(label)

            # B-side
            terminations['cable_id'].append(0)  # Placeholder
            terminations['cable_end'].append('B')
            terminations['termination_type_id'].append(interface_content_type_id)
            terminations['termination_id'].append(b_id)
            terminations['_label'].append(label)

        return cables, terminations

    def update_terminations_with_cable_ids(
        self,
        terminations: Dict[str, List[Any]],
        label_to_cable_id: Dict[str, int],
    ) -> Dict[str, List[Any]]:
        """
        Update termination records with actual cable IDs.

        Call this after loading cables and exporting to get their IDs.

        Args:
            terminations: Termination data with _label field
            label_to_cable_id: Map of cable label to cable ID

        Returns:
            Updated terminations with cable IDs (without _label)
        """
        updated = {
            'cable_id': [],  # FK uses DB column name with _id suffix
            'cable_end': [],
            'termination_type_id': [],  # FK to ContentType
            'termination_id': [],
        }

        for i, label in enumerate(terminations['_label']):
            cable_id = label_to_cable_id.get(label)
            if cable_id is None:
                continue

            updated['cable_id'].append(cable_id)
            updated['cable_end'].append(terminations['cable_end'][i])
            updated['termination_type_id'].append(terminations['termination_type_id'][i])
            updated['termination_id'].append(terminations['termination_id'][i])

        return updated

    def summary(self) -> str:
        """Return topology summary string."""
        return f"""
GPU Datacenter Topology: {self.prefix}
{'='*50}
Pods:                {self.pods}
Spine switches:      {self.total_spines} ({self.spines_per_pod} per pod)
Leaf switches:       {self.total_leaves} ({self.leaves_per_pod} per pod)
GPU servers:         {self.total_gpu_servers} ({self.gpu_servers_per_leaf} per leaf)
NICs per server:     {self.nics_per_gpu_server}
{'='*50}
Total devices:       {self.total_devices:,}
Estimated cables:    {self.estimated_cables:,}
"""
