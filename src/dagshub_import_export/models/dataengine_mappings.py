from dataclasses import dataclass


@dataclass
class DataengineMappings:
    datasources: dict[int, int]  # Maps source datasource ID to target datasource ID
    datasets: dict[int, int]  # Maps source dataset ID to target dataset ID
