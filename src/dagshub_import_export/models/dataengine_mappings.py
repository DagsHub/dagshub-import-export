from dataclasses import dataclass


@dataclass
class DataengineMappings:
    datasources: dict[int, int]
    datasets: dict[int, int]
