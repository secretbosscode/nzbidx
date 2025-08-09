"""Data models for the API service."""


from dataclasses import dataclass


@dataclass
class ExampleModel:
    """Example data model."""
    id: int
    name: str
