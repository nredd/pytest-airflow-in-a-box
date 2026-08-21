"""Small provider-shaped package used by the compatibility corpus."""

from provider_package._executor import ExampleExecutor
from provider_package._hook import ExampleHook
from provider_package._listener import ExampleListener
from provider_package._operator import ExampleOperator
from provider_package._sensor import ExampleSensor
from provider_package._timetable import ExampleTimetable

__all__ = (
    "ExampleExecutor",
    "ExampleHook",
    "ExampleListener",
    "ExampleOperator",
    "ExampleSensor",
    "ExampleTimetable",
)
