from tap_assembled.streams.agents import AgentsStream
from tap_assembled.streams.agent_states import AgentStatesStream
from tap_assembled.streams.activities import ActivitiesStream
from tap_assembled.streams.activity_types import ActivityTypesStream

AVAILABLE_STREAMS = [
    AgentsStream,
    ActivityTypesStream,
    ActivitiesStream,
    AgentStatesStream,
]

__all__ = [s.NAME for s in AVAILABLE_STREAMS]
