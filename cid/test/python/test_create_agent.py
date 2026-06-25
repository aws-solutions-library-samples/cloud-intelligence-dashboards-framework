""" Tests for the QuickSight Agent / Space / Topic helpers used by
`cid-cmd create-agent`. All AWS I/O is mocked - no live calls are made.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from cid.helpers.quicksight import QuickSight


def _qs():
    """A QuickSight helper with a mocked boto3 client (no real session)."""
    qs = QuickSight.__new__(QuickSight)
    qs.client = MagicMock()
    # account_id is a read-only property backed by awsIdentity
    qs.awsIdentity = {'Account': '123456789012'}
    return qs


def test_create_agent_wires_custom_instructions():
    """custom_instructions must be sent via CustomPromptInput.NewPrompt."""
    qs = _qs()
    qs.client.create_agent.return_value = {'AgentStatus': 'CREATING'}
    qs.create_agent(
        agent_id='cid-agent-cudos',
        name='CUDOS Cost Intelligence',
        spaces=['arn:aws:quicksight:us-east-1:123456789012:space/cid-cudos'],
        description='AI agent for CUDOS',
        starter_prompts=['What are my top cost drivers?'],
        welcome_message='Hi!',
        custom_instructions='You are a cost optimization assistant.',
    )
    kwargs = qs.client.create_agent.call_args.kwargs
    assert kwargs['CustomPromptInput'] == {
        'NewPrompt': {'CustomInstructions': 'You are a cost optimization assistant.'}
    }
    assert kwargs['AgentId'] == 'cid-agent-cudos'


def test_create_agent_omits_short_instructions():
    """Instructions shorter than the API minimum (5 chars) are not sent."""
    qs = _qs()
    qs.client.create_agent.return_value = {'AgentStatus': 'CREATING'}
    qs.create_agent(agent_id='a', name='n', custom_instructions='hi')
    assert 'CustomPromptInput' not in qs.client.create_agent.call_args.kwargs


def test_create_agent_enforces_api_limits():
    """Name <=50, StarterPrompts max 3 and each <=100 chars."""
    qs = _qs()
    qs.client.create_agent.return_value = {}
    qs.create_agent(
        agent_id='a',
        name='X' * 80,
        starter_prompts=['p' * 200, 'b', 'c', 'd'],
    )
    kwargs = qs.client.create_agent.call_args.kwargs
    assert len(kwargs['Name']) == 50
    assert len(kwargs['StarterPrompts']) == 3
    assert all(len(p) <= 100 for p in kwargs['StarterPrompts'])


def test_build_topic_columns_measure_dimension_and_time():
    """Numeric -> MEASURE/SUM, id-like numeric -> DIMENSION, datetime -> TimeGranularity."""
    qs = _qs()
    qs.describe_dataset = MagicMock(return_value=SimpleNamespace(columns=[
        {'Name': 'spend', 'Type': 'DECIMAL'},
        {'Name': 'account_id', 'Type': 'INTEGER'},
        {'Name': 'service', 'Type': 'STRING'},
        {'Name': 'usage_date', 'Type': 'DATETIME'},
    ]))
    cols = {c['ColumnName']: c for c in qs.build_topic_columns('ds-1')}

    assert cols['spend']['ColumnDataRole'] == 'MEASURE'
    assert cols['spend']['Aggregation'] == 'SUM'
    # identifier-like integer must NOT be summed
    assert cols['account_id']['ColumnDataRole'] == 'DIMENSION'
    assert cols['service']['ColumnDataRole'] == 'DIMENSION'
    # datetime uses TimeGranularity, NOT SemanticType TypeName='Date'
    assert cols['usage_date']['TimeGranularity'] == 'DAY'
    assert 'SemanticType' not in cols['usage_date']


def test_measure_heuristic_word_boundaries_and_flags():
    """Regression: substring matching mis-classified real columns (found in a
    live run). 'Forecasted Monthly Revenue' must stay a MEASURE (the 'month'
    token only matches whole words), and an 'IsLatest' flag must be a DIMENSION.
    """
    qs = _qs()
    qs.describe_dataset = MagicMock(return_value=SimpleNamespace(columns=[
        {'Name': 'Forecasted Monthly Revenue', 'Type': 'INTEGER'},
        {'Name': 'Weighted Revenue', 'Type': 'INTEGER'},
        {'Name': 'IsLatest', 'Type': 'INTEGER'},
        {'Name': 'has_discount', 'Type': 'INTEGER'},
        {'Name': 'year', 'Type': 'INTEGER'},
        {'Name': 'order_count', 'Type': 'INTEGER'},
    ]))
    cols = {c['ColumnName']: c for c in qs.build_topic_columns('ds-1')}
    assert cols['Forecasted Monthly Revenue']['ColumnDataRole'] == 'MEASURE'
    assert cols['Weighted Revenue']['ColumnDataRole'] == 'MEASURE'
    assert cols['order_count']['ColumnDataRole'] == 'MEASURE'
    assert cols['IsLatest']['ColumnDataRole'] == 'DIMENSION'
    assert cols['has_discount']['ColumnDataRole'] == 'DIMENSION'
    assert cols['year']['ColumnDataRole'] == 'DIMENSION'


def test_update_space_resources_payload_shape():
    """ResourceDetails must be the resourceArn union; enum is DATA_SET / TOPIC."""
    qs = _qs()
    qs.client.update_space_resources.return_value = {'FailedResourceOperations': []}
    add = [
        {'ResourceType': 'DASHBOARD', 'ResourceDetails': {'resourceArn': 'arn:aws:quicksight:us-east-1:123456789012:dashboard/cudos'}},
        {'ResourceType': 'DATA_SET', 'ResourceDetails': {'resourceArn': 'arn:aws:quicksight:us-east-1:123456789012:dataset/ds-1'}},
        {'ResourceType': 'TOPIC', 'ResourceDetails': {'resourceArn': 'arn:aws:quicksight:us-east-1:123456789012:topic/cid-topic-cudos'}},
    ]
    qs.update_space_resources('cid-cudos', add_resources=add)
    sent = qs.client.update_space_resources.call_args.kwargs['AddResources']
    types = {r['ResourceType'] for r in sent}
    assert types == {'DASHBOARD', 'DATA_SET', 'TOPIC'}
    for r in sent:
        assert set(r['ResourceDetails'].keys()) == {'resourceArn'}


def test_create_topic_uses_new_reader_experience():
    """Topic must request the NEW_READER_EXPERIENCE generative-Q experience."""
    qs = _qs()
    qs.client.create_topic.return_value = {}
    qs.create_topic(topic_id='t', name='n', description='d', datasets_config=[{'DatasetArn': 'a'}])
    topic = qs.client.create_topic.call_args.kwargs['Topic']
    assert topic['UserExperienceVersion'] == 'NEW_READER_EXPERIENCE'
    assert topic['DataSets'] == [{'DatasetArn': 'a'}]
